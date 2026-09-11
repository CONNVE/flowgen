use super::config::{ALLOW_SAVE_VALUE, DUPLICATE_RULE_HEADER};
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use salesforce_core::restapi::{
    CompositeCollectionCreateRequest, CompositeCollectionRetrieveRequest,
    CompositeCollectionUpdateRequest, CompositeCollectionUpsertRequest, CompositeRecordRequest,
    CompositeTreeRequest,
};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, warn, Instrument};

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to send event message to next task: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Salesforce authentication error: {source}")]
    SalesforceAuth {
        #[source]
        source: salesforce_core::client::Error,
    },
    #[error("Salesforce Composite API operation failed: {source}")]
    CompositeApiOperation {
        #[source]
        source: Box<salesforce_core::restapi::CompositeError>,
    },
    #[error("Failed to serialize or deserialize data: {source}")]
    SerdeExt {
        #[source]
        source: flowgen_core::serde::Error,
    },
    #[error(transparent)]
    EventError(#[from] flowgen_core::event::Error),
    #[error("Failed to render configuration template: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Operation requires sobject_type field to be specified")]
    MissingSObjectType,
    #[error("Operation requires payload data (explicit records or from_event: true)")]
    MissingPayload,
    #[error("Operation requires ids field to be specified")]
    MissingIds,
    #[error("Operation requires external_id_field to be specified")]
    MissingExternalIdField,
    #[error("Failed to build REST API client: {source}")]
    RestClientBuild {
        #[source]
        source: salesforce_core::restapi::ClientError,
    },
    #[error("Expected array of objects for composite records")]
    InvalidCompositeRecordFormat,
    #[error("Expected array data for composite operation")]
    InvalidCompositeDataFormat,
    #[error(
        "Record at index {index} is missing `attributes` — \
         Salesforce Composite API requires each record to include \
         `attributes: {{ type: \"...\" }}`. Set `sobject_type` in the \
         task config to auto-populate it, or add `attributes` to \
         each record in the payload."
    )]
    MissingAttributes { index: usize },
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
}

pub struct EventHandler {
    client: Arc<salesforce_core::restapi::Client>,
    config: Arc<super::config::Composite>,
    tx: Option<Sender<Event>>,
    current_task_id: usize,
    sfdc_client: Arc<tokio::sync::Mutex<salesforce_core::client::Client>>,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    /// Log any failed sub-requests from the composite response as warnings.
    /// The event is still forwarded to downstream tasks with the full Salesforce response,
    /// allowing them to process successful records.
    fn log_failures(response: &serde_json::Value) {
        if let Some(array) = response.as_array() {
            let failures: Vec<(usize, &serde_json::Value)> = array
                .iter()
                .enumerate()
                .filter(|(_, item)| {
                    item.get("success")
                        .and_then(|s| s.as_bool())
                        .is_some_and(|success| !success)
                })
                .collect();

            if !failures.is_empty() {
                for (i, item) in &failures {
                    let errors = item
                        .get("errors")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    warn!(
                        index = i,
                        errors = %errors,
                        "Composite operation completed with one or more failures"
                    );
                }
            }
        }
    }

    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            let event_value = serde_json::value::Value::try_from(event.as_ref())?;
            let config = self
                .config
                .render(&event_value)
                .map_err(|e| Error::ConfigRender { source: e })?;

            let event_data = event.data_as_json()?;

            match config.operation {
                super::config::CompositeOperation::Create => {
                    self.create(&config, &event_data, completion_tx_arc).await?
                }
                super::config::CompositeOperation::Get => {
                    self.get(&config, completion_tx_arc).await?
                }
                super::config::CompositeOperation::Update => {
                    self.update(&config, &event_data, completion_tx_arc).await?
                }
                super::config::CompositeOperation::Upsert => {
                    self.upsert(&config, &event_data, completion_tx_arc).await?
                }
                super::config::CompositeOperation::Delete => {
                    self.delete(&config, completion_tx_arc).await?
                }
                super::config::CompositeOperation::Tree => {
                    self.tree(&config, &event_data, completion_tx_arc).await?
                }
            }

            Ok(())
        })
        .await
    }

    fn get_payload_records(
        &self,
        config: &super::config::Composite,
        event_data: &serde_json::Value,
    ) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, Error> {
        let payload = config.payload.as_ref().ok_or(Error::MissingPayload)?;

        let mut records = match payload {
            super::config::CompositePayload::FromEvent { from_event } if *from_event => {
                if let serde_json::Value::Array(records) = event_data {
                    records
                        .iter()
                        .map(|r| {
                            r.as_object()
                                .cloned()
                                .ok_or(Error::InvalidCompositeRecordFormat)
                        })
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    return Err(Error::InvalidCompositeDataFormat);
                }
            }
            super::config::CompositePayload::Records(records) => records.clone(),
            _ => return Err(Error::MissingPayload),
        };

        Self::ensure_attributes(&mut records, config.sobject_type.as_deref())?;

        Ok(records)
    }

    fn ensure_attributes(
        records: &mut [serde_json::Map<String, serde_json::Value>],
        sobject_type: Option<&str>,
    ) -> Result<(), Error> {
        match sobject_type {
            Some(sobject_type) => {
                for record in records {
                    if !record.contains_key("attributes") {
                        record.insert(
                            "attributes".to_string(),
                            serde_json::json!({ "type": sobject_type }),
                        );
                    }
                }
                Ok(())
            }
            None => {
                for (index, record) in records.iter().enumerate() {
                    if !record.contains_key("attributes") {
                        return Err(Error::MissingAttributes { index });
                    }
                }
                Ok(())
            }
        }
    }

    async fn create(
        &self,
        config: &super::config::Composite,
        event_data: &serde_json::Value,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let records = self.get_payload_records(config, event_data)?;

        let composite_records: Result<Vec<CompositeRecordRequest>, Error> = records
            .iter()
            .map(|r| {
                serde_json::from_value(serde_json::Value::Object(r.clone())).map_err(|e| {
                    Error::SerdeExt {
                        source: flowgen_core::serde::Error::Serde { source: e },
                    }
                })
            })
            .collect();

        let request = CompositeCollectionCreateRequest {
            all_or_none: config.all_or_none.unwrap_or(false),
            records: composite_records?,
        };

        let mut builder = self.client.composite().create_records(&request);
        if config.allow_duplicate_save {
            builder = builder.header(DUPLICATE_RULE_HEADER, ALLOW_SAVE_VALUE);
        }
        let response = builder
            .send()
            .await
            .map_err(|e| Error::CompositeApiOperation {
                source: Box::new(e),
            })?;

        let resp = serde_json::to_value(&*response).map_err(|e| Error::SerdeExt {
            source: flowgen_core::serde::Error::Serde { source: e },
        })?;

        let mut e = EventBuilder::new()
            .data(EventData::Json(resp.clone()))
            .subject(config.name.to_owned())
            .id(format!("composite_create_{}", response.len()))
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;

        match self.tx {
            Some(_) => {
                e.completion_tx = completion_tx_arc.clone();
            }
            None => {
                // Leaf task: signal completion.
                if let Some(arc) = completion_tx_arc.as_ref() {
                    arc.signal_completion(e.data_as_json().ok());
                }
            }
        }

        Self::log_failures(&resp);

        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|e| Error::SendMessage { source: e })?;

        Ok(())
    }

    async fn get(
        &self,
        config: &super::config::Composite,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let sobject_type = config
            .sobject_type
            .as_ref()
            .ok_or(Error::MissingSObjectType)?;
        let ids = config.ids.as_ref().ok_or(Error::MissingIds)?;

        let request = CompositeCollectionRetrieveRequest {
            ids: ids.clone(),
            fields: config.fields.clone().unwrap_or_default(),
        };

        let response = self
            .client
            .composite()
            .get_records(sobject_type, &request)
            .send()
            .await
            .map_err(|e| Error::CompositeApiOperation {
                source: Box::new(e),
            })?;

        let resp = serde_json::to_value(&response).map_err(|e| Error::SerdeExt {
            source: flowgen_core::serde::Error::Serde { source: e },
        })?;

        let mut e = EventBuilder::new()
            .data(EventData::Json(resp.clone()))
            .subject(config.name.to_owned())
            .id(format!("composite_get_{}", response.len()))
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;

        match self.tx {
            None => {
                // Leaf task: signal completion.
                if let Some(arc) = completion_tx_arc.as_ref() {
                    arc.signal_completion(e.data_as_json().ok());
                }
            }
            Some(_) => {
                e.completion_tx = completion_tx_arc.clone();
            }
        }

        Self::log_failures(&resp);

        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|e| Error::SendMessage { source: e })?;

        Ok(())
    }

    async fn update(
        &self,
        config: &super::config::Composite,
        event_data: &serde_json::Value,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let records = self.get_payload_records(config, event_data)?;

        let composite_records: Result<Vec<serde_json::Map<String, serde_json::Value>>, Error> =
            records.iter().map(|r| Ok(r.clone())).collect();

        let request = CompositeCollectionUpdateRequest {
            all_or_none: config.all_or_none.unwrap_or(false),
            records: composite_records?
                .iter()
                .map(|r| {
                    serde_json::from_value(serde_json::Value::Object(r.clone())).map_err(|e| {
                        Error::SerdeExt {
                            source: flowgen_core::serde::Error::Serde { source: e },
                        }
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        };

        let mut builder = self.client.composite().update_records(&request);
        if config.allow_duplicate_save {
            builder = builder.header(DUPLICATE_RULE_HEADER, ALLOW_SAVE_VALUE);
        }
        let response = builder
            .send()
            .await
            .map_err(|e| Error::CompositeApiOperation {
                source: Box::new(e),
            })?;

        let resp = serde_json::to_value(&*response).map_err(|e| Error::SerdeExt {
            source: flowgen_core::serde::Error::Serde { source: e },
        })?;

        let mut e = EventBuilder::new()
            .data(EventData::Json(resp.clone()))
            .subject(config.name.to_owned())
            .id(format!("composite_update_{}", response.len()))
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;

        match self.tx {
            Some(_) => {
                e.completion_tx = completion_tx_arc.clone();
            }
            None => {
                // Leaf task: signal completion.
                if let Some(arc) = completion_tx_arc.as_ref() {
                    arc.signal_completion(e.data_as_json().ok());
                }
            }
        }

        Self::log_failures(&resp);

        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|e| Error::SendMessage { source: e })?;

        Ok(())
    }

    async fn upsert(
        &self,
        config: &super::config::Composite,
        event_data: &serde_json::Value,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let sobject_type = config
            .sobject_type
            .as_ref()
            .ok_or(Error::MissingSObjectType)?;
        let external_id_field = config
            .external_id_field
            .as_ref()
            .ok_or(Error::MissingExternalIdField)?;
        let records = self.get_payload_records(config, event_data)?;

        let composite_records: Result<Vec<serde_json::Map<String, serde_json::Value>>, Error> =
            records.iter().map(|r| Ok(r.clone())).collect();

        let request = CompositeCollectionUpsertRequest {
            all_or_none: config.all_or_none.unwrap_or(false),
            records: composite_records?
                .iter()
                .map(|r| {
                    serde_json::from_value(serde_json::Value::Object(r.clone())).map_err(|e| {
                        Error::SerdeExt {
                            source: flowgen_core::serde::Error::Serde { source: e },
                        }
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        };

        let mut builder =
            self.client
                .composite()
                .upsert_records(sobject_type, external_id_field, &request);
        if config.allow_duplicate_save {
            builder = builder.header(DUPLICATE_RULE_HEADER, ALLOW_SAVE_VALUE);
        }
        let response = builder
            .send()
            .await
            .map_err(|e| Error::CompositeApiOperation {
                source: Box::new(e),
            })?;

        let resp = serde_json::to_value(&*response).map_err(|e| Error::SerdeExt {
            source: flowgen_core::serde::Error::Serde { source: e },
        })?;

        let mut e = EventBuilder::new()
            .data(EventData::Json(resp.clone()))
            .subject(config.name.to_owned())
            .id(format!("composite_upsert_{}", response.len()))
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;

        match self.tx {
            Some(_) => {
                e.completion_tx = completion_tx_arc.clone();
            }
            None => {
                // Leaf task: signal completion.
                if let Some(arc) = completion_tx_arc.as_ref() {
                    arc.signal_completion(e.data_as_json().ok());
                }
            }
        }

        Self::log_failures(&resp);

        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|e| Error::SendMessage { source: e })?;

        Ok(())
    }

    async fn delete(
        &self,
        config: &super::config::Composite,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let ids = config.ids.as_ref().ok_or(Error::MissingIds)?;

        let ids_str = ids.join(",");
        let mut builder = self.client.composite().delete_records(&ids_str);
        if let Some(all_or_none) = config.all_or_none {
            builder = builder.all_or_none(all_or_none);
        }
        let response = builder
            .send()
            .await
            .map_err(|e| Error::CompositeApiOperation {
                source: Box::new(e),
            })?;

        let resp = serde_json::to_value(&*response).map_err(|e| Error::SerdeExt {
            source: flowgen_core::serde::Error::Serde { source: e },
        })?;

        let mut e = EventBuilder::new()
            .data(EventData::Json(resp.clone()))
            .subject(config.name.to_owned())
            .id(format!("composite_delete_{}", response.len()))
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;

        match self.tx {
            Some(_) => {
                e.completion_tx = completion_tx_arc.clone();
            }
            None => {
                // Leaf task: signal completion.
                if let Some(arc) = completion_tx_arc.as_ref() {
                    arc.signal_completion(e.data_as_json().ok());
                }
            }
        }

        Self::log_failures(&resp);

        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|e| Error::SendMessage { source: e })?;

        Ok(())
    }

    async fn tree(
        &self,
        config: &super::config::Composite,
        event_data: &serde_json::Value,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let sobject_type = config
            .sobject_type
            .as_ref()
            .ok_or(Error::MissingSObjectType)?;
        let records = self.get_payload_records(config, event_data)?;

        let tree_records: Result<Vec<_>, Error> = records
            .iter()
            .map(|r| {
                serde_json::from_value(serde_json::Value::Object(r.clone())).map_err(|e| {
                    Error::SerdeExt {
                        source: flowgen_core::serde::Error::Serde { source: e },
                    }
                })
            })
            .collect();

        let request = CompositeTreeRequest {
            records: tree_records?,
        };

        let mut builder = self
            .client
            .composite()
            .create_record_tree(sobject_type, &request);
        if config.allow_duplicate_save {
            builder = builder.header(DUPLICATE_RULE_HEADER, ALLOW_SAVE_VALUE);
        }
        let response = builder
            .send()
            .await
            .map_err(|e| Error::CompositeApiOperation {
                source: Box::new(e),
            })?;

        let resp = serde_json::to_value(&response).map_err(|e| Error::SerdeExt {
            source: flowgen_core::serde::Error::Serde { source: e },
        })?;

        let mut e = EventBuilder::new()
            .data(EventData::Json(resp.clone()))
            .subject(config.name.to_owned())
            .id(format!("composite_tree_{}", response.results.len()))
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;

        match self.tx {
            Some(_) => {
                e.completion_tx = completion_tx_arc.clone();
            }
            None => {
                // Leaf task: signal completion.
                if let Some(arc) = completion_tx_arc.as_ref() {
                    arc.signal_completion(e.data_as_json().ok());
                }
            }
        }

        Self::log_failures(&resp);

        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|e| Error::SendMessage { source: e })?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct Processor {
    config: Arc<super::config::Composite>,
    rx: Receiver<Event>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<EventHandler, Error> {
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|e| Error::ConfigRender { source: e })?;

        let credentials_path = init_config.credentials_path.clone();
        let sfdc_client = self
            .task_context
            .client_registry
            .get_or_init(
                flowgen_core::client_registry::ClientKey::new(self.task_type, &credentials_path),
                || async {
                    let client = salesforce_core::client::Builder::new()
                        .credentials_path(credentials_path)
                        .build()
                        .map_err(|e| Error::SalesforceAuth { source: e })?
                        .connect()
                        .await
                        .map_err(|e| Error::SalesforceAuth { source: e })?;
                    Ok(tokio::sync::Mutex::new(client))
                },
            )
            .await
            .map_err(|e| match e {
                flowgen_core::client_registry::Error::Init { source } => source,
                flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
            })?;

        let rest_client = {
            let guard = sfdc_client.lock().await;
            salesforce_core::restapi::ClientBuilder::new(guard.clone())
                .build()
                .map_err(|e| Error::RestClientBuild { source: e })?
        };

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            current_task_id: self.task_id,
            tx: self.tx.clone(),
            client: Arc::new(rest_client),
            sfdc_client,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        };
        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) => {
                        error!(error = %e, "Failed to initialize Composite API processor");
                        Err(tokio_retry::RetryError::transient(e))
                    }
                }
            },
        )
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                return Err(e);
            }
        };

        let mut handlers = Vec::new();
        loop {
            match self.rx.recv().await {
                Some(event) => {
                    if Some(event.task_id) == event_handler.current_task_id.checked_sub(1) {
                        let event_handler = Arc::clone(&event_handler);
                        let retry_strategy = retry_config.strategy();
                        let event_clone = event.clone();
                        let handle = tokio::spawn(
                            async move {
                                let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                    match event_handler.handle(event_clone.clone()).await {
                                        Ok(result) => Ok(result),
                                        Err(e) => {
                                            error!(error = %e, "Failed to process Composite API operation");
                                            let needs_reconnect = matches!(&e, Error::SalesforceAuth { .. })
                                                || matches!(&e,
                                                    Error::CompositeApiOperation { source }
                                                        if matches!(source.as_ref(), salesforce_core::restapi::composite::Error::Auth { .. })
                                                );

                                            if needs_reconnect {
                                                let mut sfdc_client =
                                                    event_handler.sfdc_client.lock().await;
                                                if let Err(reconnect_err) =
                                                    (*sfdc_client).reconnect().await
                                                {
                                                    error!(error = %reconnect_err, "Failed to reconnect");
                                                    return Err(tokio_retry::RetryError::transient(Error::SalesforceAuth {
                                                        source: reconnect_err,
                                                    }));
                                                }
                                            }

                                            let retryable = match &e {
                                                Error::CompositeApiOperation { source } => source.is_retryable(),
                                                _ => true,
                                            };
                                            if retryable {
                                                Err(tokio_retry::RetryError::transient(e))
                                            } else {
                                                Err(tokio_retry::RetryError::permanent(e))
                                            }
                                        }
                                    }
                                })
                                .await;

                                if let Err(err) = result {
                                    error!(error = %err, "Composite API operation failed after all retry attempts");
                                    let mut error_event = event_clone.clone();
                                    error_event.error = Some(err.to_string());
                                    if let Some(ref tx) = event_handler.tx {
                                        tx.send(error_event).await.ok();
                                    } else if let Some(arc) = event_clone.completion_tx.as_ref() {
                                        arc.signal_completion_with_error(err.to_string());
                                    }
                                }
                            }
                            .instrument(tracing::Span::current()),
                        );
                        handlers.push(handle);
                    }
                }
                None => {
                    futures_util::future::join_all(handlers).await;
                    return Ok(());
                }
            }
        }
    }
}

pub struct ProcessorBuilder {
    config: Option<Arc<super::config::Composite>>,
    rx: Option<Receiver<Event>>,
    tx: Option<Sender<Event>>,
    task_id: Option<usize>,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl Default for ProcessorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessorBuilder {
    pub fn new() -> Self {
        Self {
            config: None,
            rx: None,
            tx: None,
            task_id: None,
            task_context: None,
            task_type: None,
        }
    }

    pub fn config(mut self, config: Arc<super::config::Composite>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = Some(task_id);
        self
    }

    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
        self
    }

    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    pub fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingBuilderAttribute("receiver".to_string()))?,
            tx: self.tx,
            task_id: self
                .task_id
                .ok_or_else(|| Error::MissingBuilderAttribute("task_id".to_string()))?,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingBuilderAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_failures_all_success() {
        let resp = serde_json::json!([
            {"success": true, "id": "001abc"},
            {"success": true, "id": "001def"}
        ]);
        EventHandler::log_failures(&resp);
    }

    #[test]
    fn test_log_failures_one_failure() {
        let resp = serde_json::json!([
            {"success": true, "id": "001abc"},
            {"success": false, "errors": [{"message": "REQUIRED_FIELD_MISSING"}]}
        ]);
        EventHandler::log_failures(&resp);
    }

    #[test]
    fn test_log_failures_mixed() {
        let resp = serde_json::json!([
            {"success": false, "errors": [{"message": "err1"}]},
            {"success": true, "id": "001abc"},
            {"success": false, "errors": [{"message": "err2"}]}
        ]);
        EventHandler::log_failures(&resp);
    }

    #[test]
    fn test_log_failures_empty_array() {
        EventHandler::log_failures(&serde_json::json!([]));
    }

    #[test]
    fn test_log_failures_non_array() {
        EventHandler::log_failures(&serde_json::json!({"not": "an array"}));
        EventHandler::log_failures(&serde_json::json!("string"));
        EventHandler::log_failures(&serde_json::json!(null));
    }

    #[test]
    fn test_config_create_with_records() {
        let json = r#"{
            "name": "batch_create",
            "operation": "create",
            "credentials_path": "/etc/sfdc/credentials.json",
            "payload": [
                {"attributes": {"type": "Account"}, "Name": "Acme"},
                {"attributes": {"type": "Account"}, "Name": "Globex"}
            ]
        }"#;
        let config: super::super::config::Composite = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.operation,
            super::super::config::CompositeOperation::Create
        );
        match config.payload {
            Some(super::super::config::CompositePayload::Records(ref records)) => {
                assert_eq!(records.len(), 2);
            }
            _ => panic!("expected Records payload"),
        }
    }

    #[test]
    fn test_config_create_from_event() {
        let json = r#"{
            "name": "batch_create",
            "operation": "create",
            "credentials_path": "/etc/sfdc/credentials.json",
            "payload": { "from_event": true }
        }"#;
        let config: super::super::config::Composite = serde_json::from_str(json).unwrap();
        match config.payload {
            Some(super::super::config::CompositePayload::FromEvent { from_event }) => {
                assert!(from_event);
            }
            _ => panic!("expected FromEvent payload"),
        }
    }

    #[test]
    fn test_config_get() {
        let json = r#"{
            "name": "batch_get",
            "operation": "get",
            "credentials_path": "/etc/sfdc/credentials.json",
            "sobject_type": "Account",
            "ids": ["001abc", "001def"],
            "fields": ["Id", "Name"]
        }"#;
        let config: super::super::config::Composite = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.operation,
            super::super::config::CompositeOperation::Get
        );
        assert_eq!(config.sobject_type.as_deref(), Some("Account"));
        assert_eq!(config.ids.as_ref().unwrap().len(), 2);
        assert_eq!(config.fields.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_config_upsert() {
        let json = r#"{
            "name": "batch_upsert",
            "operation": "upsert",
            "credentials_path": "/etc/sfdc/credentials.json",
            "sobject_type": "Contact",
            "external_id_field": "ExternalId__c",
            "payload": [
                {"attributes": {"type": "Contact"}, "ExternalId__c": "EXT-001", "FirstName": "Alice"}
            ]
        }"#;
        let config: super::super::config::Composite = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.operation,
            super::super::config::CompositeOperation::Upsert
        );
        assert_eq!(config.external_id_field.as_deref(), Some("ExternalId__c"));
    }

    #[test]
    fn test_config_delete() {
        let json = r#"{
            "name": "batch_delete",
            "operation": "delete",
            "credentials_path": "/etc/sfdc/credentials.json",
            "ids": ["001abc", "001def"]
        }"#;
        let config: super::super::config::Composite = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.operation,
            super::super::config::CompositeOperation::Delete
        );
        assert_eq!(config.ids.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_config_tree() {
        let json = r#"{
            "name": "tree_create",
            "operation": "tree",
            "credentials_path": "/etc/sfdc/credentials.json",
            "sobject_type": "Account",
            "payload": [
                {"attributes": {"type": "Account", "referenceId": "ref1"}, "Name": "Parent"}
            ]
        }"#;
        let config: super::super::config::Composite = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.operation,
            super::super::config::CompositeOperation::Tree
        );
        assert_eq!(config.sobject_type.as_deref(), Some("Account"));
    }

    #[test]
    fn test_config_all_or_none() {
        let json = r#"{
            "name": "batch_create",
            "operation": "create",
            "credentials_path": "/etc/sfdc/credentials.json",
            "all_or_none": true,
            "payload": { "from_event": true }
        }"#;
        let config: super::super::config::Composite = serde_json::from_str(json).unwrap();
        assert_eq!(config.all_or_none, Some(true));
    }

    #[test]
    fn test_config_all_or_none_absent() {
        let json = r#"{
            "name": "batch_create",
            "operation": "create",
            "credentials_path": "/etc/sfdc/credentials.json",
            "payload": { "from_event": true }
        }"#;
        let config: super::super::config::Composite = serde_json::from_str(json).unwrap();
        assert!(config.all_or_none.is_none());
    }

    #[test]
    fn test_ensure_attributes_injects_when_sobject_type_set() {
        let mut records = vec![
            serde_json::json!({ "Name": "Acme" })
                .as_object()
                .cloned()
                .unwrap(),
            serde_json::json!({ "Name": "Globex" })
                .as_object()
                .cloned()
                .unwrap(),
        ];
        EventHandler::ensure_attributes(&mut records, Some("Account")).unwrap();
        assert_eq!(
            records[0].get("attributes").unwrap(),
            &serde_json::json!({ "type": "Account" })
        );
        assert_eq!(
            records[1].get("attributes").unwrap(),
            &serde_json::json!({ "type": "Account" })
        );
    }

    #[test]
    fn test_ensure_attributes_preserves_existing() {
        let mut records = vec![
            serde_json::json!({ "attributes": { "type": "Contact" }, "Name": "Alice" })
                .as_object()
                .cloned()
                .unwrap(),
        ];
        EventHandler::ensure_attributes(&mut records, Some("Account")).unwrap();
        assert_eq!(
            records[0].get("attributes").unwrap(),
            &serde_json::json!({ "type": "Contact" })
        );
    }

    #[test]
    fn test_ensure_attributes_errors_without_sobject_type() {
        let mut records = vec![serde_json::json!({ "Name": "Acme" })
            .as_object()
            .cloned()
            .unwrap()];
        let err = EventHandler::ensure_attributes(&mut records, None).unwrap_err();
        assert!(matches!(err, Error::MissingAttributes { index: 0 }));
    }

    #[test]
    fn test_ensure_attributes_passes_when_all_have_attributes() {
        let mut records = vec![
            serde_json::json!({ "attributes": { "type": "Account" }, "Name": "Acme" })
                .as_object()
                .cloned()
                .unwrap(),
        ];
        EventHandler::ensure_attributes(&mut records, None).unwrap();
    }

    #[test]
    fn test_ensure_attributes_empty_records() {
        let mut records: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
        EventHandler::ensure_attributes(&mut records, Some("Account")).unwrap();
        EventHandler::ensure_attributes(&mut records, None).unwrap();
    }

    #[test]
    fn test_ensure_attributes_mixed_batch_no_sobject_type_errors() {
        let mut records = vec![
            serde_json::json!({ "attributes": { "type": "Account" }, "Name": "Acme" })
                .as_object()
                .cloned()
                .unwrap(),
            serde_json::json!({ "Name": "Globex" })
                .as_object()
                .cloned()
                .unwrap(),
        ];
        let err = EventHandler::ensure_attributes(&mut records, None).unwrap_err();
        assert!(matches!(err, Error::MissingAttributes { index: 1 }));
    }

    #[test]
    fn test_ensure_attributes_mixed_batch_with_sobject_type_injects_missing() {
        let mut records = vec![
            serde_json::json!({ "attributes": { "type": "Contact" }, "Name": "Alice" })
                .as_object()
                .cloned()
                .unwrap(),
            serde_json::json!({ "Name": "Globex" })
                .as_object()
                .cloned()
                .unwrap(),
        ];
        EventHandler::ensure_attributes(&mut records, Some("Account")).unwrap();
        assert_eq!(
            records[0].get("attributes").unwrap(),
            &serde_json::json!({ "type": "Contact" })
        );
        assert_eq!(
            records[1].get("attributes").unwrap(),
            &serde_json::json!({ "type": "Account" })
        );
    }
}
