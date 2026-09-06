use super::message::MongoEventsExt;
use crate::client::MongoClientBuilder;
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventData, EventExt};
use futures::TryStreamExt;
use mongodb::bson::{oid::ObjectId, Bson, Document as BsonDocument};
use mongodb::options::{FindOneAndUpdateOptions, ReturnDocument};
use mongodb::Collection;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

use super::config::Operation;

/// Errors that can occur during MongoDB collection read/write operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Authentication error: {source}")]
    Auth {
        #[source]
        source: crate::client::Error,
    },
    #[error("Sending event to channel failed with error: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Event builder failed with error: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Missing required attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Message conversion failed with error: {source}")]
    MessageConversion {
        #[source]
        source: crate::message::Error,
    },
    #[error("Unsupported event data")]
    UnsupportedEventData,
    #[error("Invalid MongoDB document")]
    InvalidDocument,
    #[error("Event's `_id` must be an ObjectId (`{{\"$oid\": \"...\"}}`) or omitted")]
    UnsupportedIdShape,
    #[error("Invalid ObjectId in event's `_id`: {source}")]
    InvalidObjectId {
        #[source]
        source: mongodb::bson::oid::Error,
    },
    #[error("JSON serialization error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("MongoDB error: {source}")]
    MongoDB {
        #[source]
        source: mongodb::error::Error,
    },
    #[error("JSON to BSON conversion error: {source}")]
    Bson {
        #[source]
        source: mongodb::bson::extjson::de::Error,
    },
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
}

/// Event handler for processing individual events against a MongoDB collection.
pub struct EventHandler {
    client: Arc<mongodb::Client>,
    config: Arc<super::config::Collection>,
    task_id: usize,
    tx: Option<Sender<Event>>,
    task_type: &'static str,
}

impl EventHandler {
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        match self.config.operation {
            Operation::Read => self.read(event).await,
            Operation::Write => self.write(event).await,
            Operation::Upsert => self.upsert(event).await,
        }
    }

    /// Queries the configured collection with `filter` and emits each
    /// matching document as an event.
    async fn read(&self, event: Event) -> Result<(), Error> {
        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        let collection: Collection<BsonDocument> = self
            .client
            .database(&self.config.db_name)
            .collection(&self.config.collection_name);

        let filter = build_filter_doc(&self.config.filter);
        let mut cursor = collection
            .find(filter)
            .await
            .map_err(|source| Error::MongoDB { source })?;

        // Completion must only fire on the last emitted event, so the next
        // document is buffered one step ahead of the one being sent — the
        // cursor has no peek, and `try_next` on an exhausted cursor is the
        // only way to know a given document is the last one.
        let mut pending = cursor
            .try_next()
            .await
            .map_err(|source| Error::MongoDB { source })?;

        if pending.is_none() {
            let mut e = flowgen_core::event::EventBuilder::new()
                .data(EventData::Json(serde_json::json!({ "matched": 0 })))
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()
                .map_err(|source| Error::EventBuilder { source })?;

            match self.tx {
                None => {
                    if let Some(arc) = completion_tx_arc.as_ref() {
                        arc.signal_completion(e.data_as_json().ok());
                    }
                }
                Some(_) => {
                    e.completion_tx = completion_tx_arc.clone();
                }
            }

            e.send_with_logging(self.tx.as_ref())
                .await
                .map_err(|source| Error::SendMessage { source })?;

            return Ok(());
        }

        while let Some(document) = pending.take() {
            let next = cursor
                .try_next()
                .await
                .map_err(|source| Error::MongoDB { source })?;
            let is_last = next.is_none();

            let mut e = document
                .to_event(self.task_type, self.task_id)
                .map_err(|source| Error::MessageConversion { source })?;

            if is_last {
                match self.tx {
                    None => {
                        if let Some(arc) = completion_tx_arc.as_ref() {
                            arc.signal_completion(e.data_as_json().ok());
                        }
                    }
                    Some(_) => {
                        e.completion_tx = completion_tx_arc.clone();
                    }
                }
            }

            e.send_with_logging(self.tx.as_ref())
                .await
                .map_err(|source| Error::SendMessage { source })?;

            pending = next;
        }

        Ok(())
    }

    /// Inserts the incoming event's JSON payload as a document.
    async fn write(&self, event: Event) -> Result<(), Error> {
        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        let json = match &event.data {
            EventData::Json(value) => value.clone(),
            _ => return Err(Error::UnsupportedEventData),
        };

        let oid = match json.get("_id") {
            None | Some(Value::Null) => ObjectId::new(),
            Some(id) => match id
                .as_object()
                .and_then(|obj| obj.get("$oid"))
                .and_then(|v| v.as_str())
            {
                Some(s) => {
                    ObjectId::parse_str(s).map_err(|source| Error::InvalidObjectId { source })?
                }
                None => return Err(Error::UnsupportedIdShape),
            },
        };

        let mut bson_doc = match Bson::try_from(json).map_err(|source| Error::Bson { source })? {
            Bson::Document(d) => d,
            _ => return Err(Error::InvalidDocument),
        };
        bson_doc.insert("_id", Bson::ObjectId(oid));

        let subject = format!("{}.{}", self.config.db_name, self.config.collection_name);

        let collection: Collection<BsonDocument> = self
            .client
            .database(&self.config.db_name)
            .collection(&self.config.collection_name);

        let resp = collection
            .insert_one(&bson_doc)
            .await
            .map_err(|source| Error::MongoDB { source })?;
        let resp_json =
            serde_json::to_value(&resp).map_err(|source| Error::SerdeJson { source })?;

        let mut e = flowgen_core::event::EventBuilder::new()
            .data(EventData::Json(resp_json))
            .subject(subject)
            .id(resp.inserted_id.to_string())
            .task_id(self.task_id)
            .task_type(self.task_type)
            .build()
            .map_err(|source| Error::EventBuilder { source })?;

        match self.tx {
            None => {
                if let Some(arc) = completion_tx_arc.as_ref() {
                    arc.signal_completion(e.data_as_json().ok());
                }
            }
            Some(_) => {
                e.completion_tx = completion_tx_arc.clone();
            }
        }

        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|source| Error::SendMessage { source })?;

        Ok(())
    }

    /// Upserts the first document matching `filter`: applies the update
    /// operators from the incoming event's JSON payload to the match, or
    /// inserts an aggregated document if nothing matches.
    async fn upsert(&self, event: Event) -> Result<(), Error> {
        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        let json = match &event.data {
            EventData::Json(value) => value.clone(),
            _ => return Err(Error::UnsupportedEventData),
        };

        // The incoming payload is either a document whose keys are all update
        // operators (e.g. `$set`, `$inc`), applied verbatim, or a plain
        // document that is wrapped in `$set`. Any `_id` in a plain document is
        // moved to `$setOnInsert` so it is only set when a new document is
        // inserted, never on an existing match.
        let payload_doc = match Bson::try_from(json).map_err(|source| Error::Bson { source })? {
            Bson::Document(d) => d,
            _ => return Err(Error::InvalidDocument),
        };

        let update_doc = if payload_doc.keys().all(|key| key.starts_with('$')) {
            payload_doc
        } else {
            let mut set = BsonDocument::new();
            let mut set_on_insert = BsonDocument::new();
            for (key, value) in payload_doc {
                if key == "_id" {
                    set_on_insert.insert(key, value);
                } else {
                    set.insert(key, value);
                }
            }
            let mut update = BsonDocument::new();
            update.insert("$set", set);
            if !set_on_insert.is_empty() {
                update.insert("$setOnInsert", set_on_insert);
            }
            update
        };

        let collection: Collection<BsonDocument> = self
            .client
            .database(&self.config.db_name)
            .collection(&self.config.collection_name);

        let filter = build_filter_doc(&self.config.filter);

        let options = FindOneAndUpdateOptions::builder()
            .upsert(true)
            .return_document(ReturnDocument::After)
            .build();

        let doc = collection
            .find_one_and_update(filter, update_doc)
            .with_options(options)
            .await
            .map_err(|source| Error::MongoDB { source })?;

        match doc {
            Some(document) => {
                let mut e = document
                    .to_event(self.task_type, self.task_id)
                    .map_err(|source| Error::MessageConversion { source })?;

                match self.tx {
                    None => {
                        if let Some(arc) = completion_tx_arc.as_ref() {
                            arc.signal_completion(e.data_as_json().ok());
                        }
                    }
                    Some(_) => {
                        e.completion_tx = completion_tx_arc.clone();
                    }
                }

                e.send_with_logging(self.tx.as_ref())
                    .await
                    .map_err(|source| Error::SendMessage { source })?;
            }
            None => {
                let mut e = flowgen_core::event::EventBuilder::new()
                    .data(EventData::Json(serde_json::json!({ "matched": 0 })))
                    .task_id(self.task_id)
                    .task_type(self.task_type)
                    .build()
                    .map_err(|source| Error::EventBuilder { source })?;

                match self.tx {
                    None => {
                        if let Some(arc) = completion_tx_arc.as_ref() {
                            arc.signal_completion(e.data_as_json().ok());
                        }
                    }
                    Some(_) => {
                        e.completion_tx = completion_tx_arc.clone();
                    }
                }

                e.send_with_logging(self.tx.as_ref())
                    .await
                    .map_err(|source| Error::SendMessage { source })?;
            }
        }

        Ok(())
    }
}

/// MongoDB collection processor: reads, writes, or upserts documents
/// depending on `config.operation`.
#[derive(Debug)]
pub struct Processor {
    config: Arc<super::config::Collection>,
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
            .map_err(|source| Error::ConfigRender { source })?;

        let credentials_path = init_config.credentials_path.clone();
        let client_key =
            flowgen_core::client_registry::ClientKey::new(self.task_type, &credentials_path);
        let client = self
            .task_context
            .client_registry
            .get_or_init(client_key, || async {
                let mut builder = MongoClientBuilder::new();
                if let Some(path) = credentials_path {
                    builder = builder.credentials_path(path);
                }
                builder
                    .build()
                    .map_err(|source| Error::Auth { source })?
                    .connect()
                    .await
                    .map_err(|source| Error::Auth { source })
            })
            .await
            .map_err(|e| match e {
                flowgen_core::client_registry::Error::Init { source } => source,
                flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
            })?;

        Ok(EventHandler {
            client,
            task_id: self.task_id,
            tx: self.tx.clone(),
            config: Arc::new(init_config),
            task_type: self.task_type,
        })
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
                        error!(error = %e, "Failed to initialize MongoDB collection processor");
                        Err(tokio_retry::RetryError::transient(e))
                    }
                }
            },
        )
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => return Err(e),
        };

        let mut handlers = Vec::new();

        loop {
            if self.task_context.cancellation_token.is_cancelled() {
                futures::future::join_all(handlers).await;
                return Ok(());
            }

            match self.rx.recv().await {
                Some(event) => {
                    let event_handler = Arc::clone(&event_handler);
                    let retry_strategy = retry_config.strategy();

                    let handle = tokio::spawn(
                        async move {
                            let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                match event_handler.handle(event.clone()).await {
                                    Ok(result) => Ok(result),
                                    Err(e) => {
                                        error!(error = %e, "Failed to process MongoDB event");
                                        Err(tokio_retry::RetryError::transient(e))
                                    }
                                }
                            })
                            .await;

                            if let Err(err) = result {
                                error!(error = %err, "MongoDB collection processor failed after all retry attempts");
                                let mut error_event = event.clone();
                                error_event.error = Some(err.to_string());
                                if let Some(ref tx) = event_handler.tx {
                                    tx.send(error_event).await.ok();
                                }
                            }
                        }
                        .instrument(tracing::Span::current()),
                    );
                    handlers.push(handle);
                }
                None => {
                    futures::future::join_all(handlers).await;
                    return Ok(());
                }
            }
        }
    }
}

fn build_filter_doc(filter: &std::collections::HashMap<String, String>) -> BsonDocument {
    let mut d = BsonDocument::new();
    for (key, value) in filter {
        d.insert(key, value);
    }
    d
}

/// Builder for constructing `Processor` instances.
#[derive(Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::Collection>>,
    rx: Option<Receiver<Event>>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    pub fn new() -> ProcessorBuilder {
        ProcessorBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Collection>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    /// Sets an optional downstream sender. Omit for terminal (leaf) tasks.
    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
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
            task_id: self.task_id,
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
    use flowgen_core::task::runner::Runner;
    use serde_json::json;
    use std::path::PathBuf;
    use tokio::sync::mpsc::channel;

    fn mock_config(operation: Operation) -> super::super::config::Collection {
        super::super::config::Collection {
            name: "test_mongodb_collection".to_string(),
            operation,
            db_name: "test_database".to_string(),
            collection_name: "test_collection".to_string(),
            filter: Default::default(),
            credentials_path: None,
            depends_on: Some(Vec::new()),
            retry: Default::default(),
        }
    }

    fn mock_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
        let task_manager = Arc::new(
            flowgen_core::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
            as Arc<dyn flowgen_core::cache::Cache>;
        Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .task_manager(task_manager)
                .cache(cache)
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_build_filter_doc_with_data() {
        let mut filter = std::collections::HashMap::new();
        filter.insert("status".to_string(), "active".to_string());
        let doc = build_filter_doc(&filter);
        assert_eq!(doc.get_str("status").unwrap(), "active");
    }

    #[test]
    fn test_build_filter_doc_empty() {
        let filter = std::collections::HashMap::new();
        assert!(build_filter_doc(&filter).is_empty());
    }

    #[test]
    fn test_bson_try_from_primitives() {
        assert_eq!(Bson::try_from(json!(null)).unwrap(), Bson::Null);
        assert_eq!(Bson::try_from(json!(true)).unwrap(), Bson::Boolean(true));
        assert_eq!(Bson::try_from(json!(42)).unwrap(), Bson::Int32(42));
        assert_eq!(
            Bson::try_from(json!("flowgen")).unwrap(),
            Bson::String("flowgen".to_string())
        );
    }

    #[test]
    fn test_bson_try_from_large_integer_uses_int64() {
        assert_eq!(
            Bson::try_from(json!(i64::from(i32::MAX) + 1)).unwrap(),
            Bson::Int64(i64::from(i32::MAX) + 1)
        );
    }

    #[test]
    fn test_bson_try_from_float() {
        assert_eq!(Bson::try_from(json!(-0.5)).unwrap(), Bson::Double(-0.5));
    }

    #[test]
    fn test_bson_try_from_complex_structures() {
        let data = json!({"tags": ["a", "b"], "nested": {"active": true}});
        let Bson::Document(doc) = Bson::try_from(data).unwrap() else {
            panic!("expected document");
        };
        assert_eq!(doc.get_array("tags").unwrap().len(), 2);
        assert!(doc
            .get_document("nested")
            .unwrap()
            .get_bool("active")
            .unwrap());
    }

    #[tokio::test]
    async fn test_write_rejects_unsupported_event_data() {
        let client = Arc::new(
            mongodb::Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
        );
        let handler = EventHandler {
            client,
            config: Arc::new(mock_config(Operation::Write)),
            task_id: 1,
            tx: None,
            task_type: "test",
        };

        let schema = Arc::new(arrow::datatypes::Schema::empty());
        let batch = arrow::record_batch::RecordBatch::new_empty(schema);
        let event = flowgen_core::event::EventBuilder::new()
            .data(EventData::ArrowRecordBatch(batch))
            .subject("test".to_string())
            .task_id(1)
            .task_type("test")
            .build()
            .unwrap();

        let result = handler.handle(event).await;
        assert!(matches!(result, Err(Error::UnsupportedEventData)));
    }

    #[tokio::test]
    async fn test_write_rejects_id_that_is_not_an_object_id() {
        let client = Arc::new(
            mongodb::Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
        );
        let handler = EventHandler {
            client,
            config: Arc::new(mock_config(Operation::Write)),
            task_id: 1,
            tx: None,
            task_type: "test",
        };

        let event = flowgen_core::event::EventBuilder::new()
            .data(EventData::Json(
                json!({"_id": "not-extended-json", "name": "Ada"}),
            ))
            .subject("test".to_string())
            .task_id(1)
            .task_type("test")
            .build()
            .unwrap();

        let result = handler.handle(event).await;
        assert!(matches!(result, Err(Error::UnsupportedIdShape)));
    }

    #[tokio::test]
    async fn test_write_rejects_malformed_oid_hex() {
        let client = Arc::new(
            mongodb::Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
        );
        let handler = EventHandler {
            client,
            config: Arc::new(mock_config(Operation::Write)),
            task_id: 1,
            tx: None,
            task_type: "test",
        };

        let event = flowgen_core::event::EventBuilder::new()
            .data(EventData::Json(
                json!({"_id": {"$oid": "not-hex"}, "name": "Ada"}),
            ))
            .subject("test".to_string())
            .task_id(1)
            .task_type("test")
            .build()
            .unwrap();

        let result = handler.handle(event).await;
        assert!(matches!(result, Err(Error::InvalidObjectId { .. })));
    }

    #[tokio::test]
    async fn test_upsert_rejects_unsupported_event_data() {
        let client = Arc::new(
            mongodb::Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
        );
        let handler = EventHandler {
            client,
            config: Arc::new(mock_config(Operation::Upsert)),
            task_id: 1,
            tx: None,
            task_type: "test",
        };

        let schema = Arc::new(arrow::datatypes::Schema::empty());
        let batch = arrow::record_batch::RecordBatch::new_empty(schema);
        let event = flowgen_core::event::EventBuilder::new()
            .data(EventData::ArrowRecordBatch(batch))
            .subject("test".to_string())
            .task_id(1)
            .task_type("test")
            .build()
            .unwrap();

        let result = handler.handle(event).await;
        assert!(matches!(result, Err(Error::UnsupportedEventData)));
    }

    #[test]
    fn test_builder_missing_config() {
        let result = ProcessorBuilder::new().build();
        assert!(matches!(
            result,
            Err(Error::MissingBuilderAttribute(ref attr)) if attr == "config"
        ));
    }

    #[test]
    fn test_builder_missing_receiver() {
        let config = Arc::new(mock_config(Operation::Read));
        let result = ProcessorBuilder::new().config(config).build();
        assert!(matches!(
            result,
            Err(Error::MissingBuilderAttribute(ref attr)) if attr == "receiver"
        ));
    }

    #[test]
    fn test_builder_missing_task_context() {
        let (_tx, rx) = channel(1);
        let config = Arc::new(mock_config(Operation::Read));
        let result = ProcessorBuilder::new().config(config).receiver(rx).build();
        assert!(matches!(
            result,
            Err(Error::MissingBuilderAttribute(ref attr)) if attr == "task_context"
        ));
    }

    #[test]
    fn test_builder_missing_task_type() {
        let (_tx, rx) = channel(1);
        let config = Arc::new(mock_config(Operation::Read));
        let result = ProcessorBuilder::new()
            .config(config)
            .receiver(rx)
            .task_context(mock_task_context())
            .build();
        assert!(matches!(
            result,
            Err(Error::MissingBuilderAttribute(ref attr)) if attr == "task_type"
        ));
    }

    #[test]
    fn test_builder_success_with_sender() {
        let (tx, rx) = channel(1);
        let config = Arc::new(mock_config(Operation::Write));

        let processor = ProcessorBuilder::new()
            .config(config)
            .receiver(rx)
            .sender(tx)
            .task_id(5)
            .task_context(mock_task_context())
            .task_type("mongodb_collection")
            .build()
            .expect("builder should succeed");

        assert_eq!(processor.task_id, 5);
        assert_eq!(processor.task_type, "mongodb_collection");
        assert!(processor.tx.is_some());
    }

    #[test]
    fn test_builder_success_without_sender() {
        let (_tx, rx) = channel(1);
        let config = Arc::new(mock_config(Operation::Read));

        let processor = ProcessorBuilder::new()
            .config(config)
            .receiver(rx)
            .task_context(mock_task_context())
            .task_type("mongodb_collection")
            .build()
            .expect("builder should succeed");

        assert!(processor.tx.is_none());
    }

    #[tokio::test]
    async fn test_init_auth_failure() {
        let (_tx, rx) = channel(1);
        let mut config = mock_config(Operation::Read);
        config.credentials_path = Some(PathBuf::from("/invalid/credentials/path.json"));

        let processor = ProcessorBuilder::new()
            .config(Arc::new(config))
            .receiver(rx)
            .task_context(mock_task_context())
            .task_type("auth_test")
            .build()
            .unwrap();

        let result = processor.init().await;
        assert!(matches!(result, Err(Error::Auth { .. })));
    }

    #[tokio::test]
    async fn test_init_credentials_parse_failure() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "flowgen_test_mongodb_collection_parse_fail_{}.json",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, "not valid json").unwrap();

        let mut config = mock_config(Operation::Write);
        config.credentials_path = Some(path.clone());

        let (_tx, rx) = channel(1);
        let processor = ProcessorBuilder::new()
            .config(Arc::new(config))
            .receiver(rx)
            .task_context(mock_task_context())
            .task_type("connect_fail")
            .build()
            .unwrap();

        let result = processor.init().await;
        std::fs::remove_file(&path).ok();
        assert!(matches!(result, Err(Error::Auth { .. })));
    }

    #[test]
    fn test_error_display_formatting() {
        let err = Error::MissingBuilderAttribute("field".to_string());
        assert_eq!(err.to_string(), "Missing required attribute: field");

        let err = Error::UnsupportedEventData;
        assert_eq!(err.to_string(), "Unsupported event data");

        let err = Error::InvalidDocument;
        assert_eq!(err.to_string(), "Invalid MongoDB document");

        let err = Error::UnsupportedIdShape;
        assert!(err.to_string().contains("must be an ObjectId"));

        let err = Error::Auth {
            source: crate::client::Error::CredentialsFileRead {
                source: std::io::Error::new(std::io::ErrorKind::NotFound, "not found"),
            },
        };
        assert!(matches!(err, Error::Auth { .. }));

        let inner = Box::new(Error::MissingBuilderAttribute("inner".to_string()));
        let err = Error::RetryExhausted { source: inner };
        assert_eq!(
            err.to_string(),
            "Task failed after all retry attempts: Missing required attribute: inner"
        );

        let source = mongodb::error::Error::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        ));
        let err = Error::MongoDB { source };
        assert!(err.to_string().contains("MongoDB error:"));
    }
}
