use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::path::PathBuf;

/// Salesforce request header that overrides duplicate-detection rules.
/// `allowSave=true` lets create / update / upsert through even when a
/// duplicate rule would otherwise block it. Set on the request when
/// `allow_duplicate_save: true` is opted in by the flow author.
pub const DUPLICATE_RULE_HEADER: &str = "Sforce-Duplicate-Rule-Header";
pub const ALLOW_SAVE_VALUE: &str = "allowSave=true";

/// SObject CRUD operations.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SObjectOperation {
    /// Create a new record.
    Create,
    /// Get a record by ID.
    Get,
    /// Get a record by external ID field.
    GetByExternalId,
    /// Update an existing record by ID.
    Update,
    /// Upsert a record by external ID (update if exists, create if not).
    Upsert,
    /// Delete a record by ID.
    Delete,
}

/// Payload configuration for SObject operations.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Payload {
    /// Use incoming event data as payload.
    FromEvent { from_event: bool },
    /// Explicit field values.
    Fields(Map<String, Value>),
}

/// Configuration for Salesforce REST API CRUD operations.
///
/// # Examples
///
/// Create a new record with explicit payload:
/// ```yaml
/// salesforce_restapi:
///   name: create_account
///   operation: create
///   credentials_path: /path/to/salesforce_creds.json
///   sobject_type: Account
///   payload:
///     Name: "{{event.data.company_name}}"
///     Industry: "Technology"
///     BillingCity: "San Francisco"
/// ```
///
/// Create using event data as payload:
/// ```yaml
/// salesforce_restapi:
///   name: create_account
///   operation: create
///   credentials_path: /path/to/salesforce_creds.json
///   sobject_type: Account
///   payload:
///     from_event: true
/// ```
///
/// Update a record:
/// ```yaml
/// salesforce_restapi:
///   name: update_account
///   operation: update
///   credentials_path: /path/to/salesforce_creds.json
///   sobject_type: Account
///   record_id: "{{event.data.salesforce_id}}"
///   payload:
///     Name: "{{event.data.new_name}}"
///     Industry: "Manufacturing"
/// ```
///
/// Upsert by external ID:
/// ```yaml
/// salesforce_restapi:
///   name: upsert_contact
///   operation: upsert
///   credentials_path: /path/to/salesforce_creds.json
///   sobject_type: Contact
///   external_id_field: ExternalId__c
///   external_id_value: "{{event.data.external_id}}"
///   payload:
///     FirstName: "{{event.data.first_name}}"
///     LastName: "{{event.data.last_name}}"
///     Email: "{{event.data.email}}"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct SObject {
    /// Unique task identifier.
    pub name: String,
    /// SObject operation type.
    pub operation: SObjectOperation,
    /// Path to Salesforce authentication credentials.
    pub credentials_path: PathBuf,
    /// SObject type (e.g., "Account", "Contact", "CustomObject__c").
    pub sobject_type: String,
    // Fields for create, update, upsert operations.
    /// Payload configuration for record data (create, update, upsert).
    #[serde(default)]
    pub payload: Option<Payload>,

    // Fields for get, update, delete operations.
    /// Salesforce record ID (get, update, delete).
    #[serde(default)]
    pub record_id: Option<String>,

    // Fields for get operations.
    /// Comma-separated list of fields to retrieve (get, get_by_external_id).
    #[serde(default)]
    pub fields: Option<String>,

    // Fields for get_by_external_id and upsert operations.
    /// External ID field name (get_by_external_id, upsert).
    #[serde(default)]
    pub external_id_field: Option<String>,
    /// External ID value (get_by_external_id, upsert).
    #[serde(default)]
    pub external_id_value: Option<String>,

    /// Send `Sforce-Duplicate-Rule-Header: allowSave=true` so Salesforce
    /// accepts a record even when a duplicate detection rule would
    /// normally block it. Default duplicate rules ship as "Block" on
    /// Account/Lead/Contact, so without this flag composite/create
    /// calls silently no-op on duplicates. Applies to create, update,
    /// and upsert.
    #[serde(default)]
    pub allow_duplicate_save: bool,

    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for SObject {}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompositeOperation {
    Create,
    Get,
    Update,
    Upsert,
    Delete,
    Tree,
}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum CompositePayload {
    FromEvent { from_event: bool },
    Records(Vec<Map<String, Value>>),
}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Composite {
    pub name: String,
    pub operation: CompositeOperation,
    pub credentials_path: PathBuf,

    /// SObject type (e.g., "Account", "Contact"). When set, records
    /// missing `attributes` are auto-populated with
    /// `attributes: { type: sobject_type }`. Records that already
    /// include `attributes` are left untouched. Required for `get`,
    /// `upsert`, and `tree` operations.
    #[serde(default)]
    pub sobject_type: Option<String>,

    #[serde(default)]
    pub payload: Option<CompositePayload>,

    #[serde(default)]
    pub ids: Option<Vec<String>>,

    #[serde(default)]
    pub fields: Option<Vec<String>>,

    #[serde(default)]
    pub external_id_field: Option<String>,

    #[serde(default)]
    pub all_or_none: Option<bool>,

    /// Send `Sforce-Duplicate-Rule-Header: allowSave=true` so Salesforce
    /// accepts a record even when a duplicate detection rule would
    /// normally block it. Default duplicate rules ship as "Block" on
    /// Account/Lead/Contact, so without this flag composite create /
    /// update / upsert silently no-op on duplicates. Applies to
    /// create, update, upsert, and tree.
    #[serde(default)]
    pub allow_duplicate_save: bool,

    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,

    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Composite {}

/// Configuration for Salesforce REST API SOSL search operations.
///
/// # Examples
///
/// ```yaml
/// salesforce_restapi_search:
///   name: find_accounts
///   credentials_path: /path/to/salesforce_creds.json
///   query: "FIND {{{event.data.search_term}}} IN ALL FIELDS RETURNING Account(Id, Name)"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Search {
    /// Unique task identifier.
    pub name: String,
    /// Path to Salesforce authentication credentials.
    pub credentials_path: PathBuf,
    /// SOSL query string (supports Handlebars templating).
    pub query: String,
    /// Optional list of upstream task names this task depends on.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Search {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sobject_operation_serialization() {
        assert_eq!(
            serde_json::to_string(&SObjectOperation::Create).unwrap(),
            "\"create\""
        );
        assert_eq!(
            serde_json::to_string(&SObjectOperation::Get).unwrap(),
            "\"get\""
        );
        assert_eq!(
            serde_json::to_string(&SObjectOperation::GetByExternalId).unwrap(),
            "\"get_by_external_id\""
        );
        assert_eq!(
            serde_json::to_string(&SObjectOperation::Update).unwrap(),
            "\"update\""
        );
        assert_eq!(
            serde_json::to_string(&SObjectOperation::Upsert).unwrap(),
            "\"upsert\""
        );
        assert_eq!(
            serde_json::to_string(&SObjectOperation::Delete).unwrap(),
            "\"delete\""
        );
    }

    #[test]
    fn test_sobject_operation_deserialization() {
        assert_eq!(
            serde_json::from_str::<SObjectOperation>("\"create\"").unwrap(),
            SObjectOperation::Create
        );
        assert_eq!(
            serde_json::from_str::<SObjectOperation>("\"get\"").unwrap(),
            SObjectOperation::Get
        );
        assert_eq!(
            serde_json::from_str::<SObjectOperation>("\"get_by_external_id\"").unwrap(),
            SObjectOperation::GetByExternalId
        );
        assert_eq!(
            serde_json::from_str::<SObjectOperation>("\"update\"").unwrap(),
            SObjectOperation::Update
        );
        assert_eq!(
            serde_json::from_str::<SObjectOperation>("\"upsert\"").unwrap(),
            SObjectOperation::Upsert
        );
        assert_eq!(
            serde_json::from_str::<SObjectOperation>("\"delete\"").unwrap(),
            SObjectOperation::Delete
        );
    }
}
