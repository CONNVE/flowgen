//! # MongoDB Configuration
//!
//! This module provides the configuration structures necessary for interacting
//! with MongoDB in two distinct modes:
//!
//! 1. **Collection (`Collection`):** CRUD-style operations against a collection
//!    (`read`, `write`, `upsert`, and future operations like `delete`).
//! 2. **Change Stream (`ChangeStream`):** Configuration for Change Data Capture (CDC)
//!    to listen for real-time changes.

use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Operation performed against a MongoDB collection.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    /// Find documents matching `filter` and emit each as an event.
    Read,
    /// Insert the incoming event's JSON payload as a document.
    Write,
    /// Update the first document matching `filter` with the incoming event's
    /// JSON payload: a plain payload is wrapped in `$set` (its `_id`, if any,
    /// is only set on insert), or applied verbatim when every top-level key is
    /// an update operator (e.g. `$set`, `$inc`). Inserts an aggregated
    /// document if nothing matches (`find_one_and_update` with `upsert: true`).
    Upsert,
}

/// MongoDB collection task configuration: read or write documents.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Collection {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Operation to perform against the collection.
    pub operation: Operation,
    /// Path to credentials file containing MongoDB authentication details.
    /// Omit to connect to `localhost:27017` without authentication.
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// The Database Name from MongoDB.
    pub db_name: String,
    /// The Collection Name from MongoDB.
    pub collection_name: String,
    /// Key-value pairs to filter documents. Used by `operation: read` and
    /// `operation: upsert`.
    #[serde(default)]
    pub filter: HashMap<String, String>,
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Collection {}

/// MongoDB Change Data Capture reader configuration.
#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeStream {
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Path to credentials file containing MongoDB authentication details.
    /// Omit to connect to `localhost:27017` without authentication.
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// The unique name / identifier of the task.
    pub name: String,
    /// The Database Name from MongoDB.
    pub db_name: String,
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for ChangeStream {}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(operation: Operation) -> Collection {
        Collection {
            name: "task1".to_string(),
            operation,
            credentials_path: Some(PathBuf::from("/tmp/creds.json")),
            db_name: "db".to_string(),
            collection_name: "col".to_string(),
            filter: HashMap::new(),
            depends_on: None,
            retry: None,
        }
    }

    #[test]
    fn test_collection_read_serde_roundtrip() {
        let mut c = fixture(Operation::Read);
        c.filter.insert("status".to_string(), "active".to_string());

        let s = serde_json::to_string(&c).unwrap();
        let de: Collection = serde_json::from_str(&s).unwrap();
        assert_eq!(c, de);
    }

    #[test]
    fn test_collection_write_serde_roundtrip() {
        let c = fixture(Operation::Write);
        let s = serde_json::to_string(&c).unwrap();
        let de: Collection = serde_json::from_str(&s).unwrap();
        assert_eq!(c, de);
    }

    #[test]
    fn test_collection_upsert_serde_roundtrip() {
        let mut c = fixture(Operation::Upsert);
        c.filter.insert("status".to_string(), "active".to_string());

        let s = serde_json::to_string(&c).unwrap();
        let de: Collection = serde_json::from_str(&s).unwrap();
        assert_eq!(c, de);
    }

    #[test]
    fn test_operation_uses_snake_case() {
        let json = r#"{
            "name": "n", "operation": "read", "credentials_path": "/c.json",
            "db_name": "d", "collection_name": "c"
        }"#;
        let c: Collection = serde_json::from_str(json).unwrap();
        assert_eq!(c.operation, Operation::Read);
    }

    #[test]
    fn test_rejects_unknown_fields() {
        let json = r#"{
            "name": "n", "operation": "read", "credentials_path": "/c.json",
            "db_name": "d", "collection_name": "c", "bogus": 1
        }"#;
        assert!(serde_json::from_str::<Collection>(json).is_err());
    }
}
