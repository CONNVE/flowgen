//! # Flowgen MongoDB Integration
//!
//! This module provides MongoDB integration components for the Flowgen data processing framework.
//! It includes support for change data capture (CDC), collection read/write/upsert, and message conversion
//! utilities for working with MongoDB collections and change streams.
/// Configuration for MongoDB change data capture.
pub mod change_stream;
/// Configuration for MongoDB client creator.
pub mod client;
/// MongoDB collection read/write/upsert processor.
pub mod collection;
/// Configuration structures for MongoDB processors.
pub mod config;
/// Message conversion utilities for MongoDB integration.
pub mod message;
