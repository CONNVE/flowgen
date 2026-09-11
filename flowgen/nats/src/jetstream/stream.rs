use async_nats::jetstream::{self, stream::Config};

/// Errors that can occur during stream operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Stream creation error: {source}")]
    CreateStream {
        #[source]
        source: async_nats::jetstream::context::CreateStreamError,
    },
    #[error("Error getting stream: {source}")]
    GetStream {
        #[source]
        source: async_nats::jetstream::context::GetStreamError,
    },
    #[error("Stream request error: {source}")]
    Request {
        #[source]
        source: async_nats::jetstream::context::RequestError,
    },
}

/// Creates or updates a JetStream stream based on the provided configuration.
///
/// If the stream exists:
/// - Merges subjects (existing + new, deduplicated)
/// - Uses new config values if provided, otherwise keeps existing values
///
/// If the stream doesn't exist:
/// - Creates it with provided config
/// - Uses sensible defaults for unspecified values
///
/// Returns the JetStream context for further use.
pub async fn create_or_update_stream(
    jetstream: jetstream::Context,
    stream_opts: &super::config::StreamOptions,
) -> Result<jetstream::Context, Error> {
    let existing_stream = jetstream.get_stream(&stream_opts.name).await;

    match existing_stream {
        Ok(mut stream) => {
            // Stream exists, update it by merging with existing config.
            let existing_config = stream
                .info()
                .await
                .map_err(|e| Error::Request { source: e })?
                .config
                .clone();

            // Merge subjects - additive update: new subjects are added to existing ones.
            // If stream_opts.subjects is empty, existing subjects are preserved.
            let mut subjects = existing_config.subjects.clone();
            if !stream_opts.subjects.is_empty() {
                subjects.extend(stream_opts.subjects.clone());
                subjects.sort();
                subjects.dedup();
            }

            // Use new values if provided, otherwise keep existing.
            let retention = stream_opts
                .retention
                .as_ref()
                .map(|r| match r {
                    super::config::RetentionPolicy::Limits => {
                        jetstream::stream::RetentionPolicy::Limits
                    }
                    super::config::RetentionPolicy::Interest => {
                        jetstream::stream::RetentionPolicy::Interest
                    }
                    super::config::RetentionPolicy::WorkQueue => {
                        jetstream::stream::RetentionPolicy::WorkQueue
                    }
                })
                .unwrap_or(existing_config.retention);

            let discard = stream_opts
                .discard
                .as_ref()
                .map(|d| match d {
                    super::config::DiscardPolicy::Old => jetstream::stream::DiscardPolicy::Old,
                    super::config::DiscardPolicy::New => jetstream::stream::DiscardPolicy::New,
                })
                .unwrap_or(existing_config.discard);

            let discard_new_per_subject = stream_opts
                .discard_new_per_subject
                .unwrap_or(existing_config.discard_new_per_subject);

            let max_age = stream_opts.max_age.unwrap_or(existing_config.max_age);

            let max_messages_per_subject = stream_opts
                .max_messages_per_subject
                .unwrap_or(existing_config.max_messages_per_subject);

            let description = stream_opts
                .description
                .clone()
                .or(existing_config.description);

            let max_messages = stream_opts
                .max_messages
                .unwrap_or(existing_config.max_messages);

            let max_bytes = stream_opts.max_bytes.unwrap_or(existing_config.max_bytes);

            let max_message_size = stream_opts
                .max_message_size
                .unwrap_or(existing_config.max_message_size);

            let max_consumers = stream_opts
                .max_consumers
                .unwrap_or(existing_config.max_consumers);

            let duplicate_window = stream_opts
                .duplicate_window
                .unwrap_or(existing_config.duplicate_window);

            let deny_delete = stream_opts
                .deny_delete
                .unwrap_or(existing_config.deny_delete);

            let deny_purge = stream_opts.deny_purge.unwrap_or(existing_config.deny_purge);

            let allow_rollup = stream_opts
                .allow_rollup
                .unwrap_or(existing_config.allow_rollup);

            let allow_direct = stream_opts
                .allow_direct
                .unwrap_or(existing_config.allow_direct);

            let updated_config = Config {
                name: stream_opts.name.clone(),
                description,
                max_messages_per_subject,
                max_messages,
                max_bytes,
                max_message_size,
                max_consumers,
                subjects,
                discard,
                discard_new_per_subject,
                retention,
                max_age,
                duplicate_window,
                deny_delete,
                deny_purge,
                allow_rollup,
                allow_direct,
                ..existing_config
            };

            jetstream
                .update_stream(updated_config)
                .await
                .map_err(|e| Error::CreateStream { source: e })?;
        }
        Err(_) => {
            // Stream doesn't exist, create it.
            // Only set values if they're explicitly configured, otherwise let NATS use its defaults.

            // Default to wildcard if no subjects specified.
            let subjects = if stream_opts.subjects.is_empty() {
                vec![">".to_string()]
            } else {
                stream_opts.subjects.clone()
            };

            // Start with default config.
            let mut stream_config = Config {
                name: stream_opts.name.clone(),
                subjects,
                ..Default::default()
            };

            // Only set values if explicitly configured.
            if let Some(desc) = &stream_opts.description {
                stream_config.description = Some(desc.clone());
            }

            if let Some(retention) = &stream_opts.retention {
                stream_config.retention = match retention {
                    super::config::RetentionPolicy::Limits => {
                        jetstream::stream::RetentionPolicy::Limits
                    }
                    super::config::RetentionPolicy::Interest => {
                        jetstream::stream::RetentionPolicy::Interest
                    }
                    super::config::RetentionPolicy::WorkQueue => {
                        jetstream::stream::RetentionPolicy::WorkQueue
                    }
                };
            }

            if let Some(discard) = &stream_opts.discard {
                stream_config.discard = match discard {
                    super::config::DiscardPolicy::Old => jetstream::stream::DiscardPolicy::Old,
                    super::config::DiscardPolicy::New => jetstream::stream::DiscardPolicy::New,
                };
            }

            if let Some(discard_new_per_subject) = stream_opts.discard_new_per_subject {
                stream_config.discard_new_per_subject = discard_new_per_subject;
            }

            if let Some(max_age) = stream_opts.max_age {
                stream_config.max_age = max_age;
            }

            if let Some(max_msgs_per_subject) = stream_opts.max_messages_per_subject {
                stream_config.max_messages_per_subject = max_msgs_per_subject;
            }

            if let Some(max_msgs) = stream_opts.max_messages {
                stream_config.max_messages = max_msgs;
            }

            if let Some(max_bytes) = stream_opts.max_bytes {
                stream_config.max_bytes = max_bytes;
            }

            if let Some(max_msg_size) = stream_opts.max_message_size {
                stream_config.max_message_size = max_msg_size;
            }

            if let Some(max_cons) = stream_opts.max_consumers {
                stream_config.max_consumers = max_cons;
            }

            if let Some(duplicate_window) = stream_opts.duplicate_window {
                stream_config.duplicate_window = duplicate_window;
            }

            if let Some(deny_del) = stream_opts.deny_delete {
                stream_config.deny_delete = deny_del;
            }

            if let Some(deny_pur) = stream_opts.deny_purge {
                stream_config.deny_purge = deny_pur;
            }

            if let Some(allow_roll) = stream_opts.allow_rollup {
                stream_config.allow_rollup = allow_roll;
            }

            if let Some(allow_dir) = stream_opts.allow_direct {
                stream_config.allow_direct = allow_dir;
            }

            jetstream
                .create_stream(stream_config)
                .await
                .map_err(|e| Error::CreateStream { source: e })?;
        }
    }

    Ok(jetstream)
}
