use clap::Parser;
use config::Config;
use flowgen::app::App;
use flowgen::config::AppConfig;
use flowgen_core::flow::activity::{MetricsStore, OtlpMetricsStore};
use flowgen_core::flow::activity_layer::FlowActivityLayer;
use flowgen_core::telemetry::query::MemoryLogsStoreWriter;
use std::env;
use std::process;
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::{error, info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Parser)]
#[command(name = "flowgen", version, about = "Data activation with a blast 💥")]
struct Cli {
    /// Path to configuration file.
    #[arg(short, long, env = "CONFIG_PATH")]
    config: String,
}

enum LogFormat {
    Compact,
    Json,
}

/// Determines the log format from LOG_FORMAT env var or TTY detection.
fn determine_log_format() -> LogFormat {
    if let Ok(format) = env::var("LOG_FORMAT") {
        return match format.to_lowercase().as_str() {
            "compact" => LogFormat::Compact,
            _ => LogFormat::Json,
        };
    }

    match atty::is(atty::Stream::Stdout) {
        true => LogFormat::Compact,
        false => LogFormat::Json,
    }
}

fn init_tracing(metrics_store: Arc<dyn MetricsStore>, logs_writer: Option<MemoryLogsStoreWriter>) {
    let format = determine_log_format();

    let env_filter = match tracing_subscriber::EnvFilter::try_from_default_env() {
        Ok(filter) => filter,
        Err(_) => {
            tracing_subscriber::EnvFilter::new("info,opentelemetry=warn,opentelemetry_sdk=warn")
        }
    };

    let activity_layer = FlowActivityLayer::new(metrics_store);
    // Memory backend gets its own JSON fmt layer feeding the in-process
    // ring buffer. Absent on remote backends.
    let memory_layer = logs_writer.map(|w| tracing_subscriber::fmt::layer().json().with_writer(w));

    match format {
        LogFormat::Compact => {
            let fmt_layer = tracing_subscriber::fmt::layer().compact();
            tracing_subscriber::registry()
                .with(env_filter)
                .with(activity_layer)
                .with(memory_layer)
                .with(fmt_layer)
                .init();
        }
        LogFormat::Json => {
            let fmt_layer = tracing_subscriber::fmt::layer().json();
            tracing_subscriber::registry()
                .with(env_filter)
                .with(activity_layer)
                .with(memory_layer)
                .with(fmt_layer)
                .init();
        }
    }
}

#[tokio::main]
async fn main() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cli = Cli::parse();

    // Config load runs before tracing is up: tracing needs the metrics
    // store, which needs the cache, and the cache is defined in config.
    // Boot-time errors go straight to stderr — the canonical Rust CLI pattern.
    let config = match Config::builder()
        .add_source(config::File::with_name(&cli.config))
        .add_source(config::Environment::with_prefix("APP"))
        .build()
    {
        Ok(config) => config,
        Err(e) => {
            let cwd = env::current_dir()
                .map(|d| d.display().to_string())
                .unwrap_or_default();
            let msg = e.to_string();
            let msg = msg
                .chars()
                .next()
                .map(|c| c.to_uppercase().to_string() + &msg[c.len_utf8()..])
                .unwrap_or(msg);
            eprintln!("{msg} (working directory: {cwd})");
            process::exit(1);
        }
    };

    let app_config = match config.try_deserialize::<AppConfig>() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Failed to deserialize app config: {}", e);
            process::exit(1);
        }
    };

    if let Err(e) = app_config.validate() {
        eprintln!("{e}");
        process::exit(1);
    }

    let cache = match App::init_cache(&app_config, None).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to build cache: {}", e);
            process::exit(1);
        }
    };

    let metrics_store = OtlpMetricsStore::builder().build();

    let telemetry_config = match &app_config.telemetry {
        Some(t) if t.enabled => {
            let backend = match &t.backend {
                Some(flowgen::config::TelemetryBackendOptions::Remote { endpoint }) => {
                    flowgen_core::telemetry::Backend::Remote {
                        endpoint: endpoint.clone(),
                    }
                }
                Some(flowgen::config::TelemetryBackendOptions::Memory {
                    logs_per_flow,
                    metrics_per_flow,
                }) => flowgen_core::telemetry::Backend::Memory {
                    logs_per_flow: *logs_per_flow,
                    metrics_per_flow: *metrics_per_flow,
                },
                None => flowgen_core::telemetry::Backend::Memory {
                    logs_per_flow: flowgen_core::telemetry::MEMORY_LOG_CAPACITY,
                    metrics_per_flow: flowgen_core::telemetry::MEMORY_METRIC_CAPACITY,
                },
            };
            flowgen_core::telemetry::TelemetryConfig {
                backend,
                service_name: t.service_name.clone(),
                service_version: env!("CARGO_PKG_VERSION").to_string(),
                metrics_export_interval_secs: t.metrics_export_interval.as_secs(),
            }
        }
        _ => flowgen_core::telemetry::TelemetryConfig::default(),
    };
    let telemetry = match flowgen_core::telemetry::init_telemetry(telemetry_config) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Failed to initialize OpenTelemetry: {e}");
            process::exit(1);
        }
    };

    init_tracing(
        Arc::clone(&metrics_store) as Arc<dyn MetricsStore>,
        telemetry.logs_writer.clone(),
    );

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};

            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to setup SIGTERM handler: {}", e);
                    return;
                }
            };

            let mut sigint = match signal(SignalKind::interrupt()) {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to setup SIGINT handler: {}", e);
                    return;
                }
            };

            tokio::select! {
                _ = sigterm.recv() => {
                    info!("Received SIGTERM, initiating graceful shutdown...");
                }
                _ = sigint.recv() => {
                    info!("Received SIGINT, initiating graceful shutdown...");
                }
            }

            let _ = shutdown_tx.send(());

            tokio::select! {
                _ = sigterm.recv() => {}
                _ = sigint.recv() => {}
            }
            warn!("Received second signal, forcing shutdown");
            process::exit(1);
        }

        #[cfg(not(unix))]
        {
            match tokio::signal::ctrl_c().await {
                Ok(()) => {
                    info!("Received shutdown signal, initiating graceful shutdown...");
                }
                Err(err) => {
                    error!("Failed to listen for shutdown signal: {}", err);
                    return;
                }
            }

            let _ = shutdown_tx.send(());

            if tokio::signal::ctrl_c().await.is_ok() {
                warn!("Received second signal, forcing shutdown");
                process::exit(1);
            }
        }
    });

    let app = App {
        config: app_config,
        metrics_store: Arc::clone(&metrics_store) as Arc<dyn MetricsStore>,
        cache: Arc::clone(&cache),
        logs_store: telemetry.logs_store.clone(),
    };
    if let Err(e) = app.start(shutdown_rx).await {
        error!("Application failed to run: {}", e);
        process::exit(1);
    }
}
