use eyre::Result;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const SERVICE_NAME: &str = "highlane";

pub struct LogGuard {
    axiom_enabled: bool,
}

impl Drop for LogGuard {
    fn drop(&mut self) {
        if self.axiom_enabled {
            opentelemetry::global::shutdown_tracer_provider();
        }
    }
}

pub fn init() -> Result<LogGuard> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer();

    let token_set = std::env::var("AXIOM_TOKEN").is_ok();
    let dataset_set = std::env::var("AXIOM_DATASET").is_ok();

    if !token_set || !dataset_set {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .init();
        let missing = match (token_set, dataset_set) {
            (false, false) => "AXIOM_TOKEN and AXIOM_DATASET",
            (false, true) => "AXIOM_TOKEN",
            (true, false) => "AXIOM_DATASET",
            (true, true) => unreachable!(),
        };
        tracing::warn!(missing, "Axiom log shipping disabled; CLI logs only");
        return Ok(LogGuard { axiom_enabled: false });
    }

    match tracing_axiom::default(SERVICE_NAME) {
        Ok(axiom_layer) => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .with(axiom_layer)
                .init();
            tracing::info!(service = SERVICE_NAME, "Axiom log shipping enabled");
            Ok(LogGuard { axiom_enabled: true })
        }
        Err(e) => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt_layer)
                .init();
            tracing::warn!(error = %e, "failed to init Axiom layer; CLI logs only");
            Ok(LogGuard { axiom_enabled: false })
        }
    }
}
