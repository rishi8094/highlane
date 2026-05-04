use axiom_rs::Client;
use eyre::Result;
use serde_json::{Map, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

const SERVICE_NAME: &str = "highlane";
const CHANNEL_CAP: usize = 1024;
const BATCH_CAP: usize = 256;
const FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

pub struct LogGuard {
    inner: Option<GuardInner>,
}

struct GuardInner {
    shutdown: Arc<Notify>,
    handle: JoinHandle<()>,
}

impl LogGuard {
    pub async fn flush(mut self) {
        if let Some(inner) = self.inner.take() {
            inner.shutdown.notify_one();
            let _ = tokio::time::timeout(FLUSH_TIMEOUT, inner.handle).await;
        }
    }
}

pub fn init() -> Result<LogGuard> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer();

    let token_set = std::env::var("AXIOM_TOKEN").is_ok();
    let dataset = std::env::var("AXIOM_DATASET").ok().filter(|s| !s.is_empty());

    let (axiom_layer, guard_inner) = match (token_set, dataset) {
        (true, Some(dataset)) => match Client::new() {
            Ok(client) => {
                let (tx, rx) = mpsc::channel::<Value>(CHANNEL_CAP);
                let shutdown = Arc::new(Notify::new());
                let handle =
                    tokio::spawn(run_ingest(rx, client, dataset, shutdown.clone()));
                (
                    Some(AxiomLayer { tx }),
                    Some(GuardInner { shutdown, handle }),
                )
            }
            Err(e) => {
                eprintln!("[observability] failed to build axiom client: {e}");
                (None, None)
            }
        },
        _ => (None, None),
    };

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(axiom_layer)
        .init();

    if guard_inner.is_some() {
        tracing::info!(service = SERVICE_NAME, "Axiom log shipping enabled");
    } else {
        tracing::warn!(
            "Axiom log shipping disabled (set AXIOM_TOKEN and AXIOM_DATASET to enable)"
        );
    }

    Ok(LogGuard { inner: guard_inner })
}

struct AxiomLayer {
    tx: mpsc::Sender<Value>,
}

impl<S: Subscriber> Layer<S> for AxiomLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut fields = Map::with_capacity(8);
        let meta = event.metadata();

        fields.insert(
            "_time".into(),
            Value::String(chrono::Utc::now().to_rfc3339()),
        );
        fields.insert("service".into(), Value::String(SERVICE_NAME.into()));
        fields.insert("level".into(), Value::String(meta.level().to_string()));
        fields.insert("target".into(), Value::String(meta.target().into()));
        if let Some(file) = meta.file() {
            fields.insert("file".into(), Value::String(file.into()));
        }
        if let Some(line) = meta.line() {
            fields.insert("line".into(), Value::Number(line.into()));
        }

        let mut visitor = JsonVisitor(&mut fields);
        event.record(&mut visitor);

        // Best-effort send: never block the logging path. Drop on full or closed.
        let _ = self.tx.try_send(Value::Object(fields));
    }
}

struct JsonVisitor<'a>(&'a mut Map<String, Value>);

impl<'a> Visit for JsonVisitor<'a> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0
            .insert(field.name().into(), Value::String(value.into()));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().into(), Value::Bool(value));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0
            .insert(field.name().into(), Value::Number(value.into()));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0
            .insert(field.name().into(), Value::Number(value.into()));
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        if let Some(n) = serde_json::Number::from_f64(value) {
            self.0.insert(field.name().into(), Value::Number(n));
        }
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.0
            .insert(field.name().into(), Value::String(format!("{value}")));
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().into(), Value::String(format!("{value:?}")));
    }
}

async fn run_ingest(
    mut rx: mpsc::Receiver<Value>,
    client: Client,
    dataset: String,
    shutdown: Arc<Notify>,
) {
    let mut buf: Vec<Value> = Vec::with_capacity(BATCH_CAP);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.notified() => {
                while let Ok(v) = rx.try_recv() {
                    buf.push(v);
                }
                if !buf.is_empty()
                    && let Err(e) = client.ingest(&dataset, std::mem::take(&mut buf)).await
                {
                    eprintln!("[axiom] ingest error during shutdown: {e}");
                }
                return;
            }
            n = rx.recv_many(&mut buf, BATCH_CAP) => {
                if n == 0 {
                    return;
                }
                let batch = std::mem::take(&mut buf);
                if let Err(e) = client.ingest(&dataset, batch).await {
                    eprintln!("[axiom] ingest error: {e}");
                }
            }
        }
    }
}
