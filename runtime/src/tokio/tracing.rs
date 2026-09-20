//! Optional utilities to export traces to an OTLP endpoint.
//!
//! Exporters and SDK types are compiled only with the `otel` feature.

/// Configuration for exporting traces to an OTLP endpoint.
pub struct Config {
    /// The OTLP endpoint to export traces to.
    pub endpoint: String,
    /// The service name to use for the traces.
    pub name: String,
    /// The sampling rate to use for the traces.
    pub rate: f64,
}

#[cfg(feature = "otel")]
pub use implementation::export;
pub(super) use implementation::layer;

#[cfg(feature = "otel")]
mod implementation {
    use super::Config;
    use opentelemetry::{global, trace::TracerProvider};
    use opentelemetry_otlp::{ExporterBuildError, SpanExporter, WithExportConfig};
    use opentelemetry_sdk::{
        Resource,
        trace::{BatchSpanProcessor, Sampler, SdkTracerProvider, Tracer},
    };
    use std::time::Duration;

    /// Timeout for the OTLP HTTP exporter.
    const TIMEOUT: Duration = Duration::from_secs(15);

    /// Export traces to an OTLP endpoint.
    pub fn export(cfg: Config) -> Result<Tracer, ExporterBuildError> {
        // Create the OTLP HTTP exporter
        let exporter = SpanExporter::builder()
            .with_http()
            .with_endpoint(cfg.endpoint)
            .with_timeout(TIMEOUT)
            .build()?;

        // Configure the batch processor
        let batch_processor = BatchSpanProcessor::builder(exporter).build();

        // Define the resource with service name
        let resource = Resource::builder_empty()
            .with_service_name(cfg.name.clone())
            .build();

        // Build the tracer provider
        let sampler = Sampler::TraceIdRatioBased(cfg.rate);
        let tracer_provider = SdkTracerProvider::builder()
            .with_span_processor(batch_processor)
            .with_resource(resource)
            .with_sampler(sampler)
            .build();

        // Create the tracer and set it globally
        let tracer = tracer_provider.tracer(cfg.name);
        global::set_tracer_provider(tracer_provider);
        Ok(tracer)
    }

    pub(crate) fn layer<S>(
        traces: Option<Config>,
        level: tracing::Level,
    ) -> impl tracing_subscriber::Layer<S>
    where
        S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
    {
        use tracing_subscriber::Layer as _;
        traces.map(|cfg| {
            let tracer = export(cfg).expect("Failed to initialize tracer");
            tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_filter(tracing_subscriber::EnvFilter::new(level.to_string()))
        })
    }
}

#[cfg(not(feature = "otel"))]
mod implementation {
    use super::Config;

    pub(crate) fn layer<S>(
        _: Option<Config>,
        _: tracing::Level,
    ) -> impl tracing_subscriber::Layer<S>
    where
        S: tracing::Subscriber,
    {
        tracing_subscriber::layer::Identity::new()
    }
}
