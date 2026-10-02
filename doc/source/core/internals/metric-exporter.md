---
myst:
  html_meta:
    description: "Internals of Ray's metric exporter: C++ metric registration and recording, OTLP gRPC export, and OpenTelemetry SDK integration."
---

(metric-exporter)=

# Metric exporter infrastructure

This page describes `upstream/master` at commit `05e7efd5`, dated 2025-12-17.

Ray's metric export infrastructure collects metrics from C++ components, such as the raylet, the GCS, and workers, and from Python components. It aggregates the metrics and exports them to Prometheus. This page explains how metrics flow through the system, from registration to final export.

## Architecture overview

Ray's metric system uses a multi-stage pipeline:

1. **C++ components**: The raylet, the GCS, and worker processes record metrics with the OpenTelemetry SDK.
1. **OTLP export**: The C++ components export metrics to the metrics agent with the OpenTelemetry Protocol (OTLP) over gRPC.
1. **Metrics agent**: The Python metrics agent, ReporterAgent, receives and processes metrics.
1. **Aggregation**: Ray filters high-cardinality labels and aggregates values.
1. **Prometheus export**: Ray exports the final metrics in Prometheus format.

The following diagram shows the high-level flow:

```text
C++ Components (raylet, GCS, workers)
  ↓ (Record metrics via Metric::Record)
OpenTelemetryMetricRecorder (C++)
  ↓ (OTLP gRPC export)
Metrics Agent (Python - ReporterAgent)
  ↓ (Aggregate & process)
OpenTelemetryMetricRecorder (Python)
  ↓ (Prometheus format)
Prometheus Server
```

## Metric registration and recording (C++ side)

Ray's C++ components register and record metrics through the [OpenTelemetryMetricRecorder](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/src/ray/observability/open_telemetry_metric_recorder.h) singleton. The recorder supports four metric types: Gauge, Counter, Sum, and Histogram.

### Metric types

- **Gauge**: Represents a current value that can go up or down, such as the number of running tasks.
- **Counter**: A cumulative metric that only increases, such as the total number of submitted tasks.
- **Sum**: A cumulative metric that can increase or decrease, such as the number of objects in the object store. The recorder registers a sum as an UpDownCounter.
- **Histogram**: Tracks the distribution of values over time, such as task execution time.

### Registration process

The recorder registers metrics lazily, on first use. The `OpenTelemetryMetricRecorder` is a singleton, accessible through [GetInstance()](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/src/ray/observability/open_telemetry_metric_recorder.cc#L78). When a component records a metric for the first time, the recorder registers it automatically if it isn't registered already.

[open_telemetry_metric_recorder.cc](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/src/ray/observability/open_telemetry_metric_recorder.cc) defines the following registration methods:

- [RegisterGaugeMetric()](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/src/ray/observability/open_telemetry_metric_recorder.cc#L164): Registers an observable gauge with a callback.
- [RegisterCounterMetric()](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/src/ray/observability/open_telemetry_metric_recorder.cc#L203): Registers a synchronous counter.
- [RegisterSumMetric()](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/src/ray/observability/open_telemetry_metric_recorder.cc#L216): Registers a synchronous up-down counter.
- [RegisterHistogramMetric()](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/src/ray/observability/open_telemetry_metric_recorder.cc#L229): Registers a histogram with explicit bucket boundaries.

### Recording mechanisms

Ray uses two recording mechanisms, depending on the metric type:

**Observable metrics**
: Gauges are observable metrics. Observable gauges store values in an intermediate map, `observations_by_name_`, until collection time. When you call [SetMetricValue()](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/src/ray/observability/open_telemetry_metric_recorder.cc#L269) for a gauge, the recorder stores the value with its tags. During export, the OpenTelemetry SDK invokes a callback function, [DoubleGaugeCallback](https://github.com/ray-project/ray/blob/52ed7e3/src/ray/observability/open_telemetry_metric_recorder.cc#L42), which collects all stored values and clears the map to prevent stale data. The callback implementation is in [CollectGaugeMetricValues()](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/src/ray/observability/open_telemetry_metric_recorder.cc#L150).

**Synchronous metrics**
: Counters, sums, and histograms are synchronous metrics. Synchronous metrics record values directly to their instruments without intermediate storage. When you call [SetMetricValue()](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/src/ray/observability/open_telemetry_metric_recorder.cc#L269) for these types, the recorder immediately adds the value to the counter or records it in the histogram through [SetSynchronousMetricValue()](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/src/ray/observability/open_telemetry_metric_recorder.cc#L292).

### Key implementation details

- **Thread safety**: The recorder uses a mutex, `mutex_`, to protect the observations map and the registered instruments.
- **Lock ordering**: The recorder registers callbacks after it releases the mutex. This ordering prevents deadlocks between the recorder's mutex and the internal locks of the OpenTelemetry SDK. For details, see [RegisterGaugeMetric()](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/src/ray/observability/open_telemetry_metric_recorder.cc#L183-L195).
- **Lazy registration**: Registering a metric multiple times is safe. The recorder checks whether a metric is already registered before it creates a new instrument.

C++ components record metrics through the [Metric::Record()](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/src/ray/stats/metric.cc#L111) method, which forwards to [OpenTelemetryMetricRecorder::SetMetricValue()](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/src/ray/stats/metric.cc#L135).

## Metric export from C++ (OTLP gRPC)

C++ components export metrics to the metrics agent with OTLP over gRPC. The recorder configures the export process when it starts.

### OpenTelemetry SDK integration

The `OpenTelemetryMetricRecorder` initializes the OpenTelemetry SDK in its [constructor](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/src/ray/observability/open_telemetry_metric_recorder.cc#L129) and [Start()](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/src/ray/observability/open_telemetry_metric_recorder.cc#L87) method with the following components:

- **MeterProvider**: Manages meter instances and metric readers.
- **PeriodicExportingMetricReader**: Collects metrics at regular intervals and exports them.
- **OTLP gRPC Exporter**: Sends metrics to the metrics agent endpoint.

### Export configuration

When [Start()](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/src/ray/observability/open_telemetry_metric_recorder.cc#L87) runs, the recorder configures the following settings:

- **Endpoint**: The metrics agent's gRPC address, typically `127.0.0.1:port`.
- **Export interval**: How often metrics are collected and exported. This interval is configurable.
- **Export timeout**: The maximum time to wait for an export to complete.
- **Aggregation temporality**: Set to delta mode to prevent double-counting. For details, see [exporter_options.aggregation_temporality](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/src/ray/observability/open_telemetry_metric_recorder.cc#L97).

### Delta aggregation temporality

Ray uses delta aggregation temporality, so each export sends only the changes since the last export. Delta mode matters because the metrics agent accumulates metrics, and re-accumulating them during export would double-count them.

### Export process

During each export interval, the following steps run:

1. **Observable gauges**: The OpenTelemetry SDK invokes the registered callbacks, which collect values from `observations_by_name_` and clear the map.
1. **Synchronous metrics**: Values are read directly from the instruments.
1. **OTLP format**: Metrics are converted to OTLP format.
1. **gRPC export**: The OTLP gRPC exporter sends the metrics to the metrics agent over gRPC.

## Metric reception and processing (Python side)

The metrics agent, ReporterAgent, receives metrics from C++ components through a gRPC service that implements the OpenTelemetry Metrics Service interface.

### gRPC service implementation

The [ReporterAgent](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/python/ray/dashboard/modules/reporter/reporter_agent.py) class implements `MetricsServiceServicer`, which provides the [Export()](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/python/ray/dashboard/modules/reporter/reporter_agent.py#L662) method. This method receives `ExportMetricsServiceRequest` messages that contain OTLP-formatted metrics from C++ components.

### Metric processing

When the metrics agent receives metrics, the `Export()` method processes them in the following structure:

- **Resource metrics**: The top-level container for metrics from a specific resource, such as a raylet process.
- **Scope metrics**: Groups metrics by instrumentation scope.
- **Metrics**: Individual metric data points.

The method routes each metric to a handler based on its type:

- **Histogram metrics**: [_export_histogram_data()](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/python/ray/dashboard/modules/reporter/reporter_agent.py#L577) processes histograms.
- **Number metrics**: [_export_number_data()](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/python/ray/dashboard/modules/reporter/reporter_agent.py#L628) processes gauges, counters, and sums.

For histogram metrics, the metrics agent receives pre-aggregated OTLP bucket counts. Ray reconstructs observations from the bucket midpoints and records them in a single batch call to reduce lock contention.

### Conversion to internal format

The metrics agent converts metrics from OTLP format to Ray's internal metric representation and forwards them to the Python [OpenTelemetryMetricRecorder](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/python/ray/_private/telemetry/open_telemetry_metric_recorder.py) for further processing and aggregation.

## Metric aggregation and cardinality reduction (Python)

The Python [OpenTelemetryMetricRecorder](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/python/ray/_private/telemetry/open_telemetry_metric_recorder.py) handles final aggregation and cardinality reduction before exporting to Prometheus. This step manages metric cardinality to prevent metric explosion.

### OpenTelemetryMetricRecorder (Python)

[open_telemetry_metric_recorder.py](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/python/ray/_private/telemetry/open_telemetry_metric_recorder.py) defines the Python recorder. Its structure is similar to the C++ version, but it uses the Prometheus exporter instead of OTLP. The recorder maintains the following state:

- **Registered instruments**: Maps metric names to OpenTelemetry instruments.
- **Observations maps**: Store gauge, counter, and sum observations and their tag sets until collection.
- **Histogram bucket midpoints**: Pre-calculated midpoints for histogram bucket conversion when reconstructing observations from OTLP bucket counts.

For gauges, counters, and sums, the recorder uses observable instruments, which are asynchronous. Calls to `set_metric_value()` store values internally, and OpenTelemetry invokes callbacks at collection time to export aggregated observations. For histograms, OpenTelemetry doesn't support an observable histogram, so the recorder calls `record()` synchronously.

High-cardinality labels can cause metric explosion and make metrics systems unusable. Ray reduces cardinality through label filtering and value aggregation.

**Label filtering**
: Ray identifies high-cardinality labels based on the `RAY_metric_cardinality_level` environment variable. [MetricCardinality.get_high_cardinality_labels_to_drop()](https://github.com/ray-project/ray/blob/05e7efd5ef71dca7a396e6b5f15c8ff16960c5db/python/ray/_private/telemetry/metric_cardinality.py#L80) implements the logic. Each level drops a different set of labels:

  - **`legacy`**: Preserves all labels. This level was the default before Ray 2.53.
  - **`recommended`**: Drops the `WorkerId` label. This level is the default since Ray 2.53.
  - **`low`**: Drops both the `WorkerId` and `Name` labels for tasks and actors.

**Aggregation process**
: For observable gauges, counters, and sums, aggregation happens in the callback that the Python recorder registers with the OpenTelemetry SDK. The callback runs the following steps:

  1. **Collection**: The callback collects all observations for a metric from an internal observations map.
  1. **Label filtering**: The callback drops high-cardinality labels from tag sets based on `MetricCardinality.get_high_cardinality_labels_to_drop()`.
  1. **Grouping**: The callback groups observations that share the same filtered tag set.
  1. **Aggregation**: The callback aggregates each group with `MetricCardinality.get_aggregation_function()`:

     - For counters and sums, the callback always sums the values.
     - For gauges, the callback sums the values of task and actor metrics and uses the first value for other metrics.

  1. **Export**: The callback returns aggregated observations to OpenTelemetry for Prometheus export.

  This process keeps metrics manageable even when there are thousands of workers or unique task names.
