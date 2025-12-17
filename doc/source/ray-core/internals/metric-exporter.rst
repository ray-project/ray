.. _metric-exporter:

Metric Exporter Infrastructure
================================

This document is based on Ray version 2.52.1.

Ray's metric exporting infrastructure collects metrics from C++ components (raylet, GCS, workers) and Python components, aggregates them, and exports them to Prometheus. This document explains how metrics flow through the system from registration to final export.

Architecture Overview
---------------------

Ray's metric system uses a multi-stage pipeline:

1. **C++ Components**: Raylet, GCS, and worker processes record metrics using the OpenTelemetry SDK
2. **OTLP Export**: Metrics are exported via OpenTelemetry Protocol (OTLP) over gRPC to the metrics agent
3. **Metrics Agent**: The Python metrics agent (ReporterAgent) receives and processes metrics
4. **Aggregation**: High-cardinality labels are filtered and values are aggregated
5. **Prometheus Export**: Final metrics are exported in Prometheus format

The following diagram shows the high-level flow:

.. code-block:: text

  C++ Components (raylet, GCS, workers)
    ↓ (Record metrics via Metric::Record)
  OpenTelemetryMetricRecorder (C++)
    ↓ (OTLP gRPC export)
  Metrics Agent (Python - ReporterAgent)
    ↓ (Aggregate & process)
  OpenTelemetryMetricRecorder (Python)
    ↓ (Prometheus format)
  Prometheus Server

Metric Registration and Recording (C++ Side)
---------------------------------------------

Ray's C++ components register and record metrics through the `OpenTelemetryMetricRecorder <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/open_telemetry_metric_recorder.h>`__ singleton. The recorder supports four metric types: Gauge, Counter, Sum, and Histogram.

Metric Types
~~~~~~~~~~~~

- **Gauge**: Represents a current value that can go up or down (e.g., number of running tasks)
- **Counter**: A cumulative metric that only increases (e.g., total tasks submitted)
- **Sum (UpDownCounter)**: A cumulative metric that can increase or decrease (e.g., number of objects in object store)
- **Histogram**: Tracks the distribution of values over time (e.g., task execution time)

Registration Process
~~~~~~~~~~~~~~~~~~~~

Metrics are registered lazily on first use. The `OpenTelemetryMetricRecorder` uses a singleton pattern accessible via `GetInstance() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/open_telemetry_metric_recorder.cc#L75>`__. When a metric is first recorded, it's automatically registered if it hasn't been registered already.

Registration methods (defined in `open_telemetry_metric_recorder.cc <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/open_telemetry_metric_recorder.cc>`__):

- `RegisterGaugeMetric() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/open_telemetry_metric_recorder.cc#L153>`__: Registers an observable gauge with a callback
- `RegisterCounterMetric() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/open_telemetry_metric_recorder.cc#L192>`__: Registers a synchronous counter
- `RegisterSumMetric() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/open_telemetry_metric_recorder.cc#L205>`__: Registers a synchronous up-down counter
- `RegisterHistogramMetric() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/open_telemetry_metric_recorder.cc#L218>`__: Registers a histogram with explicit bucket boundaries

Recording Mechanisms
~~~~~~~~~~~~~~~~~~~~

Ray uses two different recording mechanisms depending on the metric type:

**Observable Metrics (Gauges)**
  Observable gauges store values in an intermediate map (`observations_by_name_`) until collection time. When you call `SetMetricValue() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/open_telemetry_metric_recorder.cc#L258>`__ for a gauge, the value is stored with its tags. During export, a callback function (`_DoubleGaugeCallback <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/open_telemetry_metric_recorder.cc#L39>`__) is invoked by the OpenTelemetry SDK, which collects all stored values and clears the map to prevent stale data. The callback implementation is in `CollectGaugeMetricValues() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/open_telemetry_metric_recorder.cc#L139>`__.

**Synchronous Metrics (Counters, Sums, Histograms)**
  Synchronous metrics record values directly to their instruments without intermediate storage. When you call `SetMetricValue() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/open_telemetry_metric_recorder.cc#L258>`__ for these types, the value is immediately added to the counter or recorded in the histogram via `SetSynchronousMetricValue() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/open_telemetry_metric_recorder.cc#L281>`__.

Key Implementation Details
~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Thread Safety**: The recorder uses a mutex (`mutex_`) to protect the observations map and registered instruments
- **Lock Ordering**: Callbacks are registered after releasing the mutex to prevent deadlocks between the recorder's mutex and OpenTelemetry SDK's internal locks (see `RegisterGaugeMetric() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/open_telemetry_metric_recorder.cc#L172-L184>`__ for details)
- **Lazy Registration**: Metrics can be registered multiple times safely; the recorder checks if a metric is already registered before creating a new instrument

C++ components record metrics through the `Metric::Record() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/stats/metric.cc#L111>`__ method, which forwards to `OpenTelemetryMetricRecorder::SetMetricValue() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/stats/metric.cc#L135>`__.

Metric Export from C++ (OTLP gRPC)
-----------------------------------

C++ components export metrics to the metrics agent using the OpenTelemetry Protocol (OTLP) over gRPC. The export process is configured when the recorder is started.

OpenTelemetry SDK Integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `OpenTelemetryMetricRecorder` initializes the OpenTelemetry SDK in its `constructor <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/open_telemetry_metric_recorder.cc#L118>`__ and `Start() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/open_telemetry_metric_recorder.cc#L84>`__ method with:

- **MeterProvider**: Manages meter instances and metric readers
- **PeriodicExportingMetricReader**: Collects metrics at regular intervals and exports them
- **OTLP gRPC Exporter**: Sends metrics to the metrics agent endpoint

Export Configuration
~~~~~~~~~~~~~~~~~~~~~

When `Start() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/open_telemetry_metric_recorder.cc#L84>`__ is called, the recorder configures:

- **Endpoint**: The metrics agent's gRPC address (typically `127.0.0.1:port`)
- **Export Interval**: How often metrics are collected and exported (configurable)
- **Export Timeout**: Maximum time to wait for export completion
- **Aggregation Temporality**: Set to delta mode to prevent double-counting (see `exporter_options.aggregation_temporality <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/open_telemetry_metric_recorder.cc#L94>`__)

Delta Aggregation Temporality
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray uses delta aggregation temporality, which means only the changes since the last export are sent. This is important because the metrics agent accumulates metrics, and re-accumulating them during export would lead to double-counting.

Export Process
~~~~~~~~~~~~~~

During each export interval:

1. **Observable Gauges**: The OpenTelemetry SDK invokes registered callbacks, which collect values from `observations_by_name_` and clear the map
2. **Synchronous Metrics**: Values are read directly from the instruments
3. **OTLP Format**: Metrics are converted to OTLP format
4. **gRPC Export**: Metrics are sent to the metrics agent via gRPC

Metric Reception and Processing (Python Side)
----------------------------------------------

The metrics agent (ReporterAgent) receives metrics from C++ components via a gRPC service that implements the OpenTelemetry Metrics Service interface.

gRPC Service Implementation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `ReporterAgent <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/reporter/reporter_agent.py>`__ class implements `MetricsServiceServicer`, which provides the `Export() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/reporter/reporter_agent.py#L664>`__ method. This method receives `ExportMetricsServiceRequest` messages containing OTLP-formatted metrics from C++ components.

Metric Processing
~~~~~~~~~~~~~~~~~

When metrics are received, the `Export()` method processes them in the following structure:

- **Resource Metrics**: Top-level container for metrics from a specific resource (e.g., a raylet process)
- **Scope Metrics**: Groups metrics by instrumentation scope
- **Metrics**: Individual metric data points

The method routes metrics to appropriate handlers based on their type:

- **Histogram Metrics**: Processed by `_export_histogram_data() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/reporter/reporter_agent.py#L600>`__
- **Number Metrics** (Gauge, Counter, Sum): Processed by `_export_number_data() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/reporter/reporter_agent.py#L630>`__

Conversion to Internal Format
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The metrics agent converts OTLP format to Ray's internal metric representation and forwards them to the Python `OpenTelemetryMetricRecorder <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/_private/telemetry/open_telemetry_metric_recorder.py>`__ for further processing and aggregation.

Metric Aggregation and Cardinality Reduction (Python)
------------------------------------------------------

The Python `OpenTelemetryMetricRecorder <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/_private/telemetry/open_telemetry_metric_recorder.py>`__ handles final aggregation and cardinality reduction before exporting to Prometheus. This step is crucial for managing metric cardinality and preventing metric explosion.

OpenTelemetryMetricRecorder (Python)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Python recorder (defined in `open_telemetry_metric_recorder.py <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/_private/telemetry/open_telemetry_metric_recorder.py>`__) has a similar structure to the C++ version but uses the Prometheus exporter instead of OTLP. It maintains:

- **Registered Instruments**: Maps metric names to OpenTelemetry instruments
- **Observations Map**: Stores gauge values with their tag sets until collection
- **Histogram Bucket Midpoints**: Pre-calculated midpoints for histogram bucket conversion

High-cardinality labels can cause metric explosion, making metrics systems unusable. Ray implements cardinality reduction through label filtering and value aggregation.

**Label Filtering**
  The system identifies high-cardinality labels based on the `RAY_metric_cardinality_level` environment variable. The logic is implemented in `MetricCardinality.get_high_cardinality_labels_to_drop() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/_private/telemetry/metric_cardinality.py#L50>`__:

  - **`legacy`**: All labels are preserved (default behavior before Ray 2.53)
  - **`recommended`**: The `WorkerId` label is dropped (default since Ray 2.53)
  - **`low`**: Both `WorkerId` and `Name` labels are dropped for tasks and actors

**Aggregation Process**
  For observable gauges, the aggregation happens in the `callback function <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/_private/telemetry/open_telemetry_metric_recorder.py#L58>`__ within `register_gauge_metric() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/_private/telemetry/open_telemetry_metric_recorder.py#L49>`__:

  - **Collection**: All observations for a metric are collected from `_observations_by_name <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/_private/telemetry/open_telemetry_metric_recorder.py#L61>`__
  - **Label Filtering**: High-cardinality labels are identified and removed from tag sets using `MetricCardinality.get_high_cardinality_labels_to_drop() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/_private/telemetry/open_telemetry_metric_recorder.py#L67>`__
  - **Grouping**: Observations with the same filtered tag set are grouped together
  - **Aggregation**: An aggregation function is applied to each group via `MetricCardinality.get_aggregation_function() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/_private/telemetry/open_telemetry_metric_recorder.py#L86>`__ (sum for tasks/actors, first value for others)
  - **Export**: Aggregated observations are returned to OpenTelemetry for Prometheus export

  This process ensures that metrics remain manageable even when there are thousands of workers or unique task names.
