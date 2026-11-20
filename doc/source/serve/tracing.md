---
myst:
  html_meta:
    description: "Trace Ray Serve requests across proxies, deployments, and replicas with OpenTelemetry, using the built-in file exporter or your own exporter for OTLP backends and the Datadog Agent."
---

(serve-tracing)=

# Trace requests with OpenTelemetry

Ray Serve can record an [OpenTelemetry](https://opentelemetry.io/) trace for each request it handles. A trace follows a request from the proxy that receives it through every deployment it calls, so you can see where the time goes and where errors happen. This page shows how to turn tracing on, find the spans Serve writes, add your own spans, and send traces to a tracing backend.

:::{note}
Request tracing requires Ray 2.61 or later.
:::

## How Serve traces a request

When tracing is on, Serve sets up OpenTelemetry once in each proxy and replica process and records spans for each sampled request. Serve creates four kinds of spans:

| Span name | Created by | Covers |
|---|---|---|
| `proxy_http_request <deployment> <method> <route>` | Proxy | An HTTP request, from arrival to response. |
| `proxy_grpc_request <deployment> <method>` | Proxy | A gRPC request. |
| `route_to_replica <deployment> <method>` | The caller's router, in a proxy or in a replica that calls a `DeploymentHandle` | Choosing a replica and sending the request to it. |
| `replica_handle_request <deployment> <method>` | Replica | Running the request in your deployment. |

Spans carry attributes such as `request_id`, `app`, `deployment`, `replica_id`, `call_method`, `route`, and the HTTP or gRPC status code. Serve passes the trace context along with each request, so all the spans of a request, including calls to other deployments through a `DeploymentHandle`, belong to one trace. The proxy also reads a [W3C `traceparent`](https://www.w3.org/TR/trace-context/) HTTP header or gRPC metadata entry, so a Serve request can join a trace that started in another service.

An exporter decides where spans go. Serve's built-in exporter writes them to files on each node, which is enough to try tracing out and to debug locally. To collect traces from a cluster, write a short exporter function that sends spans to a backend, such as an OpenTelemetry Collector or the Datadog Agent. Serve doesn't include a trace viewer, so use your backend's UI to explore traces.

## Install OpenTelemetry

Tracing needs the OpenTelemetry API and SDK packages:

```bash
pip install -U opentelemetry-api opentelemetry-sdk
```

Install them, and any exporter packages you use, in the Python environment of every node in the cluster. The Serve controller, proxies, and replicas all import them.

## Quick start

The following example starts Serve with tracing on, deploys an application, and sends it a request:

```{literalinclude} doc_code/tracing/quickstart.py
:start-after: __quickstart_start__
:end-before: __quickstart_end__
:language: python
```

Serve applies tracing when it starts, so call `serve.start()` with a `TracingConfig` before you call `serve.run()`. The quick start sets `sampling_ratio=1.0` so that Serve traces every request. The default traces 1% of requests.

The built-in exporter writes spans to files under `/tmp/ray/session_latest/logs/serve/spans/` on the node where each component runs. Each proxy writes `proxy_<node IP>_tracing.json`, and each replica writes `replica_<app>_<deployment>_<replica ID>_tracing.json`. Each file holds one JSON object per span. The following replica span is trimmed to its main fields:

```json
{
    "name": "replica_handle_request Greeter __call__",
    "context": {
        "trace_id": "0x1f0b6d7e4e2c3a8b9d6e5f4a3b2c1d0e",
        "span_id": "0x7a6b5c4d3e2f1a0b",
        "trace_state": "[]"
    },
    "kind": "SpanKind.SERVER",
    "parent_id": "0x3c2b1a0f9e8d7c6b",
    "status": {
        "status_code": "OK"
    },
    "attributes": {
        "request_id": "8f6d2c1e-5b4a-4f3e-9d2c-1b0a9f8e7d6c",
        "replica_id": "x8ab3k1c",
        "deployment": "Greeter",
        "app": "default",
        "call_method": "__call__",
        "route": "/"
    }
}
```

Spans with the same `trace_id` belong to the same request, and `parent_id` links each span to the span that contains it.

## Configure tracing

You can configure tracing in Python, in a Serve config file, or with environment variables. All three set the same options.

### Tracing options

`TracingConfig` has three fields:

| Field | Default | Description |
|---|---|---|
| `enabled` | `True` if `RAY_SERVE_TRACING_EXPORTER_IMPORT_PATH` is set, otherwise `False` | Whether Serve records spans. |
| `exporter_import_path` | `RAY_SERVE_TRACING_EXPORTER_IMPORT_PATH` if set, otherwise the built-in file exporter | The exporter function, as `module:function`. It can't be empty. |
| `sampling_ratio` | `RAY_SERVE_TRACING_SAMPLING_RATIO` if set, otherwise `0.01` | The fraction of requests to trace, from `0.0` to `1.0`. |

Serve samples with a parent-based sampler. When a request arrives with a `traceparent` header from a sampled trace, Serve follows the caller's decision. Otherwise, it samples by `sampling_ratio`. See the [`TracingConfig` API reference](../serve/api/doc/ray.serve.schema.TracingConfig.rst) for details.

### Configure tracing in Python

Pass a `TracingConfig`, or a dictionary with the same fields, to `serve.start()`, as the quick start does. `serve.run()` doesn't take a tracing configuration, so call `serve.start()` first. If Serve is already running, `serve.run()` uses the running instance's tracing configuration.

### Configure tracing in a config file

Set the top-level `tracing_config` field in a {ref}`Serve config file <serve-in-production-config-file>`:

```{literalinclude} doc_code/tracing/tracing_config.yaml
:start-after: __tracing_config_start__
:end-before: __tracing_config_end__
:language: yaml
```

The config deploys the application in `tracing_app.py`:

```{literalinclude} doc_code/tracing/tracing_app.py
:start-after: __tracing_app_start__
:end-before: __tracing_app_end__
:language: python
```

From the directory that holds both files, run `serve run tracing_config.yaml`, or deploy the file to a running Ray cluster with `serve deploy tracing_config.yaml`. `serve run` also accepts an import path, such as `serve run tracing_app:app`, but in that case there's no tracing option to pass, so use a config file or environment variables.

### Configure tracing with environment variables

Serve reads two environment variables:

- `RAY_SERVE_TRACING_EXPORTER_IMPORT_PATH`: the exporter function. Setting it also turns tracing on.
- `RAY_SERVE_TRACING_SAMPLING_RATIO`: the sampling ratio.

Fields that you set explicitly in `TracingConfig` or `tracing_config` always win. Serve fills in the fields you don't set from these variables as the Serve controller sees them, not as the process that calls `serve.start()` sees them. To set them for the controller, use `controller_options`. In the following example, tracing is on and the sampling ratio comes from the controller's environment:

```python
from ray import serve
from ray.serve.schema import TracingConfig

serve.start(
    tracing_config=TracingConfig(enabled=True),
    controller_options={
        "runtime_env": {
            "env_vars": {"RAY_SERVE_TRACING_SAMPLING_RATIO": "0.1"},
        }
    },
)
```

A config file accepts the same `controller_options` field. Proxies and replicas inherit the controller's environment variables, following the {ref}`runtime environment inheritance rules <runtime-environments-inheritance>`, so this is also the place to set variables that your exporter reads.

### Change the tracing configuration

Serve sets up tracing once when it starts. OpenTelemetry allows one tracer provider per process, so running proxies and replicas can't switch to a new configuration. After Serve starts, you can pass the same tracing configuration again, for example with `serve.start()` or `serve deploy`, but a different one fails with this error:

```text
Tracing config cannot be changed after Serve has started; it is an initialization-time setting. To change it, shut down and restart Serve with the new tracing config.
```

To change it, run `serve shutdown` or call `serve.shutdown()`, then start Serve again with the new configuration.

## Add spans to your application

Your deployments can record their own spans with the OpenTelemetry API. Serve sets up the tracer provider in each replica, so `opentelemetry.trace.get_tracer()` returns a tracer that exports through Serve's exporter. Call `serve.get_trace_context()` to get the current request's trace context, and pass it to the span so the span joins the request's trace:

```{literalinclude} doc_code/tracing/custom_spans.py
:start-after: __custom_spans_start__
:end-before: __custom_spans_end__
:language: python
```

The request's trace contains the proxy's spans, the `Frontend` replica's spans, a `route_to_replica Reverser` span for the handle call, the `Reverser` replica's span, and the `reverse_text` span, which is a child of the `Reverser` replica's span. `serve.get_trace_context()` returns `None` when tracing is off, and the code still runs.

To add OpenTelemetry's automatic instrumentation for FastAPI with `FastAPIInstrumentor`, create and instrument the app in a factory function. The instrumentor isn't serializable, so Serve can't ship an already-instrumented app to replicas. See the [FastAPI factory pattern](http-guide.md#fastapi-factory-pattern).

(serve-tracing-exporters)=
## Export traces to a backend

An exporter function tells Serve how to export spans. Point `exporter_import_path` at your own function to send spans anywhere OpenTelemetry supports.

### Write an exporter function

An exporter function takes no arguments and returns a list of OpenTelemetry `SpanProcessor` objects. Serve adds every processor in the list to the tracer provider, so you can export to more than one backend, or add your own processor that sets extra attributes. The simplest exporter prints spans to the stdout of each Serve process, which appears in the Ray logs:

```{literalinclude} doc_code/tracing/exporters.py
:start-after: __console_exporter_start__
:end-before: __console_exporter_end__
:language: python
```

With this function in `exporters.py`, set `exporter_import_path` to `exporters:console_exporter`.

Serve calls the function in several places:

- When Serve first starts, the controller calls it once to check that it imports, takes no arguments, and returns a list of `SpanProcessor` objects. Then the controller shuts those processors down. If the check fails, Serve doesn't start, and `serve.start()` raises an `InvalidTracingConfigError` that names the exporter and the cause. `serve deploy` returns the same error. Fix the exporter and start again; you don't need to shut Serve down first.
- Each proxy and replica calls it once when it sets up tracing.
- If the controller restarts after a failure, it doesn't call the function again.

Because Serve calls the function in several processes, and because Serve waits for the controller's call while it starts, keep the function fast and free of one-time side effects, such as registering a service. For production, prefer `BatchSpanProcessor`, which exports spans in the background. `SimpleSpanProcessor` exports each span as it ends, and the built-in exporter uses it because it's meant for local debugging.

### Make the exporter available to Serve

The exporter's module and packages must be importable on every node that runs the Serve controller, a proxy, or a replica. The controller runs on the head node, and proxies run on the head node and on nodes with replicas by default. Install the packages and the module in the cluster's image or Python environment, or add the module to `PYTHONPATH` on every node. `controller_options.runtime_env` accepts only `env_vars`, so you can't use it to install packages for the controller. When you run Serve locally from a script, a module in the same directory as the script works.

### Send traces with OTLP

Most tracing backends accept the OpenTelemetry Protocol (OTLP), either directly or through an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/). Install the OTLP exporter:

```bash
pip install -U opentelemetry-exporter-otlp-proto-grpc
```

Then return a `BatchSpanProcessor` that wraps an `OTLPSpanExporter`:

```{literalinclude} doc_code/tracing/exporters.py
:start-after: __otlp_exporter_start__
:end-before: __otlp_exporter_end__
:language: python
```

The exporter reads its settings from the standard [OpenTelemetry environment variables](https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/):

- `OTEL_EXPORTER_OTLP_ENDPOINT` sets where to send spans. It defaults to `http://localhost:4317`. An `http://` endpoint uses a plaintext connection, and an `https://` endpoint uses TLS.
- `OTEL_EXPORTER_OTLP_HEADERS` adds headers, such as an API key, to each export.
- `OTEL_SERVICE_NAME` sets the service name that your backend shows for Serve's spans.

Set them for every Serve process through `controller_options`. The following config file uses `otlp_exporter` from `exporters.py`:

```{literalinclude} doc_code/tracing/otlp_config.yaml
:start-after: __otlp_config_start__
:end-before: __otlp_config_end__
:language: yaml
```

(serve-tracing-datadog)=
### Send traces to Datadog

The Datadog Agent can receive traces over OTLP, so you can send Serve's spans to Datadog with the OTLP exporter from the previous section. Datadog documents the Agent side, including Docker and Kubernetes setups, in [Set up the Agent's OTLP endpoint](https://docs.datadoghq.com/opentelemetry/setup/otlp_ingest_in_the_agent/).

Datadog supports this setup only with an Agent on every host that generates OpenTelemetry data, and an application can't send OTLP data to an Agent on a different host. Because proxies and replicas run on many nodes, run an Agent on every node of the Ray cluster.

To send traces to Datadog, do the following:

1. Install `opentelemetry-exporter-otlp-proto-grpc` on every node, and set `exporter_import_path` to an OTLP exporter function, such as `exporters:otlp_exporter`.
1. Turn on the Agent's OTLP gRPC receiver, which is off by default. In `datadog.yaml`, set `otlp_config.receiver.protocols.grpc.endpoint` to `0.0.0.0:4317`. Or set the `DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_GRPC_ENDPOINT` environment variable to `0.0.0.0:4317` for both the core Agent and the trace Agent processes. If the Agent runs in a container, also expose port 4317. With the Datadog Operator or Helm chart, enable `otlp.receiver.protocols.grpc` instead.
1. Point Serve's exporter at the Agent on the same node by setting `OTEL_EXPORTER_OTLP_ENDPOINT` to `http://localhost:4317` in `controller_options`. If the Agent runs in its own container, use the address of that container instead. On Kubernetes, where the Agent's address is the node's IP, set the variable in the Ray container's environment instead, with the host IP from `status.hostIP`, as Datadog's Kubernetes instructions show.
1. Set `OTEL_SERVICE_NAME` so that you can find Serve's traces under a recognizable service name.
1. Send some requests, then look in Datadog APM for traces from that service with Serve's span names, such as `proxy_http_request`.

The `0.0.0.0` address accepts connections on every network interface. For better security, especially in local deployments, Datadog suggests `localhost` instead.

Serve samples requests before it exports them, so Datadog receives only the requests that `sampling_ratio` selects. Datadog's Ingestion Rules don't apply to data from the OpenTelemetry SDKs, so use `sampling_ratio` to control how much trace data Serve sends.

## Propagate trace context to other services

To continue a Serve request's trace in another service, inject the trace context into the outgoing request's headers. The following example runs two Serve applications. `Upstream` calls `Downstream` over HTTP, and the `traceparent` header that it adds links the two requests into one trace:

```{literalinclude} doc_code/tracing/propagation.py
:start-after: __propagation_start__
:end-before: __propagation_end__
:language: python
```

The downstream request's `proxy_http_request` span is a child of the upstream `call_downstream` span. The same works in the other direction: when a request reaches Serve with a `traceparent` header or gRPC metadata entry, Serve's spans join the caller's trace.

## Troubleshoot tracing

The following table lists common problems and how to fix them:

| Symptom | Cause and fix |
|---|---|
| An `ImportError` that asks you to install `opentelemetry-api` and `opentelemetry-sdk` | OpenTelemetry isn't installed where Serve runs. Install it on every node. |
| `InvalidTracingConfigError: Invalid tracing config: exporter '...' is unusable (ModuleNotFoundError: ...)` | The controller can't import the exporter. Check `exporter_import_path`, and make the module and its packages available on the head node. |
| `... is unusable (TypeError: Tracing exporter cannot take any arguments.)` or `(TypeError: Tracing exporter must be a function.)` | The exporter isn't a function that takes no arguments. |
| `... is unusable (TypeError: Output of tracing exporter needs to be of type List[SpanProcessor] ...)` | The exporter returned something other than a list of `SpanProcessor` objects. Return a list, even for one processor. |
| `... is unusable (<the exporter's own error>)` | The exporter raised when the controller called it. Fix the exporter, then start Serve again. |
| A proxy or replica fails to start with a tracing error, although Serve started | The exporter works on the head node but not where that proxy or replica runs, for example because a package, credential, or environment variable is missing there. Serve keeps replacing a failed proxy, and a failed replica fails its deployment. Make the exporter's dependencies available on every node. |
| No spans, or fewer spans than expected | Serve samples 1% of requests by default. Raise `sampling_ratio`. Also check that tracing is on, and look for span files on the node where each component runs. |
| No proxy spans | Proxies don't record request spans when Serve runs with HAProxy or direct ingress. Replica spans still work. |
| `Tracing config cannot be changed after Serve has started` | The configuration differs from the one Serve started with. Shut Serve down and start it again with the new configuration. |
| `serve.start()` hangs | A custom exporter function blocks, for example while it waits for a network connection. Make it return promptly. |

Ray Core has a separate {ref}`tracing feature <ray-tracing>` for tasks and actors, with its own configuration. Serve's request tracing doesn't use it.

## See also

- {ref}`serve-monitoring` for Serve's logs, metrics, and dashboard.
- {ref}`serve-api-observability` for the `TracingConfig` API reference.
- The [OpenTelemetry Python documentation](https://opentelemetry.io/docs/languages/python/) for the tracing API and exporters.
