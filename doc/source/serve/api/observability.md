---
myst:
  html_meta:
    description: "API reference for Ray Serve observability: custom metrics in ray.serve.metrics and the logging and tracing configs in ray.serve.schema."
---

(serve-api-observability)=

# Observability

These APIs emit custom metrics from a deployment and configure Serve's logging and tracing. The metric classes live in `ray.serve.metrics`. The logging and tracing configs live in `ray.serve.schema`.

```{eval-rst}
.. currentmodule:: ray
```

## Metrics

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/class_without_autosummary.rst

   serve.metrics.Counter
   serve.metrics.Histogram
   serve.metrics.Gauge
```

## Logging and tracing

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/autopydantic.rst

   serve.schema.LoggingConfig
   serve.schema.TracingConfig
```
