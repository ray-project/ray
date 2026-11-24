---
myst:
  html_meta:
    description: "API reference for Ray Serve configuration in ray.serve.config and the autoscaling policy APIs in ray.serve.autoscaling_policy."
---

(serve-api-config)=

# Configuration

These APIs configure Serve's proxies, controller, autoscaling, request routing, and gang scheduling. Most live in `ray.serve.config`. The autoscaling policy helpers live in `ray.serve.autoscaling_policy`.

```{eval-rst}
.. currentmodule:: ray
```

## Options and policies

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/class_without_autosummary.rst

   serve.config.ProxyLocation
   serve.config.AutoscalingContext
   serve.autoscaling_policy.replica_queue_length_autoscaling_policy
   serve.autoscaling_policy.PrometheusScalar
   serve.autoscaling_policy.PrometheusSample
   serve.autoscaling_policy.PrometheusVector
   serve.autoscaling_policy.PrometheusQueryMixin
   serve.config.AggregationFunction
   serve.config.GangPlacementStrategy
   serve.config.GangRuntimeFailurePolicy
```

## Configuration models

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/autopydantic.rst

   serve.config.ControllerOptions
   serve.config.gRPCOptions
   serve.config.HTTPOptions
   serve.config.AutoscalingConfig
   serve.config.AutoscalingPolicy
   serve.config.BackpressureConfig
   serve.config.RequestRouterConfig
   serve.config.GangSchedulingConfig
   serve.config.DeploymentActorConfig
```
