---
myst:
  html_meta:
    description: "API reference for ray.serve.schema: the status and detail models, the config schemas the Serve REST API accepts, and the response schemas it returns."
---

(serve-api-schema)=

# Schemas

The `ray.serve.schema` module defines the models that describe Serve's state and configuration. The [Serve REST API](serve-rest-api) accepts the config schemas and returns the response schemas.

```{eval-rst}
.. currentmodule:: ray
```

## Status and details

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/class_without_init_args.rst

   serve.schema.ServeActorDetails
   serve.schema.ProxyDetails
   serve.schema.ApplicationStatusOverview
   serve.schema.ServeStatus
   serve.schema.DeploymentStatusOverview
   serve.schema.EncodingType
   serve.schema.AutoscalingMetricsHealth
   serve.schema.AutoscalingStatus
   serve.schema.ScalingDecision
   serve.schema.DeploymentAutoscalingDetail
   serve.schema.ReplicaRank
```

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/class_without_autosummary.rst

   serve.schema.TaskProcessorAdapter
```

(serve-rest-api-config-schema)=

## Config schemas

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/autopydantic.rst

   serve.schema.ServeDeploySchema
   serve.schema.gRPCOptionsSchema
   serve.schema.HTTPOptionsSchema
   serve.schema.ServeApplicationSchema
   serve.schema.DeploymentSchema
   serve.schema.RayActorOptionsSchema
   serve.schema.CeleryAdapterConfig
   serve.schema.TaskProcessorConfig
   serve.schema.TaskResult
   serve.schema.ScaleDeploymentRequest
```

(serve-rest-api-response-schema)=

## Response schemas

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/autopydantic.rst

   serve.schema.ServeInstanceDetails
   serve.schema.ApplicationDetails
   serve.schema.DeploymentDetails
   serve.schema.ReplicaDetails
   serve.schema.TargetGroup
   serve.schema.Target
   serve.schema.DeploymentNode
   serve.schema.DeploymentTopology
   serve.schema.ControllerHealthMetrics
   serve.schema.DurationStats

.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/class_without_autosummary.rst

   serve.schema.APIType
   serve.schema.ApplicationStatus
   serve.schema.ProxyStatus
```
