# Ray Serve API

## Core APIs

```{eval-rst}
.. autofunction:: ray.serve.start
```

```{eval-rst}
.. autofunction:: ray.serve.deployment
```

```{eval-rst}
.. autofunction:: ray.serve.list_deployments
```

```{eval-rst}
.. autofunction:: ray.serve.get_deployment
```

```{eval-rst}
.. autofunction:: ray.serve.shutdown
```

(deployment-api)=

## Deployment API

```{eval-rst}
.. autoclass:: ray.serve.deployment.Deployment
    :members: deploy, delete, options, get_handle
```

(servehandle-api)=

## ServeHandle API

```{eval-rst}
.. autoclass:: ray.serve.handle.RayServeHandle
    :members: remote, options
```

## Batching Requests

```{eval-rst}
.. autofunction:: ray.serve.batch(max_batch_size=10, batch_wait_timeout_s=0.0)
```

## Serve REST API

### Config

#### Schema

```{eval-rst}
.. autopydantic_model:: ray.serve.schema.ServeApplicationSchema

```

#### REST API

```{eval-rst}
.. http:GET:: "/api/serve/deployments/"
    :noindex:

        Gets latest config that Serve has received. This config represents the current goal state for the Serve application. Starts a Serve application on the Ray cluster if it's not already running. See Config Schema for the output schema.
```

```{eval-rst}
.. http:PUT:: "/api/serve/deployments/"
    :noindex:

        Uploads config for Serve application to deploy. Starts a Serve application on the Ray cluster if it's not already running. See Config Schema for the request schema.
```

```{eval-rst}
.. http:DELETE:: "/api/serve/deployments/"
    :noindex:

        Shuts down the Serve application running on the Ray cluster.
```

### Status:

#### Schema

```{eval-rst}
.. autopydantic_model:: ray.serve.schema.ServeStatusSchema

```

#### REST API

```{eval-rst}
.. http:GET:: "/api/serve/deployments/status"
    :noindex:

        Gets the Serve application's current status, including all the deployment statuses. This config represents the current goal state for the Serve application. Starts a Serve application on the Ray cluster if it's not already running. See Status Schema for the output schema.
```

## Serve CLI

```{eval-rst}
.. click:: ray.serve.scripts:cli
   :prog: serve
   :show-nested:
```
