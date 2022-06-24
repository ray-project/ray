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
