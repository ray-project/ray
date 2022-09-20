(serve-api)=
# Ray Serve API

(core-apis)=

## Core APIs

```{eval-rst}
.. autofunction:: ray.serve.run
```

```{eval-rst}
.. autofunction:: ray.serve.start
```

```{eval-rst}
.. autofunction:: ray.serve.deployment
```

```{eval-rst}
.. autofunction:: ray.serve.shutdown
```

(deployment-api)=

## Deployment API

```{eval-rst}
.. autoclass:: ray.serve.deployment.Deployment
    :members: deploy, delete, options, get_handle, bind
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

## Serve CLI and REST API

Check out the [CLI](serve-cli) and [REST API](serve-rest-api) for running, debugging, inspecting, and deploying Serve applications in production:

```{toctree}
:maxdepth: 1
:name: serve-non-python-api

serve_cli
rest_api
```

## Deployment Graph APIs

```{eval-rst}
.. autofunction:: ray.serve.api.build
```

% TODO(architkulkarni): This just compiles to "alias of Deployment(name=DAGDriver,version=None,route_prefix=/)"
% in the docs, find out how to make Sphinx correctly autodocument this class.
% ```{eval-rst}
% .. autoclass:: ray.serve.drivers.DAGDriver
%     :members: predict, predict_with_route
% ```
