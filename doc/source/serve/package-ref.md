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
    
    **Example Request**:

    ..source_code:: http

    GET /api/serve/deployments/ HTTP 1.1
    Host: http://localhost:8265/
    Accept: application/json

    **Example Response**:

    ..source_code:: http

    HTTP/1.1 200 OK
    Content-Type: application/json

    {
        "import_path": "dir.subdir.a.add_and_sub.serve_dag",
        "runtime_env": {
            "working_dir": "https://github.com/ray-project/test_dag/archive/41b26242e5a10a8c167fcb952fb11d7f0b33d614.zip"
        },
        "deployments": [
            {
                "name": "Subtract",
                "num_replicas": 2,
                "ray_actor_options": {
                    "runtime_env": {
                        "py_modules": [
                            "https://github.com/ray-project/test_module/archive/aa6f366f7daa78c98408c27d917a983caa9f888b.zip"
                        ]
                    }
                }
            }
        ]
    }
```

```{eval-rst}
.. http:PUT:: "/api/serve/deployments/"
    :noindex:

        Uploads config for Serve application to deploy. Starts a Serve application on the Ray cluster if it's not already running. See Config Schema for the request schema.

    **Example Request**:

    PUT /api/serve/deployments/ HTTP 1.1
    Host: http://localhost:8265/
    Accept: application/json

    {
        "import_path": "dir.subdir.a.add_and_sub.serve_dag",
        "runtime_env": {
            "working_dir": "https://github.com/ray-project/test_dag/archive/41b26242e5a10a8c167fcb952fb11d7f0b33d614.zip"
        },
        "deployments": [
            {
                "name": "Subtract",
                "num_replicas": 2,
                "ray_actor_options": {
                    "runtime_env": {
                        "py_modules": [
                            "https://github.com/ray-project/test_module/archive/aa6f366f7daa78c98408c27d917a983caa9f888b.zip"
                        ]
                    }
                }
            }
        ]
    }

    **Example Response**

    HTTP/1.1 200 OK
    Content-Type: application/json
```

```{eval-rst}
.. http:DELETE:: "/api/serve/deployments/"
    :noindex:

        Shuts down the Serve application running on the Ray cluster.
    
    **Example Request**:

    ..source_code:: http

    DELETE /api/serve/deployments/ HTTP 1.1
    Host: http://localhost:8265/
    Accept: application/json

    **Example Response**

    HTTP/1.1 200 OK
    Content-Type: application/json
```

### Status

#### Schema

```{eval-rst}
.. autopydantic_model:: ray.serve.schema.ServeStatusSchema

```

#### REST API

```{eval-rst}
.. http:GET:: "/api/serve/deployments/status"
    :noindex:

        Gets the Serve application's current status, including all the deployment statuses. This config represents the current goal state for the Serve application. Starts a Serve application on the Ray cluster if it's not already running. See Status Schema for the output schema.
    
    **Example Request**:

    ..source_code:: http

    GET /api/serve/deployments/ HTTP 1.1
    Host: http://localhost:8265/
    Accept: application/json

    **Example Response**

    HTTP/1.1 200 OK
    Content-Type: application/json

    {
        "app_status": {
            "status": "RUNNING",
            "message": "",
            "deployment_timestamp": 1655490105.9503832
        },
        "deployment_statuses": [
            {
                "name": "Add",
                "status": "HEALTHY",
                "message": ""
            },
            {
                "name": "Subtract",
                "status": "HEALTHY",
                "message": ""
            },
            {
                "name": "Router",
                "status": "HEALTHY",
                "message": ""
            },
            {
                "name": "DAGDriver",
                "status": "HEALTHY",
                "message": ""
            }
        ]
    }
```

## Serve CLI

```{eval-rst}
.. click:: ray.serve.scripts:cli
   :prog: serve
   :show-nested:
```
