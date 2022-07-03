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

### REST API

#### `GET "/api/serve/deployments/"`

Gets latest config that Serve has received. This config represents the current goal state for the Serve application. Starts a Serve application on the Ray cluster if it's not already running. See the [config schema](serve-rest-api-config-schema) for the response's JSON schema.

**Example Request**:

```
GET /api/serve/deployments/ HTTP 1.1
Host: http://localhost:8265/
Accept: application/json
```

**Example Response**:

```http
HTTP/1.1 200 OK
Content-Type: application/json

{
    "import_path": "fruit.deployment_graph",
    "runtime_env": {
        "working_dir": "https://github.com/ray-project/serve_config_examples/archive/HEAD.zip"
    },
    "deployments": [
        {"name": "MangoStand", "user_config": {"price": 1}},
        {"name": "OrangeStand", "user_config": {"price": 2}},
        {"name": "PearStand", "user_config": {"price": 3}}
    ]
}
```

#### `PUT "/api/serve/deployments/"`

Declaratively deploys the Serve application. Starts Serve on the Ray cluster if it's not already running. See the [config schema](serve-rest-api-config-schema) for the request's JSON schema.

**Example Request**:

```
PUT /api/serve/deployments/ HTTP 1.1
Host: http://localhost:8265/
Accept: application/json

{
    "import_path": "fruit.deployment_graph",
    "runtime_env": {
        "working_dir": "https://github.com/ray-project/serve_config_examples/archive/HEAD.zip"
    },
    "deployments": [
        {"name": "MangoStand", "user_config": {"price": 1}},
        {"name": "OrangeStand", "user_config": {"price": 2}},
        {"name": "PearStand", "user_config": {"price": 3}}
    ]
}
```

**Example Response**


```http
HTTP/1.1 200 OK
Content-Type: application/json
```

#### `GET "/api/serve/deployments/status"`

Gets the Serve application's current status, including all the deployment statuses. This config represents the current goal state for the Serve application. Starts a Serve application on the Ray cluster if it's not already running. See the [status schema](serve-rest-api-status-schema) for the response's JSON schema.

**Example Request**:

```
GET /api/serve/deployments/ HTTP 1.1
Host: http://localhost:8265/
Accept: application/json
```

**Example Response**

```http
HTTP/1.1 200 OK
Content-Type: application/json

{
    "app_status": {
        "status": "RUNNING",
        "message": "",
        "deployment_timestamp": 1855994527.146304
    },
    "deployment_statuses": [
        {
            "name": "MangoStand",
            "status": "HEALTHY",
            "message": ""
        },
        {
            "name": "OrangeStand",
            "status": "HEALTHY",
            "message": ""
        },
        {
            "name": "PearStand",
            "status": "HEALTHY",
            "message": ""
        },
        {
            "name": "FruitMarket",
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

#### `DELETE "/api/serve/deployments/"`

Shuts down the Serve application running on the Ray cluster. Has no
effect if Serve is not running on the Ray cluster.
    
**Example Request**:

```
DELETE /api/serve/deployments/ HTTP 1.1
Host: http://localhost:8265/
Accept: application/json
```

**Example Response**

```http
HTTP/1.1 200 OK
Content-Type: application/json
```

(serve-rest-api-config-schema)=

### Config Schema

```{eval-rst}
.. autopydantic_model:: ray.serve.schema.ServeApplicationSchema

```

(serve-rest-api-status-schema)=

### Status Schema

```{eval-rst}
.. autopydantic_model:: ray.serve.schema.ServeStatusSchema

```

## Serve CLI

```{eval-rst}
.. click:: ray.serve.scripts:cli
   :prog: serve
   :show-nested:
```
