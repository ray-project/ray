(serve-api)=
# Ray Serve API

## Python API

(core-apis)=

```{eval-rst}
.. module:: ray
```

### Writing Applications

<!---
NOTE: `serve.deployment` and `serve.Deployment` have an autosummary-generated filename collision due to case insensitivity.
This is fixed by added custom filename mappings in `source/conf.py` (look for "autosummary_filename_map").
--->

#### Deployment Decorators

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   serve.deployment
      :noindex:
   serve.ingress
   serve.batch
   serve.multiplexed
```

#### Object Types

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/class_without_init_args.rst

   serve.Deployment
   serve.Application
   serve.handle.DeploymentHandle
   serve.handle.DeploymentHandleRef
   serve.handle.DeploymentHandleGenerator
   serve.handle.RayServeHandle
   serve.handle.RayServeSyncHandle
```

#### Advanced APIs

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   serve.get_replica_context
   serve.get_multiplexed_model_id
```

### Running Applications

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   serve.run
   serve.delete
   serve.start
   serve.shutdown
```

(serve-cli)=

## Command Line Interface (CLI)

```{eval-rst}
.. click:: ray.serve.scripts:cli
   :prog: serve
   :nested: full
```

(serve-rest-api)=

## Serve REST API

### V1 REST API (Single-application)

#### `PUT "/api/serve/deployments/"`

Declaratively deploys the Serve application. Starts Serve on the Ray cluster if it's not already running. See [single-app config schema](serve-rest-api-config-schema) for the request's JSON schema.

**Example Request**:

```http
PUT /api/serve/deployments/ HTTP/1.1
Host: http://localhost:52365/
Accept: application/json
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

**Example Response**


```http
HTTP/1.1 200 OK
Content-Type: application/json
```

#### `GET "/api/serve/deployments/"`

Gets the config for the application currently deployed on the Ray cluster. This config represents the current goal state for the Serve application. See [single-app config schema](serve-rest-api-config-schema) for the response's JSON schema.

**Example Request**:
```http
GET /api/serve/deployments/ HTTP/1.1
Host: http://localhost:52365/
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


#### `GET "/api/serve/deployments/status"`

Gets the Serve application's current status, including all the deployment statuses. See [status schema](serve-rest-api-response-schema) for the response's JSON schema.

**Example Request**:

```http
GET /api/serve/deployments/ HTTP/1.1
Host: http://localhost:52365/
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

Shuts down Serve and the Serve application running on the Ray cluster. Has no effect if Serve is not running on the Ray cluster.
    
**Example Request**:

```http
DELETE /api/serve/deployments/ HTTP/1.1
Host: http://localhost:52365/
Accept: application/json
```

**Example Response**

```http
HTTP/1.1 200 OK
Content-Type: application/json
```

### V2 REST API (Multi-application)

#### `PUT "/api/serve/applications/"`

Declaratively deploys a list of Serve applications. If Serve is already running on the Ray cluster, removes all applications not listed in the new config. If Serve is not running on the Ray cluster, starts Serve. See [multi-app config schema](serve-rest-api-config-schema) for the request's JSON schema.

**Example Request**:

```http
PUT /api/serve/applications/ HTTP/1.1
Host: http://localhost:52365/
Accept: application/json
Content-Type: application/json

{
    "applications": [
        {
            "name": "fruit_stand",
            "route_prefix": "/fruit",
            "import_path": "fruit.deployment_graph",
            "runtime_env": {
                "working_dir": "https://github.com/ray-project/serve_config_examples/archive/HEAD.zip"
            },
            "deployments": [
                {"name": "MangoStand", "user_config": {"price": 1}},
                {"name": "OrangeStand", "user_config": {"price": 2}},
                {"name": "PearStand", "user_config": {"price": 3}}
            ]
        },
        {
            "name": "calculator",
            "route_prefix": "/math",
            "import_path": "conditional_dag.serve_dag",
            "runtime_env": {
                "working_dir": "https://github.com/ray-project/test_dag/archive/HEAD.zip"
            },
            "deployments": [
                {"name": "Multiplier", "ray_actor_options": {"num_cpus": 0.5}},
                {
                    "name": "Adder", 
                    "ray_actor_options": {"env_vars": {"override_increment": "5"}}
                },
            ]
        }
    ]
}
```



**Example Response**


```http
HTTP/1.1 200 OK
Content-Type: application/json
```

#### `GET "/api/serve/applications/"`

Gets cluster-level info and comprehensive details on all Serve applications deployed on the Ray cluster. See [metadata schema](serve-rest-api-response-schema) for the response's JSON schema.

```http
GET /api/serve/applications/ HTTP/1.1
Host: http://localhost:52365/
Accept: application/json
```

**Example Response (abridged JSON)**:

```http
HTTP/1.1 200 OK
Content-Type: application/json

{
    "proxy_location": "HeadOnly",
    "http_options": {
        "host": "127.0.0.1",
        "port": 8000
    },
    "deploy_mode": "MULTI_APP",
    "applications": {
        "fruit_stand": {
            "name": "fruit_stand",
            "route_prefix": "/fruit",
            "docs_path": null,
            "status": "RUNNING",
            "message": "",
            "last_deployed_time_s": 1679952253.748111,
            "deployed_app_config": "...",
            "deployments": {
                "fruit_app_MangoStand": {
                    "name": "fruit_app_MangoStand",
                    "status": "HEALTHY",
                    "message": "",
                    "deployment_config": "...",
                    "replicas": [
                        {
                            "replica_id": "fruit_app_MangoStand#bSkrHK",
                            "state": "RUNNING",
                            "pid": 59350,
                            "actor_name": "...",
                            "actor_id": "...",
                            "node_id": "...",
                            "node_ip": "...",
                            "start_time_s": 1679952254.3458009
                        }
                    ]
                },
            }
        },
    }
}
```

#### `DELETE "/api/serve/applications/"`

Shuts down Serve and all applications running on the Ray cluster. Has no effect if Serve is not running on the Ray cluster.

**Example Request**:

```http
DELETE /api/serve/applications/ HTTP/1.1
Host: http://localhost:52365/
Accept: application/json
```

**Example Response**

```http
HTTP/1.1 200 OK
Content-Type: application/json
```

(serve-rest-api-config-schema)=
## Config Schemas

```{eval-rst}
.. currentmodule:: ray.serve
```


```{eval-rst}
.. autosummary::
   :toctree: doc/

   schema.ServeDeploySchema
   schema.HTTPOptionsSchema
   schema.ServeApplicationSchema
   schema.DeploymentSchema
   schema.RayActorOptionsSchema
```

(serve-rest-api-response-schema)=
## Response Schemas

### V1 REST API
```{eval-rst}
.. autosummary::
   :toctree: doc/

   schema.ServeStatusSchema
```

### V2 REST API
```{eval-rst}
.. autosummary::
   :toctree: doc/

   schema.ServeInstanceDetails
   schema.ApplicationDetails
   schema.DeploymentDetails
   schema.ReplicaDetails
```
