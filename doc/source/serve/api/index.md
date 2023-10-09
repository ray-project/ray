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

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/class_without_init_args.rst

   serve.Deployment
   serve.Application
```

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

#### Deployment Handles

:::{note}
Ray 2.7 introduces a new {mod}`DeploymentHandle <ray.serve.handle.DeploymentHandle>` API that will replace the existing `RayServeHandle` and `RayServeSyncHandle` APIs.
Existing code will continue to work, but you are encouraged to opt-in to the new API to avoid breakages in the future.
To opt into the new API, you can either use `handle.options(use_new_handle_api=True)` on each handle or set it globally via environment variable: `export RAY_SERVE_ENABLE_NEW_HANDLE_API=1`.
:::

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/
   :template: autosummary/class_without_init_args.rst

   serve.handle.DeploymentHandle
   serve.handle.DeploymentResponse
   serve.handle.DeploymentResponseGenerator
   serve.handle.RayServeHandle
   serve.handle.RayServeSyncHandle
```

### Running Applications

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   serve.start
   serve.run
   serve.delete
   serve.status
   serve.shutdown
```

### Configurations

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   serve.config.ProxyLocation
   serve.config.gRPCOptions
   serve.config.HTTPOptions
   serve.config.AutoscalingConfig
```

#### Advanced APIs

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   serve.get_replica_context
   serve.context.ReplicaContext
   serve.get_multiplexed_model_id
   serve.get_app_handle
   serve.get_deployment_handle
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
    "import_path": "text_ml:app",
    "runtime_env": {
        "working_dir": "https://github.com/ray-project/serve_config_examples/archive/HEAD.zip"
    },
    "deployments": [
        {"name": "Translator", "user_config": {"language": "french"}},
        {"name": "Summarizer"},
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
    "import_path": "text_ml:app",
    "runtime_env": {
        "working_dir": "https://github.com/ray-project/serve_config_examples/archive/HEAD.zip"
    },
    "deployments": [
        {"name": "Translator", "user_config": {"language": "french"}},
        {"name": "Summarizer"},
    ]
}
```


#### `GET "/api/serve/deployments/status"`

Gets the Serve application's current status, including all the deployment statuses. See [status schema](serve-rest-api-response-schema) for the response's JSON schema.

**Example Request**:

```http
GET /api/serve/deployments/status HTTP/1.1
Host: http://localhost:52365/
Accept: application/json
```

**Example Response**

```http
HTTP/1.1 200 OK
Content-Type: application/json

{
    "name": "default",
    "app_status": {
        "status": "RUNNING",
        "message": "",
        "deployment_timestamp": 1694043082.0397763
    },
    "deployment_statuses": [
        {
            "name": "Translator",
            "status": "HEALTHY",
            "message": ""
        },
        {
            "name": "Summarizer",
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
            "name": "text_app",
            "route_prefix": "/",
            "import_path": "text_ml:app",
            "runtime_env": {
                "working_dir": "https://github.com/ray-project/serve_config_examples/archive/HEAD.zip"
            },
            "deployments": [
                {"name": "Translator", "user_config": {"language": "french"}},
                {"name": "Summarizer"},
            ]
        },
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
    "controller_info": {
        "node_id": "cef533a072b0f03bf92a6b98cb4eb9153b7b7c7b7f15954feb2f38ec",
        "node_ip": "10.0.29.214",
        "actor_id": "1d214b7bdf07446ea0ed9d7001000000",
        "actor_name": "SERVE_CONTROLLER_ACTOR",
        "worker_id": "adf416ae436a806ca302d4712e0df163245aba7ab835b0e0f4d85819",
        "log_file_path": "/serve/controller_29778.log"
    },
    "proxy_location": "EveryNode",
    "http_options": {
        "host": "0.0.0.0",
        "port": 8000,
        "root_path": "",
        "request_timeout_s": null,
        "keep_alive_timeout_s": 5
    },
    "grpc_options": {
        "port": 9000,
        "grpc_servicer_functions": []
    },
    "proxies": {
        "cef533a072b0f03bf92a6b98cb4eb9153b7b7c7b7f15954feb2f38ec": {
            "node_id": "cef533a072b0f03bf92a6b98cb4eb9153b7b7c7b7f15954feb2f38ec",
            "node_ip": "10.0.29.214",
            "actor_id": "b7a16b8342e1ced620ae638901000000",
            "actor_name": "SERVE_CONTROLLER_ACTOR:SERVE_PROXY_ACTOR-cef533a072b0f03bf92a6b98cb4eb9153b7b7c7b7f15954feb2f38ec",
            "worker_id": "206b7fe05b65fac7fdceec3c9af1da5bee82b0e1dbb97f8bf732d530",
            "log_file_path": "/serve/http_proxy_10.0.29.214.log",
            "status": "HEALTHY"
        }
    },
    "deploy_mode": "MULTI_APP",
    "applications": {
        "app1": {
            "name": "app1",
            "route_prefix": "/",
            "docs_path": null,
            "status": "RUNNING",
            "message": "",
            "last_deployed_time_s": 1694042836.1912267,
            "deployed_app_config": {
                "name": "app1",
                "route_prefix": "/",
                "import_path": "src.text-test:app",
                "deployments": [
                    {
                        "name": "Translator",
                        "num_replicas": 1,
                        "user_config": {
                            "language": "german"
                        }
                    }
                ]
            },
            "deployments": {
                "Translator": {
                    "name": "Translator",
                    "status": "HEALTHY",
                    "message": "",
                    "deployment_config": {
                        "name": "Translator",
                        "num_replicas": 1,
                        "max_concurrent_queries": 100,
                        "user_config": {
                            "language": "german"
                        },
                        "graceful_shutdown_wait_loop_s": 2.0,
                        "graceful_shutdown_timeout_s": 20.0,
                        "health_check_period_s": 10.0,
                        "health_check_timeout_s": 30.0,
                        "ray_actor_options": {
                            "runtime_env": {
                                "env_vars": {}
                            },
                            "num_cpus": 1.0
                        },
                        "is_driver_deployment": false
                    },
                    "replicas": [
                        {
                            "node_id": "cef533a072b0f03bf92a6b98cb4eb9153b7b7c7b7f15954feb2f38ec",
                            "node_ip": "10.0.29.214",
                            "actor_id": "4bb8479ad0c9e9087fee651901000000",
                            "actor_name": "SERVE_REPLICA::app1#Translator#oMhRlb",
                            "worker_id": "1624afa1822b62108ead72443ce72ef3c0f280f3075b89dd5c5d5e5f",
                            "log_file_path": "/serve/deployment_Translator_app1#Translator#oMhRlb.log",
                            "replica_id": "app1#Translator#oMhRlb",
                            "state": "RUNNING",
                            "pid": 29892,
                            "start_time_s": 1694042840.577496
                        }
                    ]
                },
                "Summarizer": {
                    "name": "Summarizer",
                    "status": "HEALTHY",
                    "message": "",
                    "deployment_config": {
                        "name": "Summarizer",
                        "num_replicas": 1,
                        "max_concurrent_queries": 100,
                        "user_config": null,
                        "graceful_shutdown_wait_loop_s": 2.0,
                        "graceful_shutdown_timeout_s": 20.0,
                        "health_check_period_s": 10.0,
                        "health_check_timeout_s": 30.0,
                        "ray_actor_options": {
                            "runtime_env": {},
                            "num_cpus": 1.0
                        },
                        "is_driver_deployment": false
                    },
                    "replicas": [
                        {
                            "node_id": "cef533a072b0f03bf92a6b98cb4eb9153b7b7c7b7f15954feb2f38ec",
                            "node_ip": "10.0.29.214",
                            "actor_id": "7118ae807cffc1c99ad5ad2701000000",
                            "actor_name": "SERVE_REPLICA::app1#Summarizer#cwiPXg",
                            "worker_id": "12de2ac83c18ce4a61a443a1f3308294caf5a586f9aa320b29deed92",
                            "log_file_path": "/serve/deployment_Summarizer_app1#Summarizer#cwiPXg.log",
                            "replica_id": "app1#Summarizer#cwiPXg",
                            "state": "RUNNING",
                            "pid": 29893,
                            "start_time_s": 1694042840.5789504
                        }
                    ]
                }
            }
        }
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
   :nosignatures:
   :toctree: doc/

   schema.ServeDeploySchema
   schema.gRPCOptionsSchema
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
   :nosignatures:
   :toctree: doc/

   schema.ServeStatusSchema
```

### V2 REST API
```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   schema.ServeInstanceDetails
   schema.ApplicationDetails
   schema.DeploymentDetails
   schema.ReplicaDetails
```

## Metrics API
```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

   metrics.Counter
   metrics.Histogram
   metrics.Gauge
```