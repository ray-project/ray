---
myst:
  html_meta:
    description: "Reference for the Ray Serve REST API, which deploys, inspects, and deletes Serve applications over HTTP through the Ray dashboard port."
---

(serve-rest-api)=

# Serve REST API

The Serve REST API is exposed at the same port as the Ray dashboard. The dashboard port is `8265` by default. This port can be changed using the `--dashboard-port` argument when running `ray start`. All example requests on this page use the default port.

## `PUT "/api/serve/applications/"`

Declaratively deploys a list of Serve applications. If Serve is already running on the Ray cluster, removes all applications not listed in the new config. If Serve is not running on the Ray cluster, starts Serve. See [multi-app config schema](serve-rest-api-config-schema) for the request's JSON schema.

**Example Request**:

```http
PUT /api/serve/applications/ HTTP/1.1
Host: http://localhost:8265/
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

## `GET "/api/serve/applications/"`

Gets cluster-level info and comprehensive details on all Serve applications deployed on the Ray cluster. See [metadata schema](serve-rest-api-response-schema) for the response's JSON schema.

```http
GET /api/serve/applications/ HTTP/1.1
Host: http://localhost:8265/
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
        "grpc_servicer_functions": [],
        "request_timeout_s": null
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
                        "max_ongoing_requests": 100,
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
                        "max_ongoing_requests": 100,
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

## `DELETE "/api/serve/applications/"`

Shuts down Serve and all applications running on the Ray cluster. Has no effect if Serve is not running on the Ray cluster.

**Example Request**:

```http
DELETE /api/serve/applications/ HTTP/1.1
Host: http://localhost:8265/
Accept: application/json
```

**Example Response**

```http
HTTP/1.1 200 OK
Content-Type: application/json
```
