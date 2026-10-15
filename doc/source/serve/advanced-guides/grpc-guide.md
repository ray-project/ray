---
myst:
  html_meta:
    description: "Serve a gRPC service with Ray Serve: define the service, enable the gRPC proxy, send requests, and use streaming and model composition."
---

(serve-set-up-grpc-service)=
# Set up a gRPC service

This guide shows how to define a gRPC service, serve it with Ray Serve, and send it requests. It also covers server reflection, proxy health checks, gRPC metadata, streaming, model composition, error handling, and the gRPC context.


(custom-serve-grpc-service)=
## Define a gRPC service
To run a gRPC server, start by defining gRPC services, RPC methods, and protobufs, such as the following:

```{literalinclude} ../doc_code/grpc_proxy/user_defined_protos.proto
:start-after: __begin_proto__
:end-before: __end_proto__
:language: proto
```


This example creates a file named `user_defined_protos.proto` with two gRPC services: `UserDefinedService` and `ImageClassificationService`. `UserDefinedService` has three RPC methods: `__call__`, `Multiplexing`, and `Streaming`. `ImageClassificationService` has one RPC method: `Predict`. The file also defines the input and output types for each RPC method.

After you define the `.proto` services, use `grpcio-tools` to compile Python code for them. The following command compiles the example file:
```bash
python -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. ./user_defined_protos.proto
```

The command generates two files, `user_defined_protos_pb2.py` and `user_defined_protos_pb2_grpc.py`.

For more details on `grpcio-tools`, see [the gRPC guide to generating client and server code](https://grpc.io/docs/languages/python/basics/#generating-client-and-server-code).

:::{note}
Put the generated files in the same directory where the Ray cluster runs, so Serve can import them when it starts the proxies.
:::

(start-serve-with-grpc-proxy)=
## Start Serve with gRPC enabled
The {ref}`Serve start <serve-cli>` CLI, {func}`ray.serve.start` API, and {ref}`Serve config files <serve-in-production-config-file>` all support starting Serve with a gRPC proxy. Two options configure Serve's gRPC proxy, `grpc_port` and `grpc_servicer_functions`. `grpc_port` is the port that gRPC proxies listen on. It defaults to 9000. `grpc_servicer_functions` is a list of import paths for gRPC `add_servicer_to_server` functions to add to a gRPC proxy. It also acts as the flag that determines whether Serve starts a gRPC server. The default is an empty list, which means Serve doesn't start a gRPC server.

::::{tab-set}

:::{tab-item} CLI
```bash
ray start --head
serve start \
  --grpc-port 9000 \
  --grpc-servicer-functions user_defined_protos_pb2_grpc.add_UserDefinedServiceServicer_to_server \
  --grpc-servicer-functions user_defined_protos_pb2_grpc.add_ImageClassificationServiceServicer_to_server

```
:::

:::{tab-item} Python API
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_start_grpc_proxy__
:end-before: __end_start_grpc_proxy__
:language: python
```
:::

:::{tab-item} Serve config file
```yaml
# config.yaml
grpc_options:
  port: 9000
  grpc_servicer_functions:
    - user_defined_protos_pb2_grpc.add_UserDefinedServiceServicer_to_server
    - user_defined_protos_pb2_grpc.add_ImageClassificationServiceServicer_to_server

applications:
  - name: app1
    route_prefix: /app1
    import_path: test_deployment_v2:g
    runtime_env: {}

  - name: app2
    route_prefix: /app2
    import_path: test_deployment_v2:g2
    runtime_env: {}

```

```bash
# Start Serve with above config file.
serve run config.yaml

```

:::

::::

:::{note}
The default maximum gRPC message size is about 2 GB. To change it, set `RAY_SERVE_GRPC_MAX_MESSAGE_SIZE` to a size in bytes before you start Ray. For example, `export RAY_SERVE_GRPC_MAX_MESSAGE_SIZE=104857600` sets it to 100 MB.
:::

(deploy-serve-grpc-applications)=
## Deploy gRPC applications
gRPC applications in Serve work similarly to HTTP applications, with two differences. The input and output of each method must match the types that the `.proto` file defines. Each method name must also exactly match a predefined RPC method, including case. For example, to deploy `UserDefinedService` with the `__call__` method, name the method `__call__`, take a `UserDefinedMessage` as input, and return a `UserDefinedResponse`. Serve passes the protobuf object into the method and expects a protobuf object back from the method.

The following code defines a gRPC deployment:
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_grpc_deployment__
:end-before: __end_grpc_deployment__
:language: python
```

Deploy the application with the following code:
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_deploy_grpc_app__
:end-before: __end_deploy_grpc_app__
:language: python
```

:::{note}
As of Ray 2.7.0, `route_prefix` is still a required field because gRPC shares a code path with HTTP. Future releases will make it optional for gRPC.
:::


(send-serve-grpc-proxy-request)=
## Send gRPC requests to Serve deployments
Sending a gRPC request to a Serve deployment works similarly to sending one to any other gRPC server. Create a gRPC channel and stub, then call the RPC method on the stub with the appropriate input. The output is the protobuf object that your Serve application returns.

The following code sends a gRPC request:
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_send_grpc_requests__
:end-before: __end_send_grpc_requests__
:language: python
```

For more about gRPC clients in Python, see [the gRPC guide to creating a Python client](https://grpc.io/docs/languages/python/basics/#client).


(serve-grpc-server-reflection)=
## Explore your gRPC API with server reflection
Serve's gRPC proxy supports the [gRPC server reflection protocol](https://grpc.io/docs/guides/reflection/). With reflection enabled, standard tools such as [grpcurl](https://github.com/fullstorydev/grpcurl), [grpcui](https://github.com/fullstorydev/grpcui), and Postman can discover, describe, and call the gRPC services registered on the proxy without local copies of your `.proto` files. This works similarly to FastAPI's `/docs` page for HTTP.

The following examples use the `UserDefinedService` and generated `user_defined_protos_pb2_grpc` module from [Define a gRPC service](custom-serve-grpc-service) at the top of this guide.

Server reflection is on by default whenever the gRPC proxy runs. To turn it off, set the `enable_reflection` option to false:

::::{tab-set}

:::{tab-item} CLI
```bash
serve start \
  --grpc-port 9000 \
  --grpc-servicer-functions user_defined_protos_pb2_grpc.add_UserDefinedServiceServicer_to_server \
  --grpc-disable-reflection
```
:::

:::{tab-item} Python API
```python
from ray import serve
from ray.serve.config import gRPCOptions

serve.start(
    grpc_options=gRPCOptions(
        port=9000,
        grpc_servicer_functions=[
            "user_defined_protos_pb2_grpc.add_UserDefinedServiceServicer_to_server",
        ],
        enable_reflection=False,
    ),
)
```
:::

:::{tab-item} Serve config file
```yaml
grpc_options:
  port: 9000
  grpc_servicer_functions:
    - user_defined_protos_pb2_grpc.add_UserDefinedServiceServicer_to_server
  enable_reflection: false
```
:::

::::

Installing `ray[serve]` also installs the [`grpcio-reflection`](https://pypi.org/project/grpcio-reflection/) package, which implements the protocol.

With reflection enabled, list and describe your services with `grpcurl`:
```bash
$ grpcurl -plaintext localhost:9000 list
grpc.reflection.v1alpha.ServerReflection
userdefinedprotos.UserDefinedService

$ grpcurl -plaintext localhost:9000 describe userdefinedprotos.UserDefinedService
```

Or launch `grpcui` for an interactive web UI to browse and call your RPC methods:
```bash
grpcui -plaintext localhost:9000
```

:::{note}
Like other gRPC proxy options, `enable_reflection` is fixed at Serve startup. To change it, shut down Serve and restart it. A config update doesn't change it. Reflection advertises the names and types of all user-defined gRPC services registered on the proxy across all applications. It doesn't expose request data or your application code.

If Serve runs with the HAProxy-based proxy and you deploy more than one application, reflection requests must include the `application` metadata, such as `grpcurl -H "application: app1" ...`. Without it, HAProxy can't attribute the request to an application and returns `NOT_FOUND`. If a load balancer or ingress in front of Serve routes gRPC by path, it must also route the reflection service's path prefix, `/grpc.reflection.`.
:::

(serve-grpc-proxy-health-checks)=
## Check proxy health
Similar to the HTTP `/-/routes` and `/-/healthz` endpoints, Serve provides two gRPC service methods for health checks:
- `/ray.serve.RayServeAPIService/ListApplications` lists all applications deployed in Serve.
- `/ray.serve.RayServeAPIService/Healthz` checks the health of the proxy. It returns an `OK` status and a "success" message if the proxy is healthy.

The following protobuf defines the service methods and messages:
```proto
message ListApplicationsRequest {}

message ListApplicationsResponse {
  repeated string application_names = 1;
}

message HealthzRequest {}

message HealthzResponse {
  string message = 1;
}

service RayServeAPIService {
  rpc ListApplications(ListApplicationsRequest) returns (ListApplicationsResponse);
  rpc Healthz(HealthzRequest) returns (HealthzResponse);
}
```

Call the service methods with the following code:
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_health_check__
:end-before: __end_health_check__
:language: python
```

:::{note}
Serve provides the `RayServeAPIServiceStub` stub, and `HealthzRequest` and `ListApplicationsRequest` protobufs for you to use. You don't need to generate them from the proto file. They're available for your reference.
:::

(serve-grpc-metadata)=
## Work with gRPC metadata
gRPC uses metadata to pass request-related information, similar to HTTP headers. When you pass metadata to Serve's gRPC proxy, Serve parses and uses it. Serve also passes trailing metadata back to the client.

Serve accepts the following metadata keys:
- `application`: The name of the Serve application to route to. If you don't pass it and only one application is deployed, Serve routes to that application automatically.
- `request_id`: The request ID to track the request.
- `multiplexed_model_id`: The model ID for model multiplexing.

Serve returns the following trailing metadata key:
- `request_id`: The request ID to track the request.

The following example uses metadata:
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_metadata__   
:end-before: __end_metadata__
:language: python
```

(serve-grpc-proxy-more-examples)=
## Use streaming and model composition
Serve's gRPC proxy supports all four gRPC streaming types:
- **Unary-Unary**: A single request and a single response. This type is the default.
- **Server streaming**: A single request and a streaming response, also called Unary-Stream.
- **Client streaming**: A streaming request and a single response, also called Stream-Unary.
- **Bidirectional streaming**: A streaming request and a streaming response, also called Stream-Stream.

### Server streaming
The `app1` application that you deployed earlier on this page includes the `Streaming` method. The following code gets a streaming response:
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_streaming__   
:end-before: __end_streaming__
:language: python
```

### Client streaming
With client streaming, a client sends a stream of requests and receives a single response. Use the `gRPCInputStream` class to iterate over incoming request messages.

Define a proto file with a client streaming RPC:
```proto
service UserDefinedService {
  rpc ClientStreaming(stream UserDefinedMessage) returns (UserDefinedResponse);
}
```

The following code defines the deployment:
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_client_streaming_deployment__
:end-before: __end_client_streaming_deployment__
:language: python
```

The following client code calls the deployment:
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_client_streaming_client__
:end-before: __end_client_streaming_client__
:language: python
```

### Bidirectional streaming
With bidirectional streaming, a client sends and receives streams of messages at the same time.

Define a proto file with a bidirectional streaming RPC:
```proto
service UserDefinedService {
  rpc BidiStreaming(stream UserDefinedMessage) returns (stream UserDefinedResponse);
}
```

The following code defines the deployment:
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_bidi_streaming_deployment__
:end-before: __end_bidi_streaming_deployment__
:language: python
```

The following client code calls the deployment:
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_bidi_streaming_client__
:end-before: __end_bidi_streaming_client__
:language: python
```

(using-grpc-context-with-streaming)=
### Use gRPC context with streaming
To access the gRPC context in a streaming method, add a `grpc_context` parameter:

```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_streaming_with_context__
:end-before: __end_streaming_with_context__
:language: python
```

### Model composition
Consider the following deployments. `ImageDownloader` and `DataPreprocessor` are two separate steps that download and process the image before PyTorch can run inference. The `ImageClassifier` deployment initializes the model, calls both `ImageDownloader` and `DataPreprocessor`, and feeds the result into the ResNet model to get the classes and probabilities of the given image.

```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_model_composition_deployment__   
:end-before: __end_model_composition_deployment__
:language: python
```

Deploy the application with the following code:
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_model_composition_deploy__   
:end-before: __end_model_composition_deploy__
:language: python
```

The client code to call the application looks like the following:
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_model_composition_client__   
:end-before: __end_model_composition_client__
:language: python
```

:::{note}
At this point, two applications, `app1` and `app2`, are running on Serve. When more than one application is running, pass `application` in the metadata so Serve knows which application to route to.
:::


(serve-grpc-proxy-error-handling)=
## Handle errors
As with any other gRPC server, a request raises a `grpc.RpcError` when the response code isn't `OK`. Put your request code in a try-except block and handle the error:
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_error_handle__   
:end-before: __end_error_handle__
:language: python
```

Serve uses the following gRPC error codes:
- `NOT_FOUND`: Multiple applications are deployed to Serve, and the request metadata either doesn't name an application or names one that doesn't match a deployed application.
- `UNAVAILABLE`: The proxy is in a draining state. Only the health check methods return this code. An `UNAVAILABLE` health check means the health check failed on this node, so stop routing to this node.
- `DEADLINE_EXCEEDED`: The request took longer than the timeout setting and was canceled.
- `INTERNAL`: Other unhandled errors during the request.

(serve-grpc-proxy-grpc-context)=
## Use gRPC context
Serve provides a [gRPC context object](https://grpc.github.io/grpc/python/grpc.html#grpc.ServicerContext) to the deployment replica. Use it to get information about the request and to set response metadata, such as the status code and details. If you define the handler function with a `grpc_context` argument, Serve passes a [RayServegRPCContext](../api/doc/ray.serve.grpc_util.RayServegRPCContext.rst) object to it for each request. The following example sets a custom status code, details, and trailing metadata. You can also set a status code before raising an exception, and Serve preserves that status code in the error response. That way, you can return a meaningful status code instead of the generic `INTERNAL` error. For example, `RESOURCE_EXHAUSTED` is retryable and `INVALID_ARGUMENT` isn't.

```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_grpc_context_define_app__
:end-before: __end_grpc_context_define_app__
:language: python
```

The following client code gets those attributes:
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_grpc_context_client__
:end-before: __end_grpc_context_client__
:language: python
```

:::{note}
If the handler raises an unhandled exception without setting a status code on the `RayServegRPCContext` object, Serve returns an `INTERNAL` error code with the exception message in the details. However, if you set a status code on the context before raising the exception, Serve preserves that status code in the response. That way, you can return meaningful status codes such as `INVALID_ARGUMENT` or `RESOURCE_EXHAUSTED` even when raising exceptions.
:::
