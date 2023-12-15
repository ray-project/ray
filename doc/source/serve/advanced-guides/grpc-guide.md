(serve-set-up-grpc-service)=
# Set Up a gRPC Service

This section helps you understand how to:
- Build a user defined gRPC service and protobuf
- Start Serve with gRPC enabled
- Deploy gRPC applications
- Send gRPC requests to Serve deployments
- Check proxy health
- Work with gRPC metadata 
- Use streaming and model composition
- Handle errors
- Use gRPC context


(custom-serve-grpc-service)=
## Define a gRPC service
Running a gRPC server starts with defining gRPC services, RPC methods, and
protobufs similar to the one below.

```{literalinclude} ../doc_code/grpc_proxy/user_defined_protos.proto
:start-after: __begin_proto__
:end-before: __end_proto__
:language: proto
```


This example creates a file named `user_defined_protos.proto` with two
gRPC services: `UserDefinedService` and `ImageClassificationService`.
`UserDefinedService` has three RPC methods: `__call__`, `Multiplexing`, and `Streaming`.
`ImageClassificationService` has one RPC method: `Predict`. Their corresponding input
and output types are also defined specifically for each RPC method.

Once you define the `.proto` services, use `grpcio-tools` to compile python
code for those services. Example command looks like the following:
```bash
python -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. ./user_defined_protos.proto
```

It generates two files: `user_defined_protos_pb2.py` and
`user_defined_protos_pb2_grpc.py`.

For more details on `grpcio-tools` see [https://grpc.io/docs/languages/python/basics/#generating-client-and-server-code](https://grpc.io/docs/languages/python/basics/#generating-client-and-server-code).

:::{note}
Ensure that the generated files are in the same directory as where the Ray cluster
is running so that Serve can import them when starting the proxies.
:::

(start-serve-with-grpc-proxy)=
## Start Serve with gRPC enabled
The [Serve start](https://docs.ray.io/en/releases-2.7.0/serve/api/index.html#serve-start) CLI,
[`ray.serve.start`](https://docs.ray.io/en/releases-2.7.0/serve/api/doc/ray.serve.start.html#ray.serve.start) API,
and [Serve config files](https://docs.ray.io/en/releases-2.7.0/serve/production-guide/config.html#serve-config-files-serve-build)
all support starting Serve with a gRPC proxy. Two options are related to Serve's
gRPC proxy: `grpc_port` and `grpc_servicer_functions`. `grpc_port` is the port for gRPC
proxies to listen to. It defaults to 9000. `grpc_servicer_functions` is a list of import
paths for gRPC `add_servicer_to_server` functions to add to a gRPC proxy. It also
serves as the flag to determine whether to start gRPC server. The default is an empty
list, meaning no gRPC server is started.

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

(deploy-serve-grpc-applications)=
## Deploy gRPC applications
gRPC applications in Serve works similarly to HTTP applications. The only difference is
that the input and output of the methods need to match with what's defined in the `.proto`
file and that the method of the application needs to be an exact match (case sensitive)
with the predefined RPC methods. For example, if we want to deploy `UserDefinedService`
with `__call__` method, the method name needs to be `__call__`, the input type needs to
be `UserDefinedMessage`, and the output type needs to be `UserDefinedResponse`. Serve
passes the protobuf object into the method and expects the protobuf object back
from the method. 

Example deployment:
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_grpc_deployment__
:end-before: __end_grpc_deployment__
:language: python
```

Deploy the application:
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_deploy_grpc_app__
:end-before: __end_deploy_grpc_app__
:language: python
```

:::{note}
`route_prefix` is still a required field as of Ray 2.7.0 due to a shared code path with
HTTP. Future releases will make it optional for gRPC.
:::


(send-serve-grpc-proxy-request)=
## Send gRPC requests to serve deployments
Sending a gRPC request to a Serve deployment is similar to sending a gRPC request to
any other gRPC server. Create a gRPC channel and stub, then call the RPC
method on the stub with the appropriate input. The output is the protobuf object
that your Serve application returns.

Sending a gRPC request:
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_send_grpc_requests__
:end-before: __end_send_grpc_requests__
:language: python
```

Read more about gRPC clients in Python: [https://grpc.io/docs/languages/python/basics/#client](https://grpc.io/docs/languages/python/basics/#client)


(serve-grpc-proxy-health-checks)=
## Check proxy health
Similar to HTTP `/-/routes` and `/-/healthz` endpoints, Serve also provides gRPC
service method to be used in health check. 
- `/ray.serve.RayServeAPIService/ListApplications` is used to list all applications
  deployed in Serve. 
- `/ray.serve.RayServeAPIService/Healthz` is used to check the health of the proxy.
  It returns `OK` status and "success" message if the proxy is healthy.

The service method and protobuf are defined as below:
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

You can call the service method with the following code:
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_health_check__
:end-before: __end_health_check__
:language: python
```

:::{note}
Serve provides the `RayServeAPIServiceStub` stub, and `HealthzRequest` and
`ListApplicationsRequest` protobufs for you to use. You don't need to generate them
from the proto file. They are available for your reference.
:::

(serve-grpc-metadata)=
## Work with gRPC metadata
Just like HTTP headers, gRPC also supports metadata to pass request related information.
You can pass metadata to Serve's gRPC proxy and Serve knows how to parse and use
them. Serve also passes trailing metadata back to the client.

List of Serve accepted metadata keys:
- `application`: The name of the Serve application to route to. If not passed and only
  one application is deployed, serve routes to the only deployed app automatically.
- `request_id`: The request ID to track the request.
- `multiplexed_model_id`: The model ID to do model multiplexing.

List of Serve returned trailing metadata keys:
- `request_id`: The request ID to track the request.

Example of using metadata:
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_metadata__   
:end-before: __end_metadata__
:language: python
```

(serve-grpc-proxy-more-examples)=
## Use streaming and model composition
gRPC proxy remains at feature parity with HTTP proxy. Here are more examples of using
gRPC proxy for getting streaming response as well as doing model composition.

### Streaming
The `Steaming` method is deployed with the app named "app1" above. The following code
gets a streaming response.
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_streaming__   
:end-before: __end_streaming__
:language: python
```

### Model composition
Assuming we have the below deployments. `ImageDownloader` and `DataPreprocessor` are two
separate steps to download and process the image before PyTorch can run inference.
The `ImageClassifier` deployment initializes the model, calls both
`ImageDownloader` and `DataPreprocessor`, and feed into the resnet model to get the
classes and probabilities of the given image.

```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_model_composition_deployment__   
:end-before: __end_model_composition_deployment__
:language: python
```

We can deploy the application with the following code:
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
At this point, two applications are running on Serve, "app1" and "app2". If more
than one application is running, you need to pass `application` to the
metadata so Serve knows which application to route to.
:::


(serve-grpc-proxy-error-handling)=
## Handle errors
Similar to any other gRPC server, request throws a `grpc.RpcError` when the response
code is not "OK". Put your request code in a try-except block and handle
the error accordingly.
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_error_handle__   
:end-before: __end_error_handle__
:language: python
```

Serve uses the following gRPC error codes:
- `NOT_FOUND`: When multiple applications are deployed to Serve and the application is
  not passed in metadata or passed but no matching application.
- `UNAVAILABLE`: Only on the health check methods when the proxy is in draining state.
  When the health check is throwing `UNAVAILABLE`, it means the health check failed on
  this node and you should no longer route to this node.
- `DEADLINE_EXCEEDED`: The request took longer than the timeout setting and got cancelled.
- `INTERNAL`: Other unhandled errors during the request.

(serve-grpc-proxy-grpc-context)=
## Use gRPC context
Serve provides a [gRPC context object](https://grpc.github.io/grpc/python/grpc.html#grpc.ServicerContext)
to the deployment replica to get information
about the request as well as setting response metadata such as code and details.
If the handler function is defined with a `grpc_context` argument, Serve will pass a
[RayServegRPCContext](../api/doc/ray.serve.grpc_util.RayServegRPCContext.rst) object
in for each request. Below is an example of how to set a custom status code,
details, and trailing metadata.

```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_grpc_context_define_app__
:end-before: __end_grpc_context_define_app__
:language: python
```

The client code is defined like the following to get those attributes.
```{literalinclude} ../doc_code/grpc_proxy/grpc_guide.py
:start-after: __begin_grpc_context_client__
:end-before: __end_grpc_context_client__
:language: python
```

:::{note}
If the handler raises an unhandled exception, Serve will return an `INTERNAL` error code
with the stacktrace in the details, regardless of what code and details
are set in the `RayServegRPCContext` object.
:::
