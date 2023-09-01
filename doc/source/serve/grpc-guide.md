(serve-set-up-grpc-proxy)=
# Set Up gRPC Proxy

This section helps you understand how to:
- build a user defined gRPC service and protobuf
- start Serve with gRPC proxy
- deploy gRPC applications
- send gRPC requests to Serve deployments
- Health Checks
- gRPC request metadata 
- more examples
- error handling


(custom-grpc-service)=
## Define a gRPC Service
To run a gRPC server it starts with first defining gRPC services, rpc methods, and 
protobufs similar to the one below.

```proto
// user_defined_protos.proto

syntax = "proto3";

option java_multiple_files = true;
option java_package = "io.ray.examples.user_defined_protos";
option java_outer_classname = "UserDefinedProtos";

package userdefinedprotos;

message UserDefinedMessage {
  string name = 1;
  string origin = 2;
  int64 num = 3;
}

message UserDefinedResponse {
  string greeting = 1;
  int64 num = 2;
}

message UserDefinedMessage2 {}

message UserDefinedResponse2 {
  string greeting = 1;
}

message FruitAmounts {
    int64 orange = 1;
    int64 apple = 2;
    int64 banana = 3;
}

message FruitCosts {
    float costs = 1;
}

service UserDefinedService {
  rpc __call__(UserDefinedMessage) returns (UserDefinedResponse);
  rpc Multiplexing(UserDefinedMessage2) returns (UserDefinedResponse2);
  rpc Streaming(UserDefinedMessage) returns (stream UserDefinedResponse);
}

service FruitService {
  rpc FruitStand(FruitAmounts) returns (FruitCosts);
}
```

In this example, we created a file named `user_defined_protos.proto`. There are two
gRPC services, `UserDefinedService` and `FruitService`. `UserDefinedService` has four
rpc methods, `__call__`, `Multiplexing`, and `Streaming`. `FruitService` has one 
rpc method, `FruitStand`. Their corresponding input and output types are also defined
specifically for each rpc method.

Once the `.proto` services are defined, we can use `grpcio-tools` to compile python 
code for those services. With the following command
```bash
python -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. ./user_defined_protos.proto
```

It will generate two files, `user_defined_protos_pb2.py` and 
`user_defined_protos_pb2_grpc.py`.

For more details on `grpcio-tools` see: [https://grpc.io/docs/languages/python/basics/#generating-client-and-server-code](https://grpc.io/docs/languages/python/basics/#generating-client-and-server-code)


(start-serve-with-grpc-proxy)=
## Start Serve with gRPC Proxy
[Serve start](https://docs.ray.io/en/releases-2.7.0/serve/api/index.html#serve-start) CLI 
and [`ray.serve.start`](https://docs.ray.io/en/releases-2.7.0/serve/api/doc/ray.serve.start.html#ray.serve.start) API 
both support starting Serve with gRPC proxy. There are two options related to Serve's 
gRPC proxy, `grpc_port` and `grpc_servicer_functions`. `grpc_port` is the port for gRPC 
proxies to listen on. It defaults to 9000. `grpc_servicer_functions` is a list of import 
paths for gRPC `add_servicer_to_server` functions to add to Serve’s gRPC proxy. It also 
serves as the flag to determine whether to start gRPC server. Default empty list, 
meaning no gRPC server will be started. 

To start gRPC proxy with the CLI
```bash
ray start --head
serve start \
  --grpc-port 9000 \
  --grpc-servicer-functions user_defined_protos_pb2_grpc.add_UserDefinedServiceServicer_to_server \
  --grpc-servicer-functions user_defined_protos_pb2_grpc.add_FruitServiceServicer_to_server

```

To start gRPC proxy with Python API
```{literalinclude} doc_code/http_guide/grpc_guide.py
:start-after: __begin_start_grpc_proxy__
:end-before: __end_start_grpc_proxy__
:language: python
```


(deploy-grpc-applications)=
## Deploy gRPC Applications
gRPC application in Serve works similarly to HTTP application. The only difference is
the input and output of the methods need to match with what's defined in your `.proto`
file and that the method of the application needs to be an exact match (case sensitive)
with the predefined rpc method. For example, if we want to deploy `UserDefinedService`
with `__call__` method, the method name needs to be `__call__`, the input type needs to
be `UserDefinedMessage`, and the output type needs to be `UserDefinedResponse`. Serve
will pass the protobuf object into the method and expecting the protobuf object back
from the method. 

Example deployment:
```{literalinclude} doc_code/http_guide/grpc_guide.py
:start-after: __begin_grpc_deployment__
:end-before: __end_grpc_deployment__
:language: python
```

Deploy the application:
```{literalinclude} doc_code/http_guide/grpc_guide.py
:start-after: __begin_deploy_grpc_app__
:end-before: __end_deploy_grpc_app__
:language: python
```

:::{note}
`route_prefix` is still a required field as of Ray 2.7.0 due to shared code path with
HTTP. We will make it optional for gRPC in the future release.
:::


(send-grpc-request)=
## Send gRPC Requests to Serve deployments
Sending a gRPC request to a Serve deployment is similar to sending a gRPC request to
any other gRPC server. You would create a gRPC channel and stub, then call the method
on the stub with the appropriate input. The output will be the protobuf object returned
from your Serve application. 

Sending a gRPC request:
```{literalinclude} doc_code/http_guide/grpc_guide.py
:start-after: __begin_send_grpc_requests__
:end-before: __end_send_grpc_requests__
:language: python
```

Read more about gRPC client in Python: [https://grpc.io/docs/languages/python/basics/#client](https://grpc.io/docs/languages/python/basics/#client)


(health-checks)=
## Health Checks
Similar to HTTP's `/-/routes` and `/-/healthz` endpoints, Serve also provides gRPC
service method to be used in health check. 
- `/ray.serve.RayServeAPIService/ListApplications` is used to list all applications
  deployed in Serve. 
- `/ray.serve.RayServeAPIService/Healthz` is used to check the health of the gRPC proxy.
  It will return "OK" if the gRPC proxy is healthy.

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
```{literalinclude} doc_code/http_guide/grpc_guide.py
:start-after: __begin_health_check__
:end-before: __end_health_check__
:language: python
```


(metadata)=
## gRPC Request Metadata
Just like HTTP's headers, gRPC also supports metadata to pass request related info.
You can pass metadata to Serve's gRPC proxy and Serve will know how to parse and use
them. Serve will also pass trailing metadata back to the client.

List of Serve accepted metadata keys:
- `application`: The name of the Serve application to route to. If not passed and only
one application is deployed, serve will route to the only app automatically.
- `request_id`: The request id to track the request.
- `multiplexed_model_id`: The model id to do model multiplexing.

List of Serve returned trailing metadata keys:
- `request_id`: The request id to track the request.

Example of using metadata:
```{literalinclude} doc_code/http_guide/grpc_guide.py
:start-after: __begin_metadata__   
:end-before: __end_metadata__
:language: python
```

(more-examples)=
## More Examples
gRPC proxy will remain feature parity with HTTP Proxy. Here are more examples of using
gRPC proxy for getting streaming response as well as doing model composition.

### Streaming
`Steaming` method is deployed with app name "app1" above. We can use the following code
to get a streaming response.
```{literalinclude} doc_code/http_guide/grpc_guide.py
:start-after: __begin_streaming__   
:end-before: __end_streaming__
:language: python
```

### Model Composition
Assuming we have the below deployments. `OrangeStand` and `AppleStand` are two models
to determine the price. And there is a `FruitStand` model to call `OrangeStand` and 
`AppleStand` to get each fruit's price and combine them into a total costs. 
```{literalinclude} doc_code/http_guide/grpc_guide.py
:start-after: __begin_model_composition_deployment__   
:end-before: __end_model_composition_deployment__
:language: python
```

We can deploy the `FruitStand` model with the following code:
```{literalinclude} doc_code/http_guide/grpc_guide.py
:start-after: __begin_model_composition_deploy__   
:end-before: __end_model_composition_deploy__
:language: python
```

The client code to call `FruitStand` will look like the following:
```{literalinclude} doc_code/http_guide/grpc_guide.py
:start-after: __begin_model_composition_client__   
:end-before: __end_model_composition_client__
:language: python
```

:::{note}
At this point there are two applications running on Serve, "app1" and "app2". If there
are more than one application running, you will need to pass `application` to the
metadata so Serve knows which application to route to.
:::


(error-handling)=
## Error Handling
Similar to any other gRPC server, request will throw error when the response code is not
"OK". It's advised to put your request code in a try-except block and handle the error
accordingly.
```{literalinclude} doc_code/http_guide/grpc_guide.py
:start-after: __begin_error_handle__   
:end-before: __end_error_handle__
:language: python
```

Serve handled gRPC error codes:
- `NOT_FOUND`: When multiple application are deployed to Serve and the application is 
not passed in metadata or passed but no matching application.
- `UNAVAILABLE`: Only on the health check routes when the proxy is in draining state.
- `CANCELLED`: The request takes longer than the timeout setting and cancelled.
- `INTERNAL`: Other unhandled errors during the request.
