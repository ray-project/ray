# Experimental Direct Ingress

In the 2.1, Serve provides an alpha version of [gRPC](https://grpc.io/) ingress.

With RPC protocol, You will get:

* Standardized inference request/response schema during client and serve.
* High performant endpoint than HTTP protocol.

In this section, you will learn how to

* use Serve's built-in gRPC schema to receive client traffic
* bring your own gRPC schema into your Serve application

## Use Serve's Schema

Serve provides a simple gRPC schema to machine learning inference workload. It is designed to be kept simple, and you are encouraged to adapt it for your own need.
```
message PredictRequest {
  map<string, bytes> input = 2;
}

message PredictResponse {
  bytes prediction = 1;
}

service PredictAPIsService {
  rpc Predict(PredictRequest) returns (PredictResponse);
}
```

Take a look at the following code samples for using `DefaultgRPCDriver` in Ray Serve.

To implement the Serve, your class needs to inherit `ray.serve.drivers.DefaultgRPCDriver`.
```{literalinclude} ../serve/doc_code/direct_ingress.py
:start-after: __begin_server__
:end-before: __end_server__
:language: python
```

Client:
You can use Serve's built-in gRPC client to send query to the model.

```{literalinclude} ../serve/doc_code/direct_ingress.py
:start-after: __begin_client__
:end-before: __end_client__
:language: python
```

:::{note}
* `input` is a dictionary of `map<string, bytes> ` following the schema described above.
*  The user input data needs to be serialized to `bytes` type and fed into the `input`.
* The response will be under `bytes` type, which means the user code is responsible for serializing the output into bytes.
* By default, the gRPC port is 9000. You can change it by passing port number when calling DefaultgRPCDriver bind function.
* If the serialization/deserialization cost is huge and unnecessary, you can also bring your own schema to use! Checkout [Bring your own schema](bring-your-own-schema) section!
* There is no difference of scaling config for your business code in gRPC case, you can set the config scaling/autoscaling config inside the `serve.deployment` decorator.
:::

### Client schema code generation
You can use the client either by importing it from the `ray` Python package. Alternatively, you can just copy [Serve's protobuf file](https://github.com/ray-project/ray/blob/e16f49b327bbc1c18e8fc5d0ac4fa8c2f1144412/src/ray/protobuf/serve.proto#L214-L225) to generate the gRPC client.

* Install the gRPC code generation tools
```
pip install grpcio-tools
```

* Generate gRPC code based on the schema
```
python -m grpc_tools.protoc --proto_path=src/ray/protobuf/ --python_out=. --grpc_python_out=. src/ray/protobuf/serve.proto
```
After the two steps above, you should have `serve_pb2.py` and `serve_pb2_grpc.py` files generated.

(bring-your-own-schema)=

## Bring your own schema

If you have a customized schema to use, Serve also supports it!

Assume you have the following customized schema and have generated the corresponding gRPC code:


```
message PingRequest {
  bool no_reply = 1;
}
message PingReply {
}

message PingTimeoutRequest {}
message PingTimeoutReply {}

service TestService {
  rpc Ping(PingRequest) returns (PingReply);
  rpc PingTimeout(PingTimeoutRequest) returns (PingTimeoutReply);
}
```

After the code is generated, you can implement the business logic for gRPC server by creating a subclass of the generated `TestServiceServicer`, and then you just need two extra steps to adopt your schema into Ray Serve.

* Inherit `ray.serve.drivers.gRPCIngress` in your implementation class.
* Add the `@serve.deployment(is_driver_deployment=True)` decorator.

Server:
```{literalinclude} ../serve/doc_code/direct_ingress_with_customized_schema.py
:start-after: __begin_server__
:end-before: __end_server__
:language: python
```

Client:
You can directly use the client code to play it!
```{literalinclude} ../serve/doc_code/direct_ingress_with_customized_schema.py
:start-after: __begin_client__
:end-before: __end_client__
:language: python
```

:::{note}
*  `is_driver_deployment` (experimental flag) is needed to mark the class as driver, serve will make sure the driver class deployment gets deployed one replica per node.
* `gRPCIngress` is used for starting a gRPC server. Your driver class needs to inherit from it. 
:::
