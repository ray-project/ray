# Direct Ingress

In the 2.1, Serve provides alpha version gRPC ingress. In this section, you will learn how to 

* use Serve internal gRPC schema to send traffic
* bring your own gRPC schema into serve application

## Use Serve Schema

Internally, serve provides a simple gRPC schema.
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

Code example:

Server
```{literalinclude} ../serve/doc_code/direct_ingress.py
:start-after: __begin_server__
:end-before: __end_server__
:language: python
```

Client
```{literalinclude} ../serve/doc_code/direct_ingress.py
:start-after: __begin_client__
:end-before: __end_client__
:language: python
```

:::{note}
* `input` is a dictionary of `map<string, bytes> ` following the schema described
*  User input data needs to be serialized to bytes type and feed into the `input`.
* Response will be under bytes type, which means user code is responsible for to serialize the output into bytes. 
* By default, the gRPC port is 9000.
:::

### Client Schema code generation
There are lots of ways to generate client schema code. Here is a simple step to generate the code.
* Insintall the gRPC code generation tools
```
pip install grpcio-tools
```

* Generate gRPC code based on the schema
```
python -m grpc_tools.protoc --proto_path=src/ray/protobuf/ --python_out=. --grpc_python_out=. src/ray/protobuf/serve.proto
```
After the two steps above, you should have `serve_pb2.py` and `serve_pb2_grpc.py` files generated.(The steps that show above work for all the schemas files generations.)

## Bring your own Schema

If you have customized schema to use, serve also support it!

Assume you have the schema and generated the corresponding code,

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

Server:
```{literalinclude} ../serve/doc_code/direct_ingress_with_customized_schema.py
:start-after: __begin_server__
:end-before: __end_server__
:language: python
```

Client
```{literalinclude} ../serve/doc_code/direct_ingress_with_customized_schema.py
:start-after: __begin_client__
:end-before: __end_client__
:language: python
```

:::{note}
* To use your own schema, you need to write your driver class `MyDriver` to deploy.
*  `is_driver_deployment` is needed to mark the class as driver, serve will make sure the driver class deployment gets deployed one replica per node.
* `gRPCIngress` is used for starting a gRPC server. Your driver class needs to inherit from it. 
:::
