# Direct Ingress

In the 2.1, Serve provides an alpha version of gRPC ingress. In this section, you will learn how to

* use Serve's internal gRPC schema to send traffic
* bring your own gRPC schema into your Serve application

## Use Serve's Schema

Internally, Serve provides a simple gRPC schema.
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

Server:
```{literalinclude} ../serve/doc_code/direct_ingress.py
:start-after: __begin_server__
:end-before: __end_server__
:language: python
```

Client:
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
:::

### Client Schema code generation
There are lots of ways to generate client schema code. Here is a simple step to generate the code.
* Install the gRPC code generation tools
```
pip install grpcio-tools
```

* Generate gRPC code based on the schema
```
python -m grpc_tools.protoc --proto_path=src/ray/protobuf/ --python_out=. --grpc_python_out=. src/ray/protobuf/serve.proto
```
After the two steps above, you should have `serve_pb2.py` and `serve_pb2_grpc.py` files generated.(The steps shown above work for generation for any schema file, including "bring your own schema" described below.)

## Bring your own Schema

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

Server:
```{literalinclude} ../serve/doc_code/direct_ingress_with_customized_schema.py
:start-after: __begin_server__
:end-before: __end_server__
:language: python
```

Client:
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
