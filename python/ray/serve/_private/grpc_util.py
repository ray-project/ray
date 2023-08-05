import grpc
import pickle
from typing import Any, Sequence
from grpc.aio._server import Server


class RayServeServiceStub(object):
    def __init__(self, channel: "grpc._channel.Channel", output_protobuf: Any):
        """Constructor for the Ray Serve gRPC service stub.

        Note that `request_serializer` is using `pickle.dumps()` to serialize to
        preserve the object typing in the gPRC server. `response_deserializer` is
        using the `FromString()` on the `output_protobuf` to allow the client to
        deserialize the custom defined protobuf returning object.

        Args:
            channel: A grpc.Channel.
            output_protobuf: The protobuf class of the output.
        """
        self.Predict = channel.unary_unary(
            "/ray.serve.RayServeService/Predict",
            request_serializer=pickle.dumps,
            response_deserializer=getattr(output_protobuf, "FromString"),
        )
        self.PredictStreaming = channel.unary_stream(
            "/ray.serve.RayServeService/PredictStreaming",
            request_serializer=pickle.dumps,
            response_deserializer=getattr(output_protobuf, "FromString"),
        )


def add_RayServeServiceServicer_to_server(
    servicer: Any, server: "grpc.aio._server.Server"
):
    """Adds a RayServeServiceServicer to a gRPC Server.

    This helper function will be called by the proxy actor. The servicer will be a
    gRPCProxy instance. It will be added to the running gRPC server started by the
    proxy actor.

    Note that both `request_deserializer` and `response_serializer` are
    explicitly set to `None` to allow the servicer to take in the raw protobuf bytes
    and to return the raw protobuf bytes.
    """
    rpc_method_handlers = {
        "Predict": grpc.unary_unary_rpc_method_handler(
            servicer.Predict,
            request_deserializer=None,
            response_serializer=None,
        ),
        "PredictStreaming": grpc.unary_stream_rpc_method_handler(
            servicer.PredictStreaming,
            request_deserializer=None,
            response_serializer=None,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "ray.serve.RayServeService", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


class gRPCServer(Server):
    def __init__(self, unary_unary, unary_stream, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.unary_unary = unary_unary
        self.unary_stream = unary_stream

    def add_generic_rpc_handlers(
        self, generic_rpc_handlers: Sequence[grpc.GenericRpcHandler]
    ):
        # TODO (genesu): this should only be called before server started. Need to
        #  restart the server if need to be called again
        serve_rpc_handlers = {}
        for service_method, method_handler in generic_rpc_handlers[
            0
        ]._method_handlers.items():
            serve_method_handler = method_handler._replace(
                response_serializer=None,
                unary_unary=self.unary_unary,
                unary_stream=self.unary_stream,
            )
            serve_rpc_handlers[service_method] = serve_method_handler
        generic_rpc_handlers[0]._method_handlers = serve_rpc_handlers
        print("in add_generic_rpc_handlers", serve_rpc_handlers)
        super().add_generic_rpc_handlers(generic_rpc_handlers)


def serve_server(unary_unary, unary_stream):
    return gRPCServer(
        thread_pool=None,
        generic_handlers=(),
        interceptors=(),
        options=(),
        maximum_concurrent_rpcs=None,
        compression=None,
        unary_unary=unary_unary,
        unary_stream=unary_stream,
    )


class DummyServicer:
    def __getattr__(self, attr):
        pass
