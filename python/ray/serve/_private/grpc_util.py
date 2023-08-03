import grpc
import pickle


class RayServeServiceStub(object):
    def __init__(self, channel, output_protobuf):
        """Constructor.

        Args:
            channel: A grpc.Channel.
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


def add_RayServeServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "Predict": grpc.unary_unary_rpc_method_handler(
            servicer.Predict,
        ),
        "PredictStreaming": grpc.unary_stream_rpc_method_handler(
            servicer.PredictStreaming,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "ray.serve.RayServeService", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))
