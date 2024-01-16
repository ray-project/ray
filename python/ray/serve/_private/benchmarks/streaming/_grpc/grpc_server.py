import grpc

from ray.serve._private.benchmarks.streaming._grpc import (
    test_server_pb2,
    test_server_pb2_grpc,
)


async def _async_list(async_iterator):
    items = []
    async for item in async_iterator:
        items.append(item)

    return items


class TestGRPCServer(test_server_pb2_grpc.GRPCTestServerServicer):
    def __init__(self, tokens_per_request):
        self._tokens_per_request = tokens_per_request

    async def Unary(self, request, context):
        if request.request_data == "error":
            await context.abort(
                code=grpc.StatusCode.INTERNAL,
                details="unary rpc error",
            )

        return test_server_pb2.Response(response_data="OK")

    async def ClientStreaming(self, request_iterator, context):
        data = await _async_list(request_iterator)

        if data and data[0].request_data == "error":
            await context.abort(
                code=grpc.StatusCode.INTERNAL,
                details="client streaming rpc error",
            )

        return test_server_pb2.Response(response_data="OK")

    async def ServerStreaming(self, request, context):
        if request.request_data == "error":
            await context.abort(
                code=grpc.StatusCode.INTERNAL,
                details="OK",
            )

        for i in range(self._tokens_per_request):
            yield test_server_pb2.Response(response_data="OK")

    async def BidiStreaming(self, request_iterator, context):
        data = await _async_list(request_iterator)
        if data and data[0].request_data == "error":
            await context.abort(
                code=grpc.StatusCode.INTERNAL,
                details="bidi-streaming rpc error",
            )

        for i in range(self._tokens_per_request):
            yield test_server_pb2.Response(response_data="OK")
