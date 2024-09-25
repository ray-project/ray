import asyncio
import pickle
from typing import Optional, Tuple

import grpc

import ray
from ray import cloudpickle
from ray.anyscale.serve._private.replica_result import gRPCReplicaResult
from ray.serve._private.common import ReplicaQueueLengthInfo, RunningReplicaInfo
from ray.serve._private.replica_result import ReplicaResult
from ray.serve._private.replica_scheduler.common import PendingRequest
from ray.serve._private.replica_scheduler.replica_wrapper import ReplicaWrapper
from ray.serve.generated import serve_proprietary_pb2, serve_proprietary_pb2_grpc


class gRPCReplicaWrapper(ReplicaWrapper):
    def __init__(self, replica_info: RunningReplicaInfo):
        super().__init__(replica_info)

        assert (
            not replica_info.is_cross_language
        ), "gRPC requests not supported for Java."

        self._channel = grpc.aio.insecure_channel(
            f"{replica_info.node_ip}:{replica_info.port}"
        )
        self._stub = serve_proprietary_pb2_grpc.ASGIServiceStub(self._channel)
        self._loop = asyncio.get_running_loop()

    async def get_queue_len(self, *, deadline_s: float) -> int:
        # We can continue to use Ray remote calls to probe a replica's queue length
        obj_ref = self._actor_handle.get_num_ongoing_requests.remote()
        try:
            return await obj_ref
        except asyncio.CancelledError:
            ray.cancel(obj_ref)
            raise

    def _send_request_python(
        self, pr: PendingRequest, *, with_rejection: bool
    ) -> grpc.aio.Call:
        """Send the request to a Python replica."""

        asgi_request = serve_proprietary_pb2.ASGIRequest(
            pickled_request_metadata=pickle.dumps(pr.metadata),
            request_args=cloudpickle.dumps(pr.args),
            request_kwargs=cloudpickle.dumps(pr.kwargs),
        )
        if with_rejection:
            # Call a separate handler that may reject the request.
            # This handler is *always* a streaming call and the first message will
            # be a system message that accepts or rejects.
            return self._stub.HandleRequestWithRejection(asgi_request)
        elif pr.metadata.is_streaming:
            return self._stub.HandleRequestStreaming(asgi_request)
        else:
            return self._stub.HandleRequest(asgi_request)

    def send_request(self, pr: PendingRequest) -> ReplicaResult:
        return gRPCReplicaResult(
            self._send_request_python(pr, with_rejection=False),
            is_streaming=pr.metadata.is_streaming,
            loop=self._loop,
            on_separate_loop=True,
        )

    async def send_request_with_rejection(
        self, pr: PendingRequest
    ) -> Tuple[Optional[ReplicaResult], ReplicaQueueLengthInfo]:
        call = self._send_request_python(pr, with_rejection=True)
        try:
            first_msg = await call.__aiter__().__anext__()
            queue_len_info: ReplicaQueueLengthInfo = pickle.loads(
                first_msg.serialized_message
            )

            if not queue_len_info.accepted:
                return None, queue_len_info
            else:
                replica_result = gRPCReplicaResult(
                    call,
                    is_streaming=pr.metadata.is_streaming,
                    loop=self._loop,
                    on_separate_loop=True,
                )
                return replica_result, queue_len_info
        except asyncio.CancelledError as e:
            # HTTP client disconnected or request was explicitly canceled.
            call.cancel()
            raise e from None
