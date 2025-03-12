import asyncio
import pickle
from typing import Tuple

import grpc

from ray import cloudpickle
from ray.anyscale.serve._private.constants import (
    ANYSCALE_RAY_SERVE_REPLICA_GRPC_MAX_MESSAGE_LENGTH,
)
from ray.anyscale.serve._private.replica_result import gRPCReplicaResult
from ray.exceptions import ActorUnavailableError
from ray.serve._private.common import ReplicaQueueLengthInfo, RunningReplicaInfo
from ray.serve._private.replica_scheduler.common import PendingRequest
from ray.serve._private.replica_scheduler.replica_wrapper import (
    ActorReplicaWrapper,
    ReplicaWrapper,
    RunningReplica,
)
from ray.serve.generated import serve_proprietary_pb2, serve_proprietary_pb2_grpc


class gRPCReplicaWrapper(ReplicaWrapper):
    def __init__(self, stub, actor_id):
        self._stub = stub
        self._actor_id = actor_id
        self._loop = asyncio.get_running_loop()

    def send_request_java(self, pr: PendingRequest):
        raise RuntimeError("gRPC requests not supported for Java.")

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

    async def _parse_initial_metadata(
        self, call: grpc.aio.Call
    ) -> ReplicaQueueLengthInfo:
        # NOTE(edoakes): this is required for gRPC to raise an AioRpcError if something
        # goes wrong establishing the connection (for example, a bug in our code).
        await call.wait_for_connection()
        metadata = await call.initial_metadata()

        accepted = metadata.get("accepted", None)
        num_ongoing_requests = metadata.get("num_ongoing_requests", None)
        if accepted is None or num_ongoing_requests is None:
            code = await call.code()
            details = await call.details()
            raise RuntimeError(f"Unexpected error ({code}): {details}.")

        return ReplicaQueueLengthInfo(
            accepted=bool(int(accepted)),
            num_ongoing_requests=int(num_ongoing_requests),
        )

    async def send_request_python(
        self, pr: PendingRequest, with_rejection: bool
    ) -> Tuple[grpc.aio.Call, ReplicaQueueLengthInfo]:
        call = self._send_request_python(pr, with_rejection=with_rejection)

        if not with_rejection:
            return (
                gRPCReplicaResult(call, pr.metadata, self._actor_id, loop=self._loop),
                None,
            )

        try:
            queue_len_info = await self._parse_initial_metadata(call)
            return (
                gRPCReplicaResult(call, pr.metadata, self._actor_id, loop=self._loop),
                queue_len_info,
            )
        except asyncio.CancelledError as e:
            # HTTP client disconnected or request was explicitly canceled.
            call.cancel()
            raise e from None
        except grpc.aio.AioRpcError as e:
            # If we received an `UNAVAILABLE` grpc error, that is
            # equivalent to `RayActorError`, although we don't know
            # whether it's `ActorDiedError` or `ActorUnavailableError`.
            # Conservatively, we assume it is `ActorUnavailableError`,
            # and we raise it here so that it goes through the unified
            # code path for handling RayActorErrors.
            # The router will retry scheduling the request with the
            # cache invalidated, at which point if the actor is actually
            # dead, the router will realize through active probing.
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise ActorUnavailableError(
                    "Actor is unavailable.",
                    self._actor_id.binary(),
                )

            raise e from None


class AnyscaleRunningReplica(RunningReplica):
    def __init__(self, replica_info: RunningReplicaInfo):
        super().__init__(replica_info)

        # Lazily created
        self._channel = None
        self._stub = None

        # Replica wrappers
        self._actor_replica_wrapper = ActorReplicaWrapper(self._actor_handle)
        self._grpc_replica_wrapper = None

    @property
    def stub(self) -> bool:
        if self._stub is None:
            self._channel = grpc.aio.insecure_channel(
                f"{self._replica_info.node_ip}:{self._replica_info.port}",
                options=[
                    (
                        "grpc.max_receive_message_length",
                        ANYSCALE_RAY_SERVE_REPLICA_GRPC_MAX_MESSAGE_LENGTH,
                    )
                ],
            )
            self._stub = serve_proprietary_pb2_grpc.ASGIServiceStub(self._channel)

        return self._stub

    def _get_replica_wrapper(self, pr: PendingRequest) -> ReplicaWrapper:
        if self._grpc_replica_wrapper is None:
            self._grpc_replica_wrapper = gRPCReplicaWrapper(
                self.stub, self._actor_handle._actor_id
            )

        return (
            self._actor_replica_wrapper
            if pr.metadata._by_reference
            else self._grpc_replica_wrapper
        )
