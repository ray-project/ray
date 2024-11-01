import asyncio
import pickle
from abc import ABC, abstractmethod
from typing import Optional, Set, Tuple, Union

import ray
from ray import ObjectRef, ObjectRefGenerator
from ray.actor import ActorHandle
from ray.serve._private.common import (
    ReplicaID,
    ReplicaQueueLengthInfo,
    RunningReplicaInfo,
)
from ray.serve._private.replica_result import ActorReplicaResult, ReplicaResult
from ray.serve._private.replica_scheduler.common import PendingRequest
from ray.serve._private.utils import JavaActorHandleProxy
from ray.serve.generated.serve_pb2 import RequestMetadata as RequestMetadataProto


class ReplicaWrapper(ABC):
    """Defines the interface for a scheduler to talk to a replica.

    This is used to abstract away details of Ray actor calls for testing.
    """

    def __init__(self, replica_info: RunningReplicaInfo):
        self._replica_info = replica_info
        self._multiplexed_model_ids = set(replica_info.multiplexed_model_ids)

        if replica_info.is_cross_language:
            self._actor_handle = JavaActorHandleProxy(replica_info.actor_handle)
        else:
            self._actor_handle = replica_info.actor_handle

    @property
    def replica_id(self) -> ReplicaID:
        """ID of this replica."""
        return self._replica_info.replica_id

    @property
    def node_id(self) -> str:
        return self._replica_info.node_id

    @property
    def availability_zone(self) -> Optional[str]:
        return self._replica_info.availability_zone

    @property
    def multiplexed_model_ids(self) -> Set[str]:
        """Set of model IDs on this replica."""
        return self._multiplexed_model_ids

    @property
    def max_ongoing_requests(self) -> int:
        """Max concurrent requests that can be sent to this replica."""
        return self._replica_info.max_ongoing_requests

    @property
    def is_cross_language(self) -> bool:
        return self._replica_info.is_cross_language

    def push_proxy_handle(self, handle: ActorHandle):
        """When on proxy, push proxy's self handle to replica"""
        self._actor_handle.push_proxy_handle.remote(handle)

    @abstractmethod
    async def get_queue_len(self, *, deadline_s: float) -> int:
        """Returns current queue len for the replica.

        `deadline_s` is passed to verify backoff for testing.
        """
        raise NotImplementedError

    @abstractmethod
    def send_request(self, pr: PendingRequest) -> ReplicaResult:
        """Send request to this replica."""
        raise NotImplementedError

    @abstractmethod
    async def send_request_with_rejection(
        self, pr: PendingRequest
    ) -> Tuple[Optional[ReplicaResult], ReplicaQueueLengthInfo]:
        """Send request to this replica.

        The replica will yield a system message (ReplicaQueueLengthInfo) before
        executing the actual request. This can cause it to reject the request.

        The result will *always* be a generator, so for non-streaming requests it's up
        to the caller to resolve it to its first (and only) ObjectRef.

        Only supported for Python replicas.
        """
        raise NotImplementedError


class ActorReplicaWrapper(ReplicaWrapper):
    async def get_queue_len(self, *, deadline_s: float) -> int:
        # NOTE(edoakes): the `get_num_ongoing_requests` method name is shared by
        # the Python and Java replica implementations. If you change it, you need to
        # change both (or introduce a branch here).
        obj_ref = self._actor_handle.get_num_ongoing_requests.remote()
        try:
            return await obj_ref
        except asyncio.CancelledError:
            ray.cancel(obj_ref)
            raise

    def _send_request_java(self, pr: PendingRequest) -> ObjectRef:
        """Send the request to a Java replica.

        Does not currently support streaming.
        """
        if pr.metadata.is_streaming:
            raise RuntimeError("Streaming not supported for Java.")

        if len(pr.args) != 1:
            raise ValueError("Java handle calls only support a single argument.")

        return self._actor_handle.handle_request.remote(
            RequestMetadataProto(
                request_id=pr.metadata.request_id,
                # Default call method in java is "call," not "__call__" like Python.
                call_method="call"
                if pr.metadata.call_method == "__call__"
                else pr.metadata.call_method,
            ).SerializeToString(),
            pr.args,
        )

    def _send_request_python(
        self, pr: PendingRequest, *, with_rejection: bool
    ) -> Union[ray.ObjectRef, ObjectRefGenerator]:
        """Send the request to a Python replica."""
        if with_rejection:
            # Call a separate handler that may reject the request.
            # This handler is *always* a streaming call and the first message will
            # be a system message that accepts or rejects.
            method = self._actor_handle.handle_request_with_rejection.options(
                num_returns="streaming"
            )
        elif pr.metadata.is_streaming:
            method = self._actor_handle.handle_request_streaming.options(
                num_returns="streaming"
            )
        else:
            method = self._actor_handle.handle_request

        return method.remote(pickle.dumps(pr.metadata), *pr.args, **pr.kwargs)

    def send_request(self, pr: PendingRequest) -> ReplicaResult:
        if self._replica_info.is_cross_language:
            return ActorReplicaResult(
                self._send_request_java(pr),
                is_streaming=pr.metadata.is_streaming,
                request_id=pr.metadata.request_id,
            )
        else:
            return ActorReplicaResult(
                self._send_request_python(pr, with_rejection=False),
                is_streaming=pr.metadata.is_streaming,
                request_id=pr.metadata.request_id,
            )

    async def send_request_with_rejection(
        self, pr: PendingRequest
    ) -> Tuple[Optional[ReplicaResult], ReplicaQueueLengthInfo]:
        assert (
            not self._replica_info.is_cross_language
        ), "Request rejection not supported for Java."

        obj_ref_gen = self._send_request_python(pr, with_rejection=True)
        try:
            first_ref = await obj_ref_gen.__anext__()
            queue_len_info: ReplicaQueueLengthInfo = pickle.loads(await first_ref)

            if not queue_len_info.accepted:
                return None, queue_len_info
            else:
                return (
                    ActorReplicaResult(
                        obj_ref_gen,
                        is_streaming=pr.metadata.is_streaming,
                        request_id=pr.metadata.request_id,
                    ),
                    queue_len_info,
                )
        except asyncio.CancelledError as e:
            # HTTP client disconnected or request was explicitly canceled.
            ray.cancel(obj_ref_gen)
            raise e from None
