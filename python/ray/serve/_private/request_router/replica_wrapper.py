import asyncio
import pickle
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Set

import ray
from ray.actor import ActorHandle
from ray.serve._private.common import (
    ReplicaID,
    RunningReplicaInfo,
)
from ray.serve._private.replica_result import ActorReplicaResult, ReplicaResult
from ray.serve._private.request_router.common import PendingRequest
from ray.serve._private.utils import JavaActorHandleProxy
from ray.serve.generated.serve_pb2 import RequestMetadata as RequestMetadataProto
from ray.util.annotations import PublicAPI


class ReplicaWrapper(ABC):
    """This is used to abstract away details of the transport layer
    when communicating with the replica.
    """

    @abstractmethod
    def send_request_java(self, pr: PendingRequest) -> ReplicaResult:
        """Send request to Java replica."""
        pass

    @abstractmethod
    def send_request_python(
        self, pr: PendingRequest, *, with_rejection: bool
    ) -> ReplicaResult:
        """Send request to Python replica.

        If sending request with rejection, the replica will yield a
        system message (ReplicaQueueLengthInfo) before executing the
        actual request. This can cause it to reject the request. The
        result will *always* be a generator, so for non-streaming
        requests it's up to the caller to resolve it to its first (and
        only) ObjectRef.
        """
        pass


class ActorReplicaWrapper(ReplicaWrapper):
    def __init__(self, actor_handle):
        self._actor_handle = actor_handle

    def send_request_java(self, pr: PendingRequest) -> ActorReplicaResult:
        """Send the request to a Java replica.
        Does not currently support streaming.
        """
        if pr.metadata.is_streaming:
            raise RuntimeError("Streaming not supported for Java.")

        if len(pr.args) != 1:
            raise ValueError("Java handle calls only support a single argument.")

        return ActorReplicaResult(
            self._actor_handle.handle_request.remote(
                RequestMetadataProto(
                    request_id=pr.metadata.request_id,
                    # Default call method in java is "call," not "__call__" like Python.
                    call_method="call"
                    if pr.metadata.call_method == "__call__"
                    else pr.metadata.call_method,
                ).SerializeToString(),
                pr.args,
            ),
            pr.metadata,
        )

    def send_request_python(
        self, pr: PendingRequest, *, with_rejection: bool
    ) -> ActorReplicaResult:
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

        obj_ref_gen = method.remote(pickle.dumps(pr.metadata), *pr.args, **pr.kwargs)
        return ActorReplicaResult(
            obj_ref_gen, pr.metadata, with_rejection=with_rejection
        )


@PublicAPI(stability="alpha")
class RunningReplica:
    """Contains info on a running replica.
    Also defines the interface for a request router to talk to a replica.
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
    def actor_id(self) -> ray.ActorID:
        """Actor ID of this replica."""
        return self._actor_handle._actor_id

    @property
    def node_id(self) -> str:
        """Node ID of the node this replica is running on."""
        return self._replica_info.node_id

    @property
    def availability_zone(self) -> Optional[str]:
        """Availability zone of the node this replica is running on."""
        return self._replica_info.availability_zone

    @property
    def multiplexed_model_ids(self) -> Set[str]:
        """Set of model IDs on this replica."""
        return self._multiplexed_model_ids

    @property
    def routing_stats(self) -> Dict[str, Any]:
        """Dictionary of routing stats."""
        return self._replica_info.routing_stats

    @property
    def max_ongoing_requests(self) -> int:
        """Max concurrent requests that can be sent to this replica."""
        return self._replica_info.max_ongoing_requests

    @property
    def is_cross_language(self) -> bool:
        """Whether this replica is cross-language (Java)."""
        return self._replica_info.is_cross_language

    def _get_replica_wrapper(self, pr: PendingRequest) -> ReplicaWrapper:
        return ActorReplicaWrapper(self._actor_handle)

    def push_proxy_handle(self, handle: ActorHandle):
        """When on proxy, push proxy's self handle to replica"""
        self._actor_handle.push_proxy_handle.remote(handle)

    async def get_queue_len(self, *, deadline_s: float) -> int:
        """Returns current queue len for the replica.
        `deadline_s` is passed to verify backoff for testing.
        """
        # NOTE(edoakes): the `get_num_ongoing_requests` method name is shared by
        # the Python and Java replica implementations. If you change it, you need to
        # change both (or introduce a branch here).
        obj_ref = self._actor_handle.get_num_ongoing_requests.remote()
        try:
            return await obj_ref
        except asyncio.CancelledError:
            ray.cancel(obj_ref)
            raise

    def try_send_request(
        self, pr: PendingRequest, with_rejection: bool
    ) -> ReplicaResult:
        """Try to send the request to this replica. It may be rejected."""
        wrapper = self._get_replica_wrapper(pr)
        if self._replica_info.is_cross_language:
            assert not with_rejection, "Request rejection not supported for Java."
            return wrapper.send_request_java(pr)

        return wrapper.send_request_python(pr, with_rejection=with_rejection)
