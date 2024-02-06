import asyncio
import logging
import pickle
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Union

import ray
from ray.serve._private.common import RequestMetadata, RunningReplicaInfo
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.utils import JavaActorHandleProxy
from ray.serve.generated.serve_pb2 import RequestMetadata as RequestMetadataProto

logger = logging.getLogger(SERVE_LOGGER_NAME)


@dataclass(frozen=True)
class PendingRequest:
    args: List[Any]
    kwargs: Dict[Any, Any]
    metadata: RequestMetadata
    created_at: float = field(default_factory=time.time)
    future: asyncio.Future = field(default_factory=lambda: asyncio.Future())

    def __eq__(self, other) -> bool:
        """Request ID is expected to be unique."""
        return self.metadata.request_id == other.metadata.request_id


class ReplicaWrapper(ABC):
    """Defines the interface for a scheduler to talk to a replica.

    This is used to abstract away details of Ray actor calls for testing.
    """

    @property
    def replica_id(self) -> str:
        """Replica ID of this replica."""
        pass

    @property
    def multiplexed_model_ids(self) -> Set[str]:
        """Set of model IDs on this replica."""
        pass

    @property
    def max_concurrent_requests(self) -> int:
        """Max concurrent requests that can be sent to this replica."""
        pass

    async def get_queue_len(self, *, deadline_s: float) -> int:
        """Returns current queue len for the replica.

        `deadline_s` is passed to verify backoff for testing.
        """
        pass

    def send_request(
        self, pr: PendingRequest
    ) -> Union[ray.ObjectRef, "ray._raylet.ObjectRefGenerator"]:
        """Send request to this replica."""
        pass


class ActorReplicaWrapper:
    def __init__(self, replica_info: RunningReplicaInfo):
        self._replica_info = replica_info
        self._multiplexed_model_ids = set(replica_info.multiplexed_model_ids)

        if replica_info.is_cross_language:
            self._actor_handle = JavaActorHandleProxy(replica_info.actor_handle)
        else:
            self._actor_handle = replica_info.actor_handle

    @property
    def replica_id(self) -> str:
        return self._replica_info.replica_tag

    @property
    def node_id(self) -> str:
        return self._replica_info.node_id

    @property
    def availability_zone(self) -> Optional[str]:
        return self._replica_info.availability_zone

    @property
    def multiplexed_model_ids(self) -> Set[str]:
        return self._multiplexed_model_ids

    @property
    def max_concurrent_requests(self) -> int:
        return self._replica_info.max_concurrent_queries

    @property
    def is_cross_language(self) -> bool:
        return self._replica_info.is_cross_language

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

    def _send_request_java(self, pr: PendingRequest) -> ray.ObjectRef:
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
                endpoint=pr.metadata.endpoint,
                # Default call method in java is "call," not "__call__" like Python.
                call_method="call"
                if pr.metadata.call_method == "__call__"
                else pr.metadata.call_method,
            ).SerializeToString(),
            pr.args,
        )

    def _send_request_python(
        self, pr: PendingRequest, *, with_rejection: bool
    ) -> Union[ray.ObjectRef, "ray._raylet.ObjectRefGenerator"]:
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

    def send_request(
        self, pr: PendingRequest, *, with_rejection: bool = False
    ) -> Union[ray.ObjectRef, "ray._raylet.ObjectRefGenerator"]:
        if self._replica_info.is_cross_language:
            return self._send_request_java(pr)
        else:
            return self._send_request_python(pr, with_rejection=with_rejection)


class ReplicaScheduler(ABC):
    """Abstract interface for a replica scheduler (how the router calls it)."""

    @abstractmethod
    async def choose_replica_for_request(
        self, pending_request: PendingRequest, *, is_retry: bool = False
    ) -> ReplicaWrapper:
        pass

    @abstractmethod
    def update_replicas(self, replicas: List[ActorReplicaWrapper]):
        pass

    def update_running_replicas(self, running_replicas: List[RunningReplicaInfo]):
        """Compatibility shim for RunningReplicaInfo datatype."""
        return self.update_replicas([ActorReplicaWrapper(r) for r in running_replicas])

    @property
    @abstractmethod
    def curr_replicas(self) -> Dict[str, ReplicaWrapper]:
        pass
