import asyncio
import logging
import pickle
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import ray
from ray import ObjectRef, ObjectRefGenerator
from ray.serve._private.common import (
    ReplicaID,
    ReplicaQueueLengthInfo,
    RequestMetadata,
    RunningReplicaInfo,
)
from ray.serve._private.constants import (
    RAY_SERVE_QUEUE_LENGTH_CACHE_TIMEOUT_S,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.utils import JavaActorHandleProxy
from ray.serve.generated.serve_pb2 import RequestMetadata as RequestMetadataProto

logger = logging.getLogger(SERVE_LOGGER_NAME)


@dataclass
class PendingRequest:
    args: List[Any]
    kwargs: Dict[Any, Any]
    metadata: RequestMetadata
    created_at: float = field(default_factory=time.time)
    future: asyncio.Future = field(default_factory=lambda: asyncio.Future())

    def reset_future(self):
        """Reset the `asyncio.Future`, must be called if this request is re-used."""
        self.future = asyncio.Future()


class ReplicaWrapper(ABC):
    """Defines the interface for a scheduler to talk to a replica.

    This is used to abstract away details of Ray actor calls for testing.
    """

    @property
    def replica_id(self) -> ReplicaID:
        """ID of this replica."""
        pass

    @property
    def multiplexed_model_ids(self) -> Set[str]:
        """Set of model IDs on this replica."""
        pass

    @property
    def max_ongoing_requests(self) -> int:
        """Max concurrent requests that can be sent to this replica."""
        pass

    async def get_queue_len(self, *, deadline_s: float) -> int:
        """Returns current queue len for the replica.

        `deadline_s` is passed to verify backoff for testing.
        """
        pass

    def send_request(self, pr: PendingRequest) -> Union[ObjectRef, ObjectRefGenerator]:
        """Send request to this replica."""
        pass

    async def send_request_with_rejection(
        self,
        pr: PendingRequest,
    ) -> Tuple[Optional[ObjectRefGenerator], ReplicaQueueLengthInfo]:
        """Send request to this replica.

        The replica will yield a system message (ReplicaQueueLengthInfo) before
        executing the actual request. This can cause it to reject the request.

        The result will *always* be a generator, so for non-streaming requests it's up
        to the caller to resolve it to its first (and only) ObjectRef.

        Only supported for Python replicas.
        """
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
    def replica_id(self) -> ReplicaID:
        return self._replica_info.replica_id

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
    def max_ongoing_requests(self) -> int:
        return self._replica_info.max_ongoing_requests

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

    def send_request(self, pr: PendingRequest) -> Union[ObjectRef, ObjectRefGenerator]:
        if self._replica_info.is_cross_language:
            return self._send_request_java(pr)
        else:
            return self._send_request_python(pr, with_rejection=False)

    async def send_request_with_rejection(
        self,
        pr: PendingRequest,
    ) -> Tuple[Optional[ObjectRefGenerator], ReplicaQueueLengthInfo]:
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
                return obj_ref_gen, queue_len_info
        except asyncio.CancelledError as e:
            ray.cancel(obj_ref_gen)
            raise e from None


@dataclass(frozen=True)
class ReplicaQueueLengthCacheEntry:
    queue_len: int
    timestamp: float


class ReplicaQueueLengthCache:
    def __init__(
        self,
        *,
        staleness_timeout_s: float = RAY_SERVE_QUEUE_LENGTH_CACHE_TIMEOUT_S,
        get_curr_time_s: Optional[Callable[[], float]] = None,
    ):
        self._cache: Dict[ReplicaID, ReplicaQueueLengthCacheEntry] = {}
        self._staleness_timeout_s = staleness_timeout_s
        self._get_curr_time_s = (
            get_curr_time_s if get_curr_time_s is not None else time.time
        )

    def _is_timed_out(self, timestamp_s: int) -> bool:
        return self._get_curr_time_s() - timestamp_s > self._staleness_timeout_s

    def get(self, replica_id: ReplicaID) -> Optional[int]:
        """Get the queue length for a replica.

        Returns `None` if the replica ID is not present or the entry is timed out.
        """
        entry = self._cache.get(replica_id)
        if entry is None or self._is_timed_out(entry.timestamp):
            return None

        return entry.queue_len

    def update(self, replica_id: ReplicaID, queue_len: int):
        """Set (or update) the queue length for a replica ID."""
        self._cache[replica_id] = ReplicaQueueLengthCacheEntry(
            queue_len, self._get_curr_time_s()
        )

    def remove_inactive_replicas(self, *, active_replica_ids: Set[ReplicaID]):
        """Removes entries for all replica IDs not in the provided active set."""
        # NOTE: the size of the cache dictionary changes during this loop.
        for replica_id in list(self._cache.keys()):
            if replica_id not in active_replica_ids:
                self._cache.pop(replica_id)


class ReplicaScheduler(ABC):
    """Abstract interface for a replica scheduler (how the router calls it)."""

    @abstractmethod
    async def choose_replica_for_request(
        self, pending_request: PendingRequest, *, is_retry: bool = False
    ) -> ReplicaWrapper:
        pass

    @abstractmethod
    def update_replicas(self, replicas: List[ReplicaWrapper]):
        pass

    def update_running_replicas(self, running_replicas: List[RunningReplicaInfo]):
        """Compatibility shim for RunningReplicaInfo datatype."""
        return self.update_replicas([ActorReplicaWrapper(r) for r in running_replicas])

    @property
    @abstractmethod
    def replica_queue_len_cache(self) -> ReplicaQueueLengthCache:
        pass

    @property
    @abstractmethod
    def curr_replicas(self) -> Dict[str, ReplicaWrapper]:
        pass
