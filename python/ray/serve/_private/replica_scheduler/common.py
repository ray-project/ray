import asyncio
import logging
import pickle
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import ray
from ray.serve._private.common import RequestMetadata, RunningReplicaInfo
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.utils import JavaActorHandleProxy
from ray.serve.generated.serve_pb2 import RequestMetadata as RequestMetadataProto

logger = logging.getLogger(SERVE_LOGGER_NAME)


@dataclass
class Query:
    args: List[Any]
    kwargs: Dict[Any, Any]
    metadata: RequestMetadata


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

    async def get_queue_state(self, *, deadline_s: float) -> Tuple[int, bool]:
        """Returns tuple of (queue_len, accepted).

        `deadline_s` is passed to verify backoff for testing.
        """
        pass

    def send_query(
        self, query: Query
    ) -> Union[ray.ObjectRef, "ray._raylet.ObjectRefGenerator"]:
        """Send query to this replica."""
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

    async def get_queue_state(self, *, deadline_s: float) -> Tuple[int, bool]:
        # NOTE(edoakes): the `get_num_ongoing_requests` method name is shared by
        # the Python and Java replica implementations. If you change it, you need to
        # change both (or introduce a branch here).
        obj_ref = self._actor_handle.get_num_ongoing_requests.remote()
        try:
            queue_len = await obj_ref
            accepted = queue_len < self._replica_info.max_concurrent_queries
            return queue_len, accepted
        except asyncio.CancelledError:
            ray.cancel(obj_ref)
            raise

    def _send_query_java(self, query: Query) -> ray.ObjectRef:
        """Send the query to a Java replica.

        Does not currently support streaming.
        """
        if query.metadata.is_streaming:
            raise RuntimeError("Streaming not supported for Java.")

        if len(query.args) != 1:
            raise ValueError("Java handle calls only support a single argument.")

        return self._actor_handle.handle_request.remote(
            RequestMetadataProto(
                request_id=query.metadata.request_id,
                endpoint=query.metadata.endpoint,
                # Default call method in java is "call," not "__call__" like Python.
                call_method="call"
                if query.metadata.call_method == "__call__"
                else query.metadata.call_method,
            ).SerializeToString(),
            query.args,
        )

    def _send_query_python(
        self, query: Query
    ) -> Union[ray.ObjectRef, "ray._raylet.ObjectRefGenerator"]:
        """Send the query to a Python replica."""
        if query.metadata.is_streaming:
            method = self._actor_handle.handle_request_streaming.options(
                num_returns="streaming"
            )
        else:
            method = self._actor_handle.handle_request

        return method.remote(pickle.dumps(query.metadata), *query.args, **query.kwargs)

    def send_query(
        self, query: Query
    ) -> Union[ray.ObjectRef, "ray._raylet.ObjectRefGenerator"]:
        if self._replica_info.is_cross_language:
            return self._send_query_java(query)
        else:
            return self._send_query_python(query)


class ReplicaScheduler(ABC):
    """Abstract interface for a replica scheduler (how the router calls it)."""

    @abstractmethod
    async def assign_replica(
        self, query: Query
    ) -> Union[ray.ObjectRef, "ray._raylet.ObjectRefGenerator"]:
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
