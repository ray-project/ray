from abc import ABC
import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
import enum
import itertools
import logging
import math
import pickle
import random
import time
from typing import (
    Any,
    AsyncGenerator,
    DefaultDict,
    Deque,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)
import warnings

from starlette.requests import Request

import ray
from ray.actor import ActorHandle
from ray.dag.py_obj_scanner import _PyObjScanner
from ray.exceptions import RayActorError, RayTaskError
from ray.util import metrics
from ray._private.utils import make_asyncio_event_version_compat, load_class

from ray.serve._private.common import (
    DeploymentID,
    DeploymentInfo,
    RequestProtocol,
    RunningReplicaInfo,
)
from ray.serve._private.constants import (
    SERVE_LOGGER_NAME,
    HANDLE_METRIC_PUSH_INTERVAL_S,
    RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S,
    RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING,
)
from ray.serve._private.http_util import make_buffered_asgi_receive
from ray.serve._private.long_poll import LongPollClient, LongPollNamespace
from ray.serve._private.utils import (
    compute_iterable_delta,
    JavaActorHandleProxy,
    MetricsPusher,
)
from ray.serve.generated.serve_pb2 import (
    DeploymentRoute,
    RequestMetadata as RequestMetadataProto,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)

# Used to only print a single warning when users pass starlette requests via handle.
WARNED_ABOUT_STARLETTE_REQUESTS_ONCE = False


@dataclass
class RequestMetadata:
    request_id: str
    endpoint: str
    call_method: str = "__call__"

    # HTTP route path of the request.
    route: str = ""

    # Application name.
    app_name: str = ""

    # Multiplexed model ID.
    multiplexed_model_id: str = ""

    # If this request expects a streaming response.
    is_streaming: bool = False

    # The protocol to serve this request
    _request_protocol: RequestProtocol = RequestProtocol.UNDEFINED

    @property
    def is_http_request(self) -> bool:
        return self._request_protocol == RequestProtocol.HTTP

    @property
    def is_grpc_request(self) -> bool:
        return self._request_protocol == RequestProtocol.GRPC


@dataclass
class Query:
    args: List[Any]
    kwargs: Dict[Any, Any]
    metadata: RequestMetadata

    async def replace_known_types_in_args(self):
        """Uses the `_PyObjScanner` to find and replace known types.

        1) Replaces `asyncio.Task` objects with their results. This is used for the old
           serve handle API and should be removed once that API is deprecated & removed.
        2) Replaces `DeploymentResponse` objects with their resolved object refs. This
           enables composition without explicitly calling `_to_object_ref`.
        3) Buffers the bodies of `starlette.requests.Request` objects to avoid them
           being unserializable. This is a temporary compatibility measure and passing
           the objects should be fully disallowed in a future release.
        """
        from ray.serve.handle import (
            _DeploymentResponseBase,
            DeploymentResponse,
            DeploymentResponseGenerator,
        )

        scanner = _PyObjScanner(
            source_type=(asyncio.Task, _DeploymentResponseBase, Request)
        )

        try:
            tasks = []
            responses = []
            replacement_table = {}
            objs = scanner.find_nodes((self.args, self.kwargs))
            for obj in objs:
                if isinstance(obj, asyncio.Task):
                    tasks.append(obj)
                elif isinstance(obj, DeploymentResponseGenerator):
                    raise RuntimeError(
                        "Streaming deployment handle results cannot be passed to "
                        "downstream handle calls. If you have a use case requiring "
                        "this feature, please file a feature request on GitHub."
                    )
                elif isinstance(obj, DeploymentResponse):
                    responses.append(obj)
                elif isinstance(obj, Request):
                    global WARNED_ABOUT_STARLETTE_REQUESTS_ONCE
                    if not WARNED_ABOUT_STARLETTE_REQUESTS_ONCE:
                        # TODO(edoakes): fully disallow this in the future.
                        warnings.warn(
                            "`starlette.Request` objects should not be directly passed "
                            "via `ServeHandle` calls. Not all functionality is "
                            "guaranteed to work (e.g., detecting disconnects) and this "
                            "may be disallowed in a future release."
                        )
                        WARNED_ABOUT_STARLETTE_REQUESTS_ONCE = True

                    async def empty_send():
                        pass

                    obj._send = empty_send
                    obj._receive = make_buffered_asgi_receive(await obj.body())
                    replacement_table[obj] = obj

            # Gather `asyncio.Task` results concurrently.
            if len(tasks) > 0:
                resolved = await asyncio.gather(*tasks)
                replacement_table.update(zip(tasks, resolved))

            # Gather `DeploymentResponse` object refs concurrently.
            if len(responses) > 0:
                obj_refs = await asyncio.gather(
                    *[r._to_object_ref() for r in responses]
                )
                replacement_table.update((zip(responses, obj_refs)))

            self.args, self.kwargs = scanner.replace_nodes(replacement_table)
        finally:
            # Make the scanner GC-able to avoid memory leaks.
            scanner.clear()


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

    async def get_queue_state(self) -> Tuple[int, bool]:
        """Returns tuple of (queue_len, accepted)."""
        pass

    def send_query(
        self, query: Query
    ) -> Union[ray.ObjectRef, "ray._raylet.StreamingObjectRefGenerator"]:
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

    async def get_queue_state(self) -> Tuple[int, bool]:
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

        # Java only supports a single argument.
        arg = query.args[0]

        # Convert HTTP requests to Java-accepted format (single string).
        if query.metadata.is_http_request:
            assert isinstance(arg, bytes)
            loaded_http_input = pickle.loads(arg)
            query_string = loaded_http_input.scope.get("query_string")
            if query_string:
                arg = query_string.decode().split("=", 1)[1]
            elif loaded_http_input.body:
                arg = loaded_http_input.body.decode()

        # Default call method in java is "call," not "__call__" like Python.
        call_method = query.metadata.call_method
        if call_method == "__call__":
            call_method = "call"

        return self._actor_handle.handle_request.remote(
            RequestMetadataProto(
                request_id=query.metadata.request_id,
                endpoint=query.metadata.endpoint,
                call_method=call_method,
            ).SerializeToString(),
            [arg],
        )

    def _send_query_python(
        self, query: Query
    ) -> Union[ray.ObjectRef, "ray._raylet.StreamingObjectRefGenerator"]:
        """Send the query to a Python replica."""
        if query.metadata.is_streaming:
            obj_ref = self._actor_handle.handle_request_streaming.options(
                num_returns="streaming"
            ).remote(pickle.dumps(query.metadata), *query.args, **query.kwargs)
        else:
            _, obj_ref = self._actor_handle.handle_request.remote(
                pickle.dumps(query.metadata), *query.args, **query.kwargs
            )

        return obj_ref

    def send_query(
        self, query: Query
    ) -> Union[ray.ObjectRef, "ray._raylet.StreamingObjectRefGenerator"]:
        if self._replica_info.is_cross_language:
            return self._send_query_java(query)
        else:
            return self._send_query_python(query)


class ReplicaScheduler(ABC):
    """Abstract interface for a replica scheduler (how the router calls it)."""

    async def assign_replica(
        self, query: Query
    ) -> Union[ray.ObjectRef, "ray._raylet.StreamingObjectRefGenerator"]:
        pass

    def update_running_replicas(self, running_replicas: List[RunningReplicaInfo]):
        pass

    @property
    def curr_replicas(self) -> Dict[str, ReplicaWrapper]:
        pass


@dataclass
class PendingRequest:
    future: asyncio.Future
    metadata: RequestMetadata


class LocalityScope(str, enum.Enum):
    NODE = "NODE"
    AVAILABILITY_ZONE = "AVAILABILITY_ZONE"


class PowerOfTwoChoicesReplicaScheduler(ReplicaScheduler):
    """Chooses a replica for each request using the "power of two choices" procedure.

    Requests are scheduled in FIFO order.

    When a request comes in, two candidate replicas are chosen randomly. Each replica
    is sent a control message to fetch its queue length.

    The replica responds with two items: (queue_length, accepted). Only replicas that
    accept the request are considered; between those, the one with the lower queue
    length is chosen.

    In the case when neither replica accepts the request (e.g., their queues are full),
    the procedure is repeated with backoff. This backoff repeats indefinitely until a
    replica is chosen, so the caller should use timeouts and cancellation to avoid
    hangs.

    Each request being scheduled may spawn an independent task that runs the scheduling
    procedure concurrently. This task will not necessarily satisfy the request that
    started it (in order to maintain the FIFO order). The total number of tasks is
    capped at (2 * num_replicas).
    """

    # The sequence of backoff timeouts to use when all replicas' queues are full.
    # The last item in the list is the max timeout and will be used repeatedly.
    backoff_sequence_s = [0, 0.05, 0.1, 0.15, 0.2, 0.5, 1.0]

    # Deadline for replicas to respond with their queue length. If the response isn't
    # received within this deadline, the replica will not be considered.
    queue_len_response_deadline_s = 0.1

    # Hard limit on the maximum number of scheduling tasks to run. Having too many of
    # these tasks can cause stability issue due to too much load on the local process
    # and many too requests in flight to fetch replicas' queue lengths.
    max_num_scheduling_tasks_cap = 50

    def __init__(
        self,
        event_loop: asyncio.AbstractEventLoop,
        deployment_id: DeploymentID,
        prefer_local_node_routing: bool = False,
        prefer_local_az_routing: bool = False,
        self_node_id: Optional[str] = None,
        self_availability_zone: Optional[str] = None,
    ):
        self._loop = event_loop
        self._deployment_id = deployment_id
        self._prefer_local_node_routing = prefer_local_node_routing
        self._prefer_local_az_routing = prefer_local_az_routing
        self._self_node_id = self_node_id
        self._self_availability_zone = self_availability_zone

        # Current replicas available to be scheduled.
        # Updated via `update_replicas`.
        self._replica_id_set: Set[str] = set()
        self._replicas: Dict[str, ReplicaWrapper] = {}
        self._replicas_updated_event = make_asyncio_event_version_compat(event_loop)
        # Colocated replicas (e.g. wrt node, AZ)
        self._colocated_replica_ids: DefaultDict[LocalityScope, Set[str]] = defaultdict(
            set
        )
        self._multiplexed_model_id_to_replica_ids: DefaultDict[Set[str]] = defaultdict(
            set
        )

        # When there is no match for a multiplexed model id, we will try to fallback
        # to all replicas immediately. This set is used to make sure we only fallback
        # once for concurrent requests for the same model id.
        # Whenever there is a match, we will remove the the model id from this set.
        self._multiplexed_model_id_fallback_match: Set[str] = set()

        # Tasks running the scheduling loop. The size of this set may vary over time
        # as new tasks will be scheduled when a request comes in or new replicas are
        # added, but it will not exceed self.max_num_scheduling_tasks.
        self._scheduling_tasks: Set[asyncio.Task] = set()

        # We keep two separate queues of pending requests:
        # - self._pending_requests_to_fulfill is a queue that will be used to fulfill
        # requests in FIFO order by scheduling tasks once they've acquired a replica.
        # To avoid long tail latencies due to backoff, the scheduling task started by
        # a given request may not be the one to fulfill it.
        # - self._pending_requests_to_schedule is a queue that is used for tasks to
        # best-effort grab the metadata of requests waiting to be fulfilled. This is
        # currently used for scheduling tasks to know which multiplexed model IDs they
        # should be trying to get replicas for.
        self._pending_requests_to_fulfill: Deque[PendingRequest] = deque()
        self._pending_requests_to_schedule: Deque[PendingRequest] = deque()

    @property
    def num_pending_requests(self) -> int:
        """Current number of requests pending assignment."""
        return len(self._pending_requests_to_fulfill)

    @property
    def curr_num_scheduling_tasks(self) -> int:
        """Current number of scheduling tasks running."""
        return len(self._scheduling_tasks)

    @property
    def max_num_scheduling_tasks(self) -> int:
        """Max number of scheduling tasks to run at any time."""
        return min(self.max_num_scheduling_tasks_cap, 2 * len(self._replicas))

    @property
    def target_num_scheduling_tasks(self) -> int:
        """Target number of scheduling tasks to be running based on pending requests.

        This will never exceed `self.max_num_scheduling_tasks`.
        """
        return min(self.num_pending_requests, self.max_num_scheduling_tasks)

    @property
    def curr_replicas(self) -> Dict[str, ReplicaWrapper]:
        return self._replicas

    @property
    def app_name(self) -> str:
        return self._deployment_id.app

    def update_replicas(self, replicas: List[ReplicaWrapper]):
        """Update the set of available replicas to be considered for scheduling.

        When the set of replicas changes, we may spawn additional scheduling tasks
        if there are pending requests.
        """
        new_replicas = {}
        new_replica_id_set = set()
        new_colocated_replica_ids = defaultdict(set)
        new_multiplexed_model_id_to_replica_ids = defaultdict(set)
        for r in replicas:
            new_replicas[r.replica_id] = r
            new_replica_id_set.add(r.replica_id)
            if self._self_node_id is not None and r.node_id == self._self_node_id:
                new_colocated_replica_ids[LocalityScope.NODE].add(r.replica_id)
            if (
                self._self_availability_zone is not None
                and r.availability_zone == self._self_availability_zone
            ):
                new_colocated_replica_ids[LocalityScope.AVAILABILITY_ZONE].add(
                    r.replica_id
                )
            for model_id in r.multiplexed_model_ids:
                new_multiplexed_model_id_to_replica_ids[model_id].add(r.replica_id)

        if self._replica_id_set != new_replica_id_set:
            app_msg = f" in application '{self.app_name}'" if self.app_name else ""
            logger.info(
                f"Got updated replicas for deployment '{self._deployment_id.name}'"
                f"{app_msg}: {new_replica_id_set}.",
                extra={"log_to_stderr": False},
            )

        self._replicas = new_replicas
        self._replica_id_set = new_replica_id_set
        self._colocated_replica_ids = new_colocated_replica_ids
        self._multiplexed_model_id_to_replica_ids = (
            new_multiplexed_model_id_to_replica_ids
        )
        self._replicas_updated_event.set()
        self.maybe_start_scheduling_tasks()

    def update_running_replicas(self, running_replicas: List[RunningReplicaInfo]):
        """Shim for compatibility with the existing round robin scheduler."""
        return self.update_replicas([ActorReplicaWrapper(r) for r in running_replicas])

    def _get_candidate_replica_ids_for_multiplexed_model_id(
        self,
        model_id: str,
        get_from_all_replicas: bool = False,
    ) -> Set[str]:
        """Get multiplexed model candidates from the current replica.

        By default, we will only choose from replicas that have the requested
        multiplexed model id, if not matched, the function will return an empty set.

        If get_from_all_replicas is True, we will choose from all replicas,
        and we will choose all replicas with the least number of multiplexed model
        ids.

        """

        candidates = set()

        if not get_from_all_replicas:
            if model_id in self._multiplexed_model_id_to_replica_ids:
                candidates = self._multiplexed_model_id_to_replica_ids[model_id]
                if len(candidates) > 0:
                    return candidates
            return candidates

        # Sort the replicas by the number of multiplexed model ids they have.
        # Choose all replicas with the least number of multiplexed model ids.
        sorted_replicas = sorted(
            self._replicas.values(), key=lambda x: len(x.multiplexed_model_ids)
        )
        least_num_multiplexed_model_ids = math.inf
        for replica in sorted_replicas:
            if len(replica.multiplexed_model_ids) <= least_num_multiplexed_model_ids:
                candidates.add(replica.replica_id)
                least_num_multiplexed_model_ids = len(replica.multiplexed_model_ids)
            else:
                break

        return candidates

    async def choose_two_replicas_with_backoff(
        self,
        request_metadata: Optional[RequestMetadata] = None,
    ) -> AsyncGenerator[List[RunningReplicaInfo], None]:
        """Generator that repeatedly chooses (at most) two random available replicas.

        In the first iteration, only replicas colocated on the same node as this router
        will be considered. If those are occupied, the full set of replicas will be
        considered on subsequent iterations.

        For multiplexing, this will first attempt to choose replicas that have the
        requested model ID for a configured timeout. If no replicas with the matching
        model ID are available after that timeout, it will fall back to the regular
        procedure.

        After each iteration, there will be an increasing backoff sleep time (dictated
        by `self.backoff_sequence_s`). The caller should exit the generator to reset the
        backoff sleep time.
        """

        backoff_index = 0
        multiplexed_start_matching_time = None
        multiplexed_matching_timeout = random.uniform(
            RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S,
            RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S * 2,
        )
        tried_same_node = False
        tried_same_az = False

        while True:
            # If no replicas are available, wait until `update_replicas` is called.
            while len(self._replicas) == 0:
                app_msg = f" in application '{self.app_name}'" if self.app_name else ""
                logger.info(
                    "Tried to assign replica for deployment "
                    f"'{self._deployment_id.name}'{app_msg} but none are available. "
                    "Waiting for new replicas to be added.",
                    extra={"log_to_stderr": False},
                )
                self._replicas_updated_event.clear()
                await self._replicas_updated_event.wait()
                logger.info(
                    f"Got replicas for deployment '{self._deployment_id.name}'"
                    f"{app_msg}, waking up.",
                    extra={"log_to_stderr": False},
                )

            if multiplexed_start_matching_time is None:
                multiplexed_start_matching_time = time.time()

            candidate_replica_ids = None
            if request_metadata is not None and request_metadata.multiplexed_model_id:
                # Get candidates for multiplexed model ID.
                if (
                    time.time() - multiplexed_start_matching_time
                    < multiplexed_matching_timeout
                ):
                    candidate_replica_ids = (
                        self._get_candidate_replica_ids_for_multiplexed_model_id(
                            request_metadata.multiplexed_model_id
                        )
                    )
                    # When there is no match for a multiplexed model id, we will try to
                    # fallback to all replicas immediately.
                    if (
                        len(candidate_replica_ids) == 0
                        and request_metadata.multiplexed_model_id
                        not in self._multiplexed_model_id_fallback_match
                    ):
                        candidate_replica_ids = (
                            self._get_candidate_replica_ids_for_multiplexed_model_id(
                                request_metadata.multiplexed_model_id,
                                get_from_all_replicas=True,
                            )
                        )
                        self._multiplexed_model_id_fallback_match.add(
                            request_metadata.multiplexed_model_id
                        )
                    elif len(candidate_replica_ids) > 0:
                        self._multiplexed_model_id_fallback_match.discard(
                            request_metadata.multiplexed_model_id
                        )
                else:
                    candidate_replica_ids = (
                        self._get_candidate_replica_ids_for_multiplexed_model_id(
                            request_metadata.multiplexed_model_id,
                            get_from_all_replicas=True,
                        )
                    )
            elif (
                self._prefer_local_node_routing
                and not tried_same_node
                and len(self._colocated_replica_ids[LocalityScope.NODE]) > 0
            ):
                # Attempt to schedule requests to replicas on the same node at most once
                candidate_replica_ids = self._colocated_replica_ids[LocalityScope.NODE]
                tried_same_node = True
            elif (
                self._prefer_local_az_routing
                and not tried_same_az
                and len(self._colocated_replica_ids[LocalityScope.AVAILABILITY_ZONE])
                > 0
            ):
                # Attempt to schedule requests to replicas in the same AZ at most once
                candidate_replica_ids = self._colocated_replica_ids[
                    LocalityScope.AVAILABILITY_ZONE
                ]
                tried_same_az = True
            else:
                # On subsequent iterations or when there are no replicas on the same
                # node or AZ, consider all available replicas.
                candidate_replica_ids = self._replica_id_set

            if candidate_replica_ids:
                chosen_ids = random.sample(
                    list(candidate_replica_ids), k=min(2, len(candidate_replica_ids))
                )
                yield [self._replicas[chosen_id] for chosen_id in chosen_ids]

            await asyncio.sleep(self.backoff_sequence_s[backoff_index])
            backoff_index = min(backoff_index + 1, len(self.backoff_sequence_s) - 1)

    async def select_from_candidate_replicas(
        self, candidates: List[ReplicaWrapper]
    ) -> Optional[ReplicaWrapper]:
        """Chooses the best replica from the list of candidates.

        If none of the replicas can be scheduled, returns `None`.

        The queue length at each replica is queried directly from it. The time waited
        for these queries is capped by `self.queue_len_response_deadline_s`; if a
        replica doesn't respond within the deadline it is not considered.

        Among replicas that respond within the deadline and accept the request (don't
        have full queues), the one with the lowest queue length is chosen.
        """
        get_queue_state_tasks = []
        for c in candidates:
            t = self._loop.create_task(c.get_queue_state())
            t.replica_id = c.replica_id
            get_queue_state_tasks.append(t)

        done, pending = await asyncio.wait(
            get_queue_state_tasks,
            timeout=self.queue_len_response_deadline_s,
            return_when=asyncio.ALL_COMPLETED,
        )
        for t in pending:
            t.cancel()
            logger.warning(
                f"Failed to get queue length from replica {t.replica_id} "
                f"within {self.queue_len_response_deadline_s}s."
            )

        chosen_replica_id = None
        lowest_queue_len = math.inf
        for t in done:
            if t.exception() is not None:
                msg = (
                    "Failed to fetch queue length for "
                    f"replica {t.replica_id}: '{t.exception()}'"
                )
                # If we get a RayActorError, it means the replica actor has died. This
                # is not recoverable (the controller will start a new replica in its
                # place), so we should no longer consider it for requests.
                if isinstance(t.exception(), RayActorError):
                    self._replicas.pop(t.replica_id, None)
                    self._replica_id_set.discard(t.replica_id)
                    for id_set in self._colocated_replica_ids.values():
                        id_set.discard(t.replica_id)
                    msg += " This replica will no longer be considered for requests."

                logger.warning(msg)
            else:
                queue_len, accepted = t.result()
                if accepted and queue_len < lowest_queue_len:
                    chosen_replica_id = t.replica_id
                    lowest_queue_len = queue_len

        if chosen_replica_id is None:
            return None

        # `self._replicas` may have been updated since the candidates were chosen.
        # In that case, return `None` so a new one is selected.
        return self._replicas.get(chosen_replica_id, None)

    def _get_pending_request_matching_metadata(
        self,
        replica: ReplicaWrapper,
        request_metadata: Optional[RequestMetadata] = None,
    ) -> Optional[PendingRequest]:
        if request_metadata is None or not request_metadata.multiplexed_model_id:
            return None

        for pr in self._pending_requests_to_fulfill:
            if (
                not pr.future.done()
                and pr.metadata.multiplexed_model_id
                == request_metadata.multiplexed_model_id
            ):
                return pr

        return None

    def fulfill_next_pending_request(
        self,
        replica: ReplicaWrapper,
        request_metadata: Optional[RequestMetadata] = None,
    ):
        """Assign the replica to the next pending request in FIFO order.

        If a pending request has been cancelled, it will be popped from the queue
        and not assigned.
        """
        # First try to match a pending request based on the request metadata (currently
        # this only looks at the multiplexed model ID).
        matched_pending_request = self._get_pending_request_matching_metadata(
            replica, request_metadata
        )
        if matched_pending_request is not None:
            matched_pending_request.future.set_result(replica)
            self._pending_requests_to_fulfill.remove(matched_pending_request)
            return

        # If no pending request matches the request metadata, fulfill the next in the
        # queue in FIFO order, passing over futures that have been cancelled.
        while len(self._pending_requests_to_fulfill) > 0:
            pr = self._pending_requests_to_fulfill.popleft()
            if not pr.future.done():
                pr.future.set_result(replica)
                break

    def _get_next_pending_request_metadata_to_schedule(
        self,
    ) -> Optional[RequestMetadata]:
        while len(self._pending_requests_to_schedule) > 0:
            pr = self._pending_requests_to_schedule.popleft()
            if not pr.future.done():
                return pr.metadata

        return None

    async def fulfill_pending_requests(self):
        """Repeatedly tries to fulfill a pending request with an available replica.

        This is expected to be run inside a task in self._scheduling tasks.

        When a replica is found, this method will exit if the number of scheduling tasks
        has exceeded the target number. Else it will loop again to schedule another
        replica.
        """
        try:
            while len(self._scheduling_tasks) <= self.target_num_scheduling_tasks:
                request_metadata = self._get_next_pending_request_metadata_to_schedule()
                async for candidates in self.choose_two_replicas_with_backoff(
                    request_metadata
                ):
                    replica = await self.select_from_candidate_replicas(candidates)
                    if replica is not None:
                        self.fulfill_next_pending_request(replica, request_metadata)
                        break

        except Exception:
            logger.exception("Unexpected error in fulfill_pending_requests.")
        finally:
            self._scheduling_tasks.remove(asyncio.current_task(loop=self._loop))

    def maybe_start_scheduling_tasks(self):
        """Start scheduling tasks to fulfill pending requests if necessary.

        Starts tasks so that there is at least one task per pending request
        (respecting the max number of scheduling tasks).

        In the common case, this will start a single task when a new request comes
        in for scheduling. However, in cases where the number of available replicas
        is updated or a task exits unexpectedly, we may need to start multiple.
        """
        tasks_to_start = (
            self.target_num_scheduling_tasks - self.curr_num_scheduling_tasks
        )
        for _ in range(tasks_to_start):
            self._scheduling_tasks.add(
                self._loop.create_task(self.fulfill_pending_requests())
            )

    async def choose_replica_for_query(self, query: Query) -> ReplicaWrapper:
        """Chooses a replica to send the provided request to.

        Requests are scheduled in FIFO order, so this puts a future on the internal
        queue that will be resolved when a replica is available and it's the front of
        the queue.

        Upon cancellation (by the caller), the future is cancelled and will be passed
        over when a replica becomes available.
        """
        pending_request = PendingRequest(asyncio.Future(), query.metadata)
        try:
            self._pending_requests_to_fulfill.append(pending_request)
            self._pending_requests_to_schedule.append(pending_request)
            self.maybe_start_scheduling_tasks()
            replica = await pending_request.future
        except asyncio.CancelledError as e:
            pending_request.future.cancel()

            raise e from None

        return replica

    async def assign_replica(
        self, query: Query
    ) -> Union[ray.ObjectRef, "ray._raylet.StreamingObjectRefGenerator"]:
        """Choose a replica for the request and send it.

        This will block indefinitely if no replicas are available to handle the
        request, so it's up to the caller to time out or cancel the request.
        """
        replica = await self.choose_replica_for_query(query)
        return replica.send_query(query)


class RoundRobinReplicaScheduler(ReplicaScheduler):
    """Round-robins requests across a set of actor replicas.

    The policy respects `max_concurrent_queries` for the replicas: a replica
    is not chosen if `max_concurrent_queries` requests are already outstanding.

    This is maintained using a "tracker" object ref to determine when a given request
    has finished (to decrement the number of concurrent queries).
    """

    def __init__(self, event_loop: asyncio.AbstractEventLoop):
        self.in_flight_queries: Dict[RunningReplicaInfo, set] = dict()

        # The iterator used for load balancing among replicas. Using itertools
        # cycle, we implements a round-robin policy, skipping overloaded
        # replicas.
        # NOTE(simon): We can make this more pluggable and consider different
        # policies like: min load, pick min of two replicas, pick replicas on
        # the same node.
        self.replica_iterator = itertools.cycle(self.in_flight_queries.keys())

        # Used to unblock this replica set waiting for free replicas. A newly
        # added replica or updated max_concurrent_queries value means the
        # query that waits on a free replica might be unblocked on.
        self.config_updated_event = make_asyncio_event_version_compat(event_loop)

        # A map from multiplexed model id to a list of replicas that have the
        # model loaded.
        self.multiplexed_replicas_table: Dict[
            str, List[RunningReplicaInfo]
        ] = defaultdict(list)

    @property
    def curr_replicas(self) -> Dict[str, ReplicaWrapper]:
        return {
            r.replica_tag: ActorReplicaWrapper(r) for r in self.in_flight_queries.keys()
        }

    def _reset_replica_iterator(self):
        """Reset the iterator used to load balance replicas.

        This call is expected to be called after the replica membership has
        been updated. It will shuffle the replicas randomly to avoid multiple
        handle sending requests in the same order.
        """
        replicas = list(self.in_flight_queries.keys())
        random.shuffle(replicas)
        self.replica_iterator = itertools.cycle(replicas)

        # Update the multiplexed_replicas_table
        new_multiplexed_replicas_table = defaultdict(list)
        for replica in replicas:
            for model_id in replica.multiplexed_model_ids:
                new_multiplexed_replicas_table[model_id].append(replica)
        self.multiplexed_replicas_table = new_multiplexed_replicas_table

    def update_running_replicas(self, running_replicas: List[RunningReplicaInfo]):
        added, removed, _ = compute_iterable_delta(
            self.in_flight_queries.keys(), running_replicas
        )

        for new_replica in added:
            self.in_flight_queries[new_replica] = set()

        for removed_replica in removed:
            # Delete it directly because shutdown is processed by controller.
            # Replicas might already been deleted due to early detection of
            # actor error.
            self.in_flight_queries.pop(removed_replica, None)

        if len(added) > 0 or len(removed) > 0:
            logger.debug(f"ReplicaSet: +{len(added)}, -{len(removed)} replicas.")
            self._reset_replica_iterator()
            self.config_updated_event.set()

    def _assign_replica(self, query: Query, replica: RunningReplicaInfo):
        """Assign query to the replica.
        Args:
            query: Query object, containing the request metadata and args.
            replica: Replica object, containing the actor handle to the replica.
        Returns: object ref of the requests.
        """

        logger.debug(
            f"Assigned query {query.metadata.request_id} "
            f"to replica {replica.replica_tag}."
        )
        if replica.is_cross_language:
            # Handling requests for Java replica
            arg = query.args[0]
            if query.metadata.is_http_request:
                assert isinstance(arg, bytes)
                loaded_http_input = pickle.loads(arg)
                query_string = loaded_http_input.scope.get("query_string")
                if query_string:
                    arg = query_string.decode().split("=", 1)[1]
                elif loaded_http_input.body:
                    arg = loaded_http_input.body.decode()
            user_ref = JavaActorHandleProxy(replica.actor_handle).handle_request.remote(
                RequestMetadataProto(
                    request_id=query.metadata.request_id,
                    endpoint=query.metadata.endpoint,
                    call_method=query.metadata.call_method
                    if query.metadata.call_method != "__call__"
                    else "call",
                ).SerializeToString(),
                [arg],
            )
            self.in_flight_queries[replica].add(user_ref)
        else:
            # Directly passing args because it might contain an ObjectRef.
            tracker_ref, user_ref = replica.actor_handle.handle_request.remote(
                pickle.dumps(query.metadata), *query.args, **query.kwargs
            )
            self.in_flight_queries[replica].add(tracker_ref)
        return user_ref

    def _try_assign_replica(self, query: Query) -> Optional[ray.ObjectRef]:
        """Try to assign query to a replica, return the object ref if succeeded
        or return None if it can't assign this query to any replicas.
        """

        # Try to find a replica that can handle this query
        # If multiplexed model id is not specified, we can assign the query to
        # any non-overloaded replica.
        # If multiplexed model id is specified, we can try to assign the query
        # to a replica that has the specified model loaded and
        # is not overloaded with requests.
        # If no such replica exists, we can assign the query to any non-overloaded
        # replica.
        if (
            query.metadata.multiplexed_model_id
            and query.metadata.multiplexed_model_id in self.multiplexed_replicas_table
        ):
            # Try to find the replica that is already handling the model.
            for replica in self.multiplexed_replicas_table[
                query.metadata.multiplexed_model_id
            ]:
                if (
                    len(self.in_flight_queries[replica])
                    >= replica.max_concurrent_queries
                ):
                    # This replica is overloaded, try next one
                    continue
                logger.debug(
                    f"Assigned query {query.metadata.request_id} "
                    f"to replica {replica.replica_tag}."
                )
                return self._assign_replica(query, replica)

        for _ in range(len(self.in_flight_queries.keys())):
            replica = next(self.replica_iterator)
            if len(self.in_flight_queries[replica]) >= replica.max_concurrent_queries:
                # This replica is overloaded, try next one
                continue

            if query.metadata.multiplexed_model_id:
                # This query has a multiplexed model id, but the model is not
                # loaded on this replica. Save this replica for future queries
                # with the same model id.
                self.multiplexed_replicas_table[
                    query.metadata.multiplexed_model_id
                ].append(replica)

            logger.debug(
                f"Assigned query {query.metadata.request_id} "
                f"to replica {replica.replica_tag}."
            )
            return self._assign_replica(query, replica)
        return None

    @property
    def _all_query_refs(self):
        return list(itertools.chain.from_iterable(self.in_flight_queries.values()))

    def _drain_completed_object_refs(self) -> int:
        refs = self._all_query_refs
        # NOTE(simon): even though the timeout is 0, a large number of refs can still
        # cause some blocking delay in the event loop. Consider moving this to async?
        done, _ = ray.wait(refs, num_returns=len(refs), timeout=0)
        replicas_to_remove = []
        for replica_info, replica_in_flight_queries in self.in_flight_queries.items():
            completed_queries = replica_in_flight_queries.intersection(done)
            if len(completed_queries):
                try:
                    # NOTE(simon): this ray.get call should be cheap because all these
                    # refs are ready as indicated by previous `ray.wait` call.
                    ray.get(list(completed_queries))
                except RayActorError:
                    logger.debug(
                        f"Removing {replica_info.replica_tag} from replica set "
                        "because the actor exited."
                    )
                    replicas_to_remove.append(replica_info)
                except RayTaskError:
                    # Ignore application error.
                    pass
                except Exception:
                    logger.exception(
                        "Handle received unexpected error when processing request."
                    )

                replica_in_flight_queries.difference_update(completed_queries)

        if len(replicas_to_remove) > 0:
            for replica_info in replicas_to_remove:
                self.in_flight_queries.pop(replica_info, None)
            self._reset_replica_iterator()

        return len(done)

    async def assign_replica(self, query: Query) -> ray.ObjectRef:
        """Given a query, submit it to a replica and return the object ref.
        This method will keep track of the in flight queries for each replicas
        and only send a query to available replicas (determined by the
        max_concurrent_quries value.)
        """
        if query.metadata.is_streaming:
            raise NotImplementedError("Streaming requires new routing to be enabled.")

        assigned_ref = self._try_assign_replica(query)
        while assigned_ref is None:  # Can't assign a replica right now.
            logger.debug(
                f"Failed to assign a replica for query {query.metadata.request_id}"
            )
            # Maybe there exists a free replica, we just need to refresh our
            # query tracker.
            num_finished = self._drain_completed_object_refs()
            # All replicas are really busy, wait for a query to complete or the
            # config to be updated.
            if num_finished == 0:
                logger.debug("All replicas are busy, waiting for a free replica.")
                await asyncio.wait(
                    self._all_query_refs + [self.config_updated_event.wait()],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if self.config_updated_event.is_set():
                    self.config_updated_event.clear()
            # We are pretty sure a free replica is ready now, let's recurse and
            # assign this query a replica.
            assigned_ref = self._try_assign_replica(query)

        return assigned_ref


class Router:
    def __init__(
        self,
        controller_handle: ActorHandle,
        deployment_id: DeploymentID,
        self_node_id: str,
        self_availability_zone: Optional[str],
        event_loop: asyncio.BaseEventLoop = None,
        _use_new_routing: bool = False,
        _prefer_local_node_routing: bool = False,
        _router_cls: Optional[str] = None,
    ):
        """Used to assign requests to downstream replicas for a deployment.

        The scheduling behavior is delegated to a ReplicaScheduler; this is a thin
        wrapper that adds metrics and logging.
        """
        self._event_loop = event_loop
        self.deployment_id = deployment_id

        if _router_cls:
            self._replica_scheduler = load_class(_router_cls)(
                event_loop=event_loop, deployment_id=deployment_id
            )
        elif _use_new_routing:
            self._replica_scheduler = PowerOfTwoChoicesReplicaScheduler(
                event_loop,
                deployment_id,
                _prefer_local_node_routing,
                RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING,
                self_node_id,
                self_availability_zone,
            )
        else:
            self._replica_scheduler = RoundRobinReplicaScheduler(event_loop)
        logger.info(
            f"Using router {self._replica_scheduler.__class__}.",
            extra={"log_to_stderr": False},
        )

        # -- Metrics Registration -- #
        self.num_router_requests = metrics.Counter(
            "serve_num_router_requests",
            description="The number of requests processed by the router.",
            tag_keys=("deployment", "route", "application"),
        )
        # TODO(zcin): use deployment name and application name instead of deployment id
        self.num_router_requests.set_default_tags(
            {"deployment": deployment_id.name, "application": deployment_id.app}
        )

        self.num_queued_queries = 0
        self.num_queued_queries_gauge = metrics.Gauge(
            "serve_deployment_queued_queries",
            description=(
                "The current number of queries to this deployment waiting"
                " to be assigned to a replica."
            ),
            tag_keys=("deployment", "application"),
        )
        # TODO(zcin): use deployment name and application name instead of deployment id
        self.num_queued_queries_gauge.set_default_tags(
            {"deployment": deployment_id.name, "application": deployment_id.app}
        )

        self.long_poll_client = LongPollClient(
            controller_handle,
            {
                (
                    LongPollNamespace.RUNNING_REPLICAS,
                    deployment_id,
                ): self._replica_scheduler.update_running_replicas,
            },
            call_in_event_loop=event_loop,
        )

        # Start the metrics pusher if autoscaling is enabled.
        deployment_route = DeploymentRoute.FromString(
            ray.get(controller_handle.get_deployment_info.remote(*deployment_id))
        )
        deployment_info = DeploymentInfo.from_proto(deployment_route.deployment_info)
        self.metrics_pusher = None
        if deployment_info.deployment_config.autoscaling_config:
            self.autoscaling_enabled = True
            self.push_metrics_to_controller = (
                controller_handle.record_handle_metrics.remote
            )
            self.metrics_pusher = MetricsPusher()
            self.metrics_pusher.register_task(
                self._collect_handle_queue_metrics,
                HANDLE_METRIC_PUSH_INTERVAL_S,
                self.push_metrics_to_controller,
            )

            self.metrics_pusher.start()
        else:
            self.autoscaling_enabled = False

    def _collect_handle_queue_metrics(self) -> Dict[str, int]:
        return {self.deployment_id: self.num_queued_queries}

    async def assign_request(
        self,
        request_meta: RequestMetadata,
        *request_args,
        **request_kwargs,
    ) -> Union[ray.ObjectRef, "ray._raylet.StreamingObjectRefGenerator"]:
        """Assign a query to a replica and return the resulting object_ref."""

        self.num_router_requests.inc(tags={"route": request_meta.route})
        self.num_queued_queries += 1
        self.num_queued_queries_gauge.set(self.num_queued_queries)

        # Optimization: if there are currently zero replicas for a deployment,
        # push handle metric to controller to allow for fast cold start time.
        # Only do it for the first query to arrive on the router.
        if (
            self.autoscaling_enabled
            and len(self._replica_scheduler.curr_replicas) == 0
            and self.num_queued_queries == 1
        ):
            self.push_metrics_to_controller({self.deployment_id: 1}, time.time())

        try:
            query = Query(
                args=list(request_args),
                kwargs=request_kwargs,
                metadata=request_meta,
            )
            await query.replace_known_types_in_args()
            return await self._replica_scheduler.assign_replica(query)
        finally:
            # If the query is disconnected before assignment, this coroutine
            # gets cancelled by the caller and an asyncio.CancelledError is
            # raised. The finally block ensures that num_queued_queries
            # is correctly decremented in this case.
            self.num_queued_queries -= 1
            self.num_queued_queries_gauge.set(self.num_queued_queries)

    def shutdown(self):
        """Shutdown router gracefully.

        The metrics_pusher needs to be shutdown separately.
        """
        if self.metrics_pusher:
            self.metrics_pusher.shutdown()
