from abc import ABC
import asyncio
from collections import defaultdict
from dataclasses import dataclass
import itertools
import logging
import math
import pickle
import random
import sys
from typing import Any, AsyncGenerator, Dict, List, Optional, Set, Tuple, Union

import ray
from ray.actor import ActorHandle
from ray.dag.py_obj_scanner import _PyObjScanner
from ray.exceptions import RayActorError, RayTaskError
from ray.util import metrics

from ray.serve._private.common import RunningReplicaInfo, DeploymentInfo
from ray.serve._private.constants import (
    SERVE_LOGGER_NAME,
    HANDLE_METRIC_PUSH_INTERVAL_S,
)
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


@dataclass
class RequestMetadata:
    request_id: str
    endpoint: str
    call_method: str = "__call__"

    # This flag is set if the request is made from the HTTP proxy to a replica.
    is_http_request: bool = False

    # HTTP route path of the request.
    route: str = ""

    # Application name.
    app_name: str = ""

    # Multiplexed model ID.
    multiplexed_model_id: str = ""

    # If this request expects a streaming response.
    is_streaming: bool = False


@dataclass
class Query:
    args: List[Any]
    kwargs: Dict[Any, Any]
    metadata: RequestMetadata

    async def resolve_async_tasks(self):
        """Find all unresolved asyncio.Task and gather them all at once."""
        scanner = _PyObjScanner(source_type=asyncio.Task)
        tasks = scanner.find_nodes((self.args, self.kwargs))

        if len(tasks) > 0:
            resolved = await asyncio.gather(*tasks)
            replacement_table = dict(zip(tasks, resolved))
            self.args, self.kwargs = scanner.replace_nodes(replacement_table)

        # Make the scanner GCable to avoid memory leak
        scanner.clear()


class ReplicaWrapper(ABC):
    @property
    def replica_id(self) -> str:
        pass

    async def get_queue_len(self) -> Tuple[str, int, bool]:
        pass

    def send_query(
        self, query: Query
    ) -> Union[ray.ObjectRef, "ray._raylet.StreamingObjectRefGenerator"]:
        pass


class ActorReplicaWrapper:
    def __init__(self, replica_info: RunningReplicaInfo):
        self.replica_info = replica_info

    @property
    def replica_id(self) -> str:
        return self.replica_info.replica_tag

    async def get_queue_len(self) -> Tuple[str, int, bool]:
        return await self.replica_info.actor_handle.get_num_ongoing_requests.remote()

    def send_query(
        self, query: Query
    ) -> Union[ray.ObjectRef, "ray._raylet.StreamingObjectRefGenerator"]:
        actor = self.replica_info.actor_handle
        if query.metadata.is_streaming:
            obj_ref = actor.handle_request_streaming.options(
                num_returns="streaming"
            ).remote(pickle.dumps(query.metadata), *query.args, **query.kwargs)
        else:
            _, obj_ref = actor.handle_request.remote(
                pickle.dumps(query.metadata), *query.args, **query.kwargs
            )

        return obj_ref


class ReplicaScheduler(ABC):
    async def assign_replica(
        self, query: Query
    ) -> Union[ray.ObjectRef, "ray._raylet.StreamingObjectRefGenerator"]:
        pass

    def update_running_replicas(self, running_replicas: List[RunningReplicaInfo]):
        pass


class PowerOfTwoChoicesReplicaScheduler(ReplicaScheduler):
    """Chooses a replica for each request using the "power of two choices" procedure.

    Requests are scheduled in FIFO order.

    When a request comes in, two replicas are chosen randomly as candidates. The queue
    length of each replicas is requested from it via a control message.

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

    def __init__(
        self,
        event_loop: asyncio.AbstractEventLoop,
        deployment_name: str,
        backoff_sequence: List[float] = None,
    ):
        self._loop = event_loop
        self._deployment_name = deployment_name

        # Current replicas available to be scheduled.
        # Updated via `update_replicas`.
        self._replica_id_set: Set[str] = set()
        self._replicas: Dict[str, ReplicaWrapper] = {}
        self._replicas_updated_event = asyncio.Event()

        self._scheduling_tasks: Set[asyncio.Task] = set()
        self._pending_assignment_futures: List[asyncio.Future] = []

    @property
    def num_pending_assignments(self) -> int:
        return len(self._pending_assignment_futures)

    @property
    def curr_scheduling_tasks(self) -> int:
        return len(self._scheduling_tasks)

    @property
    def max_scheduling_tasks(self) -> int:
        return 2 * len(self._replicas)

    @property
    def target_scheduling_tasks(self) -> int:
        return min(self.num_pending_assignments, self.max_scheduling_tasks)

    def update_replicas(self, replicas: List[ReplicaWrapper]):
        self._replicas = {r.replica_id: r for r in replicas}
        new_replica_ids = set(self._replicas.keys())
        if new_replica_ids != self._replica_id_set:
            self._replica_id_set = new_replica_ids
            logger.info(
                "Got updated replicas for deployment "
                f"{self._deployment_name}: {new_replica_ids}.",
                extra={"log_to_stderr": False},
            )

        self._replicas_updated_event.set()
        self.maybe_start_scheduling_tasks()

    def update_running_replicas(self, running_replicas: List[RunningReplicaInfo]):
        """Shim for compatibility with the existing round robin scheduler."""
        return self.update_replicas([ActorReplicaWrapper(r) for r in running_replicas])

    async def choose_two_gen(self) -> AsyncGenerator[List[RunningReplicaInfo], None]:
        """TODO.

        TODO:
            - blacklist replicas so we don't try the same ones repeatedly?
        """
        backoff_index = 0
        while True:
            while len(self._replicas) == 0:
                logger.info(
                    "Tried to assign replica for deployment "
                    f"{self._deployment_name} but none available.",
                    extra={"log_to_stderr": False},
                )
                print("No replicas, waiting...")
                await self._replicas_updated_event.wait()
                print("Woke up.")
                self._replicas_updated_event.clear()

            chosen_ids = random.sample(
                self._replica_id_set, k=min(2, len(self._replica_id_set))
            )
            yield [self._replicas[chosen_id] for chosen_id in chosen_ids]

            await asyncio.sleep(self.backoff_sequence_s[backoff_index])
            backoff_index = min(backoff_index + 1, len(self.backoff_sequence_s) - 1)

    async def select_replica(
        self, replicas: List[ReplicaWrapper]
    ) -> Optional[ReplicaWrapper]:
        tasks = [r.get_queue_len() for r in replicas]
        done, pending = await asyncio.wait(
            tasks, timeout=0.1, return_when=asyncio.ALL_COMPLETED
        )

        chosen_replica_id = None
        lowest_queue_len = math.inf
        for task in done:
            if task.exception() is not None:
                print(f"Exception: {task.exception()}")
            else:
                replica_id, queue_len, accepted = task.result()
                print(task.result())
                # print(replica_id, "queue_len:", queue_len)
                if accepted and queue_len < lowest_queue_len:
                    chosen_replica_id = replica_id
                    lowest_queue_len = queue_len

        for task in pending:
            task.cancel()

        if chosen_replica_id is None:
            print("No available replicas.")
            return None

        return self._replicas[chosen_replica_id]

    def schedule_replica(self, replica):
        while len(self._pending_assignment_futures) > 0:
            fut = self._pending_assignment_futures.pop(0)
            if not fut.done():
                fut.set_result(replica)
                break

        return len(self._scheduling_tasks) <= self.target_scheduling_tasks

    async def try_schedule(self):
        curr_task = asyncio.current_task().get_name()
        print(curr_task, "try_schedule entered")
        keep_scheduling = True
        while keep_scheduling:
            async for candidates in self.choose_two_gen():
                print("GOT CANDIDATES", candidates)
                replica = await self.select_replica(candidates)
                print("GOT REPLICA", replica)
                if replica is not None:
                    keep_scheduling = self.schedule_replica(replica)
                    print(curr_task, "Got replica! Keep scheduling?", keep_scheduling)
                    break

        self._scheduling_tasks.remove(asyncio.current_task())
        print(curr_task, "try_schedule exited")

    def maybe_start_scheduling_tasks(self):
        tasks_to_start = self.target_scheduling_tasks - self.curr_scheduling_tasks
        print(f"Starting {tasks_to_start} tasks!")
        for _ in range(tasks_to_start):
            self._scheduling_tasks.add(
                self._loop.create_task(self.try_schedule()),
            )

    async def choose_replica_for_query(self, query: Query) -> ReplicaWrapper:
        """TODO."""
        fut = asyncio.Future()
        self._pending_assignment_futures.append(fut)
        try:
            self.maybe_start_scheduling_tasks()
            replica = await fut
        except asyncio.CancelledError:
            fut.cancel()

        return replica

    async def assign_replica(
        self, query: Query
    ) -> Union[ray.ObjectRef, "ray._raylet.StreamingObjectRefGenerator"]:
        replica = await self.choose_replica_for_query(query)
        print("GOT REPLICA", replica)
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

        # Python 3.8 has deprecated the 'loop' parameter, and Python 3.10 has
        # removed it alltogether. Call accordingly.
        if sys.version_info.major >= 3 and sys.version_info.minor >= 10:
            self.config_updated_event = asyncio.Event()
        else:
            self.config_updated_event = asyncio.Event(loop=event_loop)

        # A map from multiplexed model id to a list of replicas that have the
        # model loaded.
        self.multiplexed_replicas_table: Dict[
            str, List[RunningReplicaInfo]
        ] = defaultdict(list)

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
            for mdoel_id in replica.multiplexed_model_ids:
                new_multiplexed_replicas_table[mdoel_id].append(replica)
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

        await query.resolve_async_tasks()
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
        deployment_name: str,
        event_loop: asyncio.BaseEventLoop = None,
        _use_new_routing: bool = False,
    ):
        """Router process incoming queries: assign a replica.

        Args:
            controller_handle: The controller handle.
        """
        self._event_loop = event_loop
        if _use_new_routing:
            self._replica_scheduler = PowerOfTwoChoicesReplicaScheduler(
                event_loop, deployment_name
            )
            logger.info(
                "Using PowerOfTwoChoicesReplicaScheduler.",
                extra={"log_to_stderr": False},
            )
        else:
            self._replica_scheduler = RoundRobinReplicaScheduler(event_loop)
            logger.info(
                "Using RoundRobinReplicaScheduler.",
                extra={"log_to_stderr": False},
            )

        # -- Metrics Registration -- #
        self.num_router_requests = metrics.Counter(
            "serve_num_router_requests",
            description="The number of requests processed by the router.",
            tag_keys=("deployment", "route", "application"),
        )
        self.num_router_requests.set_default_tags({"deployment": deployment_name})

        self.num_queued_queries = 0
        self.num_queued_queries_gauge = metrics.Gauge(
            "serve_deployment_queued_queries",
            description=(
                "The current number of queries to this deployment waiting"
                " to be assigned to a replica."
            ),
            tag_keys=("deployment", "application"),
        )
        self.num_queued_queries_gauge.set_default_tags({"deployment": deployment_name})

        self.long_poll_client = LongPollClient(
            controller_handle,
            {
                (
                    LongPollNamespace.RUNNING_REPLICAS,
                    deployment_name,
                ): self._replica_scheduler.update_running_replicas,
            },
            call_in_event_loop=event_loop,
        )

        # Start the metrics pusher if autoscaling is enabled.
        self.deployment_name = deployment_name
        deployment_route = DeploymentRoute.FromString(
            ray.get(controller_handle.get_deployment_info.remote(self.deployment_name))
        )
        deployment_info = DeploymentInfo.from_proto(deployment_route.deployment_info)
        if deployment_info.deployment_config.autoscaling_config:
            self.metrics_pusher = MetricsPusher(
                controller_handle.record_handle_metrics.remote,
                HANDLE_METRIC_PUSH_INTERVAL_S,
                self._collect_handle_queue_metrics,
            )
            self.metrics_pusher.start()

    def _collect_handle_queue_metrics(self) -> Dict[str, int]:
        return {self.deployment_name: self.num_queued_queries}

    async def assign_request(
        self,
        request_meta: RequestMetadata,
        *request_args,
        **request_kwargs,
    ) -> Union[ray.ObjectRef, "ray._raylet.StreamingObjectRefGenerator"]:
        """Assign a query and returns an object ref represent the result."""

        self.num_router_requests.inc(
            tags={"route": request_meta.route, "application": request_meta.app_name}
        )
        self.num_queued_queries += 1
        self.num_queued_queries_gauge.set(
            self.num_queued_queries,
            tags={
                "application": request_meta.app_name,
            },
        )

        query = Query(
            args=list(request_args),
            kwargs=request_kwargs,
            metadata=request_meta,
        )
        await query.resolve_async_tasks()
        result = await self._replica_scheduler.assign_replica(query)

        self.num_queued_queries -= 1
        self.num_queued_queries_gauge.set(
            self.num_queued_queries,
            tags={
                "application": request_meta.app_name,
            },
        )

        return result
