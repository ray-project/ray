from abc import ABC
import asyncio
from collections import defaultdict
from dataclasses import dataclass
import itertools
import logging
import pickle
import random
import sys
from typing import Any, Dict, List, Optional, Union

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

    # This flag will be set to true if the input argument is manually pickled
    # and it needs to be deserialized by the replica.
    http_arg_is_pickled: bool = False

    # HTTP route path of the request.
    route: str = ""

    # Application Name
    app_name: str = ""

    # Multiplexed model ID
    multiplexed_model_id: str = ""


@dataclass
class Query:
    args: List[Any]
    kwargs: Dict[Any, Any]
    metadata: RequestMetadata
    return_num: int = 2

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


class ReplicaScheduler(ABC):
    async def assign_replica(
        self, query: Query
    ) -> Union[ray.ObjectRef, "ray._raylet.StreamingObjectRefGenerator"]:
        pass

    def update_running_replicas(self, running_replicas: List[RunningReplicaInfo]):
        pass


class RoundRobinStreamingReplicaScheduler(ReplicaScheduler):
    """Round-robins requests across a set of actor replicas using streaming calls.

    This policy does *not* respect `max_concurrent_queries`.
    """

    def __init__(self):
        self._replica_iterator = itertools.cycle([])
        self._replicas_updated_event = asyncio.Event()

    async def assign_replica(
        self, query: Query
    ) -> "ray._raylet.StreamingObjectRefGenerator":
        replica = None
        while replica is None:
            try:
                replica = next(self._replica_iterator)
            except StopIteration:
                logger.info(
                    "Tried to assign replica but none available",
                    extra={"log_to_stderr": False},
                )
                await self._replicas_updated_event.wait()

        if replica.is_cross_language:
            raise RuntimeError(
                "Streaming is not yet supported for cross-language actors."
            )

        return replica.actor_handle.handle_request_streaming.options(
            num_returns="streaming"
        ).remote(pickle.dumps(query.metadata), *query.args, **query.kwargs)

    def update_running_replicas(self, running_replicas: List[RunningReplicaInfo]):
        random.shuffle(running_replicas)
        self._replica_iterator = itertools.cycle(running_replicas)
        self._replicas_updated_event.set()


class RoundRobinReplicaScheduler(ReplicaScheduler):
    """Round-robins requests across a set of actor replicas.

    The policy respects `max_concurrent_queries` for the replicas: a replica
    is not chosen if `max_concurrent_queries` requests are already outstanding.

    This is maintained using a "tracker" object ref to determine when a given request
    has finished (to decrement the number of concurrent queries).
    """

    def __init__(
        self,
        event_loop: asyncio.AbstractEventLoop,
    ):
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
            if query.metadata.http_arg_is_pickled:
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
        assigned_ref = self._try_assign_replica(query)
        while assigned_ref is None:  # Can't assign a replica right now.
            logger.debug(
                "Failed to assign a replica for " f"query {query.metadata.request_id}"
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
        _stream: bool = False,
    ):
        """Router process incoming queries: assign a replica.

        Args:
            controller_handle: The controller handle.
        """
        self._event_loop = event_loop
        if _stream:
            self._replica_scheduler = RoundRobinStreamingReplicaScheduler()
        else:
            self._replica_scheduler = RoundRobinReplicaScheduler(event_loop)

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
