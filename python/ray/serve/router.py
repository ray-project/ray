import sys
import asyncio
import pickle
import itertools
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import random
import os

from ray.actor import ActorHandle
from ray.serve.common import str, ReplicaTag, RunningReplicaInfo
from ray.serve.long_poll import LongPollClient, LongPollNamespace
from ray.serve.utils import compute_iterable_delta, logger

import ray
from ray.util import metrics


@dataclass
class RequestMetadata:
    request_id: str
    endpoint: str

    call_method: str = "__call__"
    shard_key: Optional[str] = None

    http_method: str = "GET"
    http_headers: Dict[str, str] = field(default_factory=dict)

    # This flag will be set to true if the input argument is manually pickled
    # and it needs to be deserialized by the replica.
    http_arg_is_pickled: bool = False

    version: str = ""
    prev_version: str = ""

    def __post_init__(self):
        self.http_headers.setdefault("X-Serve-Call-Method", self.call_method)
        self.http_headers.setdefault("X-Serve-Shard-Key", self.shard_key)


@dataclass
class Query:
    args: List[Any]
    kwargs: Dict[Any, Any]
    metadata: RequestMetadata


class ReplicaSet:
    """Data structure representing a set of replica actor handles"""

    def __init__(
            self,
            deployment_name,
            event_loop: asyncio.AbstractEventLoop,
    ):
        self.deployment_name = deployment_name
        self.in_flight_queries: Dict[ReplicaTag, set] = dict()
        # The iterator used for load balancing among replicas. Using itertools
        # cycle, we implements a round-robin policy, skipping overloaded
        # replicas.
        # NOTE(simon): We can make this more pluggable and consider different
        # policies like: min load, pick min of two replicas, pick replicas on
        # the same node.
        self.replica_iterator = itertools.cycle(self.in_flight_queries.keys())
        self.replica_infos: Dict[ReplicaTag, RunningReplicaInfo] = dict()

        # Used to unblock this replica set waiting for free replicas. A newly
        # added replica or updated max_concurrent_queries value means the
        # query that waits on a free replica might be unblocked on.

        # Python 3.8 has deprecated the 'loop' parameter, and Python 3.10 has
        # removed it alltogether. Call accordingly.
        if sys.version_info.major >= 3 and sys.version_info.minor >= 10:
            self.config_updated_event = asyncio.Event()
        else:
            self.config_updated_event = asyncio.Event(loop=event_loop)

        self.num_queued_queries = 0
        self.num_queued_queries_gauge = metrics.Gauge(
            "serve_deployment_queued_queries",
            description=(
                "The current number of queries to this deployment waiting"
                " to be assigned to a replica."),
            tag_keys=("deployment", "endpoint"))
        self.num_queued_queries_gauge.set_default_tags({
            "deployment": self.deployment_name
        })

    def update_running_replicas(self,
                                running_replicas: List[RunningReplicaInfo]):
        added, removed, _ = compute_iterable_delta(
            self.in_flight_queries.keys(), running_replicas)

        for new_replica in added:
            logger.info(f"\n$$$$$ new_replica: {new_replica.deployment_name}, version: {new_replica.version}, prev_version: {new_replica.prev_version}")
            self.in_flight_queries[new_replica] = set()

        for removed_replica in removed:
            # Delete it directly because shutdown is processed by controller.
            logger.info(f"\n$$$$$ removed_replica: {removed_replica.deployment_name}, version: {removed_replica.version}, prev_version: {removed_replica.prev_version}")
            del self.in_flight_queries[removed_replica]

        if len(added) > 0 or len(removed) > 0:
            # Shuffle the keys to avoid synchronization across clients.
            replicas = list(self.in_flight_queries.keys())
            random.shuffle(replicas)

            self.replica_iterator = itertools.cycle(replicas)
            logger.info(
                f"ReplicaSet: +{len(added)}, -{len(removed)} replicas.")
            self.config_updated_event.set()

    def _try_assign_replica_pipeline_aware(
        self,
        query: Query,
        my_router_version: str,
        my_router_prev_version: Optional[str]
    ) -> Optional[ray.ObjectRef]:
        """
        Pipeline aware replica assignment.

        Ideally we should do random assignment to 2 running replica versions
        while ensuring A->B and A'->B' only communication while getting
        requests randomly assigned from upstream to {A, A'}.

        For simplicity of demo, this tracks version of each replica as well as
        most up-to-date deployment handle / router version history, and only
        assign to latest replica when applicable.

        """

        # upstream_router_version = query.metadata.version
        # upstream_router_prev_version = query.metadata.prev_version

        # Slow and dirty stuff for hackathon demo
        replica_versions = set()
        replica_prev_versions = set()
        replicas = list(self.in_flight_queries.keys())
        for replica in replicas:
            replica_versions.add(replica.version)
            replica_prev_versions.add(replica.prev_version)

        # older_version, newer_version = None, None

        # if len(replica_versions) == 2:
        #     for ele in replica_versions:
        #         if newer_version is None:
        #             newer_version = ele
        #         if older_version is None:
        #             older_version = ele
        #         if int(ele.split("_")[-1]) > int(newer_version.split("_")[-1]):
        #             newer_version = ele

        #     older_replicas = []
        #     newer_replicas = []
        #     for replica in replicas:
        #         if replica.version == older_version:
        #             older_replicas.append(replica)
        #         else:
        #             newer_replicas.append(replica)

        #     logger.info(f"\n\n\n # of old: {len(older_replicas)}, # of new: {len(newer_replicas)} \n\n\n")

        #     for replica in newer_replicas:
        #         logger.info(
        #             f"Assigned query {query.metadata.request_id} from router client version {query.metadata.version}, router client prev version {query.metadata.prev_version}"
        #             f"to replica {replica.replica_tag} with replica version {replica.version}, replica prev_version {replica.prev_version}"
        #         )
        #         # Directly passing args because it might contain an ObjectRef.
        #         tracker_ref, user_ref = replica.actor_handle.handle_request.remote(
        #             pickle.dumps(query.metadata), *query.args, **query.kwargs)
        #         self.in_flight_queries[replica].add(tracker_ref)
        #         return user_ref
        #     return None

        # else:
        #     # just pick
        #     return self._try_assign_replica(query)


        logger.info(f"# cur versions: {len(replica_versions)}")
        logger.info(f"# prev versions: {len(replica_prev_versions)}")

        for _ in range(len(self.in_flight_queries.keys())):
            replica = next(self.replica_iterator)
            if len(self.in_flight_queries[replica]
                   ) >= replica.max_concurrent_queries:
                # This replica is overloaded, try next one
                continue

            if len(replica_versions) == 2:
                if replica.prev_version is None:
                    logger.info("===[handle]=== Avoiding old version replica.")
                    continue

            logger.info(
                f"Assigned query {query.metadata.request_id} from router client version {query.metadata.version}, router client prev version {query.metadata.prev_version}"
                f"to replica {replica.replica_tag} with replica version {replica.version}, replica prev_version {replica.prev_version}"
            )
            # Directly passing args because it might contain an ObjectRef.
            tracker_ref, user_ref = replica.actor_handle.handle_request.remote(
                pickle.dumps(query.metadata), *query.args, **query.kwargs)
            self.in_flight_queries[replica].add(tracker_ref)
            return user_ref
        return None

        # if upstream_router_prev_version is None:
        #     # R1, LoadTest -> Ensemble
        #     if my_router_version is not None and my_router_prev_version is not None:
        #         # Currnet layer has two version of replicas, always assign to larger group.

        #     else:

        #     logger.info("==[R1]== Only one version of upstream router in pipeline, doing round robin to next deployment replicas.")
        #     return self._try_assign_replica(query)
        # else:
        #     logger.info("Two versions of upstream routers available.")

        #     # R3, only one downstream replica version available, just send

        #     if len(replica_versions) < 2:
        #         logger.info("==[R3]== Only ony version of downstream replica version available, just send.")
        #         return self._try_assign_replica(query)

        #     # R2 (a), old ensemble router -> old obj router


        #     # R2 (b), new ensemble router -> new obj router


    def _try_assign_replica(self, query: Query) -> Optional[ray.ObjectRef]:
        """Try to assign query to a replica, return the object ref if succeeded
        or return None if it can't assign this query to any replicas.
        """

        for _ in range(len(self.in_flight_queries.keys())):
            replica = next(self.replica_iterator)
            if len(self.in_flight_queries[replica]
                   ) >= replica.max_concurrent_queries:
                # This replica is overloaded, try next one
                continue

            logger.info(
                f"Assigned query {query.metadata.request_id} from router client version {query.metadata.version}, router client prev version {query.metadata.prev_version}"
                f"to replica {replica.replica_tag} with replica version {replica.version}, replica prev_version {replica.prev_version}"
            )
            # Directly passing args because it might contain an ObjectRef.
            tracker_ref, user_ref = replica.actor_handle.handle_request.remote(
                pickle.dumps(query.metadata), *query.args, **query.kwargs)
            self.in_flight_queries[replica].add(tracker_ref)
            return user_ref
        return None

    @property
    def _all_query_refs(self):
        return list(
            itertools.chain.from_iterable(self.in_flight_queries.values()))

    def _drain_completed_object_refs(self) -> int:
        refs = self._all_query_refs
        done, _ = ray.wait(refs, num_returns=len(refs), timeout=0)
        for replica_in_flight_queries in self.in_flight_queries.values():
            replica_in_flight_queries.difference_update(done)
        return len(done)

    async def assign_replica(
        self,
        router_version: str,
        router_prev_version: Optional[str],
        query: Query
    ) -> ray.ObjectRef:
        """Given a query, submit it to a replica and return the object ref.
        This method will keep track of the in flight queries for each replicas
        and only send a query to available replicas (determined by the
        max_concurrent_quries value.)
        """
        endpoint = query.metadata.endpoint
        self.num_queued_queries += 1
        self.num_queued_queries_gauge.set(
            self.num_queued_queries, tags={"endpoint": endpoint})

        # if os.environ.get("PIPELINE_AWARE_ROUTING", "0") == "1":
        assigned_ref = self._try_assign_replica_pipeline_aware(query, router_version, router_prev_version)
        # else:
            # assigned_ref = self._try_assign_replica(query)
        while assigned_ref is None:  # Can't assign a replica right now.
            logger.debug("Failed to assign a replica for "
                         f"query {query.metadata.request_id}")
            # Maybe there exists a free replica, we just need to refresh our
            # query tracker.
            num_finished = self._drain_completed_object_refs()
            # All replicas are really busy, wait for a query to complete or the
            # config to be updated.
            if num_finished == 0:
                logger.debug(
                    "All replicas are busy, waiting for a free replica.")
                await asyncio.wait(
                    self._all_query_refs + [self.config_updated_event.wait()],
                    return_when=asyncio.FIRST_COMPLETED)
                if self.config_updated_event.is_set():
                    self.config_updated_event.clear()
            # We are pretty sure a free replica is ready now, let's recurse and
            # assign this query a replica.
            # if os.environ.get("PIPELINE_AWARE_ROUTING", "0") == "1":
            assigned_ref = self._try_assign_replica_pipeline_aware(query, router_version, router_prev_version)
            # else:
                # assigned_ref = self._try_assign_replica(query)
        self.num_queued_queries -= 1
        self.num_queued_queries_gauge.set(
            self.num_queued_queries, tags={"endpoint": endpoint})
        return assigned_ref


class Router:
    def __init__(
            self,
            controller_handle: ActorHandle,
            deployment_name: str,
            version: Optional[str] = None,
            prev_version: Optional[str] = None,
            event_loop: asyncio.BaseEventLoop = None,
    ):
        """Router process incoming queries: assign a replica.

        Args:
            controller_handle(ActorHandle): The controller handle.
        """
        self._event_loop = event_loop
        self._replica_set = ReplicaSet(deployment_name, event_loop)

        # -- Metrics Registration -- #
        self.num_router_requests = metrics.Counter(
            "serve_num_router_requests",
            description="The number of requests processed by the router.",
            tag_keys=("deployment", ))
        self.num_router_requests.set_default_tags({
            "deployment": deployment_name
        })

        self.long_poll_client = LongPollClient(
            controller_handle,
            {
                (LongPollNamespace.RUNNING_REPLICAS, deployment_name): self.
                _replica_set.update_running_replicas,
            },
            call_in_event_loop=event_loop,
        )
        self._version = version
        self._prev_version = prev_version

        logger.info(f"\n ===== Created router for {deployment_name} with version {version}, prev_version {prev_version} \n")

    async def assign_request(
            self,
            request_meta: RequestMetadata,
            *request_args,
            **request_kwargs,
    ):
        """Assign a query and returns an object ref represent the result"""

        self.num_router_requests.inc()
        return await self._replica_set.assign_replica(
            self._version,
            self._prev_version,
            Query(
                args=list(request_args),
                kwargs=request_kwargs,
                metadata=request_meta,
            ))
