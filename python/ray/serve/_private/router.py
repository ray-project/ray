import asyncio
import logging
import threading
import time
from collections import defaultdict
from functools import partial
from typing import Any, Dict, List, Optional, Tuple, Union

import ray
from ray._private.utils import load_class
from ray.actor import ActorHandle
from ray.dag.py_obj_scanner import _PyObjScanner
from ray.serve._private.common import DeploymentID, RequestMetadata, RunningReplicaInfo
from ray.serve._private.constants import (
    HANDLE_METRIC_PUSH_INTERVAL_S,
    RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE,
    RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.long_poll import LongPollClient, LongPollNamespace
from ray.serve._private.metrics_utils import InMemoryMetricsStore, MetricsPusher
from ray.serve._private.replica_scheduler import (
    PowerOfTwoChoicesReplicaScheduler,
    Query,
)
from ray.serve.config import AutoscalingConfig
from ray.util import metrics

logger = logging.getLogger(SERVE_LOGGER_NAME)
PUSH_METRICS_TO_CONTROLLER_TASK_NAME = "push_metrics_to_controller"
RECORD_METRICS_TASK_NAME = "record_metrics"


class Router:
    def __init__(
        self,
        controller_handle: ActorHandle,
        deployment_id: DeploymentID,
        handle_id: str,
        self_node_id: str,
        self_actor_id: str,
        self_availability_zone: Optional[str],
        event_loop: asyncio.BaseEventLoop = None,
        _prefer_local_node_routing: bool = False,
        _router_cls: Optional[str] = None,
    ):
        """Used to assign requests to downstream replicas for a deployment.

        The scheduling behavior is delegated to a ReplicaScheduler; this is a thin
        wrapper that adds metrics and logging.
        """
        self._event_loop = event_loop
        self.deployment_id = deployment_id
        self.handle_id = handle_id

        if _router_cls:
            self._replica_scheduler = load_class(_router_cls)(
                event_loop=event_loop, deployment_id=deployment_id
            )
        else:
            self._replica_scheduler = PowerOfTwoChoicesReplicaScheduler(
                event_loop,
                deployment_id,
                _prefer_local_node_routing,
                RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING,
                self_node_id,
                self_actor_id,
                self_availability_zone,
            )

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
        self.num_queued_queries_gauge.set_default_tags(
            {"deployment": deployment_id.name, "application": deployment_id.app}
        )

        self.long_poll_client = LongPollClient(
            controller_handle,
            {
                (
                    LongPollNamespace.RUNNING_REPLICAS,
                    deployment_id,
                ): self.update_running_replicas,
                (
                    LongPollNamespace.AUTOSCALING_CONFIG,
                    deployment_id,
                ): self.update_autoscaling_config,
            },
            call_in_event_loop=event_loop,
        )

        # For autoscaling deployments.
        self.autoscaling_config = None
        # Track queries sent to replicas for the autoscaling algorithm.
        self.num_requests_sent_to_replicas = defaultdict(int)
        # We use Ray object ref callbacks to update state when tracking
        # number of requests running on replicas. The callbacks will be
        # called from a C++ thread into the router's async event loop,
        # so non-atomic read and write operations need to be guarded by
        # this thread-safe lock.
        self._queries_lock = threading.Lock()
        # Regularly aggregate and push autoscaling metrics to controller
        self.metrics_pusher = MetricsPusher()
        self.metrics_store = InMemoryMetricsStore()
        self.push_metrics_to_controller = controller_handle.record_handle_metrics.remote

    def update_running_replicas(self, running_replicas: List[RunningReplicaInfo]):
        self._replica_scheduler.update_running_replicas(running_replicas)

        # Prune list of replica ids in self.num_queries_sent_to_replicas
        # We want to avoid self.num_queries_sent_to_replicas from
        # growing in memory as the deployment upscales and
        # downscales over time.
        running_replica_set = {replica.replica_tag for replica in running_replicas}
        with self._queries_lock:
            self.num_requests_sent_to_replicas = defaultdict(
                int,
                {
                    id: self.num_requests_sent_to_replicas[id]
                    for id, num_queries in self.num_requests_sent_to_replicas.items()
                    if num_queries or id in running_replica_set
                },
            )

    def update_autoscaling_config(self, autoscaling_config: AutoscalingConfig):
        self.autoscaling_config = autoscaling_config

        # Start the metrics pusher if autoscaling is enabled.
        if self.autoscaling_config:
            # Optimization for autoscaling cold start time. If there are
            # currently 0 replicas for the deployment, and there is at
            # least one queued query on this router, then immediately
            # push handle metric to the controller.
            if (
                len(self._replica_scheduler.curr_replicas) == 0
                and self.num_queued_queries
            ):
                self.push_metrics_to_controller(
                    self._get_aggregated_requests(), time.time()
                )

            if RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE:
                # Record number of queued + ongoing requests at regular
                # intervals into the in-memory metrics store
                self.metrics_pusher.register_or_update_task(
                    RECORD_METRICS_TASK_NAME,
                    self._add_autoscaling_metrics_point,
                    min(0.5, self.autoscaling_config.metrics_interval_s),
                )
                # Push metrics to the controller periodically.
                self.metrics_pusher.register_or_update_task(
                    PUSH_METRICS_TO_CONTROLLER_TASK_NAME,
                    self._get_aggregated_requests,
                    self.autoscaling_config.metrics_interval_s,
                    self.push_metrics_to_controller,
                )
            else:
                self.metrics_pusher.register_or_update_task(
                    PUSH_METRICS_TO_CONTROLLER_TASK_NAME,
                    self._get_aggregated_requests,
                    HANDLE_METRIC_PUSH_INTERVAL_S,
                    self.push_metrics_to_controller,
                )

            self.metrics_pusher.start()
        else:
            if self.metrics_pusher:
                self.metrics_pusher.shutdown()

    def _add_autoscaling_metrics_point(self):
        timestamp = time.time()
        self.metrics_store.add_metrics_point(
            {"queued": self.num_queued_queries}, timestamp
        )
        if RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE:
            self.metrics_store.add_metrics_point(
                self.num_requests_sent_to_replicas, timestamp
            )

    def _get_aggregated_requests(self):
        if RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE:
            look_back_period = self.autoscaling_config.look_back_period_s
            running_requests = {
                replica_id: self.metrics_store.window_average(
                    replica_id, time.time() - look_back_period
                )
                # If data hasn't been recorded yet, return current
                # number of queued and ongoing requests.
                or num_requests
                for replica_id, num_requests in self.num_requests_sent_to_replicas.items()  # noqa: E501
            }
            return (
                self.deployment_id,
                self.handle_id,
                self.num_queued_queries,
                running_requests,
            )
        else:
            return self.deployment_id, self.handle_id, self.num_queued_queries, dict()

    def process_finished_request(self, replica_tag, *args):
        with self._queries_lock:
            self.num_requests_sent_to_replicas[replica_tag] -= 1

    async def _replace_known_types_in_args(
        self, request_args: Tuple[Any], request_kwargs: Dict[str, Any]
    ) -> Tuple[Tuple[Any], Dict[str, Any]]:
        """Uses the `_PyObjScanner` to find and replace known types.

        1) Replaces `asyncio.Task` objects with their results. This is used for the old
           serve handle API and should be removed once that API is deprecated & removed.
        2) Replaces `DeploymentResponse` objects with their resolved object refs. This
           enables composition without explicitly calling `_to_object_ref`.
        """
        from ray.serve.handle import (
            DeploymentResponse,
            DeploymentResponseGenerator,
            _DeploymentResponseBase,
        )

        scanner = _PyObjScanner(source_type=(asyncio.Task, _DeploymentResponseBase))

        try:
            tasks = []
            responses = []
            replacement_table = {}
            objs = scanner.find_nodes((request_args, request_kwargs))
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

            for task in tasks:
                # NOTE(edoakes): this is a hack to enable the legacy behavior of passing
                # `asyncio.Task` objects directly to downstream handle calls without
                # `await`. Because the router now runs on a separate loop, the
                # `asyncio.Task` can't directly be awaited here. So we use the
                # thread-safe `concurrent.futures.Future` instead.
                # This can be removed when `RayServeHandle` is fully deprecated.
                if hasattr(task, "_ray_serve_object_ref_future"):
                    future = task._ray_serve_object_ref_future
                    replacement_table[task] = await asyncio.wrap_future(future)
                else:
                    replacement_table[task] = task

            # Gather `DeploymentResponse` object refs concurrently.
            if len(responses) > 0:
                obj_refs = await asyncio.gather(
                    *[r._to_object_ref() for r in responses]
                )
                replacement_table.update((zip(responses, obj_refs)))

            return scanner.replace_nodes(replacement_table)
        finally:
            # Make the scanner GC-able to avoid memory leaks.
            scanner.clear()

    async def assign_request(
        self,
        request_meta: RequestMetadata,
        *request_args,
        **request_kwargs,
    ) -> Union[ray.ObjectRef, "ray._raylet.ObjectRefGenerator"]:
        """Assign a query to a replica and return the resulting object_ref."""

        self.num_router_requests.inc(tags={"route": request_meta.route})
        self.num_queued_queries += 1
        self.num_queued_queries_gauge.set(self.num_queued_queries)

        # Optimization: if there are currently zero replicas for a deployment,
        # push handle metric to controller to allow for fast cold start time.
        # Only do it for the first query to arrive on the router.
        # NOTE(zcin): this is making the assumption that this method DOES
        # NOT give up the async event loop above this conditional. If
        # you need to yield the event loop above this conditional, you
        # will need to remove the check "self.num_queued_queries == 1"
        if (
            self.autoscaling_config
            and len(self._replica_scheduler.curr_replicas) == 0
            and self.num_queued_queries == 1
        ):
            self.push_metrics_to_controller(
                self._get_aggregated_requests(), time.time()
            )

        try:
            request_args, request_kwargs = await self._replace_known_types_in_args(
                request_args, request_kwargs
            )
            query = Query(
                args=list(request_args),
                kwargs=request_kwargs,
                metadata=request_meta,
            )
            ref, replica_tag = await self._replica_scheduler.assign_replica(query)

            # Keep track of requests that have been sent out to replicas
            if RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE:
                self.num_requests_sent_to_replicas[replica_tag] += 1
                callback = partial(self.process_finished_request, replica_tag)
                if isinstance(ref, ray.ObjectRef):
                    ref._on_completed(callback)
                else:
                    ref.completed()._on_completed(callback)

            return ref
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
