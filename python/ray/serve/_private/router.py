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
from ray.serve._private.config import DeploymentConfig
from ray.serve._private.constants import (
    HANDLE_METRIC_PUSH_INTERVAL_S,
    RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE,
    RAY_SERVE_ENABLE_QUEUE_LENGTH_CACHE,
    RAY_SERVE_ENABLE_STRICT_MAX_CONCURRENT_QUERIES,
    RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.long_poll import LongPollClient, LongPollNamespace
from ray.serve._private.metrics_utils import InMemoryMetricsStore, MetricsPusher
from ray.serve._private.replica_scheduler import (
    PendingRequest,
    PowerOfTwoChoicesReplicaScheduler,
)
from ray.serve.config import AutoscalingConfig
from ray.serve.exceptions import BackPressureError
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
        enable_queue_len_cache: bool = RAY_SERVE_ENABLE_QUEUE_LENGTH_CACHE,
        enable_strict_max_concurrent_queries: bool = RAY_SERVE_ENABLE_STRICT_MAX_CONCURRENT_QUERIES,  # noqa: E501
    ):
        """Used to assign requests to downstream replicas for a deployment.

        The scheduling behavior is delegated to a ReplicaScheduler; this is a thin
        wrapper that adds metrics and logging.
        """
        self._event_loop = event_loop
        self.deployment_id = deployment_id
        self.handle_id = handle_id
        self._enable_queue_len_cache = enable_queue_len_cache
        self._enable_strict_max_concurrent_queries = (
            enable_strict_max_concurrent_queries
        )

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
                use_replica_queue_len_cache=enable_queue_len_cache,
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
                    LongPollNamespace.DEPLOYMENT_CONFIG,
                    deployment_id,
                ): self.update_deployment_config,
            },
            call_in_event_loop=event_loop,
        )

        # The config for the deployment this router sends requests to will be broadcast
        # by the controller. That means it is not available until we get the first
        # update. This includes an optional autoscaling config.
        self.deployment_config: Optional[DeploymentConfig] = None
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

    @property
    def curr_autoscaling_config(self) -> Optional[AutoscalingConfig]:
        if self.deployment_config is None:
            return None

        return self.deployment_config.autoscaling_config

    def update_deployment_config(self, deployment_config: DeploymentConfig):
        """Update the config for the deployment this router sends requests to."""
        self.deployment_config = deployment_config

        # Start the metrics pusher if autoscaling is enabled.
        autoscaling_config = self.curr_autoscaling_config
        if autoscaling_config:
            # Optimization for autoscaling cold start time. If there are
            # currently 0 replicas for the deployment, and there is at
            # least one queued request on this router, then immediately
            # push handle metric to the controller.
            if (
                len(self._replica_scheduler.curr_replicas) == 0
                and self.num_queued_queries
            ):
                self.push_metrics_to_controller(
                    **self._get_aggregated_requests(), send_timestamp=time.time()
                )

            if RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE:
                # Record number of queued + ongoing requests at regular
                # intervals into the in-memory metrics store
                self.metrics_pusher.register_or_update_task(
                    RECORD_METRICS_TASK_NAME,
                    self._add_autoscaling_metrics_point,
                    min(0.5, autoscaling_config.metrics_interval_s),
                )
                # Push metrics to the controller periodically.
                self.metrics_pusher.register_or_update_task(
                    PUSH_METRICS_TO_CONTROLLER_TASK_NAME,
                    self._get_aggregated_requests,
                    autoscaling_config.metrics_interval_s,
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
        running_requests = dict()
        autoscaling_config = self.curr_autoscaling_config
        if RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE and autoscaling_config:
            look_back_period = autoscaling_config.look_back_period_s
            running_requests = {
                replica_id: self.metrics_store.window_average(
                    replica_id, time.time() - look_back_period
                )
                # If data hasn't been recorded yet, return current
                # number of queued and ongoing requests.
                or num_requests
                for replica_id, num_requests in self.num_requests_sent_to_replicas.items()  # noqa: E501
            }

        return {
            "deployment_id": self.deployment_id,
            "handle_id": self.handle_id,
            "queued_requests": self.num_queued_queries,
            "running_requests": running_requests,
        }

    def process_finished_request(self, replica_tag, *args):
        with self._queries_lock:
            self.num_requests_sent_to_replicas[replica_tag] -= 1

    async def _resolve_deployment_responses(
        self, request_args: Tuple[Any], request_kwargs: Dict[str, Any]
    ) -> Tuple[Tuple[Any], Dict[str, Any]]:
        """Replaces `DeploymentResponse` objects with their resolved object refs.

        Uses the `_PyObjScanner` to find and replace the objects. This
        enables composition without explicitly calling `_to_object_ref`.
        """
        from ray.serve.handle import (
            DeploymentResponse,
            DeploymentResponseGenerator,
            _DeploymentResponseBase,
        )

        scanner = _PyObjScanner(source_type=_DeploymentResponseBase)

        try:
            responses = []
            replacement_table = {}
            objs = scanner.find_nodes((request_args, request_kwargs))
            for obj in objs:
                if isinstance(obj, DeploymentResponseGenerator):
                    raise RuntimeError(
                        "Streaming deployment handle results cannot be passed to "
                        "downstream handle calls. If you have a use case requiring "
                        "this feature, please file a feature request on GitHub."
                    )
                elif isinstance(obj, DeploymentResponse):
                    responses.append(obj)

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

    async def schedule_and_send_request(
        self, pr: PendingRequest
    ) -> Tuple[Union[ray.ObjectRef, "ray._raylet.ObjectRefGenerator"], str]:
        """Choose a replica for the request and send it.

        This will block indefinitely if no replicas are available to handle the
        request, so it's up to the caller to time out or cancel the request.
        """
        replica = await self._replica_scheduler.choose_replica_for_request(pr)

        # If the queue len cache is disabled or we're sending a request to Java,
        # then directly send the query and hand the response back. The replica will
        # never reject requests in this code path.
        if not self._enable_strict_max_concurrent_queries or replica.is_cross_language:
            return replica.send_request(pr), replica.replica_id

        while True:
            obj_ref_or_gen, queue_len_info = await replica.send_request_with_rejection(
                pr
            )
            if self._enable_queue_len_cache:
                self._replica_scheduler.replica_queue_len_cache.update(
                    replica.replica_id, queue_len_info.num_ongoing_requests
                )
            if queue_len_info.accepted:
                return obj_ref_or_gen, replica.replica_id

            # If the replica rejects the request, retry the scheduling process. The
            # request will be placed on the front of the queue to avoid tail latencies.
            # TODO(edoakes): this retry procedure is not perfect because it'll reset the
            # process of choosing candidates replicas (i.e., for locality-awareness).
            replica = await self._replica_scheduler.choose_replica_for_request(
                pr, is_retry=True
            )

    async def assign_request(
        self,
        request_meta: RequestMetadata,
        *request_args,
        **request_kwargs,
    ) -> Union[ray.ObjectRef, "ray._raylet.ObjectRefGenerator"]:
        """Assign a request to a replica and return the resulting object_ref."""

        self.num_router_requests.inc(tags={"route": request_meta.route})

        num_queued_requests = self.num_queued_queries
        max_queued_requests = (
            self.deployment_config.max_queued_requests
            if self.deployment_config is not None
            else -1
        )
        if max_queued_requests != -1 and num_queued_requests >= max_queued_requests:
            e = BackPressureError(
                num_queued_requests=num_queued_requests,
                max_queued_requests=max_queued_requests,
            )
            logger.warning(e.message)
            raise e

        self.num_queued_queries += 1
        self.num_queued_queries_gauge.set(self.num_queued_queries)

        # Optimization: if there are currently zero replicas for a deployment,
        # push handle metric to controller to allow for fast cold start time.
        # Only do it for the first request to arrive on the router.
        # NOTE(zcin): this is making the assumption that this method DOES
        # NOT give up the async event loop above this conditional. If
        # you need to yield the event loop above this conditional, you
        # will need to remove the check "self.num_queued_queries == 1"
        if (
            self.curr_autoscaling_config
            and len(self._replica_scheduler.curr_replicas) == 0
            and self.num_queued_queries == 1
        ):
            self.push_metrics_to_controller(
                **self._get_aggregated_requests(), send_timestamp=time.time()
            )

        try:
            request_args, request_kwargs = await self._resolve_deployment_responses(
                request_args, request_kwargs
            )
            ref, replica_tag = await self.schedule_and_send_request(
                PendingRequest(
                    args=list(request_args),
                    kwargs=request_kwargs,
                    metadata=request_meta,
                ),
            )

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
            # If the request is disconnected before assignment, this coroutine
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
