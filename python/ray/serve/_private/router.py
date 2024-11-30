import asyncio
import concurrent.futures
import logging
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import contextmanager
from functools import partial
from typing import Any, Coroutine, DefaultDict, Dict, List, Optional, Tuple, Union

import ray
from ray.actor import ActorHandle
from ray.exceptions import ActorDiedError, ActorUnavailableError, RayError
from ray.serve._private.common import (
    DeploymentHandleSource,
    DeploymentID,
    ReplicaID,
    RequestMetadata,
    RunningReplicaInfo,
)
from ray.serve._private.config import DeploymentConfig
from ray.serve._private.constants import (
    HANDLE_METRIC_PUSH_INTERVAL_S,
    RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE,
    RAY_SERVE_HANDLE_AUTOSCALING_METRIC_RECORD_PERIOD_S,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.long_poll import LongPollClient, LongPollNamespace
from ray.serve._private.metrics_utils import InMemoryMetricsStore, MetricsPusher
from ray.serve._private.replica_result import ReplicaResult
from ray.serve._private.replica_scheduler import PendingRequest, ReplicaScheduler
from ray.serve._private.utils import resolve_deployment_response
from ray.serve.config import AutoscalingConfig
from ray.serve.exceptions import BackPressureError
from ray.util import metrics

logger = logging.getLogger(SERVE_LOGGER_NAME)


QUEUED_REQUESTS_KEY = "queued"


class RouterMetricsManager:
    """Manages metrics for the router."""

    PUSH_METRICS_TO_CONTROLLER_TASK_NAME = "push_metrics_to_controller"
    RECORD_METRICS_TASK_NAME = "record_metrics"

    def __init__(
        self,
        deployment_id: DeploymentID,
        handle_id: str,
        self_actor_id: str,
        handle_source: DeploymentHandleSource,
        controller_handle: ActorHandle,
        router_requests_counter: metrics.Counter,
        queued_requests_gauge: metrics.Gauge,
        running_requests_gauge: metrics.Gauge,
    ):
        self._handle_id = handle_id
        self._deployment_id = deployment_id
        self._self_actor_id = self_actor_id
        self._handle_source = handle_source
        self._controller_handle = controller_handle

        # Exported metrics
        self.num_router_requests = router_requests_counter
        self.num_router_requests.set_default_tags(
            {
                "deployment": deployment_id.name,
                "application": deployment_id.app_name,
                "handle": self._handle_id,
                "actor_id": self._self_actor_id,
            }
        )

        self.num_queued_requests = 0
        self.num_queued_requests_gauge = queued_requests_gauge
        self.num_queued_requests_gauge.set_default_tags(
            {
                "deployment": deployment_id.name,
                "application": deployment_id.app_name,
                "handle": self._handle_id,
                "actor_id": self._self_actor_id,
            }
        )
        self.num_queued_requests_gauge.set(0)

        # Track queries sent to replicas for the autoscaling algorithm.
        self.num_requests_sent_to_replicas: DefaultDict[ReplicaID, int] = defaultdict(
            int
        )
        self.num_running_requests_gauge = running_requests_gauge
        self.num_running_requests_gauge.set_default_tags(
            {
                "deployment": deployment_id.name,
                "application": deployment_id.app_name,
                "handle": self._handle_id,
                "actor_id": self._self_actor_id,
            }
        )
        # We use Ray object ref callbacks to update state when tracking
        # number of requests running on replicas. The callbacks will be
        # called from a C++ thread into the router's async event loop,
        # so non-atomic read and write operations need to be guarded by
        # this thread-safe lock.
        self._queries_lock = threading.Lock()
        # Regularly aggregate and push autoscaling metrics to controller
        self.metrics_pusher = MetricsPusher()
        self.metrics_store = InMemoryMetricsStore()
        self.deployment_config: Optional[DeploymentConfig] = None
        # Track whether the metrics manager has been shutdown
        self._shutdown: bool = False

    @contextmanager
    def wrap_request_assignment(self, request_meta: RequestMetadata):
        max_queued_requests = (
            self.deployment_config.max_queued_requests
            if self.deployment_config is not None
            else -1
        )
        if (
            max_queued_requests != -1
            and self.num_queued_requests >= max_queued_requests
        ):
            e = BackPressureError(
                num_queued_requests=self.num_queued_requests,
                max_queued_requests=max_queued_requests,
            )
            logger.warning(e.message)
            raise e

        try:
            self.inc_num_total_requests(request_meta.route)
            self.inc_num_queued_requests()

            yield
        finally:
            # If the request is disconnected before assignment, this coroutine
            # gets cancelled by the caller and an asyncio.CancelledError is
            # raised. The finally block ensures that num_queued_requests
            # is correctly decremented in this case.
            self.dec_num_queued_requests()

    def update_running_replicas(self, running_replicas: List[RunningReplicaInfo]):
        """Prune list of replica ids in self.num_queries_sent_to_replicas.

        We want to avoid self.num_queries_sent_to_replicas from growing
        in memory as the deployment upscales and downscales over time.
        """

        running_replica_set = {replica.replica_id for replica in running_replicas}
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
    def autoscaling_config(self) -> Optional[AutoscalingConfig]:
        if self.deployment_config is None:
            return None

        return self.deployment_config.autoscaling_config

    def update_deployment_config(
        self, deployment_config: DeploymentConfig, curr_num_replicas: int
    ):
        """Update the config for the deployment this router sends requests to."""

        if self._shutdown:
            return

        self.deployment_config = deployment_config

        # Start the metrics pusher if autoscaling is enabled.
        autoscaling_config = self.autoscaling_config
        if autoscaling_config:
            self.metrics_pusher.start()
            # Optimization for autoscaling cold start time. If there are
            # currently 0 replicas for the deployment, and there is at
            # least one queued request on this router, then immediately
            # push handle metric to the controller.
            if self.should_send_scaled_to_zero_optimized_push(curr_num_replicas):
                self.push_autoscaling_metrics_to_controller()

            if RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE:
                # Record number of queued + ongoing requests at regular
                # intervals into the in-memory metrics store
                self.metrics_pusher.register_or_update_task(
                    self.RECORD_METRICS_TASK_NAME,
                    self._add_autoscaling_metrics_point,
                    min(
                        RAY_SERVE_HANDLE_AUTOSCALING_METRIC_RECORD_PERIOD_S,
                        autoscaling_config.metrics_interval_s,
                    ),
                )
                # Push metrics to the controller periodically.
                self.metrics_pusher.register_or_update_task(
                    self.PUSH_METRICS_TO_CONTROLLER_TASK_NAME,
                    self.push_autoscaling_metrics_to_controller,
                    autoscaling_config.metrics_interval_s,
                )
            else:
                self.metrics_pusher.register_or_update_task(
                    self.PUSH_METRICS_TO_CONTROLLER_TASK_NAME,
                    self.push_autoscaling_metrics_to_controller,
                    HANDLE_METRIC_PUSH_INTERVAL_S,
                )

        else:
            if self.metrics_pusher:
                self.metrics_pusher.stop_tasks()

    def inc_num_total_requests(self, route: str):
        self.num_router_requests.inc(tags={"route": route})

    def inc_num_queued_requests(self):
        self.num_queued_requests += 1
        self.num_queued_requests_gauge.set(self.num_queued_requests)

    def dec_num_queued_requests(self):
        self.num_queued_requests -= 1
        self.num_queued_requests_gauge.set(self.num_queued_requests)

    def inc_num_running_requests_for_replica(self, replica_id: ReplicaID):
        with self._queries_lock:
            self.num_requests_sent_to_replicas[replica_id] += 1
            self.num_running_requests_gauge.set(
                sum(self.num_requests_sent_to_replicas.values())
            )

    def dec_num_running_requests_for_replica(self, replica_id: ReplicaID):
        with self._queries_lock:
            self.num_requests_sent_to_replicas[replica_id] -= 1
            self.num_running_requests_gauge.set(
                sum(self.num_requests_sent_to_replicas.values())
            )

    def should_send_scaled_to_zero_optimized_push(self, curr_num_replicas: int) -> bool:
        return (
            self.autoscaling_config is not None
            and curr_num_replicas == 0
            and self.num_queued_requests > 0
        )

    def push_autoscaling_metrics_to_controller(self):
        """Pushes queued and running request metrics to the controller.

        These metrics are used by the controller for autoscaling.
        """

        self._controller_handle.record_handle_metrics.remote(
            send_timestamp=time.time(),
            deployment_id=self._deployment_id,
            handle_id=self._handle_id,
            actor_id=self._self_actor_id,
            handle_source=self._handle_source,
            **self._get_aggregated_requests(),
        )

    def _add_autoscaling_metrics_point(self):
        """Adds metrics point for queued and running requests at replicas.

        Also prunes keys in the in memory metrics store with outdated datapoints.
        """

        timestamp = time.time()
        self.metrics_store.add_metrics_point(
            {QUEUED_REQUESTS_KEY: self.num_queued_requests}, timestamp
        )
        if RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE:
            self.metrics_store.add_metrics_point(
                self.num_requests_sent_to_replicas, timestamp
            )

        # Prevent in memory metrics store memory from growing
        start_timestamp = time.time() - self.autoscaling_config.look_back_period_s
        self.metrics_store.prune_keys_and_compact_data(start_timestamp)

    def _get_aggregated_requests(self):
        running_requests = dict()
        if RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE and self.autoscaling_config:
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

        return {
            "queued_requests": self.num_queued_requests,
            "running_requests": running_requests,
        }

    async def shutdown(self):
        """Shutdown metrics manager gracefully."""

        if self.metrics_pusher:
            await self.metrics_pusher.graceful_shutdown()

        self._shutdown = True


class Router(ABC):
    @abstractmethod
    def running_replicas_populated(self) -> bool:
        pass

    @abstractmethod
    def assign_request(
        self,
        request_meta: RequestMetadata,
        *request_args,
        **request_kwargs,
    ) -> concurrent.futures.Future[ReplicaResult]:
        pass

    @abstractmethod
    def shutdown(self) -> concurrent.futures.Future:
        pass


class AsyncioRouter:
    def __init__(
        self,
        controller_handle: ActorHandle,
        deployment_id: DeploymentID,
        handle_id: str,
        self_actor_id: str,
        handle_source: DeploymentHandleSource,
        event_loop: asyncio.BaseEventLoop,
        replica_scheduler: Optional[ReplicaScheduler],
        enable_strict_max_ongoing_requests: bool,
        resolve_request_arg_func: Coroutine = resolve_deployment_response,
    ):
        """Used to assign requests to downstream replicas for a deployment.

        The scheduling behavior is delegated to a ReplicaScheduler; this is a thin
        wrapper that adds metrics and logging.
        """

        self._event_loop = event_loop
        self.deployment_id = deployment_id
        self._enable_strict_max_ongoing_requests = enable_strict_max_ongoing_requests

        self._replica_scheduler: ReplicaScheduler = replica_scheduler
        self._resolve_request_arg_func = resolve_request_arg_func

        # Flipped to `True` once the router has received a non-empty
        # replica set at least once.
        self._running_replicas_populated: bool = False

        # The config for the deployment this router sends requests to will be broadcast
        # by the controller. That means it is not available until we get the first
        # update. This includes an optional autoscaling config.
        self.deployment_config: Optional[DeploymentConfig] = None

        # Initializing `self._metrics_manager` before `self.long_poll_client` is
        # necessary to avoid race condition where `self.update_deployment_config()`
        # might be called before `self._metrics_manager` instance is created.
        self._metrics_manager = RouterMetricsManager(
            deployment_id,
            handle_id,
            self_actor_id,
            handle_source,
            controller_handle,
            metrics.Counter(
                "serve_num_router_requests",
                description="The number of requests processed by the router.",
                tag_keys=("deployment", "route", "application", "handle", "actor_id"),
            ),
            metrics.Gauge(
                "serve_deployment_queued_queries",
                description=(
                    "The current number of queries to this deployment waiting"
                    " to be assigned to a replica."
                ),
                tag_keys=("deployment", "application", "handle", "actor_id"),
            ),
            metrics.Gauge(
                "serve_num_ongoing_requests_at_replicas",
                description=(
                    "The current number of requests to this deployment that "
                    "have been submitted to a replica."
                ),
                tag_keys=("deployment", "application", "handle", "actor_id"),
            ),
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
            call_in_event_loop=self._event_loop,
        )

    def running_replicas_populated(self) -> bool:
        return self._running_replicas_populated

    def update_running_replicas(self, running_replicas: List[RunningReplicaInfo]):
        self._replica_scheduler.update_running_replicas(running_replicas)
        self._metrics_manager.update_running_replicas(running_replicas)

        if running_replicas:
            self._running_replicas_populated = True

    def update_deployment_config(self, deployment_config: DeploymentConfig):
        self._metrics_manager.update_deployment_config(
            deployment_config,
            curr_num_replicas=len(self._replica_scheduler.curr_replicas),
        )

    async def _resolve_request_arguments(
        self, request_args: Tuple[Any], request_kwargs: Dict[str, Any]
    ) -> Tuple[Tuple[Any], Dict[str, Any]]:
        """Asynchronously resolve and replace top-level request args and kwargs."""
        new_args = list(request_args)
        new_kwargs = request_kwargs.copy()

        # Map from index -> task for resolving positional arg
        resolve_arg_tasks = {}
        for i, obj in enumerate(request_args):
            task = await self._resolve_request_arg_func(obj)
            if task is not None:
                resolve_arg_tasks[i] = task

        # Map from key -> task for resolving key-word arg
        resolve_kwarg_tasks = {}
        for k, obj in request_kwargs.items():
            task = await self._resolve_request_arg_func(obj)
            if task is not None:
                resolve_kwarg_tasks[k] = task

        # Gather all argument resolution tasks concurrently.
        if resolve_arg_tasks or resolve_kwarg_tasks:
            all_tasks = list(resolve_arg_tasks.values()) + list(
                resolve_kwarg_tasks.values()
            )
            await asyncio.wait(all_tasks)

        # Update new args and new kwargs with resolved arguments
        for index, task in resolve_arg_tasks.items():
            new_args[index] = task.result()
        for key, task in resolve_kwarg_tasks.items():
            new_kwargs[key] = task.result()

        # Return new args and new kwargs
        return new_args, new_kwargs

    def _process_finished_request(
        self,
        replica_id: ReplicaID,
        parent_request_id: str,
        response_id: str,
        result: Union[Any, RayError],
    ):
        self._metrics_manager.dec_num_running_requests_for_replica(replica_id)
        if isinstance(result, ActorDiedError):
            # Replica has died but controller hasn't notified the router yet.
            # Don't consider this replica for requests in the future, and retry
            # scheduling request.
            self._replica_scheduler.on_replica_actor_died(replica_id)
            logger.warning(
                f"{replica_id} will not be considered for future "
                "requests because it has died."
            )
        elif isinstance(result, ActorUnavailableError):
            # There are network issues, or replica has died but GCS is down so
            # ActorUnavailableError will be raised until GCS recovers. For the
            # time being, invalidate the cache entry so that we don't try to
            # send requests to this replica without actively probing, and retry
            # scheduling request.
            self._replica_scheduler.on_replica_actor_unavailable(replica_id)
            logger.warning(
                f"Request failed because {replica_id} is temporarily unavailable."
            )

    async def schedule_and_send_request(
        self, pr: PendingRequest
    ) -> Tuple[ReplicaResult, ReplicaID]:
        """Choose a replica for the request and send it.

        This will block indefinitely if no replicas are available to handle the
        request, so it's up to the caller to time out or cancel the request.
        """
        replica = await self._replica_scheduler.choose_replica_for_request(pr)

        # If the queue len cache is disabled or we're sending a request to Java,
        # then directly send the query and hand the response back. The replica will
        # never reject requests in this code path.
        if not self._enable_strict_max_ongoing_requests or replica.is_cross_language:
            return replica.send_request(pr), replica.replica_id

        while True:
            replica_result = None
            try:
                (
                    replica_result,
                    queue_len_info,
                ) = await replica.send_request_with_rejection(pr)
                self._replica_scheduler.on_new_queue_len_info(
                    replica.replica_id, queue_len_info
                )
                if queue_len_info.accepted:
                    return replica_result, replica.replica_id
            except asyncio.CancelledError:
                # NOTE(edoakes): this is not strictly necessary because there are
                # currently no `await` statements between getting the ref and returning,
                # but I'm adding it defensively.
                if replica_result is not None:
                    replica_result.cancel()

                raise
            except ActorDiedError:
                # Replica has died but controller hasn't notified the router yet.
                # Don't consider this replica for requests in the future, and retry
                # scheduling request.
                self._replica_scheduler.on_replica_actor_died(replica.replica_id)
                logger.warning(
                    f"{replica.replica_id} will not be considered for future "
                    "requests because it has died."
                )
            except ActorUnavailableError:
                # There are network issues, or replica has died but GCS is down so
                # ActorUnavailableError will be raised until GCS recovers. For the
                # time being, invalidate the cache entry so that we don't try to
                # send requests to this replica without actively probing, and retry
                # scheduling request.
                self._replica_scheduler.on_replica_actor_unavailable(replica.replica_id)
                logger.warning(f"{replica.replica_id} is temporarily unavailable.")

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
    ) -> ReplicaResult:
        """Assign a request to a replica and return the resulting object_ref."""

        response_id = uuid.uuid4()
        assign_request_task = asyncio.current_task()
        ray.serve.context._add_request_pending_assignment(
            request_meta.internal_request_id, response_id, assign_request_task
        )
        assign_request_task.add_done_callback(
            lambda _: ray.serve.context._remove_request_pending_assignment(
                request_meta.internal_request_id, response_id
            )
        )

        with self._metrics_manager.wrap_request_assignment(request_meta):
            # Optimization: if there are currently zero replicas for a deployment,
            # push handle metric to controller to allow for fast cold start time.
            if self._metrics_manager.should_send_scaled_to_zero_optimized_push(
                curr_num_replicas=len(self._replica_scheduler.curr_replicas)
            ):
                self._metrics_manager.push_autoscaling_metrics_to_controller()

            replica_result = None
            try:
                request_args, request_kwargs = await self._resolve_request_arguments(
                    request_args, request_kwargs
                )
                replica_result, replica_id = await self.schedule_and_send_request(
                    PendingRequest(
                        args=list(request_args),
                        kwargs=request_kwargs,
                        metadata=request_meta,
                    ),
                )

                # Keep track of requests that have been sent out to replicas
                if RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE:
                    _request_context = ray.serve.context._serve_request_context.get()
                    request_id: str = _request_context.request_id
                    self._metrics_manager.inc_num_running_requests_for_replica(
                        replica_id
                    )
                    callback = partial(
                        self._process_finished_request,
                        replica_id,
                        request_id,
                        response_id,
                    )
                    replica_result.add_done_callback(callback)

                return replica_result
            except asyncio.CancelledError:
                # NOTE(edoakes): this is not strictly necessary because
                # there are currently no `await` statements between
                # getting the ref and returning, but I'm adding it defensively.
                if replica_result is not None:
                    replica_result.cancel()

                raise

    async def shutdown(self):
        await self._metrics_manager.shutdown()


class SingletonThreadRouter(Router):
    """Wrapper class that runs an AsyncioRouter on a separate thread.

    The motivation for this is to avoid user code blocking the event loop and
    preventing the router from making progress.

    Maintains a singleton event loop running in a daemon thread that is shared by
    all AsyncioRouters.
    """

    _asyncio_loop: Optional[asyncio.AbstractEventLoop] = None
    _asyncio_loop_creation_lock = threading.Lock()

    def __init__(self, **passthrough_kwargs):
        assert (
            "event_loop" not in passthrough_kwargs
        ), "SingletonThreadRouter manages the router event loop."

        self._asyncio_router = AsyncioRouter(
            event_loop=self._get_singleton_asyncio_loop(), **passthrough_kwargs
        )

    @classmethod
    def _get_singleton_asyncio_loop(cls) -> asyncio.AbstractEventLoop:
        """Get singleton asyncio loop running in a daemon thread.

        This method is thread safe.
        """
        with cls._asyncio_loop_creation_lock:
            if cls._asyncio_loop is None:
                cls._asyncio_loop = asyncio.new_event_loop()
                thread = threading.Thread(
                    daemon=True,
                    target=cls._asyncio_loop.run_forever,
                )
                thread.start()

        return cls._asyncio_loop

    def running_replicas_populated(self) -> bool:
        return self._asyncio_router.running_replicas_populated()

    def assign_request(
        self,
        request_meta: RequestMetadata,
        *request_args,
        **request_kwargs,
    ) -> concurrent.futures.Future[ReplicaResult]:
        return asyncio.run_coroutine_threadsafe(
            self._asyncio_router.assign_request(
                request_meta, *request_args, **request_kwargs
            ),
            loop=self._asyncio_loop,
        )

    def shutdown(self) -> concurrent.futures.Future:
        return asyncio.run_coroutine_threadsafe(
            self._asyncio_router.shutdown(), loop=self._asyncio_loop
        )
