import asyncio
import concurrent.futures
import logging
import threading
import time
import weakref
from abc import ABC, abstractmethod
from asyncio import AbstractEventLoop
from collections import defaultdict
from collections.abc import MutableMapping
from contextlib import contextmanager
from functools import lru_cache, partial
from typing import (
    Any,
    Callable,
    Coroutine,
    DefaultDict,
    Dict,
    List,
    Optional,
    Union,
)

import ray
from ray.actor import ActorHandle
from ray.exceptions import ActorDiedError, ActorUnavailableError, RayError
from ray.serve._private.common import (
    DeploymentHandleSource,
    DeploymentID,
    DeploymentTargetInfo,
    ReplicaID,
    RequestMetadata,
    RunningReplicaInfo,
)
from ray.serve._private.config import DeploymentConfig
from ray.serve._private.constants import (
    RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE,
    RAY_SERVE_HANDLE_AUTOSCALING_METRIC_PUSH_INTERVAL_S,
    RAY_SERVE_HANDLE_AUTOSCALING_METRIC_RECORD_INTERVAL_S,
    RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.long_poll import LongPollClient, LongPollNamespace
from ray.serve._private.metrics_utils import InMemoryMetricsStore, MetricsPusher
from ray.serve._private.replica_result import ReplicaResult
from ray.serve._private.request_router import PendingRequest, RequestRouter
from ray.serve._private.request_router.pow_2_router import (
    PowerOfTwoChoicesRequestRouter,
)
from ray.serve._private.request_router.replica_wrapper import RunningReplica
from ray.serve._private.usage import ServeUsageTag
from ray.serve._private.utils import (
    generate_request_id,
    resolve_deployment_response,
    run_coroutine_or_future_threadsafe,
)
from ray.serve.config import AutoscalingConfig
from ray.serve.exceptions import BackPressureError, DeploymentUnavailableError
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
        # The config for the deployment this router sends requests to will be broadcast
        # by the controller. That means it is not available until we get the first
        # update. This includes an optional autoscaling config.
        self._deployment_config: Optional[DeploymentConfig] = None
        # Track whether the metrics manager has been shutdown
        self._shutdown: bool = False

    @contextmanager
    def wrap_request_assignment(self, request_meta: RequestMetadata):
        max_queued_requests = (
            self._deployment_config.max_queued_requests
            if self._deployment_config is not None
            else -1
        )
        if (
            max_queued_requests != -1
            and self.num_queued_requests >= max_queued_requests
        ):
            # Due to the async nature of request handling, we may reject more requests
            # than strictly necessary. This is more likely to happen during
            # high concurrency. Here's why:
            #
            # When multiple requests arrive simultaneously with max_queued_requests=1:
            # 1. First request increments num_queued_requests to 1
            # 2. Before that request gets assigned to a replica and decrements the counter,
            #    we yield to the event loop
            # 3. Other requests see num_queued_requests=1 and get rejected, even though
            #    the first request will soon free up the queue slot
            #
            # For example, with max_queued_requests=1 and 4 simultaneous requests:
            # - Request 1 gets queued (num_queued_requests=1)
            # - Requests 2,3,4 get rejected since queue appears full
            # - Request 1 gets assigned and frees queue slot (num_queued_requests=0)
            # - But we already rejected Request 2 which could have been queued
            e = BackPressureError(
                num_queued_requests=self.num_queued_requests,
                max_queued_requests=max_queued_requests,
            )
            logger.warning(e.message)
            raise e

        self.inc_num_total_requests(request_meta.route)
        yield

    @contextmanager
    def wrap_queued_request(self, is_retry: bool, num_curr_replicas: int):
        """Increment queued requests gauge and maybe push autoscaling metrics to controller."""
        try:
            self.inc_num_queued_requests()
            # Optimization: if there are currently zero replicas for a deployment,
            # push handle metric to controller to allow for fast cold start time.
            # Only do this on the first attempt to route the request.
            if not is_retry and self.should_send_scaled_to_zero_optimized_push(
                curr_num_replicas=num_curr_replicas
            ):
                self.push_autoscaling_metrics_to_controller()

            yield
        finally:
            # If the request is disconnected before assignment, this coroutine
            # gets cancelled by the caller and an asyncio.CancelledError is
            # raised. The finally block ensures that num_queued_requests
            # is correctly decremented in this case.
            self.dec_num_queued_requests()

    def _update_running_replicas(self, running_replicas: List[RunningReplicaInfo]):
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
        if self._deployment_config is None:
            return None

        return self._deployment_config.autoscaling_config

    def update_deployment_config(
        self, deployment_config: DeploymentConfig, curr_num_replicas: int
    ):
        """Update the config for the deployment this router sends requests to."""

        if self._shutdown:
            return

        self._deployment_config = deployment_config

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
                        RAY_SERVE_HANDLE_AUTOSCALING_METRIC_RECORD_INTERVAL_S,
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
                    RAY_SERVE_HANDLE_AUTOSCALING_METRIC_PUSH_INTERVAL_S,
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


async def create_event() -> asyncio.Event:
    """Helper to create an asyncio event in the current event loop."""
    return asyncio.Event()


class AsyncioRouter:
    def __init__(
        self,
        controller_handle: ActorHandle,
        deployment_id: DeploymentID,
        handle_id: str,
        self_actor_id: str,
        handle_source: DeploymentHandleSource,
        event_loop: asyncio.BaseEventLoop,
        enable_strict_max_ongoing_requests: bool,
        node_id: str,
        availability_zone: Optional[str],
        prefer_local_node_routing: bool,
        resolve_request_arg_func: Coroutine = resolve_deployment_response,
        request_router_class: Optional[Callable] = None,
        request_router_kwargs: Optional[Dict[str, Any]] = None,
        request_router: Optional[RequestRouter] = None,
        _request_router_initialized_event: Optional[asyncio.Event] = None,
    ):
        """Used to assign requests to downstream replicas for a deployment.

        The routing behavior is delegated to a RequestRouter; this is a thin
        wrapper that adds metrics and logging.
        """
        self._controller_handle = controller_handle
        self.deployment_id = deployment_id
        self._self_actor_id = self_actor_id
        self._handle_source = handle_source
        self._event_loop = event_loop
        self._request_router_class = request_router_class
        self._request_router_kwargs = (
            request_router_kwargs if request_router_kwargs else {}
        )
        self._enable_strict_max_ongoing_requests = enable_strict_max_ongoing_requests
        self._node_id = node_id
        self._availability_zone = availability_zone
        self._prefer_local_node_routing = prefer_local_node_routing
        # By default, deployment is available unless we receive news
        # otherwise through a long poll broadcast from the controller.
        self._deployment_available = True

        # The request router will be lazy loaded to decouple form the initialization.
        self._request_router: Optional[RequestRouter] = request_router

        if _request_router_initialized_event:
            self._request_router_initialized = _request_router_initialized_event
        else:
            future = asyncio.run_coroutine_threadsafe(create_event(), self._event_loop)
            self._request_router_initialized = future.result()

        if self._request_router:
            self._request_router_initialized.set()
        self._resolve_request_arg_func = resolve_request_arg_func
        self._running_replicas: Optional[List[RunningReplicaInfo]] = None

        # Flipped to `True` once the router has received a non-empty
        # replica set at least once.
        self._running_replicas_populated: bool = False

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

        # The Router needs to stay informed about changes to the target deployment's
        # running replicas and deployment config. We do this via the long poll system.
        # However, for efficiency, we don't want to create a LongPollClient for every
        # DeploymentHandle, so we use a shared LongPollClient that all Routers
        # register themselves with. But first, the router needs to get a fast initial
        # update so that it can start serving requests, which we do with a dedicated
        # LongPollClient that stops running once the shared client takes over.

        self.long_poll_client = LongPollClient(
            controller_handle,
            {
                (
                    LongPollNamespace.DEPLOYMENT_TARGETS,
                    deployment_id,
                ): self.update_deployment_targets,
                (
                    LongPollNamespace.DEPLOYMENT_CONFIG,
                    deployment_id,
                ): self.update_deployment_config,
            },
            call_in_event_loop=self._event_loop,
        )

        shared = SharedRouterLongPollClient.get_or_create(
            controller_handle, self._event_loop
        )
        shared.register(self)

    @property
    def request_router(self) -> Optional[RequestRouter]:
        """Get and lazy loading request router.

        If the request_router_class not provided, and the request router is not
        yet initialized, then it will return None. Otherwise, if request router
        is not yet initialized, it will be initialized and returned. Also,
        setting `self._request_router_initialized` to signal that the request
        router is initialized.
        """
        if not self._request_router and self._request_router_class:
            request_router = self._request_router_class(
                deployment_id=self.deployment_id,
                handle_source=self._handle_source,
                self_node_id=self._node_id,
                self_actor_id=self._self_actor_id,
                self_actor_handle=ray.get_runtime_context().current_actor
                if ray.get_runtime_context().get_actor_id()
                else None,
                # Streaming ObjectRefGenerators are not supported in Ray Client
                use_replica_queue_len_cache=self._enable_strict_max_ongoing_requests,
                create_replica_wrapper_func=lambda r: RunningReplica(r),
                prefer_local_node_routing=self._prefer_local_node_routing,
                prefer_local_az_routing=RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING,
                self_availability_zone=self._availability_zone,
            )
            request_router.initialize_state(**(self._request_router_kwargs))

            # Populate the running replicas if they are already available.
            if self._running_replicas is not None:
                request_router._update_running_replicas(self._running_replicas)

            self._request_router = request_router
            self._request_router_initialized.set()

            # Log usage telemetry to indicate that custom request router
            # feature is being used in this cluster.
            if self._request_router_class is not PowerOfTwoChoicesRequestRouter:
                ServeUsageTag.CUSTOM_REQUEST_ROUTER_USED.record("1")
        return self._request_router

    def running_replicas_populated(self) -> bool:
        return self._running_replicas_populated

    def update_deployment_targets(self, deployment_target_info: DeploymentTargetInfo):
        self._deployment_available = deployment_target_info.is_available

        running_replicas = deployment_target_info.running_replicas
        if self.request_router:
            self.request_router._update_running_replicas(running_replicas)
        else:
            # In this case, the request router hasn't been initialized yet.
            # Store the running replicas so that we can update the request
            # router once it is initialized.
            self._running_replicas = running_replicas
        self._metrics_manager._update_running_replicas(running_replicas)

        if running_replicas:
            self._running_replicas_populated = True

    def update_deployment_config(self, deployment_config: DeploymentConfig):
        self._request_router_class = (
            deployment_config.request_router_config.get_request_router_class()
        )
        self._request_router_kwargs = (
            deployment_config.request_router_config.request_router_kwargs
        )
        self._metrics_manager.update_deployment_config(
            deployment_config,
            curr_num_replicas=len(self.request_router.curr_replicas),
        )

    async def _resolve_request_arguments(
        self,
        pr: PendingRequest,
    ) -> None:
        """Asynchronously resolve and replace top-level request args and kwargs."""
        if pr.resolved:
            return

        new_args = list(pr.args)
        new_kwargs = pr.kwargs.copy()

        # Map from index -> task for resolving positional arg
        resolve_arg_tasks = {}
        for i, obj in enumerate(pr.args):
            task = await self._resolve_request_arg_func(obj, pr.metadata)
            if task is not None:
                resolve_arg_tasks[i] = task

        # Map from key -> task for resolving key-word arg
        resolve_kwarg_tasks = {}
        for k, obj in pr.kwargs.items():
            task = await self._resolve_request_arg_func(obj, pr.metadata)
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

        pr.args = new_args
        pr.kwargs = new_kwargs
        pr.resolved = True

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
            # routing request.
            if self.request_router:
                self.request_router.on_replica_actor_died(replica_id)
            logger.warning(
                f"{replica_id} will not be considered for future "
                "requests because it has died."
            )
        elif isinstance(result, ActorUnavailableError):
            # There are network issues, or replica has died but GCS is down so
            # ActorUnavailableError will be raised until GCS recovers. For the
            # time being, invalidate the cache entry so that we don't try to
            # send requests to this replica without actively probing, and retry
            # routing request.
            if self.request_router:
                self.request_router.on_replica_actor_unavailable(replica_id)
            logger.warning(
                f"Request failed because {replica_id} is temporarily unavailable."
            )

    async def _route_and_send_request_once(
        self,
        pr: PendingRequest,
        response_id: str,
        is_retry: bool,
    ) -> Optional[ReplicaResult]:
        result: Optional[ReplicaResult] = None
        replica: Optional[RunningReplica] = None
        try:
            num_curr_replicas = len(self.request_router.curr_replicas)
            with self._metrics_manager.wrap_queued_request(is_retry, num_curr_replicas):
                # If the pending request is uninitialized, we do so by resolving the
                # request arguments. This should only be done once per request, and
                # should happen after incrementing `num_queued_requests`, so that Serve
                # can upscale the downstream deployment while arguments are resolving.
                if not pr.resolved:
                    await self._resolve_request_arguments(pr)

                replica = await self.request_router._choose_replica_for_request(
                    pr, is_retry=is_retry
                )

                # If the queue len cache is disabled or we're sending a request to Java,
                # then directly send the query and hand the response back. The replica will
                # never reject requests in this code path.
                with_rejection = (
                    self._enable_strict_max_ongoing_requests
                    and not replica.is_cross_language
                )
                result = replica.try_send_request(pr, with_rejection=with_rejection)

                # Proactively update the queue length cache.
                self.request_router.on_send_request(replica.replica_id)

            # Keep track of requests that have been sent out to replicas
            if RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE:
                _request_context = ray.serve.context._get_serve_request_context()
                request_id: str = _request_context.request_id
                self._metrics_manager.inc_num_running_requests_for_replica(
                    replica.replica_id
                )
                callback = partial(
                    self._process_finished_request,
                    replica.replica_id,
                    request_id,
                    response_id,
                )
                result.add_done_callback(callback)

            if not with_rejection:
                return result

            queue_info = await result.get_rejection_response()
            self.request_router.on_new_queue_len_info(replica.replica_id, queue_info)
            if queue_info.accepted:
                self.request_router.on_request_routed(pr, replica.replica_id, result)
                return result

        except asyncio.CancelledError:
            # NOTE(edoakes): this is not strictly necessary because there are
            # currently no `await` statements between getting the ref and returning,
            # but I'm adding it defensively.
            if result is not None:
                result.cancel()

            raise
        except ActorDiedError:
            # Replica has died but controller hasn't notified the router yet.
            # Don't consider this replica for requests in the future, and retry
            # routing request.
            if replica is not None:
                self.request_router.on_replica_actor_died(replica.replica_id)
                logger.warning(
                    f"{replica.replica_id} will not be considered for future "
                    "requests because it has died."
                )
        except ActorUnavailableError:
            # There are network issues, or replica has died but GCS is down so
            # ActorUnavailableError will be raised until GCS recovers. For the
            # time being, invalidate the cache entry so that we don't try to
            # send requests to this replica without actively probing, and retry
            # routing request.
            if replica is not None:
                self.request_router.on_replica_actor_unavailable(replica.replica_id)
                logger.warning(f"{replica.replica_id} is temporarily unavailable.")

        return None

    async def route_and_send_request(
        self,
        pr: PendingRequest,
        response_id: str,
    ) -> ReplicaResult:
        """Choose a replica for the request and send it.

        This will block indefinitely if no replicas are available to handle the
        request, so it's up to the caller to time out or cancel the request.
        """
        # Wait for the router to be initialized before sending the request.
        await self._request_router_initialized.wait()

        is_retry = False
        while True:
            result = await self._route_and_send_request_once(
                pr,
                response_id,
                is_retry,
            )
            if result is not None:
                return result

            # If the replica rejects the request, retry the routing process. The
            # request will be placed on the front of the queue to avoid tail latencies.
            # TODO(edoakes): this retry procedure is not perfect because it'll reset the
            # process of choosing candidates replicas (i.e., for locality-awareness).
            is_retry = True

    async def assign_request(
        self,
        request_meta: RequestMetadata,
        *request_args,
        **request_kwargs,
    ) -> ReplicaResult:
        """Assign a request to a replica and return the resulting object_ref."""

        if not self._deployment_available:
            raise DeploymentUnavailableError(self.deployment_id)

        response_id = generate_request_id()
        assign_request_task = asyncio.current_task()
        ray.serve.context._add_request_pending_assignment(
            request_meta.internal_request_id, response_id, assign_request_task
        )
        assign_request_task.add_done_callback(
            lambda _: ray.serve.context._remove_request_pending_assignment(
                request_meta.internal_request_id, response_id
            )
        )

        # Wait for the router to be initialized before sending the request.
        await self._request_router_initialized.wait()

        with self._metrics_manager.wrap_request_assignment(request_meta):
            replica_result = None
            try:
                replica_result = await self.route_and_send_request(
                    PendingRequest(
                        args=list(request_args),
                        kwargs=request_kwargs,
                        metadata=request_meta,
                    ),
                    response_id,
                )
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
        """Routes assign_request call on the internal asyncio loop.

        This method uses `run_coroutine_threadsafe` to execute the actual request
        assignment logic (`_asyncio_router.assign_request`) on the dedicated
        asyncio event loop thread. It returns a `concurrent.futures.Future` that
        can be awaited or queried from the calling thread.

        Returns:
            A concurrent.futures.Future resolving to the ReplicaResult representing
            the assigned request.
        """

        def asyncio_future_callback(
            asyncio_future: asyncio.Future, concurrent_future: concurrent.futures.Future
        ):
            """Callback attached to the asyncio Task running assign_request.

            This runs when the asyncio Task finishes (completes, fails, or is cancelled).
            Its primary goal is to propagate cancellation initiated via the
            `concurrent_future` back to the `ReplicaResult` in situations where
            asyncio_future didn't see the cancellation event in time. Think of it
            like a second line of defense for cancellation of replica results.
            """
            # Check if the cancellation originated from the concurrent.futures.Future
            if (
                concurrent_future.cancelled()
                and not asyncio_future.cancelled()
                and asyncio_future.exception() is None
            ):
                result: ReplicaResult = asyncio_future.result()
                logger.info(
                    "Asyncio task completed despite cancellation attempt. "
                    "Attempting to cancel the request that was assigned to a replica."
                )
                result.cancel()

        task = self._asyncio_loop.create_task(
            self._asyncio_router.assign_request(
                request_meta, *request_args, **request_kwargs
            )
        )
        # Route the actual request assignment coroutine on the asyncio loop thread.
        concurrent_future = run_coroutine_or_future_threadsafe(
            task,
            loop=self._asyncio_loop,
        )
        task.add_done_callback(lambda _: asyncio_future_callback(_, concurrent_future))
        return concurrent_future

    def shutdown(self) -> concurrent.futures.Future:
        return asyncio.run_coroutine_threadsafe(
            self._asyncio_router.shutdown(), loop=self._asyncio_loop
        )


class SharedRouterLongPollClient:
    def __init__(self, controller_handle: ActorHandle, event_loop: AbstractEventLoop):
        self.controller_handler = controller_handle
        self.event_loop = event_loop

        # We use a WeakSet to store the Routers so that we don't prevent them
        # from being garbage-collected.
        self.routers: MutableMapping[
            DeploymentID, weakref.WeakSet[AsyncioRouter]
        ] = defaultdict(weakref.WeakSet)

        # Creating the LongPollClient implicitly starts it
        self.long_poll_client = LongPollClient(
            controller_handle,
            key_listeners={},
            call_in_event_loop=self.event_loop,
        )

    @classmethod
    @lru_cache(maxsize=None)
    def get_or_create(
        cls, controller_handle: ActorHandle, event_loop: AbstractEventLoop
    ) -> "SharedRouterLongPollClient":
        shared = cls(controller_handle=controller_handle, event_loop=event_loop)
        logger.info(f"Started {shared}.")
        return shared

    def update_deployment_targets(
        self,
        deployment_target_info: DeploymentTargetInfo,
        deployment_id: DeploymentID,
    ) -> None:
        for router in self.routers[deployment_id]:
            router.update_deployment_targets(deployment_target_info)
            router.long_poll_client.stop()

    def update_deployment_config(
        self, deployment_config: DeploymentConfig, deployment_id: DeploymentID
    ) -> None:
        for router in self.routers[deployment_id]:
            router.update_deployment_config(deployment_config)
            router.long_poll_client.stop()

    def register(self, router: AsyncioRouter) -> None:
        # We need to run the underlying method in the same event loop that runs
        # the long poll loop, because we need to mutate the mapping of routers,
        # which are also being iterated over by the key listener callbacks.
        # If those happened concurrently in different threads,
        # we could get a `RuntimeError: Set changed size during iteration`.
        # See https://github.com/ray-project/ray/pull/53613 for more details.
        self.event_loop.call_soon_threadsafe(self._register, router)

    def _register(self, router: AsyncioRouter) -> None:
        self.routers[router.deployment_id].add(router)

        # Remove the entries for any deployment ids that no longer have any routers.
        # The WeakSets will automatically lose track of Routers that get GC'd,
        # but the outer dict will keep the key around, so we need to clean up manually.
        # Note the list(...) to avoid mutating self.routers while iterating over it.
        for deployment_id, routers in list(self.routers.items()):
            if not routers:
                self.routers.pop(deployment_id)

        # Register the new listeners on the long poll client.
        # Some of these listeners may already exist, but it's safe to add them again.
        key_listeners = {
            (LongPollNamespace.DEPLOYMENT_TARGETS, deployment_id): partial(
                self.update_deployment_targets, deployment_id=deployment_id
            )
            for deployment_id in self.routers.keys()
        } | {
            (LongPollNamespace.DEPLOYMENT_CONFIG, deployment_id): partial(
                self.update_deployment_config, deployment_id=deployment_id
            )
            for deployment_id in self.routers.keys()
        }
        self.long_poll_client.add_key_listeners(key_listeners)


class CurrentLoopRouter(Router):
    """Wrapper class that runs an AsyncioRouter on the current asyncio loop.
    Note that this class is NOT THREAD-SAFE, and all methods are expected to be
    invoked from a single asyncio event loop.
    """

    def __init__(self, **passthrough_kwargs):
        assert (
            "event_loop" not in passthrough_kwargs
        ), "CurrentLoopRouter uses the current event loop."

        self._asyncio_loop = asyncio.get_running_loop()
        self._asyncio_router = AsyncioRouter(
            event_loop=self._asyncio_loop,
            _request_router_initialized_event=asyncio.Event(),
            **passthrough_kwargs,
        )

    def running_replicas_populated(self) -> bool:
        return self._asyncio_router.running_replicas_populated()

    def assign_request(
        self,
        request_meta: RequestMetadata,
        *request_args,
        **request_kwargs,
    ) -> asyncio.Future[ReplicaResult]:
        return self._asyncio_loop.create_task(
            self._asyncio_router.assign_request(
                request_meta, *request_args, **request_kwargs
            ),
        )

    def shutdown(self) -> asyncio.Future:
        return self._asyncio_loop.create_task(self._asyncio_router.shutdown())
