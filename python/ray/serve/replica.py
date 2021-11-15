import asyncio
import logging
import pickle
import traceback
import inspect
from typing import Any, Callable, Optional, Tuple, Dict
import time

import starlette.responses

import ray
from ray import cloudpickle
from ray.actor import ActorHandle
from ray._private.async_compat import sync_to_async

from ray.serve.autoscaling_metrics import start_metrics_pusher
from ray.serve.common import str, ReplicaTag
from ray.serve.config import DeploymentConfig
from ray.serve.http_util import ASGIHTTPSender
from ray.serve.utils import parse_request_item, _get_logger
from ray.serve.exceptions import RayServeException
from ray.util import metrics
from ray.serve.router import Query, RequestMetadata
from ray.serve.constants import (
    RECONFIGURE_METHOD,
    DEFAULT_LATENCY_BUCKET_MS,
)
from ray.serve.version import DeploymentVersion
from ray.exceptions import RayTaskError

logger = _get_logger()


def create_replica_wrapper(name: str, serialized_deployment_def: bytes):
    """Creates a replica class wrapping the provided function or class.

    This approach is picked over inheritance to avoid conflict between user
    provided class and the RayServeReplica class.
    """
    serialized_deployment_def = serialized_deployment_def

    # TODO(architkulkarni): Add type hints after upgrading cloudpickle
    class RayServeWrappedReplica(object):
        async def __init__(self, deployment_name, replica_tag, init_args,
                           init_kwargs, deployment_config_proto_bytes: bytes,
                           version: DeploymentVersion, controller_name: str,
                           detached: bool):
            deployment_def = cloudpickle.loads(serialized_deployment_def)
            deployment_config = DeploymentConfig.from_proto_bytes(
                deployment_config_proto_bytes)

            if inspect.isfunction(deployment_def):
                is_function = True
            elif inspect.isclass(deployment_def):
                is_function = False
            else:
                assert False, ("deployment_def must be function, class, or "
                               "corresponding import path.")

            # Set the controller name so that serve.connect() in the user's
            # code will connect to the instance that this deployment is running
            # in.
            ray.serve.api._set_internal_replica_context(
                deployment_name,
                replica_tag,
                controller_name,
                servable_object=None)

            assert controller_name, "Must provide a valid controller_name"

            controller_namespace = ray.serve.api._get_controller_namespace(
                detached)
            controller_handle = ray.get_actor(
                controller_name, namespace=controller_namespace)

            # This closure initializes user code and finalizes replica
            # startup. By splitting the initialization step like this,
            # we can already access this actor before the user code
            # has finished initializing.
            # The supervising state manager can then wait
            # for allocation of this replica by using the `is_allocated`
            # method. After that, it calls `reconfigure` to trigger
            # user code initialization.
            async def initialize_replica():
                if is_function:
                    _callable = deployment_def
                else:
                    # This allows deployments to define an async __init__
                    # method (required for FastAPI).
                    _callable = deployment_def.__new__(deployment_def)
                    await sync_to_async(_callable.__init__)(*init_args,
                                                            **init_kwargs)
                # Setting the context again to update the servable_object.
                ray.serve.api._set_internal_replica_context(
                    deployment_name,
                    replica_tag,
                    controller_name,
                    servable_object=_callable)

                self.replica = RayServeReplica(
                    _callable, deployment_name, replica_tag, deployment_config,
                    deployment_config.user_config, version, is_function,
                    controller_handle)

            # Is it fine that replica is None here?
            # Should we add a check in all methods that use self.replica
            # or, alternatively, create an async get_replica() method?
            self.replica = None
            self._initialize_replica = initialize_replica

            # asyncio.Event used to signal that the replica is shutting down.
            self.shutdown_event = asyncio.Event()

        @ray.method(num_returns=2)
        async def handle_request(
                self,
                pickled_request_metadata: bytes,
                *request_args,
                **request_kwargs,
        ):
            # The request metadata should be pickled for performance.
            request_metadata: RequestMetadata = pickle.loads(
                pickled_request_metadata)

            # Directly receive input because it might contain an ObjectRef.
            query = Query(request_args, request_kwargs, request_metadata)
            return await self.replica.handle_request(query)

        async def is_allocated(self):
            """poke the replica to check whether it's alive.

            When calling this method on an ActorHandle, it will complete as
            soon as the actor has started running. We use this mechanism to
            detect when a replica has been allocated a worker slot.
            At this time, the replica can transition from PENDING_ALLOCATION
            to PENDING_INITIALIZATION startup state.
            """
            pass

        async def reconfigure(self, user_config: Optional[Any] = None
                              ) -> Tuple[DeploymentConfig, DeploymentVersion]:
            if self.replica is None:
                await self._initialize_replica()
            if user_config is not None:
                await self.replica.reconfigure(user_config)

            return self.get_metadata()

        def get_metadata(self) -> Tuple[DeploymentConfig, DeploymentVersion]:
            return self.replica.deployment_config, self.replica.version

        async def prepare_for_shutdown(self):
            self.shutdown_event.set()
            if self.replica is not None:
                return await self.replica.prepare_for_shutdown()

        async def run_forever(self):
            await self.shutdown_event.wait()

    RayServeWrappedReplica.__name__ = name
    return RayServeWrappedReplica


def wrap_to_ray_error(function_name: str,
                      exception: Exception) -> RayTaskError:
    """Utility method to wrap exceptions in user code."""

    try:
        # Raise and catch so we can access traceback.format_exc()
        raise exception
    except Exception as e:
        traceback_str = ray._private.utils.format_error_message(
            traceback.format_exc())
        return ray.exceptions.RayTaskError(function_name, traceback_str, e)


class RayServeReplica:
    """Handles requests with the provided callable."""

    def __init__(self, _callable: Callable, deployment_name: str,
                 replica_tag: ReplicaTag, deployment_config: DeploymentConfig,
                 user_config: Any, version: DeploymentVersion,
                 is_function: bool, controller_handle: ActorHandle) -> None:
        self.deployment_config = deployment_config
        self.deployment_name = deployment_name
        self.replica_tag = replica_tag
        self.callable = _callable
        self.is_function = is_function
        self.user_config = user_config
        self.version = version

        self.num_ongoing_requests = 0

        self.request_counter = metrics.Counter(
            "serve_deployment_request_counter",
            description=("The number of queries that have been "
                         "processed in this replica."),
            tag_keys=("deployment", "replica"))
        self.request_counter.set_default_tags({
            "deployment": self.deployment_name,
            "replica": self.replica_tag
        })

        self.error_counter = metrics.Counter(
            "serve_deployment_error_counter",
            description=("The number of exceptions that have "
                         "occurred in this replica."),
            tag_keys=("deployment", "replica"))
        self.error_counter.set_default_tags({
            "deployment": self.deployment_name,
            "replica": self.replica_tag
        })

        self.restart_counter = metrics.Counter(
            "serve_deployment_replica_starts",
            description=("The number of times this replica "
                         "has been restarted due to failure."),
            tag_keys=("deployment", "replica"))
        self.restart_counter.set_default_tags({
            "deployment": self.deployment_name,
            "replica": self.replica_tag
        })

        self.processing_latency_tracker = metrics.Histogram(
            "serve_deployment_processing_latency_ms",
            description="The latency for queries to be processed.",
            boundaries=DEFAULT_LATENCY_BUCKET_MS,
            tag_keys=("deployment", "replica"))
        self.processing_latency_tracker.set_default_tags({
            "deployment": self.deployment_name,
            "replica": self.replica_tag
        })

        self.num_processing_items = metrics.Gauge(
            "serve_replica_processing_queries",
            description="The current number of queries being processed.",
            tag_keys=("deployment", "replica"))
        self.num_processing_items.set_default_tags({
            "deployment": self.deployment_name,
            "replica": self.replica_tag
        })

        self.restart_counter.inc()

        self._shutdown_wait_loop_s = (
            deployment_config.graceful_shutdown_wait_loop_s)

        if deployment_config.autoscaling_config:
            config = deployment_config.autoscaling_config
            start_metrics_pusher(
                interval_s=config.metrics_interval_s,
                collection_callback=self._collect_autoscaling_metrics,
                controller_handle=controller_handle)

        ray_logger = logging.getLogger("ray")
        for handler in ray_logger.handlers:
            handler.setFormatter(
                logging.Formatter(
                    handler.formatter._fmt +
                    f" component=serve deployment={self.deployment_name} "
                    f"replica={self.replica_tag}"))

    def _get_handle_request_stats(self) -> Optional[Dict[str, int]]:
        actor_stats = (
            ray.runtime_context.get_runtime_context()._get_actor_call_stats())
        method_stat = actor_stats.get("RayServeWrappedReplica.handle_request")
        return method_stat

    def _collect_autoscaling_metrics(self):
        method_stat = self._get_handle_request_stats()

        num_inflight_requests = 0
        if method_stat is not None:
            num_inflight_requests = (
                method_stat["pending"] + method_stat["running"])

        return {self.replica_tag: num_inflight_requests}

    def get_runner_method(self, request_item: Query) -> Callable:
        method_name = request_item.metadata.call_method
        if not hasattr(self.callable, method_name):
            # Filter to methods that don't start with '__' prefix.
            def callable_method_filter(attr):
                if attr.startswith("__"):
                    return False
                elif not callable(getattr(self.callable, attr)):
                    return False

                return True

            methods = list(filter(callable_method_filter, dir(self.callable)))
            raise RayServeException(f"Tried to call a method '{method_name}' "
                                    "that does not exist. Available methods: "
                                    f"{methods}.")
        if self.is_function:
            return self.callable
        return getattr(self.callable, method_name)

    async def ensure_serializable_response(self, response: Any) -> Any:
        if isinstance(response, starlette.responses.StreamingResponse):

            async def mock_receive():
                # This is called in a tight loop in response() just to check
                # for an http disconnect.  So rather than return immediately
                # we should suspend execution to avoid wasting CPU cycles.
                never_set_event = asyncio.Event()
                await never_set_event.wait()

            sender = ASGIHTTPSender()
            await response(scope=None, receive=mock_receive, send=sender)
            return sender.build_starlette_response()
        return response

    async def invoke_single(self, request_item: Query) -> Any:
        logger.debug("Replica {} started executing request {}".format(
            self.replica_tag, request_item.metadata.request_id))
        args, kwargs = parse_request_item(request_item)

        start = time.time()
        method_to_call = None
        try:
            runner_method = self.get_runner_method(request_item)
            method_to_call = sync_to_async(runner_method)
            result = None
            if len(inspect.signature(runner_method).parameters) > 0:
                result = await method_to_call(*args, **kwargs)
            else:
                # The method doesn't take in anything, including the request
                # information, so we pass nothing into it
                result = await method_to_call()

            result = await self.ensure_serializable_response(result)
            self.request_counter.inc()
        except Exception as e:
            import os
            if "RAY_PDB" in os.environ:
                ray.util.pdb.post_mortem()
            function_name = "unknown"
            if method_to_call is not None:
                function_name = method_to_call.__name__
            result = wrap_to_ray_error(function_name, e)
            self.error_counter.inc()

        latency_ms = (time.time() - start) * 1000
        self.processing_latency_tracker.observe(latency_ms)

        return result

    async def reconfigure(self, user_config: Any):
        self.user_config = user_config
        self.version = DeploymentVersion(
            self.version.code_version, user_config=user_config)
        if self.is_function:
            raise ValueError(
                "deployment_def must be a class to use user_config")
        elif not hasattr(self.callable, RECONFIGURE_METHOD):
            raise RayServeException("user_config specified but deployment " +
                                    self.deployment_name + " missing " +
                                    RECONFIGURE_METHOD + " method")
        reconfigure_method = sync_to_async(
            getattr(self.callable, RECONFIGURE_METHOD))
        await reconfigure_method(user_config)

    async def handle_request(self, request: Query) -> asyncio.Future:
        request.tick_enter_replica = time.time()
        logger.debug("Replica {} received request {}".format(
            self.replica_tag, request.metadata.request_id))

        num_running_requests = self._get_handle_request_stats()["running"]
        self.num_processing_items.set(num_running_requests)

        result = await self.invoke_single(request)
        request_time_ms = (time.time() - request.tick_enter_replica) * 1000
        logger.debug("Replica {} finished request {} in {:.2f}ms".format(
            self.replica_tag, request.metadata.request_id, request_time_ms))

        # Returns a small object for router to track request status.
        return b"", result

    async def prepare_for_shutdown(self):
        """Perform graceful shutdown.

        Trigger a graceful shutdown protocol that will wait for all the queued
        tasks to be completed and return to the controller.
        """
        while True:
            # Sleep first because we want to make sure all the routers receive
            # the notification to remove this replica first.
            await asyncio.sleep(self._shutdown_wait_loop_s)
            method_stat = self._get_handle_request_stats()
            # The handle_request method wasn't even invoked.
            if method_stat is None:
                break
            # The handle_request method has 0 inflight requests.
            if method_stat["running"] + method_stat["pending"] == 0:
                break
            else:
                logger.info(
                    "Waiting for an additional "
                    f"{self._shutdown_wait_loop_s}s to shut down because "
                    f"there are {self.num_ongoing_requests} ongoing requests.")

        # Explicitly call the del method to trigger clean up.
        # We set the del method to noop after succssifully calling it so the
        # destructor is called only once.
        try:
            if hasattr(self.callable, "__del__"):
                # Make sure to accept `async def __del__(self)` as well.
                await sync_to_async(self.callable.__del__)()
        except Exception as e:
            logger.exception(
                f"Exception during graceful shutdown of replica: {e}")
        finally:
            if hasattr(self.callable, "__del__"):
                del self.callable.__del__
