import aiorwlock
import asyncio
from importlib import import_module
import inspect
import logging
import os
import pickle
import time
from typing import Any, Callable, Optional, Tuple, Dict

import starlette.responses

import ray
from ray import cloudpickle
from ray.actor import ActorClass, ActorHandle
from ray.remote_function import RemoteFunction
from ray.util import metrics
from ray._private.async_compat import sync_to_async

from ray.serve.autoscaling_metrics import start_metrics_pusher
from ray.serve.common import HEALTH_CHECK_CONCURRENCY_GROUP, ReplicaTag
from ray.serve.config import DeploymentConfig
from ray.serve.constants import (
    HEALTH_CHECK_METHOD,
    RECONFIGURE_METHOD,
    DEFAULT_LATENCY_BUCKET_MS,
    SERVE_LOGGER_NAME,
)
from ray.serve.deployment import Deployment
from ray.serve.exceptions import RayServeException
from ray.serve.http_util import ASGIHTTPSender
from ray.serve.logging_utils import access_log_msg, configure_component_logger
from ray.serve.router import Query, RequestMetadata
from ray.serve.utils import parse_import_path, parse_request_item, wrap_to_ray_error
from ray.serve.version import DeploymentVersion

logger = logging.getLogger(SERVE_LOGGER_NAME)


def create_replica_wrapper(name: str):
    """Creates a replica class wrapping the provided function or class.

    This approach is picked over inheritance to avoid conflict between user
    provided class and the RayServeReplica class.
    """

    # TODO(architkulkarni): Add type hints after upgrading cloudpickle
    class RayServeWrappedReplica(object):
        async def __init__(
            self,
            deployment_name,
            replica_tag,
            serialized_deployment_def: bytes,
            serialized_init_args: bytes,
            serialized_init_kwargs: bytes,
            deployment_config_proto_bytes: bytes,
            version: DeploymentVersion,
            controller_name: str,
            controller_namespace: str,
            detached: bool,
        ):
            configure_component_logger(
                component_type="deployment",
                component_name=deployment_name,
                component_id=replica_tag,
            )

            deployment_def = cloudpickle.loads(serialized_deployment_def)

            if isinstance(deployment_def, str):
                import_path = deployment_def
                module_name, attr_name = parse_import_path(import_path)
                deployment_def = getattr(import_module(module_name), attr_name)
                # For ray or serve decorated class or function, strip to return
                # original body
                if isinstance(deployment_def, RemoteFunction):
                    deployment_def = deployment_def._function
                elif isinstance(deployment_def, ActorClass):
                    deployment_def = deployment_def.__ray_metadata__.modified_class
                elif isinstance(deployment_def, Deployment):
                    logger.warning(
                        f'The import path "{import_path}" contains a '
                        "decorated Serve deployment. The decorator's settings "
                        "are ignored when deploying via import path."
                    )
                    deployment_def = deployment_def.func_or_class

            init_args = cloudpickle.loads(serialized_init_args)
            init_kwargs = cloudpickle.loads(serialized_init_kwargs)

            deployment_config = DeploymentConfig.from_proto_bytes(
                deployment_config_proto_bytes
            )

            if inspect.isfunction(deployment_def):
                is_function = True
            elif inspect.isclass(deployment_def):
                is_function = False
            else:
                assert False, (
                    "deployment_def must be function, class, or "
                    "corresponding import path. Instead, it's type was "
                    f"{type(deployment_def)}."
                )

            # Set the controller name so that serve.connect() in the user's
            # code will connect to the instance that this deployment is running
            # in.
            ray.serve.context.set_internal_replica_context(
                deployment_name,
                replica_tag,
                controller_name,
                controller_namespace,
                servable_object=None,
            )

            assert controller_name, "Must provide a valid controller_name"

            controller_handle = ray.get_actor(
                controller_name, namespace=controller_namespace
            )

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
                    await sync_to_async(_callable.__init__)(*init_args, **init_kwargs)

                # Setting the context again to update the servable_object.
                ray.serve.context.set_internal_replica_context(
                    deployment_name,
                    replica_tag,
                    controller_name,
                    controller_namespace,
                    servable_object=_callable,
                )

                self.replica = RayServeReplica(
                    _callable,
                    deployment_name,
                    replica_tag,
                    deployment_config,
                    deployment_config.user_config,
                    version,
                    is_function,
                    controller_handle,
                )

            # Is it fine that replica is None here?
            # Should we add a check in all methods that use self.replica
            # or, alternatively, create an async get_replica() method?
            self.replica = None
            self._initialize_replica = initialize_replica

        @ray.method(num_returns=2)
        async def handle_request(
            self,
            pickled_request_metadata: bytes,
            *request_args,
            **request_kwargs,
        ):
            # The request metadata should be pickled for performance.
            request_metadata: RequestMetadata = pickle.loads(pickled_request_metadata)

            # Directly receive input because it might contain an ObjectRef.
            query = Query(request_args, request_kwargs, request_metadata)
            return await self.replica.handle_request(query)

        async def is_allocated(self) -> str:
            """poke the replica to check whether it's alive.

            When calling this method on an ActorHandle, it will complete as
            soon as the actor has started running. We use this mechanism to
            detect when a replica has been allocated a worker slot.
            At this time, the replica can transition from PENDING_ALLOCATION
            to PENDING_INITIALIZATION startup state.

            Return the NodeID of this replica
            """
            return ray.get_runtime_context().node_id

        async def reconfigure(
            self, user_config: Optional[Any] = None, _after: Optional[Any] = None
        ) -> Tuple[DeploymentConfig, DeploymentVersion]:
            # Unused `_after` argument is for scheduling: passing an ObjectRef
            # allows delaying reconfiguration until after this call has returned.
            if self.replica is None:
                await self._initialize_replica()
            if user_config is not None:
                await self.replica.reconfigure(user_config)

            return self.get_metadata()

        def get_metadata(self) -> Tuple[DeploymentConfig, DeploymentVersion]:
            return self.replica.deployment_config, self.replica.version

        async def prepare_for_shutdown(self):
            if self.replica is not None:
                return await self.replica.prepare_for_shutdown()

        @ray.method(concurrency_group=HEALTH_CHECK_CONCURRENCY_GROUP)
        async def check_health(self):
            await self.replica.check_health()

    RayServeWrappedReplica.__name__ = name
    return RayServeWrappedReplica


class RayServeReplica:
    """Handles requests with the provided callable."""

    def __init__(
        self,
        _callable: Callable,
        deployment_name: str,
        replica_tag: ReplicaTag,
        deployment_config: DeploymentConfig,
        user_config: Any,
        version: DeploymentVersion,
        is_function: bool,
        controller_handle: ActorHandle,
    ) -> None:
        self.deployment_config = deployment_config
        self.deployment_name = deployment_name
        self.replica_tag = replica_tag
        self.callable = _callable
        self.is_function = is_function
        self.user_config = user_config
        self.version = version
        self.rwlock = aiorwlock.RWLock()

        user_health_check = getattr(_callable, HEALTH_CHECK_METHOD, None)
        if not callable(user_health_check):

            def user_health_check():
                pass

        self.user_health_check = sync_to_async(user_health_check)

        self.num_ongoing_requests = 0

        self.request_counter = metrics.Counter(
            "serve_deployment_request_counter",
            description=(
                "The number of queries that have been processed in this replica."
            ),
            tag_keys=("deployment", "replica"),
        )
        self.request_counter.set_default_tags(
            {"deployment": self.deployment_name, "replica": self.replica_tag}
        )

        self.error_counter = metrics.Counter(
            "serve_deployment_error_counter",
            description=(
                "The number of exceptions that have occurred in this replica."
            ),
            tag_keys=("deployment", "replica"),
        )
        self.error_counter.set_default_tags(
            {"deployment": self.deployment_name, "replica": self.replica_tag}
        )

        self.restart_counter = metrics.Counter(
            "serve_deployment_replica_starts",
            description=(
                "The number of times this replica has been restarted due to failure."
            ),
            tag_keys=("deployment", "replica"),
        )
        self.restart_counter.set_default_tags(
            {"deployment": self.deployment_name, "replica": self.replica_tag}
        )

        self.processing_latency_tracker = metrics.Histogram(
            "serve_deployment_processing_latency_ms",
            description="The latency for queries to be processed.",
            boundaries=DEFAULT_LATENCY_BUCKET_MS,
            tag_keys=("deployment", "replica"),
        )
        self.processing_latency_tracker.set_default_tags(
            {"deployment": self.deployment_name, "replica": self.replica_tag}
        )

        self.num_processing_items = metrics.Gauge(
            "serve_replica_processing_queries",
            description="The current number of queries being processed.",
            tag_keys=("deployment", "replica"),
        )
        self.num_processing_items.set_default_tags(
            {"deployment": self.deployment_name, "replica": self.replica_tag}
        )

        self.restart_counter.inc()

        self._shutdown_wait_loop_s = deployment_config.graceful_shutdown_wait_loop_s

        if deployment_config.autoscaling_config:
            process_remote_func = controller_handle.record_autoscaling_metrics.remote
            config = deployment_config.autoscaling_config
            start_metrics_pusher(
                interval_s=config.metrics_interval_s,
                collection_callback=self._collect_autoscaling_metrics,
                metrics_process_func=process_remote_func,
            )

        # NOTE(edoakes): we used to recommend that users use the "ray" logger
        # and tagged the logs with metadata as below. We now recommend using
        # the "ray.serve" 'component logger' (as of Ray 1.13). This is left to
        # maintain backwards compatibility with users who were using the
        # existing logger. We can consider removing it in Ray 2.0.
        ray_logger = logging.getLogger("ray")
        for handler in ray_logger.handlers:
            handler.setFormatter(
                logging.Formatter(
                    handler.formatter._fmt
                    + f" component=serve deployment={self.deployment_name} "
                    f"replica={self.replica_tag}"
                )
            )

    async def check_health(self):
        await self.user_health_check()

    def _get_handle_request_stats(self) -> Optional[Dict[str, int]]:
        actor_stats = ray.runtime_context.get_runtime_context()._get_actor_call_stats()
        method_stat = actor_stats.get("RayServeWrappedReplica.handle_request")
        return method_stat

    def _collect_autoscaling_metrics(self):
        method_stat = self._get_handle_request_stats()

        num_inflight_requests = 0
        if method_stat is not None:
            num_inflight_requests = method_stat["pending"] + method_stat["running"]

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
            raise RayServeException(
                f"Tried to call a method '{method_name}' "
                "that does not exist. Available methods: "
                f"{methods}."
            )
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
            return sender.build_asgi_response()
        return response

    async def invoke_single(self, request_item: Query) -> Tuple[Any, bool]:
        """Executes the provided request on this replica.

        Returns the user-provided output and a boolean indicating if the
        request succeeded (user code didn't raise an exception).
        """
        logger.debug(
            "Replica {} started executing request {}".format(
                self.replica_tag, request_item.metadata.request_id
            )
        )
        args, kwargs = parse_request_item(request_item)

        method_to_call = None
        success = True
        try:
            runner_method = self.get_runner_method(request_item)
            method_to_call = sync_to_async(runner_method)
            result = None
            if len(inspect.signature(runner_method).parameters) > 0:
                result = await method_to_call(*args, **kwargs)
            else:
                # When access via http http_arg_is_pickled with no args:
                # args = (<starlette.requests.Request object at 0x7fe900694cc0>,)
                # When access via python with no args:
                # args = ()
                if len(args) == 1 and isinstance(args[0], starlette.requests.Request):
                    # The method doesn't take in anything, including the request
                    # information, so we pass nothing into it
                    result = await method_to_call()
                else:
                    # Will throw due to signature mismatch if user attempts to
                    # call with non-empty args
                    result = await method_to_call(*args, **kwargs)

            result = await self.ensure_serializable_response(result)
            self.request_counter.inc()
        except Exception as e:
            logger.exception(f"Request failed due to {type(e).__name__}:")
            success = False
            if "RAY_PDB" in os.environ:
                ray.util.pdb.post_mortem()
            function_name = "unknown"
            if method_to_call is not None:
                function_name = method_to_call.__name__
            result = wrap_to_ray_error(function_name, e)
            self.error_counter.inc()

        return result, success

    async def reconfigure(self, user_config: Any):
        async with self.rwlock.writer_lock:
            self.user_config = user_config
            self.version = DeploymentVersion(
                self.version.code_version, user_config=user_config
            )
            if self.is_function:
                raise ValueError("deployment_def must be a class to use user_config")
            elif not hasattr(self.callable, RECONFIGURE_METHOD):
                raise RayServeException(
                    "user_config specified but deployment "
                    + self.deployment_name
                    + " missing "
                    + RECONFIGURE_METHOD
                    + " method"
                )
            reconfigure_method = sync_to_async(
                getattr(self.callable, RECONFIGURE_METHOD)
            )
            await reconfigure_method(user_config)

    async def handle_request(self, request: Query) -> asyncio.Future:
        async with self.rwlock.reader_lock:
            num_running_requests = self._get_handle_request_stats()["running"]
            self.num_processing_items.set(num_running_requests)

            start_time = time.time()
            result, success = await self.invoke_single(request)
            latency_ms = (time.time() - start_time) * 1000

            self.processing_latency_tracker.observe(latency_ms)

            logger.info(
                access_log_msg(
                    method="HANDLE",
                    route=request.metadata.call_method,
                    status="OK" if success else "ERROR",
                    latency_ms=latency_ms,
                )
            )

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
                    f"there are {self.num_ongoing_requests} ongoing requests."
                )

        # Explicitly call the del method to trigger clean up.
        # We set the del method to noop after succssifully calling it so the
        # destructor is called only once.
        try:
            if hasattr(self.callable, "__del__"):
                # Make sure to accept `async def __del__(self)` as well.
                await sync_to_async(self.callable.__del__)()
        except Exception as e:
            logger.exception(f"Exception during graceful shutdown of replica: {e}")
        finally:
            if hasattr(self.callable, "__del__"):
                del self.callable.__del__
