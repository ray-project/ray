import asyncio
import logging
import pickle
import traceback
import inspect
from typing import Any, Callable, Optional
import time

import starlette.responses

import ray
from ray import cloudpickle
from ray.actor import ActorHandle
from ray._private.async_compat import sync_to_async

from ray.serve.autoscaling_metrics import start_metrics_pusher
from ray.serve.common import DeploymentTag, ReplicaTag
from ray.serve.http_util import ASGIHTTPSender
from ray.serve.utils import parse_request_item, _get_logger
from ray.serve.exceptions import RayServeException
from ray.util import metrics
from ray.serve.config import DeploymentConfig
from ray.serve.long_poll import LongPollClient, LongPollNamespace
from ray.serve.router import Query, RequestMetadata
from ray.serve.constants import (
    BACKEND_RECONFIGURE_METHOD,
    DEFAULT_LATENCY_BUCKET_MS,
)
from ray.serve.version import DeploymentVersion
from ray.exceptions import RayTaskError

logger = _get_logger()


def create_deployment_replica(name: str, serialized_deployment_def: bytes):
    """Creates a replica class wrapping the provided function or class.

    This approach is picked over inheritance to avoid conflict between user
    provided class and the RayServeReplica class.
    """
    serialized_deployment_def = serialized_deployment_def

    # TODO(architkulkarni): Add type hints after upgrading cloudpickle
    class RayServeWrappedReplica(object):
        async def __init__(self, deployment_tag, replica_tag, init_args,
                           deployment_config_proto_bytes: bytes,
                           version: DeploymentVersion, controller_name: str,
                           detached: bool):
            deployment = cloudpickle.loads(serialized_deployment_def)
            deployment_config = DeploymentConfig.from_proto_bytes(
                deployment_config_proto_bytes)

            if inspect.isfunction(deployment):
                is_function = True
            elif inspect.isclass(deployment):
                is_function = False
            else:
                assert False, ("deployment_def must be function, class, or "
                               "corresponding import path.")

            # Set the controller name so that serve.connect() in the user's
            # deployment code will connect to the instance that this deployment
            # is running in.
            ray.serve.api._set_internal_replica_context(
                deployment_tag,
                replica_tag,
                controller_name,
                servable_object=None)
            if is_function:
                _callable = deployment
            else:
                # This allows deployments to define an async __init__ method
                # (required for FastAPI deployment definition).
                _callable = deployment.__new__(deployment)
                await sync_to_async(_callable.__init__)(*init_args)
            # Setting the context again to update the servable_object.
            ray.serve.api._set_internal_replica_context(
                deployment_tag,
                replica_tag,
                controller_name,
                servable_object=_callable)

            assert controller_name, "Must provide a valid controller_name"
            controller_namespace = ray.serve.api._get_controller_namespace(
                detached)
            controller_handle = ray.get_actor(
                controller_name, namespace=controller_namespace)
            self.deployment = RayServeReplica(
                _callable, deployment_tag, replica_tag, deployment_config,
                deployment_config.user_config, version, is_function,
                controller_handle)

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
            return await self.deployment.handle_request(query)

        async def reconfigure(self, user_config: Optional[Any] = None) -> None:
            await self.deployment.reconfigure(user_config)

        def get_version(self) -> DeploymentVersion:
            return self.deployment.version

        async def prepare_for_shutdown(self):
            self.shutdown_event.set()
            return await self.deployment.prepare_for_shutdown()

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

    def __init__(self, _callable: Callable, deployment_tag: DeploymentTag,
                 replica_tag: ReplicaTag, deployment_config: DeploymentConfig,
                 user_config: Any, version: DeploymentVersion,
                 is_function: bool, controller_handle: ActorHandle) -> None:
        self.deployment_tag = deployment_tag
        self.replica_tag = replica_tag
        self.callable = _callable
        self.is_function = is_function

        self.deployment_config = deployment_config
        self.user_config = user_config
        self.version = version

        self.num_ongoing_requests = 0

        self.request_counter = metrics.Counter(
            "serve_deployment_request_counter",
            description=("The number of queries that have been "
                         "processed in this replica."),
            tag_keys=("deployment", "replica"))
        self.request_counter.set_default_tags({
            "deployment": self.deployment_tag,
            "replica": self.replica_tag
        })

        self.loop = asyncio.get_event_loop()
        self.long_poll_client = LongPollClient(
            controller_handle,
            {
                (LongPollNamespace.deployment_configS, self.deployment_tag): self.  # noqa: E501
                _update_deployment_configs,
            },
            call_in_event_loop=self.loop,
        )

        self.error_counter = metrics.Counter(
            "serve_deployment_error_counter",
            description=("The number of exceptions that have "
                         "occurred in this replica."),
            tag_keys=("deployment", "replica"))
        self.error_counter.set_default_tags({
            "deployment": self.deployment_tag,
            "replica": self.replica_tag
        })

        self.restart_counter = metrics.Counter(
            "serve_deployment_replica_starts",
            description=("The number of times this replica "
                         "has been restarted due to failure."),
            tag_keys=("deployment", "replica"))
        self.restart_counter.set_default_tags({
            "deployment": self.deployment_tag,
            "replica": self.replica_tag
        })

        self.processing_latency_tracker = metrics.Histogram(
            "serve_deployment_processing_latency_ms",
            description="The latency for queries to be processed.",
            boundaries=DEFAULT_LATENCY_BUCKET_MS,
            tag_keys=("deployment", "replica"))
        self.processing_latency_tracker.set_default_tags({
            "deployment": self.deployment_tag,
            "replica": self.replica_tag
        })

        self.num_processing_items = metrics.Gauge(
            "serve_replica_processing_queries",
            description="The current number of queries being processed.",
            tag_keys=("deployment", "replica"))
        self.num_processing_items.set_default_tags({
            "deployment": self.deployment_tag,
            "replica": self.replica_tag
        })

        self.restart_counter.inc()

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
                    f" component=serve deployment={self.deployment_tag} "
                    f"replica={self.replica_tag}"))

    def _collect_autoscaling_metrics(self):
        # TODO(simon): Instead of relying on this counter, we should get the
        # outstanding actor calls properly from Ray's core worker.
        return {self.replica_tag: self.num_ongoing_requests}

    def get_runner_method(self, request_item: Query) -> Callable:
        method_name = request_item.metadata.call_method
        if not hasattr(self.callable, method_name):
            raise RayServeException("Deployment doesn't have method {} "
                                    "which is specified in the request. "
                                    "The available methods are {}".format(
                                        method_name, dir(self.callable)))
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
            method_to_call = sync_to_async(
                self.get_runner_method(request_item))
            result = await method_to_call(*args, **kwargs)

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

    async def reconfigure(
            self, user_config: Optional[Any] = None) -> DeploymentVersion:
        if user_config:
            self.user_config = user_config
            self.version = DeploymentVersion(
                self.version.code_version, user_config=user_config)
            if self.is_function:
                raise ValueError(
                    "deployment_def must be a class to use user_config")
            elif not hasattr(self.callable, BACKEND_RECONFIGURE_METHOD):
                raise RayServeException("user_config specified but deployment "
                                        + self.deployment_tag + " missing " +
                                        BACKEND_RECONFIGURE_METHOD + " method")
            reconfigure_method = sync_to_async(
                getattr(self.callable, BACKEND_RECONFIGURE_METHOD))
            await reconfigure_method(user_config)

    def _update_deployment_configs(self, new_config_bytes: bytes) -> None:
        self.deployment_config = DeploymentConfig.from_proto_bytes(
            new_config_bytes)

    async def handle_request(self, request: Query) -> asyncio.Future:
        request.tick_enter_replica = time.time()
        logger.debug("Replica {} received request {}".format(
            self.replica_tag, request.metadata.request_id))

        self.num_ongoing_requests += 1
        self.num_processing_items.set(self.num_ongoing_requests)

        # Trigger a context switch so we can enqueue more requests in the
        # meantime. Without this line and if the function is synchronous,
        # other requests won't even get enqueued as await self.invoke_single
        # doesn't context switch.
        await asyncio.sleep(0)

        result = await self.invoke_single(request)
        self.num_ongoing_requests -= 1
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
        sleep_time = self.deployment_config.experimental_graceful_shutdown_wait_loop_s  # noqa: E501
        while True:
            # Sleep first because we want to make sure all the routers receive
            # the notification to remove this replica first.
            await asyncio.sleep(sleep_time)
            if self.num_ongoing_requests == 0:
                break
            else:
                logger.info(
                    f"Waiting for an additional {sleep_time}s to shut down "
                    f"because there are {self.num_ongoing_requests} "
                    "ongoing requests.")

        # Explicitly call the del method to trigger clean up.
        # We set the del method to noop after succssifully calling it so the
        # destructor is called only once.
        try:
            if hasattr(self.callable, "__del__"):
                self.callable.__del__()
        except Exception:
            logger.exception("Exception during graceful shutdown of replica.")
        finally:
            self.callable.__del__ = lambda _self: None
