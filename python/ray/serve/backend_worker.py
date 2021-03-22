import asyncio
import logging
import traceback
import inspect
from collections.abc import Iterable
from itertools import groupby
from typing import Union, List, Any, Callable, Type
import time

import starlette.responses
from starlette.requests import Request

import ray
from ray.actor import ActorHandle
from ray._private.async_compat import sync_to_async

from ray.serve.batching import _BatchQueue
from ray.serve.utils import (ASGIHTTPSender, parse_request_item, _get_logger,
                             chain_future, unpack_future, import_attr)
from ray.serve.exceptions import RayServeException
from ray.util import metrics
from ray.serve.config import BackendConfig
from ray.serve.long_poll import LongPollClient, LongPollNamespace
from ray.serve.router import Query, RequestMetadata
from ray.serve.constants import (
    BACKEND_RECONFIGURE_METHOD,
    DEFAULT_LATENCY_BUCKET_MS,
)
from ray.exceptions import RayTaskError

logger = _get_logger()


def create_backend_replica(backend_def: Union[Callable, Type[Callable], str]):
    """Creates a replica class wrapping the provided function or class.

    This approach is picked over inheritance to avoid conflict between user
    provided class and the RayServeReplica class.
    """
    backend_def = backend_def

    # TODO(architkulkarni): Add type hints after upgrading cloudpickle
    class RayServeWrappedReplica(object):
        def __init__(self, backend_tag, replica_tag, init_args,
                     backend_config: BackendConfig, controller_name: str):
            if isinstance(backend_def, str):
                backend = import_attr(backend_def)
            else:
                backend = backend_def

            if inspect.isfunction(backend):
                is_function = True
            elif inspect.isclass(backend):
                is_function = False
            else:
                assert False, ("backend_def must be function, class, or "
                               "corresponding import path.")

            # Set the controller name so that serve.connect() in the user's
            # backend code will connect to the instance that this backend is
            # running in.
            ray.serve.api._set_internal_replica_context(
                backend_tag, replica_tag, controller_name)
            if is_function:
                _callable = backend
            else:
                _callable = backend(*init_args)

            assert controller_name, "Must provide a valid controller_name"
            controller_handle = ray.get_actor(controller_name)
            self.backend = RayServeReplica(_callable, backend_config,
                                           is_function, controller_handle)

        @ray.method(num_returns=2)
        async def handle_request(
                self,
                request_metadata: RequestMetadata,
                *request_args,
                **request_kwargs,
        ):
            # Directly receive input because it might contain an ObjectRef.
            query = Query(request_args, request_kwargs, request_metadata)
            return await self.backend.handle_request(query)

        def ready(self):
            pass

        async def drain_pending_queries(self):
            return await self.backend.drain_pending_queries()

    if isinstance(backend_def, str):
        RayServeWrappedReplica.__name__ = "RayServeReplica_{}".format(
            backend_def)
    else:
        RayServeWrappedReplica.__name__ = "RayServeReplica_{}".format(
            backend_def.__name__)
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

    def __init__(self, _callable: Callable, backend_config: BackendConfig,
                 is_function: bool, controller_handle: ActorHandle) -> None:
        self.backend_tag = ray.serve.api.get_replica_context().backend_tag
        self.replica_tag = ray.serve.api.get_replica_context().replica_tag
        self.callable = _callable
        self.is_function = is_function

        self.config = backend_config
        self.batch_queue = _BatchQueue(self.config.max_batch_size or 1,
                                       self.config.batch_wait_timeout)
        self.reconfigure(self.config.user_config)

        self.num_ongoing_requests = 0

        self.request_counter = metrics.Counter(
            "serve_backend_request_counter",
            description=("The number of queries that have been "
                         "processed in this replica."),
            tag_keys=("backend", ))
        self.request_counter.set_default_tags({"backend": self.backend_tag})

        self.loop = asyncio.get_event_loop()
        self.long_poll_client = LongPollClient(
            controller_handle,
            {
                (LongPollNamespace.BACKEND_CONFIGS, self.backend_tag): self.
                _update_backend_configs,
            },
            call_in_event_loop=self.loop,
        )

        self.error_counter = metrics.Counter(
            "serve_backend_error_counter",
            description=("The number of exceptions that have "
                         "occurred in the backend."),
            tag_keys=("backend", ))
        self.error_counter.set_default_tags({"backend": self.backend_tag})

        self.restart_counter = metrics.Counter(
            "serve_backend_replica_starts",
            description=("The number of times this replica "
                         "has been restarted due to failure."),
            tag_keys=("backend", "replica"))
        self.restart_counter.set_default_tags({
            "backend": self.backend_tag,
            "replica": self.replica_tag
        })

        self.queuing_latency_tracker = metrics.Histogram(
            "serve_backend_queuing_latency_ms",
            description=("The latency for queries in the replica's queue "
                         "waiting to be processed or batched."),
            boundaries=DEFAULT_LATENCY_BUCKET_MS,
            tag_keys=("backend", "replica"))
        self.queuing_latency_tracker.set_default_tags({
            "backend": self.backend_tag,
            "replica": self.replica_tag
        })

        self.processing_latency_tracker = metrics.Histogram(
            "serve_backend_processing_latency_ms",
            description="The latency for queries to be processed.",
            boundaries=DEFAULT_LATENCY_BUCKET_MS,
            tag_keys=("backend", "replica", "batch_size"))
        self.processing_latency_tracker.set_default_tags({
            "backend": self.backend_tag,
            "replica": self.replica_tag
        })

        self.num_queued_items = metrics.Gauge(
            "serve_replica_queued_queries",
            description=("The current number of queries queued in "
                         "the backend replicas."),
            tag_keys=("backend", "replica"))
        self.num_queued_items.set_default_tags({
            "backend": self.backend_tag,
            "replica": self.replica_tag
        })

        self.num_processing_items = metrics.Gauge(
            "serve_replica_processing_queries",
            description="The current number of queries being processed.",
            tag_keys=("backend", "replica"))
        self.num_processing_items.set_default_tags({
            "backend": self.backend_tag,
            "replica": self.replica_tag
        })

        self.restart_counter.inc()

        ray_logger = logging.getLogger("ray")
        for handler in ray_logger.handlers:
            handler.setFormatter(
                logging.Formatter(
                    handler.formatter._fmt +
                    f" component=serve backend={self.backend_tag} "
                    f"replica={self.replica_tag}"))

        asyncio.get_event_loop().create_task(self.main_loop())

    def get_runner_method(self, request_item: Query) -> Callable:
        method_name = request_item.metadata.call_method
        if not hasattr(self.callable, method_name):
            raise RayServeException("Backend doesn't have method {} "
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
        arg = parse_request_item(request_item)

        start = time.time()
        try:
            # TODO(simon): Split this section out when invoke_batch is removed.
            if self.config.internal_metadata.is_asgi_app:
                request: Request = arg
                scope = request.scope
                root_path = self.config.internal_metadata.path_prefix

                # The incoming scope["path"] contains prefixed path and it
                # won't be stripped by FastAPI.
                request.scope["path"] = scope["path"].replace(root_path, "", 1)
                # root_path is used such that the reverse look up and
                # redirection works.
                request.scope["root_path"] = root_path

                sender = ASGIHTTPSender()
                await self.callable._serve_asgi_app(
                    request.scope,
                    request._receive,
                    sender,
                )
                result = sender.build_starlette_response()
            else:
                method_to_call = sync_to_async(
                    self.get_runner_method(request_item))
                result = await method_to_call(arg)
            result = await self.ensure_serializable_response(result)
            self.request_counter.inc()
        except Exception as e:
            import os
            if "RAY_PDB" in os.environ:
                ray.util.pdb.post_mortem()
            result = wrap_to_ray_error(method_to_call.__name__, e)
            self.error_counter.inc()

        latency_ms = (time.time() - start) * 1000
        self.processing_latency_tracker.observe(
            latency_ms, tags={"batch_size": "1"})

        return result

    async def invoke_batch(self, request_item_list: List[Query]) -> List[Any]:
        args = []
        call_methods = set()
        batch_size = len(request_item_list)

        # Construct the batch of requests
        for item in request_item_list:
            logger.debug("Replica {} started executing request {}".format(
                self.replica_tag, item.metadata.request_id))
            args.append(parse_request_item(item))
            call_methods.add(self.get_runner_method(item))

        timing_start = time.time()
        try:
            if len(call_methods) != 1:
                raise RayServeException(
                    f"Queries contain mixed calling methods: {call_methods}. "
                    "Please only send the same type of requests in batching "
                    "mode.")

            self.request_counter.inc(batch_size)

            call_method = sync_to_async(call_methods.pop())
            result_list = await call_method(args)

            if not isinstance(result_list, Iterable) or isinstance(
                    result_list, (dict, set)):
                error_message = ("RayServe expects an ordered iterable object "
                                 "but the replica returned a {}".format(
                                     type(result_list)))
                raise RayServeException(error_message)

            # Normalize the result into a list type. This operation is fast
            # in Python because it doesn't copy anything.
            result_list = list(result_list)

            if (len(result_list) != batch_size):
                error_message = ("Worker doesn't preserve batch size. The "
                                 "input has length {} but the returned list "
                                 "has length {}. Please return a list of "
                                 "results with length equal to the batch size"
                                 ".".format(batch_size, len(result_list)))
                raise RayServeException(error_message)
            for i, result in enumerate(result_list):
                result_list[i] = (await
                                  self.ensure_serializable_response(result))
        except Exception as e:
            wrapped_exception = wrap_to_ray_error(call_method.__name__, e)
            self.error_counter.inc()
            result_list = [wrapped_exception for _ in range(batch_size)]

        latency_ms = (time.time() - timing_start) * 1000
        self.processing_latency_tracker.observe(
            latency_ms, tags={"batch_size": str(batch_size)})

        return result_list

    async def main_loop(self) -> None:
        while True:
            # NOTE(simon): There's an issue when user updated batch size and
            # batch wait timeout during the execution, these values will not be
            # updated until after the current iteration.
            batch = await self.batch_queue.wait_for_batch()

            # Record metrics
            self.num_queued_items.set(self.batch_queue.qsize())
            self.num_processing_items.set(self.num_ongoing_requests -
                                          self.batch_queue.qsize())
            for query in batch:
                queuing_time = (time.time() - query.tick_enter_replica) * 1000
                self.queuing_latency_tracker.observe(queuing_time)

            all_evaluated_futures = []

            if not self.config.internal_metadata.accepts_batches:
                query = batch[0]
                evaluated = asyncio.ensure_future(self.invoke_single(query))
                all_evaluated_futures = [evaluated]
                chain_future(evaluated, query.async_future)
            else:
                get_call_method = (
                    lambda query: query.metadata.call_method  # noqa: E731
                )
                sorted_batch = sorted(batch, key=get_call_method)
                for _, group in groupby(sorted_batch, key=get_call_method):
                    group = list(group)
                    evaluated = asyncio.ensure_future(self.invoke_batch(group))
                    all_evaluated_futures.append(evaluated)
                    result_futures = [q.async_future for q in group]
                    chain_future(
                        unpack_future(evaluated, len(group)), result_futures)

            if self.config.internal_metadata.is_blocking:
                # We use asyncio.wait here so if the result is exception,
                # it will not be raised.
                await asyncio.wait(all_evaluated_futures)

    def reconfigure(self, user_config) -> None:
        if user_config:
            if self.is_function:
                raise ValueError(
                    "backend_def must be a class to use user_config")
            elif not hasattr(self.callable, BACKEND_RECONFIGURE_METHOD):
                raise RayServeException("user_config specified but backend " +
                                        self.backend_tag + " missing " +
                                        BACKEND_RECONFIGURE_METHOD + " method")
            reconfigure_method = getattr(self.callable,
                                         BACKEND_RECONFIGURE_METHOD)
            reconfigure_method(user_config)

    def _update_backend_configs(self, new_config: BackendConfig) -> None:
        self.config = new_config
        self.batch_queue.set_config(self.config.max_batch_size or 1,
                                    self.config.batch_wait_timeout)
        self.reconfigure(self.config.user_config)

    async def handle_request(self, request: Query) -> asyncio.Future:
        request.tick_enter_replica = time.time()
        logger.debug("Replica {} received request {}".format(
            self.replica_tag, request.metadata.request_id))
        request.async_future = asyncio.get_event_loop().create_future()
        self.num_ongoing_requests += 1

        self.batch_queue.put(request)
        result = await request.async_future
        request_time_ms = (time.time() - request.tick_enter_replica) * 1000
        logger.debug("Replica {} finished request {} in {:.2f}ms".format(
            self.replica_tag, request.metadata.request_id, request_time_ms))

        self.num_ongoing_requests -= 1
        # Returns a small object for router to track request status.
        return b"", result

    async def drain_pending_queries(self):
        """Perform graceful shutdown.

        Trigger a graceful shutdown protocol that will wait for all the queued
        tasks to be completed and return to the controller.
        """
        sleep_time = self.config.experimental_graceful_shutdown_wait_loop_s
        while True:
            # Sleep first because we want to make sure all the routers receive
            # the notification to remove this replica first.
            await asyncio.sleep(sleep_time)

            num_queries_waiting = self.batch_queue.qsize()
            if (num_queries_waiting == 0) and (self.num_ongoing_requests == 0):
                break
            else:
                logger.info(
                    f"Waiting for an additional {sleep_time}s "
                    f"to shutdown replica {self.replica_tag} because "
                    f"num_queries_waiting {num_queries_waiting} and "
                    f"num_ongoing_requests {self.num_ongoing_requests}")

        ray.actor.exit_actor()
