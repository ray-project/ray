import asyncio
import traceback
import inspect
from collections.abc import Iterable
from itertools import groupby
from typing import Union, List, Any, Callable, Type
import time

import starlette.responses

import ray
from ray.actor import ActorHandle
from ray.async_compat import sync_to_async

from ray.serve.utils import (parse_request_item, _get_logger, chain_future,
                             unpack_future)
from ray.serve.exceptions import RayServeException
from ray.util import metrics
from ray.serve.config import BackendConfig
from ray.serve.long_poll import LongPollAsyncClient
from ray.serve.router import Query, RequestMetadata
from ray.serve.constants import (
    BACKEND_RECONFIGURE_METHOD,
    DEFAULT_LATENCY_BUCKET_MS,
    LongPollKey,
)
from ray.exceptions import RayTaskError

logger = _get_logger()


class BatchQueue:
    def __init__(self, max_batch_size: int, timeout_s: float) -> None:
        self.queue = asyncio.Queue()
        self.full_batch_event = asyncio.Event()
        self.max_batch_size = max_batch_size
        self.timeout_s = timeout_s

    def set_config(self, max_batch_size: int, timeout_s: float) -> None:
        self.max_batch_size = max_batch_size
        self.timeout_s = timeout_s

    def put(self, request: Query) -> None:
        self.queue.put_nowait(request)
        # Signal when the full batch is ready. The event will be reset
        # in wait_for_batch.
        if self.queue.qsize() == self.max_batch_size:
            self.full_batch_event.set()

    def qsize(self) -> int:
        return self.queue.qsize()

    async def wait_for_batch(self) -> List[Query]:
        """Wait for batch respecting self.max_batch_size and self.timeout_s.

        Returns a batch of up to self.max_batch_size items, waiting for up
        to self.timeout_s for a full batch. After the timeout, returns as many
        items as are ready.

        Always returns a batch with at least one item - will block
        indefinitely until an item comes in.
        """
        curr_timeout = self.timeout_s
        batch = []
        while len(batch) == 0:
            loop_start = time.time()

            # If the timeout is 0, wait for any item to be available on the
            # queue.
            if curr_timeout == 0:
                batch.append(await self.queue.get())
            # If the timeout is nonzero, wait for either the timeout to occur
            # or the max batch size to be ready.
            else:
                try:
                    await asyncio.wait_for(self.full_batch_event.wait(),
                                           curr_timeout)
                except asyncio.TimeoutError:
                    pass

            # Pull up to the max_batch_size requests off the queue.
            while len(batch) < self.max_batch_size and not self.queue.empty():
                batch.append(self.queue.get_nowait())

            # Reset the event if there are fewer than max_batch_size requests
            # in the queue.
            if (self.queue.qsize() < self.max_batch_size
                    and self.full_batch_event.is_set()):
                self.full_batch_event.clear()

            # Adjust the timeout based on the time spent in this iteration.
            curr_timeout = max(0, curr_timeout - (time.time() - loop_start))

        return batch


def create_backend_replica(func_or_class: Union[Callable, Type[Callable]]):
    """Creates a replica class wrapping the provided function or class.

    This approach is picked over inheritance to avoid conflict between user
    provided class and the RayServeReplica class.
    """

    if inspect.isfunction(func_or_class):
        is_function = True
    elif inspect.isclass(func_or_class):
        is_function = False
    else:
        assert False, "func_or_class must be function or class."

    # TODO(architkulkarni): Add type hints after upgrading cloudpickle
    class RayServeWrappedReplica(object):
        def __init__(self, backend_tag, replica_tag, init_args,
                     backend_config: BackendConfig, controller_name: str):
            # Set the controller name so that serve.connect() in the user's
            # backend code will connect to the instance that this backend is
            # running in.
            ray.serve.api._set_internal_replica_context(
                backend_tag, replica_tag, controller_name)
            if is_function:
                _callable = func_or_class
            else:
                _callable = func_or_class(*init_args)

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

    RayServeWrappedReplica.__name__ = "RayServeReplica_{}".format(
        func_or_class.__name__)
    return RayServeWrappedReplica


def wrap_to_ray_error(exception: Exception) -> RayTaskError:
    """Utility method to wrap exceptions in user code."""

    try:
        # Raise and catch so we can access traceback.format_exc()
        raise exception
    except Exception as e:
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        return ray.exceptions.RayTaskError(str(e), traceback_str, e.__class__)


class RayServeReplica:
    """Handles requests with the provided callable."""

    def __init__(self, _callable: Callable, backend_config: BackendConfig,
                 is_function: bool, controller_handle: ActorHandle) -> None:
        self.backend_tag = ray.serve.api.get_current_backend_tag()
        self.replica_tag = ray.serve.api.get_current_replica_tag()
        self.callable = _callable
        self.is_function = is_function

        self.config = backend_config
        self.batch_queue = BatchQueue(self.config.max_batch_size or 1,
                                      self.config.batch_wait_timeout)
        self.reconfigure(self.config.user_config)

        self.num_ongoing_requests = 0

        self.request_counter = metrics.Count(
            "serve_backend_request_counter",
            description=("The number of queries that have been "
                         "processed in this replica."),
            tag_keys=("backend", ))
        self.request_counter.set_default_tags({"backend": self.backend_tag})

        self.long_poll_client = LongPollAsyncClient(controller_handle, {
            LongPollKey.BACKEND_CONFIGS: self._update_backend_configs,
        })

        self.error_counter = metrics.Count(
            "serve_backend_error_counter",
            description=("The number of exceptions that have "
                         "occurred in the backend."),
            tag_keys=("backend", ))
        self.error_counter.set_default_tags({"backend": self.backend_tag})

        self.restart_counter = metrics.Count(
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

        self.restart_counter.record(1)

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
            # response contains a generator/iterator which is not serializable.
            # Exhaust the generator/iterator and store the results in a buffer.
            body_buffer = []

            async def mock_send(message):
                assert message["type"] in {
                    "http.response.start", "http.response.body"
                }
                if (message["type"] == "http.response.body"):
                    body_buffer.append(message["body"])

            async def mock_receive():
                # This is called in a tight loop in response() just to check
                # for an http disconnect.  So rather than return immediately
                # we should suspend execution to avoid wasting CPU cycles.
                never_set_event = asyncio.Event()
                await never_set_event.wait()

            await response(scope=None, receive=mock_receive, send=mock_send)
            content = b"".join(body_buffer)
            return starlette.responses.Response(
                content,
                status_code=response.status_code,
                headers=response.headers,
                media_type=response.media_type)
        else:
            return response

    async def invoke_single(self, request_item: Query) -> Any:
        logger.debug("Replica {} started executing request {}".format(
            self.replica_tag, request_item.metadata.request_id))
        method_to_call = sync_to_async(self.get_runner_method(request_item))
        arg = parse_request_item(request_item)

        start = time.time()
        try:
            result = await method_to_call(arg)
            result = await self.ensure_serializable_response(result)
            self.request_counter.record(1)
        except Exception as e:
            import os
            if "RAY_PDB" in os.environ:
                ray.util.pdb.post_mortem()
            result = wrap_to_ray_error(e)
            self.error_counter.record(1)

        latency_ms = (time.time() - start) * 1000
        self.processing_latency_tracker.record(
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

            self.request_counter.record(batch_size)

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
            wrapped_exception = wrap_to_ray_error(e)
            self.error_counter.record(1)
            result_list = [wrapped_exception for _ in range(batch_size)]

        latency_ms = (time.time() - timing_start) * 1000
        self.processing_latency_tracker.record(
            latency_ms, tags={"batch_size": str(batch_size)})

        return result_list

    async def main_loop(self) -> None:
        while True:
            # NOTE(simon): There's an issue when user updated batch size and
            # batch wait timeout during the execution, these values will not be
            # updated until after the current iteration.
            batch = await self.batch_queue.wait_for_batch()

            # Record metrics
            self.num_queued_items.record(self.batch_queue.qsize())
            self.num_processing_items.record(self.num_ongoing_requests -
                                             self.batch_queue.qsize())
            for query in batch:
                queuing_time = (time.time() - query.tick_enter_replica) * 1000
                self.queuing_latency_tracker.record(queuing_time)

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
                    "argument func_or_class must be a class to use user_config"
                )
            elif not hasattr(self.callable, BACKEND_RECONFIGURE_METHOD):
                raise RayServeException("user_config specified but backend " +
                                        self.backend_tag + " missing " +
                                        BACKEND_RECONFIGURE_METHOD + " method")
            reconfigure_method = getattr(self.callable,
                                         BACKEND_RECONFIGURE_METHOD)
            reconfigure_method(user_config)

    async def _update_backend_configs(self, backend_configs):
        # TODO(ilr) remove this loop when we poll per key
        for backend_tag, config in backend_configs.items():
            if backend_tag == self.backend_tag:
                self._update_config(config)

    def _update_config(self, new_config: BackendConfig) -> None:
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
