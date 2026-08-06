import collections
import copy
import gc
import itertools
import logging
import os
import queue
import sys
import threading
import time
import weakref
from multiprocessing import TimeoutError
from typing import Any, Callable, Dict, Hashable, Iterable, List, Optional, Tuple

import ray
from ray._common.usage import usage_lib
from ray.util import log_once

try:
    from joblib.parallel import BatchedCalls, parallel_backend
except ImportError:
    BatchedCalls = None
    parallel_backend = None

try:
    from joblib._parallel_backends import SafeFunction
except ImportError:
    # SafeFunction is a legacy compatibility wrapper removed in Joblib 1.5.
    SafeFunction = None


logger = logging.getLogger(__name__)

RAY_ADDRESS_ENV = "RAY_ADDRESS"


def _put_in_dict_registry(
    obj: Any, registry_hashable: Dict[Hashable, ray.ObjectRef]
) -> ray.ObjectRef:
    if obj not in registry_hashable:
        ret = ray.put(obj)
        registry_hashable[obj] = ret
    else:
        ret = registry_hashable[obj]
    return ret


def _put_in_list_registry(
    obj: Any, registry: List[Tuple[Any, ray.ObjectRef]]
) -> ray.ObjectRef:
    try:
        ret = next((ref for o, ref in registry if o is obj))
    except StopIteration:
        ret = ray.put(obj)
        registry.append((obj, ret))
    return ret


def ray_put_if_needed(
    obj: Any,
    registry: Optional[List[Tuple[Any, ray.ObjectRef]]] = None,
    registry_hashable: Optional[Dict[Hashable, ray.ObjectRef]] = None,
) -> ray.ObjectRef:
    """ray.put obj in object store if it's not an ObjRef and bigger than 100 bytes,
    with support for list and dict registries"""
    if isinstance(obj, ray.ObjectRef) or sys.getsizeof(obj) < 100:
        return obj
    ret = obj
    if registry_hashable is not None:
        try:
            ret = _put_in_dict_registry(obj, registry_hashable)
        except TypeError:
            if registry is not None:
                ret = _put_in_list_registry(obj, registry)
    elif registry is not None:
        ret = _put_in_list_registry(obj, registry)
    return ret


def ray_get_if_needed(obj: Any) -> Any:
    """If obj is an ObjectRef, do ray.get, otherwise return obj"""
    if isinstance(obj, ray.ObjectRef):
        return ray.get(obj)
    return obj


if BatchedCalls is not None:

    class RayBatchedCalls(BatchedCalls):
        """Joblib's BatchedCalls with basic Ray object store management

        This functionality is provided through the put_items_in_object_store,
        which uses external registries (list and dict) containing objects
        and their ObjectRefs."""

        def put_items_in_object_store(
            self,
            registry: Optional[List[Tuple[Any, ray.ObjectRef]]] = None,
            registry_hashable: Optional[Dict[Hashable, ray.ObjectRef]] = None,
        ):
            """Puts all applicable (kw)args in self.items in object store

            Takes two registries - list for unhashable objects and dict
            for hashable objects. The registries are a part of a Pool object.
            The method iterates through all entries in items list (usually,
            there will be only one, but the number depends on joblib Parallel
            settings) and puts all of the args and kwargs into the object
            store, updating the registries.
            If an arg or kwarg is already in a registry, it will not be
            put again, and instead, the cached object ref will be used."""
            new_items = []
            for func, args, kwargs in self.items:
                args = [
                    ray_put_if_needed(arg, registry, registry_hashable) for arg in args
                ]
                kwargs = {
                    k: ray_put_if_needed(v, registry, registry_hashable)
                    for k, v in kwargs.items()
                }
                new_items.append((func, args, kwargs))
            self.items = new_items

        def __call__(self):
            # Exactly the same as in BatchedCalls, with the
            # difference being that it gets args and kwargs from
            # object store (which have been put in there by
            # put_items_in_object_store)

            # Set the default nested backend to self._backend but do
            # not set the change the default number of processes to -1
            with parallel_backend(self._backend, n_jobs=self._n_jobs):
                return [
                    func(
                        *[ray_get_if_needed(arg) for arg in args],
                        **{k: ray_get_if_needed(v) for k, v in kwargs.items()},
                    )
                    for func, args, kwargs in self.items
                ]

        def __reduce__(self):
            # Exactly the same as in BatchedCalls, with the
            # difference being that it returns RayBatchedCalls
            # instead
            if self._reducer_callback is not None:
                self._reducer_callback()
            # no need pickle the callback.
            return (
                RayBatchedCalls,
                (self.items, (self._backend, self._n_jobs), None, self._pickle_cache),
            )

else:
    RayBatchedCalls = None


# Helper function to divide a by b and round the result up.
def div_round_up(a, b):
    return -(-a // b)


class PoolTaskError(Exception):
    def __init__(self, underlying):
        self.underlying = underlying


class ResultThread(threading.Thread):
    """Thread that collects results from distributed actors.

    It winds down when either:
        - A pre-specified number of objects has been processed
        - When the END_SENTINEL (submitted through self.add_object_ref())
            has been received and all objects received before that have been
            processed.

    Initialize the thread with total_object_refs = float('inf') to wait for the
    END_SENTINEL.

    Args:
        object_refs: ObjectRefs to Ray Actor calls.
            Thread tracks whether they are ready. More ObjectRefs may be added
            with add_object_ref (or _add_object_ref internally) until the object
            count reaches total_object_refs.
        single_result: Should be True if the thread is managing function
            with a single result (like apply_async). False if the thread is managing
            a function with a List of results.
        callback: called only once at the end of the thread
            if no results were errors. If single_result=True, and result is
            not an error, callback is invoked with the result as the only
            argument. If single_result=False, callback is invoked with
            a list of all the results as the only argument.
        error_callback: called only once on the first result
            that errors. Should take an Exception as the only argument.
            If no result errors, this callback is not called.
        total_object_refs: Number of ObjectRefs that this thread
            expects to be ready. May be more than len(object_refs) since
            more ObjectRefs can be submitted after the thread starts.
            If None, defaults to len(object_refs). If float("inf"), thread runs
            until END_SENTINEL (submitted through self.add_object_ref())
            has been received and all objects received before that have
            been processed.
    """

    END_SENTINEL = None

    def __init__(
        self,
        object_refs: list,
        single_result: bool = False,
        callback: callable = None,
        error_callback: callable = None,
        total_object_refs: Optional[int] = None,
    ):
        threading.Thread.__init__(self, daemon=True)
        self._got_error = False
        self._object_refs = []
        self._num_ready = 0
        self._results = []
        self._ready_index_queue = queue.Queue()
        self._single_result = single_result
        self._callback = callback
        self._error_callback = error_callback
        self._total_object_refs = (
            len(object_refs) if total_object_refs is None else total_object_refs
        )
        self._indices = {}
        # Thread-safe queue used to add ObjectRefs to fetch after creating
        # this thread (used to lazily submit for imap and imap_unordered).
        self._new_object_refs = queue.Queue()
        for object_ref in object_refs:
            self._add_object_ref(object_ref)

    def _add_object_ref(self, object_ref):
        self._indices[object_ref] = len(self._object_refs)
        self._object_refs.append(object_ref)
        self._results.append(None)

    def add_object_ref(self, object_ref):
        self._new_object_refs.put(object_ref)

    def finish(self, total_object_refs):
        self._total_object_refs = total_object_refs
        # Wake the thread if it is waiting for its first dynamically submitted
        # ObjectRef (including the empty-iterable case).
        self._new_object_refs.put(self.END_SENTINEL)

    def run(self):
        unready = copy.copy(self._object_refs)
        aggregated_batch_results = []

        # Run for a specific number of objects if self._total_object_refs is finite.
        # Otherwise, process all objects received prior to the stop signal, given by
        # self.add_object(END_SENTINEL).
        while self._num_ready < self._total_object_refs:
            # Get as many new IDs from the queue as possible without blocking,
            # unless we have no IDs to wait on, in which case we block.
            ready_id = None
            while ready_id is None:
                try:
                    block = len(unready) == 0
                    new_object_ref = self._new_object_refs.get(block=block)
                    if new_object_ref is self.END_SENTINEL:
                        # Receiving the END_SENTINEL object is the signal to stop.
                        # Store the total number of objects unless the producer
                        # already supplied it before all refs were dispatched.
                        if self._total_object_refs == float("inf"):
                            self._total_object_refs = len(self._object_refs)
                        if self._num_ready >= self._total_object_refs:
                            return
                    else:
                        self._add_object_ref(new_object_ref)
                        unready.append(new_object_ref)
                except queue.Empty:
                    # queue.Empty means no result was retrieved if block=False.
                    pass

                # Check if any of the available IDs are done. The timeout is required
                # here to periodically check for new IDs from self._new_object_refs.
                # NOTE(edoakes): the choice of a 100ms timeout here is arbitrary. Too
                # low of a timeout would cause higher overhead from busy spinning and
                # too high would cause higher tail latency to fetch the first result in
                # some cases.
                ready, unready = ray.wait(unready, num_returns=1, timeout=0.1)
                if len(ready) > 0:
                    ready_id = ready[0]

            try:
                batch = ray.get(ready_id)
            except ray.exceptions.RayError as e:
                batch = [e]

            # The exception callback is called only once on the first result
            # that errors. If no result errors, it is never called.
            if not self._got_error:
                for result in batch:
                    if isinstance(result, Exception):
                        self._got_error = True
                        if self._error_callback is not None:
                            self._error_callback(result)
                        break
                    else:
                        aggregated_batch_results.append(result)

            self._num_ready += 1
            self._results[self._indices[ready_id]] = batch
            self._ready_index_queue.put(self._indices[ready_id])

        # The regular callback is called only once on the entire List of
        # results as long as none of the results were errors. If any results
        # were errors, the regular callback is never called; instead, the
        # exception callback is called on the first erroring result.
        #
        # This callback is called outside the while loop to ensure that it's
        # called on the entire list of results– not just a single batch.
        if not self._got_error and self._callback is not None:
            if not self._single_result:
                self._callback(aggregated_batch_results)
            else:
                # On a thread handling a function with a single result
                # (e.g. apply_async), we call the callback on just that result
                # instead of on a list encaspulating that result
                self._callback(aggregated_batch_results[0])

    def got_error(self):
        # Should only be called after the thread finishes.
        return self._got_error

    def result(self, index):
        # Should only be called on results that are ready.
        return self._results[index]

    def results(self):
        # Should only be called after the thread finishes.
        return self._results

    def next_ready_index(self, timeout=None):
        try:
            return self._ready_index_queue.get(timeout=timeout)
        except queue.Empty:
            # queue.Queue signals a timeout by raising queue.Empty.
            raise TimeoutError


class AsyncResult:
    """An asynchronous interface to task results.

    This should not be constructed directly.
    """

    def __init__(
        self,
        chunk_object_refs,
        callback=None,
        error_callback=None,
        single_result=False,
        total_object_refs=None,
        pool=None,
    ):
        self._single_result = single_result
        # Hold a strong reference to the pool so it cannot be garbage
        # collected while submitted work is still in flight. Submission is
        # asynchronous (a dispatcher thread hands batches to actors), so
        # without this a pattern like ``Pool(2).apply_async(f, (x,)).get()``
        # could drop the pool before the dispatcher drains its queue, silently
        # abandoning the work. Released once the result is ready in ``get``.
        self._pool = pool
        self._result_thread = ResultThread(
            chunk_object_refs,
            single_result,
            callback,
            error_callback,
            total_object_refs=total_object_refs,
        )
        self._result_thread.start()

    def add_object_ref(self, object_ref):
        self._result_thread.add_object_ref(object_ref)

    def wait(self, timeout: Optional[float] = None):
        """
        Returns once the result is ready or the timeout expires (does not
        raise TimeoutError).

        Args:
            timeout: timeout in milliseconds.
        """

        self._result_thread.join(timeout)

    def get(self, timeout=None):
        self.wait(timeout)
        if self._result_thread.is_alive():
            raise TimeoutError

        # The result is ready, so the submitted work no longer needs the pool
        # to be kept alive; release the reference held for it.
        self._pool = None

        results = []
        for batch in self._result_thread.results():
            for result in batch:
                if isinstance(result, PoolTaskError):
                    raise result.underlying
                elif isinstance(result, Exception):
                    raise result
            results.extend(batch)

        if self._single_result:
            return results[0]

        return results

    def ready(self):
        """
        Returns true if the result is ready, else false if the tasks are still
        running.
        """

        return not self._result_thread.is_alive()

    def successful(self):
        """
        Returns true if none of the submitted tasks errored, else false. Should
        only be called once the result is ready (can be checked using `ready`).
        """

        if not self.ready():
            raise ValueError(f"{self!r} not ready")
        return not self._result_thread.got_error()


class IMapIterator:
    """Base class for OrderedIMapIterator and UnorderedIMapIterator."""

    def __init__(self, pool, func, iterable, chunksize=None):
        self._pool = pool
        self._func = func
        self._next_chunk_index = 0
        self._finished_iterating = False
        # List of bools indicating if the given chunk is ready or not for all
        # submitted chunks. Ordering mirrors that in the in the ResultThread.
        self._submitted_chunks = []
        self._ready_objects = collections.deque()
        self._iterator = iter(iterable)
        if isinstance(iterable, collections.abc.Iterator):
            # Got iterator (which has no len() function).
            # Make default chunksize 1 instead of using _calculate_chunksize().
            # Indicate unknown queue length, requiring explicit stopping.
            self._chunksize = chunksize or 1
            result_list_size = float("inf")
        else:
            self._chunksize = chunksize or pool._calculate_chunksize(iterable)
            result_list_size = div_round_up(len(iterable), chunksize)

        self._result_thread = ResultThread([], total_object_refs=result_list_size)
        self._result_thread.start()

        for _ in range(len(self._pool._actor_pool)):
            self._submit_next_chunk()

    def _submit_next_chunk(self):
        # The full iterable has already been submitted, so no-op.
        if self._finished_iterating:
            return

        chunk_iterator = itertools.islice(self._iterator, self._chunksize)

        # Check whether we have run out of samples.
        # This consumes the original iterator, so we convert to a list and back
        chunk_list = list(chunk_iterator)
        if len(chunk_list) < self._chunksize:
            # Reached end of self._iterator
            self._finished_iterating = True
            if len(chunk_list) == 0:
                self._result_thread.finish(len(self._submitted_chunks))
                # Nothing to do, return.
                return
        chunk_iterator = iter(chunk_list)

        final_chunk = self._finished_iterating

        def add_object_ref(object_ref):
            self._result_thread.add_object_ref(object_ref)
            if final_chunk:
                self._result_thread.add_object_ref(ResultThread.END_SENTINEL)

        self._pool._submit_chunk(
            self._func,
            chunk_iterator,
            self._chunksize,
            add_object_ref=add_object_ref,
        )
        self._submitted_chunks.append(False)

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        # Should be implemented by subclasses.
        raise NotImplementedError


class OrderedIMapIterator(IMapIterator):
    """Iterator to the results of tasks submitted using `imap`.

    The results are returned in the same order that they were submitted, even
    if they don't finish in that order. Only one batch of tasks per actor
    process is submitted at a time - the rest are submitted as results come in.

    Should not be constructed directly.
    """

    def next(self, timeout=None):
        if len(self._ready_objects) == 0:
            if self._finished_iterating and (
                self._next_chunk_index == len(self._submitted_chunks)
            ):
                # Finish when all chunks have been dispatched and processed
                # Notify the calling process that the work is done.
                raise StopIteration

            # This loop will break when the next index in order is ready or
            # self._result_thread.next_ready_index() raises a timeout.
            index = -1
            while index != self._next_chunk_index:
                start = time.time()
                index = self._result_thread.next_ready_index(timeout=timeout)
                self._submit_next_chunk()
                self._submitted_chunks[index] = True
                if timeout is not None:
                    timeout = max(0, timeout - (time.time() - start))

            while (
                self._next_chunk_index < len(self._submitted_chunks)
                and self._submitted_chunks[self._next_chunk_index]
            ):
                for result in self._result_thread.result(self._next_chunk_index):
                    self._ready_objects.append(result)
                self._next_chunk_index += 1

        return self._ready_objects.popleft()


class UnorderedIMapIterator(IMapIterator):
    """Iterator to the results of tasks submitted using `imap`.

    The results are returned in the order that they finish. Only one batch of
    tasks per actor process is submitted at a time - the rest are submitted as
    results come in.

    Should not be constructed directly.
    """

    def next(self, timeout=None):
        if len(self._ready_objects) == 0:
            if self._finished_iterating and (
                self._next_chunk_index == len(self._submitted_chunks)
            ):
                # Finish when all chunks have been dispatched and processed
                # Notify the calling process that the work is done.
                raise StopIteration

            index = self._result_thread.next_ready_index(timeout=timeout)
            self._submit_next_chunk()

            for result in self._result_thread.result(index):
                self._ready_objects.append(result)
            self._next_chunk_index += 1

        return self._ready_objects.popleft()


@ray.remote(num_cpus=0)
class PoolActor:
    """Actor used to process tasks submitted to a Pool."""

    def __init__(self, initializer=None, initargs=None):
        if initializer:
            initargs = initargs or ()
            initializer(*initargs)

    def ping(self):
        # Used to wait for this actor to be initialized.
        pass

    def run_batch(self, func, batch):
        results = []
        for args, kwargs in batch:
            args = args or ()
            kwargs = kwargs or {}
            try:
                results.append(func(*args, **kwargs))
            except Exception as e:
                results.append(PoolTaskError(e))
        return results


# https://docs.python.org/3/library/multiprocessing.html#module-multiprocessing.pool
class Pool:
    """A pool of actor processes that is used to process tasks in parallel.

    Args:
        processes: number of actor processes to start in the pool. Defaults to
            the number of cores in the Ray cluster if one is already running,
            otherwise the number of cores on this machine.
        initializer: function to be run in each actor when it starts up.
        initargs: iterable of arguments to the initializer function.
        maxtasksperchild: maximum number of tasks to run in each actor process.
            After a process has executed this many tasks, it will be killed and
            replaced with a new one.
        context: Accepted for ``multiprocessing.Pool`` API compatibility but
            ignored; Ray controls process initialization. A warning is logged
            if a non-None value is supplied.
        ray_address: address of the Ray cluster to run on. If None, a new local
            Ray cluster will be started on this machine. Otherwise, this will
            be passed to `ray.init()` to connect to a running cluster. This may
            also be specified using the `RAY_ADDRESS` environment variable.
        ray_remote_args: arguments used to configure the Ray Actors making up
            the pool. See :func:`ray.remote` for details.
        min_size: minimum number of actors to keep alive. Defaults to 0 (reap
            all idle actors). Supplying this argument enables autoscaling.
        max_size: maximum number of actors. Defaults to ``processes`` when it
            is given, otherwise the number of cluster CPUs. May exceed the
            current cluster CPUs; pending actors surface demand to the
            autoscaler. Supplying this argument enables autoscaling.
        initial_size: number of actors to pre-warm at startup. Defaults to 0
            (fully lazy). Supplying this argument enables autoscaling.
        idle_timeout_s: seconds an actor may be idle before being reaped.
            Defaults to 60. Supplying this argument enables autoscaling.

    By default the pool eagerly creates a fixed number of actors
    (``processes``). Supplying any of ``min_size``, ``max_size``,
    ``initial_size``, or ``idle_timeout_s`` instead creates an autoscaling
    pool: actors request ``num_cpus=1`` so pending placements drive the
    autoscaler, the pool grows on demand up to ``max_size``, and idle actors
    are reaped down to ``min_size`` after ``idle_timeout_s``. Actors that
    crash are replaced automatically in both modes.
    """

    def __init__(
        self,
        processes: Optional[int] = None,
        initializer: Optional[Callable] = None,
        initargs: Optional[Iterable] = None,
        maxtasksperchild: Optional[int] = None,
        context: Any = None,
        ray_address: Optional[str] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        # Autoscaling (experimental). Supplying any of these four arguments
        # enables autoscaling: the pool keeps warm actors and grows on demand
        # (capped at max_size) instead of eagerly creating a fixed pool with
        # a readiness barrier. Idle actors are reaped after idle_timeout_s
        # down to min_size, so the cluster can scale back down.
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
        initial_size: Optional[int] = None,
        idle_timeout_s: Optional[float] = None,
    ):
        usage_lib.record_library_usage("util.multiprocessing.Pool")

        self._closed = False
        self._initializer = initializer
        self._initargs = initargs
        self._maxtasksperchild = maxtasksperchild or -1
        self._actor_deletion_ids = []
        self._registry: List[Tuple[Any, ray.ObjectRef]] = []
        self._registry_hashable: Dict[Hashable, ray.ObjectRef] = {}
        self._ray_remote_args = ray_remote_args or {}
        self._pool_actor = None

        # Autoscaling is enabled when any size argument is supplied. The flag
        # only gates construction-time behavior (num_cpus default, readiness
        # barrier, validation); submission and actor lifecycle are identical
        # in both modes. A fixed pool is simply an autoscaling pool with
        # min_size == max_size == processes.
        self._autoscale = any(
            v is not None for v in (min_size, max_size, initial_size, idle_timeout_s)
        )
        self._min_size = 0 if min_size is None else min_size
        self._idle_timeout_s = 60.0 if idle_timeout_s is None else idle_timeout_s
        self._pool_lock = threading.Lock()
        self._last_used: List[float] = []
        self._ready_actor_indices = collections.deque()
        self._starting_actor_refs = {}
        self._running_actor_refs = {}
        self._batch_queue = queue.Queue()
        self._dispatcher_wakeup = threading.Event()
        self._dispatcher_terminate = threading.Event()
        self._dispatcher_thread: Optional[threading.Thread] = None
        # True once at least one actor has become ready. Used to decide whether
        # a failed actor startup should close the pool (it never became healthy)
        # or just drop the slot so the resize step can retry (it was healthy).
        self._pool_ever_healthy = False
        # Count of consecutive actor-startup failures while no actor is making
        # progress. Resets to zero whenever an actor becomes ready. Bounds the
        # retry of failed replacements so a pool whose actors persistently fail
        # to start fails fast instead of churning forever.
        self._startup_failures = 0

        if context and log_once("context_argument_warning"):
            logger.warning(
                "The 'context' argument is not supported using "
                "ray. Please refer to the documentation for how "
                "to control ray initialization."
            )

        if self._autoscale:
            # Resource-bearing actors so pending placements drive the
            # autoscaler (num_cpus=0 actors surface no CPU demand).
            self._ray_remote_args.setdefault("num_cpus", 1)

        processes, ray_cpus = self._init_ray(processes, ray_address)

        if self._autoscale:
            if max_size is not None:
                self._max_size = max_size
            elif processes is not None:
                self._max_size = processes
            else:
                self._max_size = ray_cpus
            self._initial_size = 0 if initial_size is None else initial_size
            if self._max_size <= 0:
                raise ValueError("max_size must be greater than 0.")
            if not 0 <= self._min_size <= self._max_size:
                raise ValueError("min_size must be between 0 and max_size.")
            if not 0 <= self._initial_size <= self._max_size:
                raise ValueError("initial_size must be between 0 and max_size.")
            if self._idle_timeout_s < 0:
                raise ValueError("idle_timeout_s must be greater than or equal to 0.")
        else:
            # A fixed pool is an autoscaling pool with
            # min_size == max_size == initial_size == processes: the desired
            # size is always `processes`, so actors are never reaped and a
            # crashed actor is always replaced.
            self._min_size = processes
            self._max_size = processes
            self._initial_size = processes

        self._start_actor_pool()

    def _init_ray(self, processes=None, ray_address=None):
        # Initialize ray. If ray is already initialized, we do nothing.
        # Else, the priority is:
        # ray_address argument > RAY_ADDRESS > start new local cluster.
        if not ray.is_initialized():
            # Cluster mode.
            if ray_address is None and (
                RAY_ADDRESS_ENV in os.environ
                or ray._private.utils.read_ray_address() is not None
            ):
                init_kwargs = {}
                if os.environ.get(RAY_ADDRESS_ENV) == "local":
                    init_kwargs["num_cpus"] = processes
                ray.init(**init_kwargs)
            elif ray_address is not None:
                init_kwargs = {}
                if ray_address == "local":
                    init_kwargs["num_cpus"] = processes
                ray.init(address=ray_address, **init_kwargs)
            # Local mode.
            else:
                ray.init(num_cpus=processes)

        ray_cpus = int(ray.cluster_resources().get("CPU", 0))
        if not self._autoscale:
            if processes is None:
                processes = ray_cpus
            if processes <= 0:
                raise ValueError("Processes in the pool must be >0.")
            # A fixed pool must fit the current cluster. An autoscaling pool
            # may exceed it: pending num_cpus=1 actors drive the autoscaler.
            if ray_cpus < processes:
                raise ValueError(
                    "Tried to start a pool with {} processes on an "
                    "existing ray cluster, but there are only {} "
                    "CPUs in the ray cluster.".format(processes, ray_cpus)
                )

        return processes, ray_cpus

    def _start_actor_pool(self):
        # Validate actor options synchronously even when no actors are
        # initially created. Dispatcher failures must not strand results.
        self._pool_actor = PoolActor.options(**self._ray_remote_args)
        # Fixed-length slot list. A slot is None until its actor is created:
        # autoscaling pools create actors lazily on demand, while fixed pools
        # create all of them below. Lazy creation lets pending num_cpus=1
        # actors drive the autoscaler instead of deadlocking construction
        # (the #22048 deadlock lever).
        target = self._max_size
        self._actor_pool: List[Optional[Tuple[Any, int]]] = [None] * target
        self._last_used = [0.0] * target
        for i in range(self._initial_size):
            self._start_actor(i, demand=False)

        if not self._autoscale:
            # Fixed pools preserve the blocking readiness barrier: the
            # constructor does not return until every actor has passed ping
            # (initializers have run). Runs before the dispatcher starts, so
            # the completed ping refs are simply moved to the ready queue by
            # the dispatcher's first state update.
            try:
                ray.get(list(self._starting_actor_refs))
            except Exception:
                for slot in self._actor_pool:
                    if slot is not None:
                        ray.kill(slot[0])
                self._starting_actor_refs.clear()
                raise

        wakeup = self._dispatcher_wakeup
        pool_ref = weakref.ref(self, lambda _: wakeup.set())
        self._dispatcher_thread = threading.Thread(
            target=Pool._dispatch_batches,
            args=(pool_ref,),
            daemon=True,
        )
        self._dispatcher_thread.start()

    def _wait_for_stopping_actors(self, timeout=None):
        if len(self._actor_deletion_ids) == 0:
            return
        if timeout is not None:
            timeout = float(timeout)

        _, deleting = ray.wait(
            self._actor_deletion_ids,
            num_returns=len(self._actor_deletion_ids),
            timeout=timeout,
        )
        self._actor_deletion_ids = deleting

    def _stop_actor(self, actor):
        # Check and clean up any outstanding IDs corresponding to deletions.
        self._wait_for_stopping_actors(timeout=0.0)
        # The deletion task will block until the actor has finished executing
        # all pending tasks.
        try:
            self._actor_deletion_ids.append(actor.__ray_terminate__.remote())
        except ray.exceptions.RayActorError:
            # The actor is already dead; there is nothing to stop gracefully.
            pass

    def _new_actor_entry(self):
        # NOTE(edoakes): The initializer function can't currently be used to
        # modify the global namespace (e.g., import packages or set globals)
        # due to a limitation in cloudpickle.
        # Cache the PoolActor with options
        if not self._pool_actor:
            self._pool_actor = PoolActor.options(**self._ray_remote_args)
        return (self._pool_actor.remote(self._initializer, self._initargs), 0)

    def _start_actor(self, actor_index, demand=True):
        entry = self._new_actor_entry()
        self._actor_pool[actor_index] = entry
        self._last_used[actor_index] = time.monotonic()
        actor, _ = entry
        try:
            object_ref = actor.ping.remote()
        except Exception:
            # The readiness ping could not be submitted (for example, the
            # actor handle is already unusable). Clear the slot so it is not
            # left as an untracked entry, kill the actor, and re-raise so the
            # caller decides how to handle the failure.
            self._actor_pool[actor_index] = None
            self._last_used[actor_index] = 0.0
            ray.kill(actor)
            raise
        self._starting_actor_refs[object_ref] = (actor_index, demand)
        self._wake_dispatcher_when_ready(object_ref)

    def _enqueue_batch(self, func, batch, add_object_ref):
        with self._pool_lock:
            if self._closed:
                if self._dispatcher_terminate.is_set():
                    error = RuntimeError("Pool was terminated")
                else:
                    error = ValueError("Pool not running")
            else:
                self._batch_queue.put((func, batch, add_object_ref))
                self._dispatcher_wakeup.set()
                return
        add_object_ref(ray.put([PoolTaskError(error)]))

    def _wake_dispatcher_when_ready(self, object_ref):
        # ObjectRef callbacks may run on a core-worker thread. They only wake
        # the dispatcher; all pool state remains owned by the dispatcher.
        # Capture the Event rather than the Pool so the callback does not
        # extend the Pool's lifetime.
        wakeup = self._dispatcher_wakeup
        object_ref._on_completed(lambda _: wakeup.set())

    def _drain_batch_queue(self, pending):
        while True:
            try:
                pending.append(self._batch_queue.get_nowait())
            except queue.Empty:
                return

    def _update_actor_states(self):
        refs = list(self._starting_actor_refs) + list(self._running_actor_refs)
        if not refs:
            return
        ready, _ = ray.wait(refs, num_returns=len(refs), timeout=0)
        now = time.monotonic()
        for object_ref in ready:
            if object_ref in self._starting_actor_refs:
                actor_index, _ = self._starting_actor_refs.pop(object_ref)
                try:
                    ray.get(object_ref)
                except ray.exceptions.RayError as error:
                    self._actor_pool[actor_index] = None
                    self._last_used[actor_index] = 0.0
                    if not self._pool_ever_healthy:
                        # The pool has not served any work yet, so a startup
                        # failure means it cannot function at all; fail fast
                        # rather than hang.
                        return error
                    # The pool was already serving work, so a failed actor
                    # (for example a replacement started by the resize step)
                    # is just dropped; the resize step will retry. Closing the
                    # pool here would also kill healthy actors. Bound the
                    # retries, though: if replacements keep failing while no
                    # actor makes progress, fail fast instead of churning.
                    self._startup_failures += 1
                    if self._startup_failures > self._max_size:
                        return error
                    continue
                else:
                    self._last_used[actor_index] = now
                    self._pool_ever_healthy = True
                    self._startup_failures = 0
                    self._ready_actor_indices.append(actor_index)
            else:
                actor_index, retire = self._running_actor_refs.pop(object_ref)
                try:
                    ray.get(object_ref)
                except ray.exceptions.RayActorError:
                    # The actor died while running the batch. The batch's error
                    # is surfaced to the caller through the ResultThread; drop
                    # the slot here so the resize step replaces the actor.
                    self._actor_pool[actor_index] = None
                    self._last_used[actor_index] = 0.0
                    continue
                except Exception:
                    # The batch failed without killing the actor (for example,
                    # an unserializable return value raises RayTaskError). The
                    # error is surfaced to the caller through the ResultThread;
                    # the actor is still usable, so handle it like any other
                    # completed batch below. Swallowing the exception here is
                    # required: an exception escaping would kill the dispatcher
                    # thread and hang the pool.
                    pass
                if retire:
                    actor, _ = self._actor_pool[actor_index]
                    self._stop_actor(actor)
                    self._actor_pool[actor_index] = None
                    self._last_used[actor_index] = 0.0
                else:
                    self._last_used[actor_index] = now
                    self._pool_ever_healthy = True
                    self._startup_failures = 0
                    self._ready_actor_indices.append(actor_index)
        return None

    def _resize_actor_pool(self, desired):
        live = sum(slot is not None for slot in self._actor_pool)
        while live < desired:
            actor_index = self._actor_pool.index(None)
            try:
                self._start_actor(actor_index)
            except Exception as error:
                # _start_actor already cleared the slot and killed the actor.
                # Fail fast when nothing else can serve work or generate a
                # completion wakeup to retry: either the pool has not been
                # healthy yet, or no actor is live or starting. Otherwise
                # other live/starting actors produce wakeups, so a later
                # resize can retry.
                if not self._pool_ever_healthy or (
                    live == 0 and not self._starting_actor_refs
                ):
                    return error
                break
            live += 1

        # Pending actors created for backlog are only demand signals. Remove
        # excess ones once the backlog shrinks; initial warm actors are left
        # for normal idle-timeout reaping.
        for object_ref, (actor_index, demand) in list(
            self._starting_actor_refs.items()
        ):
            if live <= desired:
                break
            if demand:
                actor, _ = self._actor_pool[actor_index]
                self._starting_actor_refs.pop(object_ref)
                self._actor_pool[actor_index] = None
                self._last_used[actor_index] = 0.0
                ray.kill(actor)
                live -= 1
        return None

    def _dispatch_ready_batches(self, pending):
        while pending and self._ready_actor_indices:
            func, batch, add_object_ref = pending[0]
            actor_index = self._ready_actor_indices.popleft()
            actor, count = self._actor_pool[actor_index]
            try:
                object_ref = actor.run_batch.remote(func, batch)
            except ray.exceptions.RayActorError:
                # The actor died after it became ready; once the death is
                # known, submitting to its handle raises synchronously. Drop
                # the slot (the resize step replaces the actor) and retry the
                # batch, which is still at the front of the queue, on another
                # ready actor or the replacement.
                self._actor_pool[actor_index] = None
                self._last_used[actor_index] = 0.0
                continue
            except Exception as error:
                # Submission may fail synchronously (for example, when func or
                # batch contains an unserializable value). Fail only this
                # batch and keep the dispatcher and actor available for later
                # work, matching multiprocessing.Pool's asynchronous errors.
                add_object_ref(ray.put([PoolTaskError(error)]))
                pending.popleft()
                self._ready_actor_indices.append(actor_index)
                continue

            add_object_ref(object_ref)
            pending.popleft()
            count += 1
            assert self._maxtasksperchild == -1 or count <= self._maxtasksperchild
            retire = count == self._maxtasksperchild
            self._actor_pool[actor_index] = (actor, count)
            self._running_actor_refs[object_ref] = (actor_index, retire)
            self._wake_dispatcher_when_ready(object_ref)

    def _reap_idle_actors(self):
        now = time.monotonic()
        live = sum(slot is not None for slot in self._actor_pool)
        for actor_index in list(self._ready_actor_indices):
            if live <= self._min_size:
                break
            if now - self._last_used[actor_index] > self._idle_timeout_s:
                actor, _ = self._actor_pool[actor_index]
                self._ready_actor_indices.remove(actor_index)
                self._actor_pool[actor_index] = None
                self._last_used[actor_index] = 0.0
                self._stop_actor(actor)
                live -= 1

    def _fail_pending_batches(self, pending, error):
        self._drain_batch_queue(pending)
        while pending:
            _, _, add_object_ref = pending.popleft()
            add_object_ref(ray.put([PoolTaskError(error)]))

    def _stop_pool_actors(self, force=False):
        starting = {
            actor_index for actor_index, _ in self._starting_actor_refs.values()
        }
        for actor_index, slot in enumerate(self._actor_pool):
            if slot is None:
                continue
            actor, _ = slot
            if force or actor_index in starting:
                ray.kill(actor)
            else:
                self._stop_actor(actor)

    def _next_idle_timeout(self):
        live = sum(slot is not None for slot in self._actor_pool)
        if live <= self._min_size or not self._ready_actor_indices:
            return None
        next_deadline = min(
            self._last_used[actor_index] + self._idle_timeout_s
            for actor_index in self._ready_actor_indices
        )
        return max(0.0, next_deadline - time.monotonic())

    @staticmethod
    def _dispatch_batches(pool_ref):
        # State-machine invariants:
        # - only this thread mutates actor lifecycle and scheduling state;
        # - starting actors carry resource demand but never receive batches;
        # - ready actors have no in-flight batch;
        # - running actors have exactly one ObjectRef in _running_actor_refs;
        # - completion callbacks only set _dispatcher_wakeup.
        pending = collections.deque()
        while True:
            pool = pool_ref()
            if pool is None:
                return

            # Clear before checking queues and refs. A callback racing after
            # this point leaves the Event set, so the following wait cannot
            # lose a completion notification.
            pool._dispatcher_wakeup.clear()
            pool._drain_batch_queue(pending)

            with pool._pool_lock:
                if pool._dispatcher_terminate.is_set():
                    # Dispatch queued batches to the actors that are ready and
                    # then let terminate() kill the actors, so that batches
                    # already accepted by the pool fail with the actors (a
                    # RayError) rather than a submission error. This matches the
                    # previous eager pool, which had submitted every batch to an
                    # actor before terminate() could run. Batches that cannot be
                    # dispatched are failed as terminated.
                    pool._update_actor_states()
                    pool._dispatch_ready_batches(pending)
                    pool._fail_pending_batches(
                        pending, RuntimeError("Pool was terminated")
                    )
                    return

                startup_error = pool._update_actor_states()
                if startup_error is not None:
                    pool._closed = True
                    pool._fail_pending_batches(pending, startup_error)
                    pool._stop_pool_actors(force=True)
                    return

                desired = min(
                    pool._max_size,
                    max(
                        pool._min_size,
                        len(pending) + len(pool._running_actor_refs),
                    ),
                )
                resize_error = pool._resize_actor_pool(desired)
                if resize_error is not None:
                    pool._closed = True
                    pool._fail_pending_batches(pending, resize_error)
                    pool._stop_pool_actors(force=True)
                    return
                pool._dispatch_ready_batches(pending)
                pool._reap_idle_actors()

                if pool._closed and not pending and pool._batch_queue.empty():
                    pool._stop_pool_actors()
                    return

                idle_timeout = pool._next_idle_timeout()

            wakeup = pool._dispatcher_wakeup
            if (
                not pending
                and not pool._running_actor_refs
                and pool._batch_queue.empty()
            ):
                pool = None
            wakeup.wait(timeout=idle_timeout)

    def apply(
        self,
        func: Callable,
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict] = None,
    ):
        """Run the given function on a random actor process and return the
        result synchronously.

        Args:
            func: function to run.
            args: optional arguments to the function.
            kwargs: optional keyword arguments to the function.

        Returns:
            The result.
        """

        return self.apply_async(func, args, kwargs).get()

    def apply_async(
        self,
        func: Callable,
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict] = None,
        callback: Callable[[Any], None] = None,
        error_callback: Callable[[Exception], None] = None,
    ):
        """Run the given function on a random actor process and return an
        asynchronous interface to the result.

        Args:
            func: function to run.
            args: optional arguments to the function.
            kwargs: optional keyword arguments to the function.
            callback: callback to be executed on the result once it is finished
                only if it succeeds.
            error_callback: callback to be executed the result once it is
                finished only if the task errors. The exception raised by the
                task will be passed as the only argument to the callback.

        Returns:
            AsyncResult containing the result.
        """

        self._check_running()
        func = self._convert_to_ray_batched_calls_if_needed(func)
        result = AsyncResult(
            [],
            callback,
            error_callback,
            single_result=True,
            total_object_refs=1,
            pool=self,
        )
        self._enqueue_batch(func, [(args, kwargs)], result.add_object_ref)
        return result

    def _convert_to_ray_batched_calls_if_needed(self, func: Callable) -> Callable:
        """Convert joblib's BatchedCalls to RayBatchedCalls for ObjectRef caching.

        This converts joblib's BatchedCalls callable, which is a collection of
        functions with their args and kwargs to be ran sequentially in an
        Actor, to a RayBatchedCalls callable, which provides identical
        functionality in addition to a method which ensures that common
        args and kwargs are put into the object store just once, saving time
        and memory. That method is then ran.

        If func is not a BatchedCalls instance, it is returned without changes.

        The ObjectRefs are cached inside two registries (_registry and
        _registry_hashable), which are common for the entire Pool and are
        cleaned on close."""
        if RayBatchedCalls is None:
            return func
        orginal_func = func
        # SafeFunction is a Python 2 leftover and can be
        # safely removed.
        if SafeFunction is not None and isinstance(func, SafeFunction):
            func = func.func
        if isinstance(func, BatchedCalls):
            func = RayBatchedCalls(
                func.items,
                (func._backend, func._n_jobs),
                func._reducer_callback,
                func._pickle_cache,
            )
            # go through all the items and replace args and kwargs with
            # ObjectRefs, caching them in registries
            func.put_items_in_object_store(self._registry, self._registry_hashable)
        else:
            func = orginal_func
        return func

    def _calculate_chunksize(self, iterable):
        chunksize, extra = divmod(len(iterable), len(self._actor_pool) * 4)
        if extra:
            chunksize += 1
        return chunksize

    def _submit_chunk(
        self,
        func,
        iterator,
        chunksize,
        unpack_args=False,
        add_object_ref=None,
    ):
        chunk = []
        while len(chunk) < chunksize:
            try:
                args = next(iterator)
                if not unpack_args:
                    args = (args,)
                chunk.append((args, {}))
            except StopIteration:
                break

        # Nothing to submit. The caller should prevent this.
        assert len(chunk) > 0

        self._enqueue_batch(func, chunk, add_object_ref)

    def _chunk_and_run(
        self, func, iterable, chunksize, unpack_args, callback, error_callback
    ):
        if not hasattr(iterable, "__len__"):
            iterable = list(iterable)
        if chunksize is None:
            chunksize = self._calculate_chunksize(iterable)

        num_chunks = 0 if len(iterable) == 0 else div_round_up(len(iterable), chunksize)
        result = AsyncResult(
            [],
            callback,
            error_callback,
            total_object_refs=num_chunks,
            pool=self,
        )
        iterator = iter(iterable)
        for _ in range(num_chunks):
            self._submit_chunk(
                func,
                iterator,
                chunksize,
                unpack_args=unpack_args,
                add_object_ref=result.add_object_ref,
            )
        return result

    def _map_async(
        self,
        func,
        iterable,
        chunksize=None,
        unpack_args=False,
        callback=None,
        error_callback=None,
    ):
        self._check_running()
        return self._chunk_and_run(
            func, iterable, chunksize, unpack_args, callback, error_callback
        )

    def map(self, func: Callable, iterable: Iterable, chunksize: Optional[int] = None):
        """Run the given function on each element in the iterable using the
        actor processes and return the results synchronously.

        Args:
            func: function to run.
            iterable: iterable of objects to be passed as the sole argument to
                func.
            chunksize: number of tasks to submit as a batch to each actor
                process. If unspecified, a suitable chunksize will be chosen.

        Returns:
            A list of results.
        """

        return self._map_async(
            func, iterable, chunksize=chunksize, unpack_args=False
        ).get()

    def map_async(
        self,
        func: Callable,
        iterable: Iterable,
        chunksize: Optional[int] = None,
        callback: Callable[[List], None] = None,
        error_callback: Callable[[Exception], None] = None,
    ):
        """Run the given function on each element in the iterable using the
        actor processes and return an asynchronous interface to the results.

        Args:
            func: function to run.
            iterable: iterable of objects to be passed as the only argument to
                func.
            chunksize: number of tasks to submit as a batch to each actor
                process. If unspecified, a suitable chunksize will be chosen.
            callback: Will only be called if none of the results were errors,
                and will only be called once after all results are finished.
                A Python List of all the finished results will be passed as the
                only argument to the callback.
            error_callback: callback executed on the first errored result.
                The Exception raised by the task will be passed as the only
                argument to the callback.

        Returns:
            AsyncResult
        """
        return self._map_async(
            func,
            iterable,
            chunksize=chunksize,
            unpack_args=False,
            callback=callback,
            error_callback=error_callback,
        )

    def starmap(self, func, iterable, chunksize=None):
        """Same as `map`, but unpacks each element of the iterable as the
        arguments to func like: [func(*args) for args in iterable].
        """

        return self._map_async(
            func, iterable, chunksize=chunksize, unpack_args=True
        ).get()

    def starmap_async(
        self,
        func: Callable,
        iterable: Iterable,
        callback: Callable[[List], None] = None,
        error_callback: Callable[[Exception], None] = None,
    ):
        """Same as `map_async`, but unpacks each element of the iterable as the
        arguments to func like: [func(*args) for args in iterable].
        """

        return self._map_async(
            func,
            iterable,
            unpack_args=True,
            callback=callback,
            error_callback=error_callback,
        )

    def imap(self, func: Callable, iterable: Iterable, chunksize: Optional[int] = 1):
        """Same as `map`, but only submits one batch of tasks to each actor
        process at a time.

        This can be useful if the iterable of arguments is very large or each
        task's arguments consumes a large amount of resources.

        The results are returned in the order corresponding to their arguments
        in the iterable.

        Args:
            func: Function to apply to each element of ``iterable``.
            iterable: Iterable of arguments to ``func``.
            chunksize: Number of elements to send to each worker per batch.

        Returns:
            OrderedIMapIterator
        """

        self._check_running()
        return OrderedIMapIterator(self, func, iterable, chunksize=chunksize)

    def imap_unordered(
        self, func: Callable, iterable: Iterable, chunksize: Optional[int] = 1
    ):
        """Same as `map`, but only submits one batch of tasks to each actor
        process at a time.

        This can be useful if the iterable of arguments is very large or each
        task's arguments consumes a large amount of resources.

        The results are returned in the order that they finish.

        Args:
            func: Function to apply to each element of ``iterable``.
            iterable: Iterable of arguments to ``func``.
            chunksize: Number of elements to send to each worker per batch.

        Returns:
            UnorderedIMapIterator
        """

        self._check_running()
        return UnorderedIMapIterator(self, func, iterable, chunksize=chunksize)

    def _check_running(self):
        if self._closed:
            raise ValueError("Pool not running")

    def __enter__(self):
        self._check_running()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.terminate()

    def close(self):
        """Close the pool.

        Prevents any more tasks from being submitted on the pool but allows
        outstanding work to finish.
        """

        self._registry.clear()
        self._registry_hashable.clear()
        with self._pool_lock:
            self._closed = True
        self._dispatcher_wakeup.set()
        gc.collect()

    def terminate(self):
        """Close the pool.

        Prevents any more tasks from being submitted on the pool and stops
        outstanding work.
        """

        self._registry.clear()
        self._registry_hashable.clear()
        with self._pool_lock:
            self._closed = True
            self._dispatcher_terminate.set()
        self._dispatcher_wakeup.set()
        if self._dispatcher_thread is not None:
            self._dispatcher_thread.join()
        with self._pool_lock:
            self._stop_pool_actors(force=True)
        gc.collect()

    def join(self):
        """Wait for the actors in a closed pool to exit.

        If the pool was closed using `close`, this will return once all
        outstanding work is completed.

        If the pool was closed using `terminate`, this will return quickly.
        """

        if not self._closed:
            raise ValueError("Pool is still running")
        if self._dispatcher_thread is not None:
            self._dispatcher_thread.join()
        self._wait_for_stopping_actors()
