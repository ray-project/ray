from typing import Callable, List, Tuple, Optional, Any, Dict, Hashable
import logging
from multiprocessing import TimeoutError
import os
import time
import random
import collections
import threading
import queue
import copy
import gc
import sys
try:
    from joblib.parallel import BatchedCalls, parallel_backend
    from joblib._parallel_backends import SafeFunction
except ImportError:
    BatchedCalls = None
    parallel_backend = None
    SafeFunction = None

import ray
from ray.util import log_once

logger = logging.getLogger(__name__)

RAY_ADDRESS_ENV = "RAY_ADDRESS"


def _put_in_dict_registry(
        obj: Any,
        registry_hashable: Dict[Hashable, ray.ObjectRef]) -> ray.ObjectRef:
    if obj not in registry_hashable:
        ret = ray.put(obj)
        registry_hashable[obj] = ret
    else:
        ret = registry_hashable[obj]
    return ret


def _put_in_list_registry(
        obj: Any, registry: List[Tuple[Any, ray.ObjectRef]]) -> ray.ObjectRef:
    try:
        ret = next((ref for o, ref in registry if o is obj))
    except StopIteration:
        ret = ray.put(obj)
        registry.append((obj, ret))
    return ret


def ray_put_if_needed(
        obj: Any,
        registry: Optional[List[Tuple[Any, ray.ObjectRef]]] = None,
        registry_hashable: Optional[Dict[Hashable, ray.ObjectRef]] = None
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
                registry_hashable: Optional[Dict[Hashable,
                                                 ray.ObjectRef]] = None):
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
                    ray_put_if_needed(arg, registry, registry_hashable)
                    for arg in args
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
                        **{k: ray_get_if_needed(v)
                           for k, v in kwargs.items()})
                    for func, args, kwargs in self.items
                ]

        def __reduce__(self):
            # Exactly the same as in BatchedCalls, with the
            # difference being that it returns RayBatchedCalls
            # instead
            if self._reducer_callback is not None:
                self._reducer_callback()
            # no need pickle the callback.
            return (RayBatchedCalls, (self.items, (self._backend,
                                                   self._n_jobs), None,
                                      self._pickle_cache))
else:
    RayBatchedCalls = None


# Helper function to divide a by b and round the result up.
def div_round_up(a, b):
    return -(-a // b)


class PoolTaskError(Exception):
    def __init__(self, underlying):
        self.underlying = underlying


class ResultThread(threading.Thread):
    def __init__(self,
                 object_refs,
                 callback=None,
                 error_callback=None,
                 total_object_refs=None):
        threading.Thread.__init__(self, daemon=True)
        self._got_error = False
        self._object_refs = []
        self._num_ready = 0
        self._results = []
        self._ready_index_queue = queue.Queue()
        self._callback = callback
        self._error_callback = error_callback
        self._total_object_refs = total_object_refs or len(object_refs)
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

    def run(self):
        unready = copy.copy(self._object_refs)
        while self._num_ready < self._total_object_refs:
            # Get as many new IDs from the queue as possible without blocking,
            # unless we have no IDs to wait on, in which case we block.
            while True:
                try:
                    block = len(unready) == 0
                    new_object_ref = self._new_object_refs.get(block=block)
                    self._add_object_ref(new_object_ref)
                    unready.append(new_object_ref)
                except queue.Empty:
                    # queue.Empty means no result was retrieved if block=False.
                    break

            [ready_id], unready = ray.wait(unready, num_returns=1)
            try:
                batch = ray.get(ready_id)
            except ray.exceptions.RayError as e:
                batch = [e]
            for result in batch:
                if isinstance(result, Exception):
                    self._got_error = True
                    if self._error_callback is not None:
                        self._error_callback(result)
                elif self._callback is not None:
                    self._callback(result)

            self._num_ready += 1
            self._results[self._indices[ready_id]] = batch
            self._ready_index_queue.put(self._indices[ready_id])

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

    def __init__(self,
                 chunk_object_refs,
                 callback=None,
                 error_callback=None,
                 single_result=False):
        self._single_result = single_result
        self._result_thread = ResultThread(chunk_object_refs, callback,
                                           error_callback)
        self._result_thread.start()

    def wait(self, timeout=None):
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
        # List of bools indicating if the given chunk is ready or not for all
        # submitted chunks. Ordering mirrors that in the in the ResultThread.
        self._submitted_chunks = []
        self._ready_objects = collections.deque()
        if not hasattr(iterable, "__len__"):
            iterable = [iterable]
        self._iterator = iter(iterable)
        self._chunksize = chunksize or pool._calculate_chunksize(iterable)
        self._total_chunks = div_round_up(len(iterable), chunksize)
        self._result_thread = ResultThread(
            [], total_object_refs=self._total_chunks)
        self._result_thread.start()

        for _ in range(len(self._pool._actor_pool)):
            self._submit_next_chunk()

    def _submit_next_chunk(self):
        # The full iterable has been submitted, so no-op.
        if len(self._submitted_chunks) >= self._total_chunks:
            return

        actor_index = len(self._submitted_chunks) % len(self._pool._actor_pool)
        new_chunk_id = self._pool._submit_chunk(self._func, self._iterator,
                                                self._chunksize, actor_index)
        self._submitted_chunks.append(False)
        self._result_thread.add_object_ref(new_chunk_id)

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
            if self._next_chunk_index == self._total_chunks:
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

            while self._next_chunk_index < len(
                    self._submitted_chunks
            ) and self._submitted_chunks[self._next_chunk_index]:
                for result in self._result_thread.result(
                        self._next_chunk_index):
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
            if self._next_chunk_index == self._total_chunks:
                raise StopIteration

            index = self._result_thread.next_ready_index(timeout=timeout)
            self._submit_next_chunk()

            for result in self._result_thread.result(index):
                self._ready_objects.append(result)
            self._next_chunk_index += 1

        return self._ready_objects.popleft()


@ray.remote(num_cpus=1)
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
        ray_address: address of the Ray cluster to run on. If None, a new local
            Ray cluster will be started on this machine. Otherwise, this will
            be passed to `ray.init()` to connect to a running cluster. This may
            also be specified using the `RAY_ADDRESS` environment variable.
    """

    def __init__(self,
                 processes=None,
                 initializer=None,
                 initargs=None,
                 maxtasksperchild=None,
                 context=None,
                 ray_address=None):
        self._closed = False
        self._initializer = initializer
        self._initargs = initargs
        self._maxtasksperchild = maxtasksperchild or -1
        self._actor_deletion_ids = []
        self._registry: List[Tuple[Any, ray.ObjectRef]] = []
        self._registry_hashable: Dict[Hashable, ray.ObjectRef] = {}

        if context and log_once("context_argument_warning"):
            logger.warning("The 'context' argument is not supported using "
                           "ray. Please refer to the documentation for how "
                           "to control ray initialization.")

        processes = self._init_ray(processes, ray_address)
        self._start_actor_pool(processes)

    def _init_ray(self, processes=None, ray_address=None):
        # Initialize ray. If ray is already initialized, we do nothing.
        # Else, the priority is:
        # ray_address argument > RAY_ADDRESS > start new local cluster.
        if not ray.is_initialized():
            # Cluster mode.
            if ray_address is None and RAY_ADDRESS_ENV in os.environ:
                logger.info("Connecting to ray cluster at address='{}'".format(
                    os.environ[RAY_ADDRESS_ENV]))
                ray.init()
            elif ray_address is not None:
                logger.info(
                    f"Connecting to ray cluster at address='{ray_address}'")
                ray.init(address=ray_address)
            # Local mode.
            else:
                logger.info("Starting local ray cluster")
                ray.init(num_cpus=processes)

        ray_cpus = int(ray.state.cluster_resources()["CPU"])
        if processes is None:
            processes = ray_cpus
        if processes <= 0:
            raise ValueError("Processes in the pool must be >0.")
        if ray_cpus < processes:
            raise ValueError("Tried to start a pool with {} processes on an "
                             "existing ray cluster, but there are only {} "
                             "CPUs in the ray cluster.".format(
                                 processes, ray_cpus))

        return processes

    def _start_actor_pool(self, processes):
        self._actor_pool = [self._new_actor_entry() for _ in range(processes)]
        ray.get([actor.ping.remote() for actor, _ in self._actor_pool])

    def _wait_for_stopping_actors(self, timeout=None):
        if len(self._actor_deletion_ids) == 0:
            return
        if timeout is not None:
            timeout = float(timeout)

        _, deleting = ray.wait(
            self._actor_deletion_ids,
            num_returns=len(self._actor_deletion_ids),
            timeout=timeout)
        self._actor_deletion_ids = deleting

    def _stop_actor(self, actor):
        # Check and clean up any outstanding IDs corresponding to deletions.
        self._wait_for_stopping_actors(timeout=0.0)
        # The deletion task will block until the actor has finished executing
        # all pending tasks.
        self._actor_deletion_ids.append(actor.__ray_terminate__.remote())

    def _new_actor_entry(self):
        # NOTE(edoakes): The initializer function can't currently be used to
        # modify the global namespace (e.g., import packages or set globals)
        # due to a limitation in cloudpickle.
        return (PoolActor.remote(self._initializer, self._initargs), 0)

    def _random_actor_index(self):
        return random.randrange(len(self._actor_pool))

    # Batch should be a list of tuples: (args, kwargs).
    def _run_batch(self, actor_index, func, batch):
        actor, count = self._actor_pool[actor_index]
        object_ref = actor.run_batch.remote(func, batch)
        count += 1
        assert self._maxtasksperchild == -1 or count <= self._maxtasksperchild
        if count == self._maxtasksperchild:
            self._stop_actor(actor)
            actor, count = self._new_actor_entry()
        self._actor_pool[actor_index] = (actor, count)
        return object_ref

    def apply(self, func, args=None, kwargs=None):
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

    def apply_async(self,
                    func,
                    args=None,
                    kwargs=None,
                    callback=None,
                    error_callback=None):
        """Run the given function on a random actor process and return an
        asynchronous interface to the result.

        Args:
            func: function to run.
            args: optional arguments to the function.
            kwargs: optional keyword arguments to the function.
            callback: callback to be executed on the result once it is finished
                if it succeeds.
            error_callback: callback to be executed the result once it is
                finished if the task errors. The exception raised by the
                task will be passed as the only argument to the callback.

        Returns:
            AsyncResult containing the result.
        """

        self._check_running()
        func = self._convert_to_ray_batched_calls_if_needed(func)
        object_ref = self._run_batch(self._random_actor_index(), func,
                                     [(args, kwargs)])
        return AsyncResult(
            [object_ref], callback, error_callback, single_result=True)

    def _convert_to_ray_batched_calls_if_needed(self,
                                                func: Callable) -> Callable:
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
        if isinstance(func, SafeFunction):
            func = func.func
        if isinstance(func, BatchedCalls):
            func = RayBatchedCalls(func.items, (func._backend, func._n_jobs),
                                   func._reducer_callback, func._pickle_cache)
            # go through all the items and replace args and kwargs with
            # ObjectRefs, caching them in registries
            func.put_items_in_object_store(self._registry,
                                           self._registry_hashable)
        else:
            func = orginal_func
        return func

    def _calculate_chunksize(self, iterable):
        chunksize, extra = divmod(len(iterable), len(self._actor_pool) * 4)
        if extra:
            chunksize += 1
        return chunksize

    def _submit_chunk(self,
                      func,
                      iterator,
                      chunksize,
                      actor_index,
                      unpack_args=False):
        chunk = []
        while len(chunk) < chunksize:
            try:
                args = next(iterator)
                if not unpack_args:
                    args = (args, )
                chunk.append((args, {}))
            except StopIteration:
                break

        # Nothing to submit. The caller should prevent this.
        assert len(chunk) > 0

        return self._run_batch(actor_index, func, chunk)

    def _chunk_and_run(self, func, iterable, chunksize=None,
                       unpack_args=False):
        if not hasattr(iterable, "__len__"):
            iterable = list(iterable)

        if chunksize is None:
            chunksize = self._calculate_chunksize(iterable)

        iterator = iter(iterable)
        chunk_object_refs = []
        while len(chunk_object_refs) * chunksize < len(iterable):
            actor_index = len(chunk_object_refs) % len(self._actor_pool)
            chunk_object_refs.append(
                self._submit_chunk(
                    func,
                    iterator,
                    chunksize,
                    actor_index,
                    unpack_args=unpack_args))

        return chunk_object_refs

    def _map_async(self,
                   func,
                   iterable,
                   chunksize=None,
                   unpack_args=False,
                   callback=None,
                   error_callback=None):
        self._check_running()
        object_refs = self._chunk_and_run(
            func, iterable, chunksize=chunksize, unpack_args=unpack_args)
        return AsyncResult(object_refs, callback, error_callback)

    def map(self, func, iterable, chunksize=None):
        """Run the given function on each element in the iterable round-robin
        on the actor processes and return the results synchronously.

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
            func, iterable, chunksize=chunksize, unpack_args=False).get()

    def map_async(self,
                  func,
                  iterable,
                  chunksize=None,
                  callback=None,
                  error_callback=None):
        """Run the given function on each element in the iterable round-robin
        on the actor processes and return an asynchronous interface to the
        results.

        Args:
            func: function to run.
            iterable: iterable of objects to be passed as the only argument to
                func.
            chunksize: number of tasks to submit as a batch to each actor
                process. If unspecified, a suitable chunksize will be chosen.
            callback: callback to be executed on each successful result once it
                is finished.
            error_callback: callback to be executed on each errored result once
                it is finished. The exception raised by the task will be passed
                as the only argument to the callback.

        Returns:
            AsyncResult
        """
        return self._map_async(
            func,
            iterable,
            chunksize=chunksize,
            unpack_args=False,
            callback=callback,
            error_callback=error_callback)

    def starmap(self, func, iterable, chunksize=None):
        """Same as `map`, but unpacks each element of the iterable as the
        arguments to func like: [func(*args) for args in iterable].
        """

        return self._map_async(
            func, iterable, chunksize=chunksize, unpack_args=True).get()

    def starmap_async(self, func, iterable, callback=None,
                      error_callback=None):
        """Same as `map_async`, but unpacks each element of the iterable as the
        arguments to func like: [func(*args) for args in iterable].
        """

        return self._map_async(
            func,
            iterable,
            unpack_args=True,
            callback=callback,
            error_callback=error_callback)

    def imap(self, func, iterable, chunksize=1):
        """Same as `map`, but only submits one batch of tasks to each actor
        process at a time.

        This can be useful if the iterable of arguments is very large or each
        task's arguments consumes a large amount of resources.

        The results are returned in the order corresponding to their arguments
        in the iterable.

        Returns:
            OrderedIMapIterator
        """

        self._check_running()
        return OrderedIMapIterator(self, func, iterable, chunksize=chunksize)

    def imap_unordered(self, func, iterable, chunksize=1):
        """Same as `map`, but only submits one batch of tasks to each actor
        process at a time.

        This can be useful if the iterable of arguments is very large or each
        task's arguments consumes a large amount of resources.

        The results are returned in the order that they finish.

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
        for actor, _ in self._actor_pool:
            self._stop_actor(actor)
        self._closed = True
        gc.collect()

    def terminate(self):
        """Close the pool.

        Prevents any more tasks from being submitted on the pool and stops
        outstanding work.
        """

        if not self._closed:
            self.close()
        for actor, _ in self._actor_pool:
            ray.kill(actor)

    def join(self):
        """Wait for the actors in a closed pool to exit.

        If the pool was closed using `close`, this will return once all
        outstanding work is completed.

        If the pool was closed using `terminate`, this will return quickly.
        """

        if not self._closed:
            raise ValueError("Pool is still running")
        self._wait_for_stopping_actors()
