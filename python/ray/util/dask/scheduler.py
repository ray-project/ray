import atexit
import threading
from collections import defaultdict
from dataclasses import dataclass
from multiprocessing.pool import ThreadPool

import ray

from dask.core import istask, ishashable, _execute_task
from dask.system import CPU_COUNT
from dask.threaded import pack_exception, _thread_get_id

from .callbacks import local_ray_callbacks, unpack_ray_callbacks
from .common import unpack_object_refs
from .scheduler_utils import get_async, apply_sync

main_thread = threading.current_thread()
default_pool = None
pools = defaultdict(dict)
pools_lock = threading.Lock()


def ray_dask_get(dsk, keys, **kwargs):
    """
    A Dask-Ray scheduler. This scheduler will send top-level (non-inlined) Dask
    tasks to a Ray cluster for execution. The scheduler will wait for the
    tasks to finish executing, fetch the results, and repackage them into the
    appropriate Dask collections. This particular scheduler uses a threadpool
    to submit Ray tasks.

    This can be passed directly to `dask.compute()`, as the scheduler:

    >>> dask.compute(obj, scheduler=ray_dask_get)

    You can override the currently active global Dask-Ray callbacks (e.g.
    supplied via a context manager), the number of threads to use when
    submitting the Ray tasks, or the threadpool used to submit Ray tasks:

    >>> dask.compute(
            obj,
            scheduler=ray_dask_get,
            ray_callbacks=some_ray_dask_callbacks,
            num_workers=8,
            pool=some_cool_pool,
        )

    Args:
        dsk (Dict): Dask graph, represented as a task DAG dictionary.
        keys (List[str]): List of Dask graph keys whose values we wish to
            compute and return.
        ray_callbacks (Optional[list[callable]]): Dask-Ray callbacks.
        num_workers (Optional[int]): The number of worker threads to use in
            the Ray task submission traversal of the Dask graph.
        pool (Optional[ThreadPool]): A multiprocessing threadpool to use to
            submit Ray tasks.

    Returns:
        Computed values corresponding to the provided keys.
    """
    num_workers = kwargs.pop("num_workers", None)
    pool = kwargs.pop("pool", None)
    # We attempt to reuse any other thread pools that have been created within
    # this thread and with the given number of workers. We reuse a global
    # thread pool if num_workers is not given and we're in the main thread.
    global default_pool
    thread = threading.current_thread()
    if pool is None:
        with pools_lock:
            if num_workers is None and thread is main_thread:
                if default_pool is None:
                    default_pool = ThreadPool(CPU_COUNT)
                    atexit.register(default_pool.close)
                pool = default_pool
            elif thread in pools and num_workers in pools[thread]:
                pool = pools[thread][num_workers]
            else:
                pool = ThreadPool(num_workers)
                atexit.register(pool.close)
                pools[thread][num_workers] = pool

    ray_callbacks = kwargs.pop("ray_callbacks", None)
    persist = kwargs.pop("ray_persist", False)

    with local_ray_callbacks(ray_callbacks) as ray_callbacks:
        # Unpack the Ray-specific callbacks.
        (
            ray_presubmit_cbs,
            ray_postsubmit_cbs,
            ray_pretask_cbs,
            ray_posttask_cbs,
            ray_postsubmit_all_cbs,
            ray_finish_cbs,
        ) = unpack_ray_callbacks(ray_callbacks)
        # NOTE: We hijack Dask's `get_async` function, injecting a different
        # task executor.
        object_refs = get_async(
            _apply_async_wrapper(
                pool.apply_async,
                _rayify_task_wrapper,
                ray_presubmit_cbs,
                ray_postsubmit_cbs,
                ray_pretask_cbs,
                ray_posttask_cbs,
            ),
            len(pool._pool),
            dsk,
            keys,
            get_id=_thread_get_id,
            pack_exception=pack_exception,
            **kwargs,
        )
        if ray_postsubmit_all_cbs is not None:
            for cb in ray_postsubmit_all_cbs:
                cb(object_refs, dsk)
        # NOTE: We explicitly delete the Dask graph here so object references
        # are garbage-collected before this function returns, i.e. before all
        # Ray tasks are done. Otherwise, no intermediate objects will be
        # cleaned up until all Ray tasks are done.
        del dsk
        if persist:
            result = object_refs
        else:
            result = ray_get_unpack(object_refs)
        if ray_finish_cbs is not None:
            for cb in ray_finish_cbs:
                cb(result)

    # cleanup pools associated with dead threads.
    with pools_lock:
        active_threads = set(threading.enumerate())
        if thread is not main_thread:
            for t in list(pools):
                if t not in active_threads:
                    for p in pools.pop(t).values():
                        p.close()
    return result


def _apply_async_wrapper(apply_async, real_func, *extra_args, **extra_kwargs):
    """
    Wraps the given pool `apply_async` function, hotswapping `real_func` in as
    the function to be applied and adding `extra_args` and `extra_kwargs` to
    `real_func`'s call.

    Args:
        apply_async (callable): The pool function to be wrapped.
        real_func (callable): The real function that we wish the pool apply
            function to execute.
        *extra_args: Extra positional arguments to pass to the `real_func`.
        **extra_kwargs: Extra keyword arguments to pass to the `real_func`.

    Returns:
        A wrapper function that will ignore it's first `func` argument and
        pass `real_func` in its place. To be passed to `dask.local.get_async`.
    """

    def wrapper(func, args=(), kwds=None, callback=None):  # noqa: M511
        if not kwds:
            kwds = {}
        return apply_async(
            real_func,
            args=args + extra_args,
            kwds=dict(kwds, **extra_kwargs),
            callback=callback,
        )

    return wrapper


def _rayify_task_wrapper(
        key,
        task_info,
        dumps,
        loads,
        get_id,
        pack_exception,
        ray_presubmit_cbs,
        ray_postsubmit_cbs,
        ray_pretask_cbs,
        ray_posttask_cbs,
):
    """
    The core Ray-Dask task execution wrapper, to be given to the thread pool's
    `apply_async` function. Exactly the same as `execute_task`, except that it
    calls `_rayify_task` on the task instead of `_execute_task`.

    Args:
        key (str): The Dask graph key whose corresponding task we wish to
            execute.
        task_info: The task to execute and its dependencies.
        dumps (callable): A result serializing function.
        loads (callable): A task_info deserializing function.
        get_id (callable): An ID generating function.
        pack_exception (callable): An exception serializing function.
        ray_presubmit_cbs (callable): Pre-task submission callbacks.
        ray_postsubmit_cbs (callable): Post-task submission callbacks.
        ray_pretask_cbs (callable): Pre-task execution callbacks.
        ray_posttask_cbs (callable): Post-task execution callbacks.

    Returns:
        A 3-tuple of the task's key, a literal or a Ray object reference for a
        Ray task's result, and whether the Ray task submission failed.
    """
    try:
        task, deps = loads(task_info)
        result = _rayify_task(
            task,
            key,
            deps,
            ray_presubmit_cbs,
            ray_postsubmit_cbs,
            ray_pretask_cbs,
            ray_posttask_cbs,
        )
        id = get_id()
        result = dumps((result, id))
        failed = False
    except BaseException as e:
        result = pack_exception(e, dumps)
        failed = True
    return key, result, failed


def _rayify_task(
        task,
        key,
        deps,
        ray_presubmit_cbs,
        ray_postsubmit_cbs,
        ray_pretask_cbs,
        ray_posttask_cbs,
):
    """
    Rayifies the given task, submitting it as a Ray task to the Ray cluster.

    Args:
        task (tuple): A Dask graph value, being either a literal, dependency
            key, Dask task, or a list thereof.
        key (str): The Dask graph key for the given task.
        deps (dict): The dependencies of this task.
        ray_presubmit_cbs (callable): Pre-task submission callbacks.
        ray_postsubmit_cbs (callable): Post-task submission callbacks.
        ray_pretask_cbs (callable): Pre-task execution callbacks.
        ray_posttask_cbs (callable): Post-task execution callbacks.

    Returns:
        A literal, a Ray object reference representing a submitted task, or a
        list thereof.
    """
    if isinstance(task, list):
        # Recursively rayify this list. This will still bottom out at the first
        # actual task encountered, inlining any tasks in that task's arguments.
        return [
            _rayify_task(
                t,
                key,
                deps,
                ray_presubmit_cbs,
                ray_postsubmit_cbs,
                ray_pretask_cbs,
                ray_posttask_cbs,
            ) for t in task
        ]
    elif istask(task):
        # Unpacks and repacks Ray object references and submits the task to the
        # Ray cluster for execution.
        if ray_presubmit_cbs is not None:
            alternate_returns = [
                cb(task, key, deps) for cb in ray_presubmit_cbs
            ]
            for alternate_return in alternate_returns:
                # We don't submit a Ray task if a presubmit callback returns
                # a non-`None` value, instead we return said value.
                # NOTE: This returns the first non-None presubmit callback
                # return value.
                if alternate_return is not None:
                    return alternate_return

        func, args = task[0], task[1:]
        if func is multiple_return_get:
            return _execute_task(task, deps)
        # If the function's arguments contain nested object references, we must
        # unpack said object references into a flat set of arguments so that
        # Ray properly tracks the object dependencies between Ray tasks.
        arg_object_refs, repack = unpack_object_refs(args, deps)
        # Submit the task using a wrapper function.
        object_refs = dask_task_wrapper.options(
            name=f"dask:{key!s}",
            num_returns=(1 if not isinstance(func, MultipleReturnFunc) else
                         func.num_returns),
        ).remote(
            func,
            repack,
            key,
            ray_pretask_cbs,
            ray_posttask_cbs,
            *arg_object_refs,
        )

        if ray_postsubmit_cbs is not None:
            for cb in ray_postsubmit_cbs:
                cb(task, key, deps, object_refs)

        return object_refs
    elif not ishashable(task):
        return task
    elif task in deps:
        return deps[task]
    else:
        return task


@ray.remote
def dask_task_wrapper(func, repack, key, ray_pretask_cbs, ray_posttask_cbs,
                      *args):
    """
    A Ray remote function acting as a Dask task wrapper. This function will
    repackage the given flat `args` into its original data structures using
    `repack`, execute any Dask subtasks within the repackaged arguments
    (inlined by Dask's optimization pass), and then pass the concrete task
    arguments to the provide Dask task function, `func`.

    Args:
        func (callable): The Dask task function to execute.
        repack (callable): A function that repackages the provided args into
            the original (possibly nested) Python objects.
        key (str): The Dask key for this task.
        ray_pretask_cbs (callable): Pre-task execution callbacks.
        ray_posttask_cbs (callable): Post-task execution callback.
        *args (ObjectRef): Ray object references representing the Dask task's
            arguments.

    Returns:
        The output of the Dask task. In the context of Ray, a
        dask_task_wrapper.remote() invocation will return a Ray object
        reference representing the Ray task's result.
    """
    if ray_pretask_cbs is not None:
        pre_states = [
            cb(key, args) if cb is not None else None for cb in ray_pretask_cbs
        ]
    repacked_args, repacked_deps = repack(args)
    # Recursively execute Dask-inlined tasks.
    actual_args = [_execute_task(a, repacked_deps) for a in repacked_args]
    # Execute the actual underlying Dask task.
    result = func(*actual_args)

    if ray_posttask_cbs is not None:
        for cb, pre_state in zip(ray_posttask_cbs, pre_states):
            if cb is not None:
                cb(key, result, pre_state)

    return result


def ray_get_unpack(object_refs):
    """
    Unpacks object references, gets the object references, and repacks.
    Traverses arbitrary data structures.

    Args:
        object_refs: A (potentially nested) Python object containing Ray object
            references.

    Returns:
        The input Python object with all contained Ray object references
        resolved with their concrete values.
    """
    if isinstance(object_refs, tuple):
        object_refs = list(object_refs)

    if isinstance(object_refs, list) and any(not isinstance(x, ray.ObjectRef)
                                             for x in object_refs):
        # We flatten the object references before calling ray.get(), since Dask
        # loves to nest collections in nested tuples and Ray expects a flat
        # list of object references. We repack the results after ray.get()
        # completes.
        object_refs, repack = unpack_object_refs(*object_refs)
        computed_result = ray.get(object_refs)
        return repack(computed_result)
    else:
        return ray.get(object_refs)


def ray_dask_get_sync(dsk, keys, **kwargs):
    """
    A synchronous Dask-Ray scheduler. This scheduler will send top-level
    (non-inlined) Dask tasks to a Ray cluster for execution. The scheduler will
    wait for the tasks to finish executing, fetch the results, and repackage
    them into the appropriate Dask collections. This particular scheduler
    submits Ray tasks synchronously, which can be useful for debugging.

    This can be passed directly to `dask.compute()`, as the scheduler:

    >>> dask.compute(obj, scheduler=ray_dask_get_sync)

    You can override the currently active global Dask-Ray callbacks (e.g.
    supplied via a context manager):

    >>> dask.compute(
            obj,
            scheduler=ray_dask_get_sync,
            ray_callbacks=some_ray_dask_callbacks,
        )

    Args:
        dsk (Dict): Dask graph, represented as a task DAG dictionary.
        keys (List[str]): List of Dask graph keys whose values we wish to
            compute and return.

    Returns:
        Computed values corresponding to the provided keys.
    """

    ray_callbacks = kwargs.pop("ray_callbacks", None)
    persist = kwargs.pop("ray_persist", False)

    with local_ray_callbacks(ray_callbacks) as ray_callbacks:
        # Unpack the Ray-specific callbacks.
        (
            ray_presubmit_cbs,
            ray_postsubmit_cbs,
            ray_pretask_cbs,
            ray_posttask_cbs,
            ray_postsubmit_all_cbs,
            ray_finish_cbs,
        ) = unpack_ray_callbacks(ray_callbacks)
        # NOTE: We hijack Dask's `get_async` function, injecting a different
        # task executor.
        object_refs = get_async(
            _apply_async_wrapper(
                apply_sync,
                _rayify_task_wrapper,
                ray_presubmit_cbs,
                ray_postsubmit_cbs,
                ray_pretask_cbs,
                ray_posttask_cbs,
            ),
            1,
            dsk,
            keys,
            **kwargs,
        )
        if ray_postsubmit_all_cbs is not None:
            for cb in ray_postsubmit_all_cbs:
                cb(object_refs, dsk)
        # NOTE: We explicitly delete the Dask graph here so object references
        # are garbage-collected before this function returns, i.e. before all
        # Ray tasks are done. Otherwise, no intermediate objects will be
        # cleaned up until all Ray tasks are done.
        del dsk
        if persist:
            result = object_refs
        else:
            result = ray_get_unpack(object_refs)
        if ray_finish_cbs is not None:
            for cb in ray_finish_cbs:
                cb(result)

        return result


@dataclass
class MultipleReturnFunc:
    func: callable
    num_returns: int

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


def multiple_return_get(multiple_returns, idx):
    return multiple_returns[idx]
