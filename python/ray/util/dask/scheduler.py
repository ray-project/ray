import atexit
from collections import defaultdict
from multiprocessing.pool import ThreadPool
import threading

import ray

from dask.core import istask, ishashable, _execute_task
from dask.local import get_async, apply_sync
from dask.system import CPU_COUNT
from dask.threaded import pack_exception, _thread_get_id

from .common import unpack_object_refs

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

    You can override the number of threads to use when submitting the
    Ray tasks, or the threadpool used to submit Ray tasks:

    >>> dask.compute(
            obj,
            scheduler=ray_dask_get,
            num_workers=8,
            pool=some_cool_pool,
        )

    Args:
        dsk (Dict): Dask graph, represented as a task DAG dictionary.
        keys (List[str]): List of Dask graph keys whose values we wish to
            compute and return.
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

    # NOTE: We hijack Dask's `get_async` function, injecting a different task
    # executor.
    object_refs = get_async(
        _apply_async_wrapper(pool.apply_async, _rayify_task_wrapper),
        len(pool._pool),
        dsk,
        keys,
        get_id=_thread_get_id,
        pack_exception=pack_exception,
        **kwargs,
    )
    # NOTE: We explicitly delete the Dask graph here so object references
    # are garbage-collected before this function returns, i.e. before all Ray
    # tasks are done. Otherwise, no intermediate objects will be cleaned up
    # until all Ray tasks are done.
    del dsk
    result = ray_get_unpack(object_refs)

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

    def wrapper(func, args=(), kwds={}, callback=None):  # noqa: M511
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

    Returns:
        A 3-tuple of the task's key, a literal or a Ray object reference for a
        Ray task's result, and whether the Ray task submission failed.
    """
    try:
        task, deps = loads(task_info)
        result = _rayify_task(task, key, deps)
        id = get_id()
        result = dumps((result, id))
        failed = False
    except BaseException as e:
        result = pack_exception(e, dumps)
        failed = True
    return key, result, failed


def _rayify_task(task, key, deps):
    """
    Rayifies the given task, submitting it as a Ray task to the Ray cluster.

    Args:
        task: A Dask graph value, being either a literal, dependency key, Dask
            task, or a list thereof.
        key: The Dask graph key for the given task.
        deps: The dependencies of this task.

    Returns:
        A literal, a Ray object reference representing a submitted task, or a
        list thereof.
    """
    if isinstance(task, list):
        # Recursively rayify this list. This will still bottom out at the first
        # actual task encountered, inlining any tasks in that task's arguments.
        return [_rayify_task(t, deps) for t in task]
    elif istask(task):
        # Unpacks and repacks Ray object references and submits the task to the
        # Ray cluster for execution.
        func, args = task[0], task[1:]
        # If the function's arguments contain nested object references, we must
        # unpack said object references into a flat set of arguments so that
        # Ray properly tracks the object dependencies between Ray tasks.
        object_refs, repack = unpack_object_refs(args, deps)
        # Submit the task using a wrapper function.
        return dask_task_wrapper.options(name=f"dask:{key!s}").remote(
            func, repack, *object_refs)
    elif not ishashable(task):
        return task
    elif task in deps:
        return deps[task]
    else:
        return task


@ray.remote
def dask_task_wrapper(func, repack, *args):
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
        *args (ObjectRef): Ray object references representing the Dask task's
            arguments.

    Returns:
        The output of the Dask task. In the context of Ray, a
        dask_task_wrapper.remote() invocation will return a Ray object
        reference representing the Ray task's result.
    """
    repacked_args, repacked_deps = repack(args)
    # Recursively execute Dask-inlined tasks.
    actual_args = [_execute_task(a, repacked_deps) for a in repacked_args]
    # Execute the actual underlying Dask task.
    return func(*actual_args)


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
    if isinstance(object_refs,
                  (tuple, list)) and any(not isinstance(x, ray.ObjectRef)
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

    Args:
        dsk (Dict): Dask graph, represented as a task DAG dictionary.
        keys (List[str]): List of Dask graph keys whose values we wish to
            compute and return.

    Returns:
        Computed values corresponding to the provided keys.
    """
    # NOTE: We hijack Dask's `get_async` function, injecting a different task
    # executor.
    object_refs = get_async(
        _apply_async_wrapper(apply_sync, _rayify_task_wrapper),
        1,
        dsk,
        keys,
        **kwargs,
    )
    # NOTE: We explicitly delete the Dask graph here so object references
    # are garbage-collected before this function returns, i.e. before all Ray
    # tasks are done. Otherwise, no intermediate objects will be cleaned up
    # until all Ray tasks are done.
    del dsk
    return ray_get_unpack(object_refs)
