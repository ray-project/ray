"""
The following is adapted from Dask release 2021.03.1:
    https://github.com/dask/dask/blob/2021.03.1/dask/local.py
"""

import os
from queue import Queue, Empty

from dask import config
from dask.callbacks import local_callbacks, unpack_callbacks
from dask.core import (_execute_task, flatten, get_dependencies, has_tasks,
                       reverse_dict)
from dask.order import order

if os.name == "nt":
    # Python 3 windows Queue.get doesn't handle interrupts properly. To
    # workaround this we poll at a sufficiently large interval that it
    # shouldn't affect performance, but small enough that users trying to kill
    # an application shouldn't care.
    def queue_get(q):
        while True:
            try:
                return q.get(block=True, timeout=0.1)
            except Empty:
                pass

else:

    def queue_get(q):
        return q.get()


def start_state_from_dask(dsk, cache=None, sortkey=None):
    """Start state from a dask
    Examples
    --------
    >>> dsk = {
        'x': 1,
        'y': 2,
        'z': (inc, 'x'),
        'w': (add, 'z', 'y')}  # doctest: +SKIP
    >>> from pprint import pprint  # doctest: +SKIP
    >>> pprint(start_state_from_dask(dsk))  # doctest: +SKIP
    {'cache': {'x': 1, 'y': 2},
     'dependencies': {'w': {'z', 'y'}, 'x': set(), 'y': set(), 'z': {'x'}},
     'dependents': {'w': set(), 'x': {'z'}, 'y': {'w'}, 'z': {'w'}},
     'finished': set(),
     'ready': ['z'],
     'released': set(),
     'running': set(),
     'waiting': {'w': {'z'}},
     'waiting_data': {'x': {'z'}, 'y': {'w'}, 'z': {'w'}}}
    """
    if sortkey is None:
        sortkey = order(dsk).get
    if cache is None:
        cache = config.get("cache", None)
    if cache is None:
        cache = dict()
    data_keys = set()
    for k, v in dsk.items():
        if not has_tasks(dsk, v):
            cache[k] = v
            data_keys.add(k)

    dsk2 = dsk.copy()
    dsk2.update(cache)

    dependencies = {k: get_dependencies(dsk2, k) for k in dsk}
    waiting = {
        k: v.copy()
        for k, v in dependencies.items() if k not in data_keys
    }

    dependents = reverse_dict(dependencies)
    for a in cache:
        for b in dependents.get(a, ()):
            waiting[b].remove(a)
    waiting_data = {k: v.copy() for k, v in dependents.items() if v}

    ready_set = {k for k, v in waiting.items() if not v}
    ready = sorted(ready_set, key=sortkey, reverse=True)
    waiting = {k: v for k, v in waiting.items() if v}

    state = {
        "dependencies": dependencies,
        "dependents": dependents,
        "waiting": waiting,
        "waiting_data": waiting_data,
        "cache": cache,
        "ready": ready,
        "running": set(),
        "finished": set(),
        "released": set(),
    }

    return state


def execute_task(key, task_info, dumps, loads, get_id, pack_exception):
    """
    Compute task and handle all administration
    See Also
    --------
    _execute_task : actually execute task
    """
    try:
        task, data = loads(task_info)
        result = _execute_task(task, data)
        id = get_id()
        result = dumps((result, id))
        failed = False
    except BaseException as e:
        result = pack_exception(e, dumps)
        failed = True
    return key, result, failed


def release_data(key, state, delete=True):
    """Remove data from temporary storage
    See Also
    --------
    finish_task
    """
    if key in state["waiting_data"]:
        assert not state["waiting_data"][key]
        del state["waiting_data"][key]

    state["released"].add(key)

    if delete:
        del state["cache"][key]


DEBUG = False


def finish_task(dsk,
                key,
                state,
                results,
                sortkey,
                delete=True,
                release_data=release_data):
    """
    Update execution state after a task finishes
    Mutates.  This should run atomically (with a lock).
    """
    for dep in sorted(state["dependents"][key], key=sortkey, reverse=True):
        s = state["waiting"][dep]
        s.remove(key)
        if not s:
            del state["waiting"][dep]
            state["ready"].append(dep)

    for dep in state["dependencies"][key]:
        if dep in state["waiting_data"]:
            s = state["waiting_data"][dep]
            s.remove(key)
            if not s and dep not in results:
                if DEBUG:
                    from chest.core import nbytes

                    print("Key: %s\tDep: %s\t NBytes: %.2f\t Release" %
                          (key, dep,
                           sum(map(nbytes, state["cache"].values()) / 1e6)))
                release_data(dep, state, delete=delete)
        elif delete and dep not in results:
            release_data(dep, state, delete=delete)

    state["finished"].add(key)
    state["running"].remove(key)

    return state


def nested_get(ind, coll):
    """Get nested index from collection
    Examples
    --------
    >>> nested_get(1, 'abc')
    'b'
    >>> nested_get([1, 0], 'abc')
    ('b', 'a')
    >>> nested_get([[1, 0], [0, 1]], 'abc')
    (('b', 'a'), ('a', 'b'))
    """
    if isinstance(ind, list):
        return tuple([nested_get(i, coll) for i in ind])
    else:
        return coll[ind]


def default_get_id():
    """Default get_id"""
    return None


def default_pack_exception(e, dumps):
    raise


def reraise(exc, tb=None):
    if exc.__traceback__ is not tb:
        raise exc.with_traceback(tb)
    raise exc


def identity(x):
    """Identity function. Returns x.
    >>> identity(3)
    3
    """
    return x


def get_async(apply_async,
              num_workers,
              dsk,
              result,
              cache=None,
              get_id=default_get_id,
              rerun_exceptions_locally=None,
              pack_exception=default_pack_exception,
              raise_exception=reraise,
              callbacks=None,
              dumps=identity,
              loads=identity,
              **kwargs):
    """Asynchronous get function
    This is a general version of various asynchronous schedulers for dask.  It
    takes a an apply_async function as found on Pool objects to form a more
    specific ``get`` method that walks through the dask array with parallel
    workers, avoiding repeat computation and minimizing memory use.
    Parameters
    ----------
    apply_async : function
        Asynchronous apply function as found on Pool or ThreadPool
    num_workers : int
        The number of active tasks we should have at any one time
    dsk : dict
        A dask dictionary specifying a workflow
    result : key or list of keys
        Keys corresponding to desired data
    cache : dict-like, optional
        Temporary storage of results
    get_id : callable, optional
        Function to return the worker id, takes no arguments. Examples are
        `threading.current_thread` and `multiprocessing.current_process`.
    rerun_exceptions_locally : bool, optional
        Whether to rerun failing tasks in local process to enable debugging
        (False by default)
    pack_exception : callable, optional
        Function to take an exception and ``dumps`` method, and return a
        serialized tuple of ``(exception, traceback)`` to send back to the
        scheduler. Default is to just raise the exception.
    raise_exception : callable, optional
        Function that takes an exception and a traceback, and raises an error.
    dumps: callable, optional
        Function to serialize task data and results to communicate between
        worker and parent.  Defaults to identity.
    loads: callable, optional
        Inverse function of `dumps`.  Defaults to identity.
    callbacks : tuple or list of tuples, optional
        Callbacks are passed in as tuples of length 5. Multiple sets of
        callbacks may be passed in as a list of tuples. For more information,
        see the dask.diagnostics documentation.
    See Also
    --------
    threaded.get
    """
    queue = Queue()

    if isinstance(result, list):
        result_flat = set(flatten(result))
    else:
        result_flat = {result}
    results = set(result_flat)

    dsk = dict(dsk)
    with local_callbacks(callbacks) as callbacks:
        _, _, pretask_cbs, posttask_cbs, _ = unpack_callbacks(callbacks)
        started_cbs = []
        succeeded = False
        # if start_state_from_dask fails, we will have something
        # to pass to the final block.
        state = {}
        try:
            for cb in callbacks:
                if cb[0]:
                    cb[0](dsk)
                started_cbs.append(cb)

            keyorder = order(dsk)

            state = start_state_from_dask(
                dsk, cache=cache, sortkey=keyorder.get)

            for _, start_state, _, _, _ in callbacks:
                if start_state:
                    start_state(dsk, state)

            if rerun_exceptions_locally is None:
                rerun_exceptions_locally = config.get(
                    "rerun_exceptions_locally", False)

            if state["waiting"] and not state["ready"]:
                raise ValueError("Found no accessible jobs in dask")

            def fire_task():
                """ Fire off a task to the thread pool """
                # Choose a good task to compute
                key = state["ready"].pop()
                state["running"].add(key)
                for f in pretask_cbs:
                    f(key, dsk, state)

                # Prep data to send
                data = {
                    dep: state["cache"][dep]
                    for dep in get_dependencies(dsk, key)
                }
                # Submit
                apply_async(
                    execute_task,
                    args=(
                        key,
                        dumps((dsk[key], data)),
                        dumps,
                        loads,
                        get_id,
                        pack_exception,
                    ),
                    callback=queue.put,
                )

            # Seed initial tasks into the thread pool
            while state["ready"] and len(state["running"]) < num_workers:
                fire_task()

            # Main loop, wait on tasks to finish, insert new ones
            while state["waiting"] or state["ready"] or state["running"]:
                key, res_info, failed = queue_get(queue)
                if failed:
                    exc, tb = loads(res_info)
                    if rerun_exceptions_locally:
                        data = {
                            dep: state["cache"][dep]
                            for dep in get_dependencies(dsk, key)
                        }
                        task = dsk[key]
                        _execute_task(task, data)  # Re-execute locally
                    else:
                        raise_exception(exc, tb)
                res, worker_id = loads(res_info)
                state["cache"][key] = res
                finish_task(dsk, key, state, results, keyorder.get)
                for f in posttask_cbs:
                    f(key, res, dsk, state, worker_id)

                while state["ready"] and len(state["running"]) < num_workers:
                    fire_task()

            succeeded = True

        finally:
            for _, _, _, _, finish in started_cbs:
                if finish:
                    finish(dsk, state, not succeeded)

    return nested_get(result, state["cache"])


def apply_sync(func, args=(), kwds={}, callback=None):
    """ A naive synchronous version of apply_async """
    res = func(*args, **kwds)
    if callback is not None:
        callback(res)
