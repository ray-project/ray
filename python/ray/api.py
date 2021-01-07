import inspect
import json
import logging
import time
import sys

import ray.worker
from ray import profiling, Language
from ray.exceptions import ObjectStoreFullError
from ray.util.inspect import is_cython
from ray.worker import LOCAL_MODE
from ray._private.client_mode_hook import client_mode_hook

from ray.exceptions import RayError, RayTaskError

logger = logging.getLogger(__name__)


@client_mode_hook
def put(value):
    """Store an object in the object store.

    The object may not be evicted while a reference to the returned ID exists.

    Args:
        value: The Python object to be stored.

    Returns:
        The object ref assigned to this value.
    """
    worker = ray.worker.global_worker
    worker.check_connected()
    with profiling.profile("ray.put"):
        try:
            object_ref = worker.put_object(value, pin_object=True)
        except ObjectStoreFullError:
            logger.info(
                "Put failed since the value was either too large or the "
                "store was full of pinned objects.")
            raise
        return object_ref


# Global variable to make sure we only send out the warning once.
blocking_get_inside_async_warned = False


@client_mode_hook
def get(object_refs, *, timeout=None):
    """Get a remote object or a list of remote objects from the object store.

    This method blocks until the object corresponding to the object ref is
    available in the local object store. If this object is not in the local
    object store, it will be shipped from an object store that has it (once the
    object has been created). If object_refs is a list, then the objects
    corresponding to each object in the list will be returned.

    This method will issue a warning if it's running inside async context,
    you can use ``await object_ref`` instead of ``ray.get(object_ref)``. For
    a list of object refs, you can use ``await asyncio.gather(*object_refs)``.

    Args:
        object_refs: Object ref of the object to get or a list of object refs
            to get.
        timeout (Optional[float]): The maximum amount of time in seconds to
            wait before returning.

    Returns:
        A Python object or a list of Python objects.

    Raises:
        GetTimeoutError: A GetTimeoutError is raised if a timeout is set and
            the get takes longer than timeout to return.
        Exception: An exception is raised if the task that created the object
            or that created one of the objects raised an exception.
    """
    worker = ray.worker.global_worker
    worker.check_connected()

    if hasattr(
            worker,
            "core_worker") and worker.core_worker.current_actor_is_asyncio():
        global blocking_get_inside_async_warned
        if not blocking_get_inside_async_warned:
            logger.warning("Using blocking ray.get inside async actor. "
                           "This blocks the event loop. Please use `await` "
                           "on object ref with asyncio.gather if you want to "
                           "yield execution to the event loop instead.")
            blocking_get_inside_async_warned = True

    with profiling.profile("ray.get"):
        is_individual_id = isinstance(object_refs, ray.ObjectRef)
        if is_individual_id:
            object_refs = [object_refs]

        if not isinstance(object_refs, list):
            raise ValueError("'object_refs' must either be an object ref "
                             "or a list of object refs.")

        global last_task_error_raise_time
        # TODO(ujvl): Consider how to allow user to retrieve the ready objects.
        values, debugger_breakpoint = worker.get_objects(
            object_refs, timeout=timeout)
        for i, value in enumerate(values):
            if isinstance(value, RayError):
                last_task_error_raise_time = time.time()
                if isinstance(value, ray.exceptions.ObjectLostError):
                    worker.core_worker.dump_object_store_memory_usage()
                if isinstance(value, RayTaskError):
                    raise value.as_instanceof_cause()
                else:
                    raise value

        if is_individual_id:
            values = values[0]

        if debugger_breakpoint != b"":
            frame = sys._getframe().f_back
            rdb = ray.util.pdb.connect_ray_pdb(
                None, None, False, None,
                debugger_breakpoint.decode() if debugger_breakpoint else None)
            rdb.set_trace(frame=frame)

        return values


# Global variable to make sure we only send out the warning once.
blocking_wait_inside_async_warned = False


@client_mode_hook
def wait(object_refs, *, num_returns=1, timeout=None, fetch_local=True):
    """Return a list of IDs that are ready and a list of IDs that are not.

    If timeout is set, the function returns either when the requested number of
    IDs are ready or when the timeout is reached, whichever occurs first. If it
    is not set, the function simply waits until that number of objects is ready
    and returns that exact number of object refs.

    This method returns two lists. The first list consists of object refs that
    correspond to objects that are available in the object store. The second
    list corresponds to the rest of the object refs (which may or may not be
    ready).

    Ordering of the input list of object refs is preserved. That is, if A
    precedes B in the input list, and both are in the ready list, then A will
    precede B in the ready list. This also holds true if A and B are both in
    the remaining list.

    This method will issue a warning if it's running inside an async context.
    Instead of ``ray.wait(object_refs)``, you can use
    ``await asyncio.wait(object_refs)``.

    Args:
        object_refs (List[ObjectRef]): List of object refs for objects that may
            or may not be ready. Note that these IDs must be unique.
        num_returns (int): The number of object refs that should be returned.
        timeout (float): The maximum amount of time in seconds to wait before
            returning.
        fetch_local (bool): If True, wait for the object to be downloaded onto
            the local node before returning it as ready. If False, ray.wait()
            will not trigger fetching of objects to the local node and will
            return immediately once the object is available anywhere in the
            cluster.

    Returns:
        A list of object refs that are ready and a list of the remaining object
        IDs.
    """
    worker = ray.worker.global_worker
    worker.check_connected()

    if hasattr(worker,
               "core_worker") and worker.core_worker.current_actor_is_asyncio(
               ) and timeout != 0:
        global blocking_wait_inside_async_warned
        if not blocking_wait_inside_async_warned:
            logger.debug("Using blocking ray.wait inside async method. "
                         "This blocks the event loop. Please use `await` "
                         "on object ref with asyncio.wait. ")
            blocking_wait_inside_async_warned = True

    if isinstance(object_refs, ray.ObjectRef):
        raise TypeError(
            "wait() expected a list of ray.ObjectRef, got a single "
            "ray.ObjectRef")

    if not isinstance(object_refs, list):
        raise TypeError("wait() expected a list of ray.ObjectRef, "
                        f"got {type(object_refs)}")

    if timeout is not None and timeout < 0:
        raise ValueError("The 'timeout' argument must be nonnegative. "
                         f"Received {timeout}")

    for object_ref in object_refs:
        if not isinstance(object_ref, ray.ObjectRef):
            raise TypeError("wait() expected a list of ray.ObjectRef, "
                            f"got list containing {type(object_ref)}")

    # TODO(swang): Check main thread.
    with profiling.profile("ray.wait"):

        # TODO(rkn): This is a temporary workaround for
        # https://github.com/ray-project/ray/issues/997. However, it should be
        # fixed in Arrow instead of here.
        if len(object_refs) == 0:
            return [], []

        if len(object_refs) != len(set(object_refs)):
            raise ValueError("Wait requires a list of unique object refs.")
        if num_returns <= 0:
            raise ValueError(
                "Invalid number of objects to return %d." % num_returns)
        if num_returns > len(object_refs):
            raise ValueError("num_returns cannot be greater than the number "
                             "of objects provided to ray.wait.")

        timeout = timeout if timeout is not None else 10**6
        timeout_milliseconds = int(timeout * 1000)
        ready_ids, remaining_ids = worker.core_worker.wait(
            object_refs,
            num_returns,
            timeout_milliseconds,
            worker.current_task_id,
            fetch_local,
        )
        return ready_ids, remaining_ids


@client_mode_hook
def cancel(object_ref, *, force=False, recursive=True):
    """Cancels a task according to the following conditions.

    If the specified task is pending execution, it will not be executed. If
    the task is currently executing, the behavior depends on the ``force``
    flag. When ``force=False``, a KeyboardInterrupt will be raised in Python
    and when ``force=True``, the executing the task will immediately exit. If
    the task is already finished, nothing will happen.

    Only non-actor tasks can be canceled. Canceled tasks will not be
    retried (max_retries will not be respected).

    Calling ray.get on a canceled task will raise a TaskCancelledError or a
    WorkerCrashedError if ``force=True``.

    Args:
        object_ref (ObjectRef): ObjectRef returned by the task
            that should be canceled.
        force (boolean): Whether to force-kill a running task by killing
            the worker that is running the task.
        recursive (boolean): Whether to try to cancel tasks submitted by the
            task specified.
    Raises:
        TypeError: This is also raised for actor tasks.
    """
    worker = ray.worker.global_worker
    worker.check_connected()

    if not isinstance(object_ref, ray.ObjectRef):
        raise TypeError(
            "ray.cancel() only supported for non-actor object refs. "
            f"Got: {type(object_ref)}.")
    return worker.core_worker.cancel_task(object_ref, force, recursive)


@client_mode_hook
def get_actor(name):
    """Get a handle to a detached actor.

    Gets a handle to a detached actor with the given name. The actor must
    have been created with Actor.options(name="name").remote().

    Returns:
        ActorHandle to the actor.

    Raises:
        ValueError if the named actor does not exist.
    """
    if not name:
        raise ValueError("Please supply a non-empty value to get_actor")
    worker = ray.worker.global_worker
    worker.check_connected()
    return worker.core_worker.get_named_actor_handle(name)


def get_gpu_ids():
    """Get the IDs of the GPUs that are available to the worker.

    If the CUDA_VISIBLE_DEVICES environment variable was set when the worker
    started up, then the IDs returned by this method will be a subset of the
    IDs in CUDA_VISIBLE_DEVICES. If not, the IDs will fall in the range
    [0, NUM_GPUS - 1], where NUM_GPUS is the number of GPUs that the node has.

    Returns:
        A list of GPU IDs.
    """
    worker = ray.worker.global_worker
    worker.check_connected()

    # TODO(ilr) Handle inserting resources in local mode
    all_resource_ids = worker.core_worker.resource_ids()
    assigned_ids = []
    for resource, assignment in all_resource_ids.items():
        # Handle both normal and placement group GPU resources.
        if resource == "GPU" or resource.startswith("GPU_group_"):
            for resource_id, _ in assignment:
                assigned_ids.append(resource_id)
    # If the user had already set CUDA_VISIBLE_DEVICES, then respect that (in
    # the sense that only GPU IDs that appear in CUDA_VISIBLE_DEVICES should be
    # returned).
    if worker.original_gpu_ids is not None:
        assigned_ids = [
            worker.original_gpu_ids[gpu_id] for gpu_id in assigned_ids
        ]
        # Give all GPUs in local_mode.
        if worker.mode == LOCAL_MODE:
            max_gpus = worker.node.get_resource_spec().num_gpus
            assigned_ids = worker.original_gpu_ids[:max_gpus]

    return assigned_ids


def get_resource_ids():
    """Get the IDs of the resources that are available to the worker.

    Returns:
        A dictionary mapping the name of a resource to a list of pairs, where
        each pair consists of the ID of a resource and the fraction of that
        resource reserved for this worker.
    """
    worker = ray.worker.global_worker
    worker.check_connected()

    if worker.mode == LOCAL_MODE:
        raise RuntimeError("ray.get_resource_ids() currently does not work in "
                           "local_mode.")

    return worker.core_worker.resource_ids()


def get_dashboard_url():
    """Get the URL to access the Ray dashboard.

    Note that the URL does not specify which node the dashboard is on.

    Returns:
        The URL of the dashboard as a string.
    """
    ray.worker.global_worker.check_connected()
    return ray.worker._global_node.webui_url


@client_mode_hook
def is_initialized():
    """Check if ray.init has been called yet.

    Returns:
        True if ray.init has already been called and false otherwise.
    """
    return ray.worker.global_worker.connected


@client_mode_hook
def kill(actor, *, no_restart=True):
    """Kill an actor forcefully.

    This will interrupt any running tasks on the actor, causing them to fail
    immediately. ``atexit`` handlers installed in the actor will not be run.

    If you want to kill the actor but let pending tasks finish,
    you can call ``actor.__ray_terminate__.remote()`` instead to queue a
    termination task. Any ``atexit`` handlers installed in the actor *will*
    be run in this case.

    If the actor is a detached actor, subsequent calls to get its handle via
    ray.get_actor will fail.

    Args:
        actor (ActorHandle): Handle to the actor to kill.
        no_restart (bool): Whether or not this actor should be restarted if
            it's a restartable actor.
    """
    worker = ray.worker.global_worker
    worker.check_connected()
    if not isinstance(actor, ray.actor.ActorHandle):
        raise ValueError("ray.kill() only supported for actors. "
                         "Got: {}.".format(type(actor)))
    worker.core_worker.kill_actor(actor._ray_actor_id, no_restart)


@client_mode_hook
def shutdown():
    """Disconnect the worker, and terminate processes started by ray.init().

    This will automatically run at the end when a Python process that uses Ray
    exits. It is ok to run this twice in a row. The primary use case for this
    function is to cleanup state between tests.

    Note that this will clear any remote function definitions, actor
    definitions, and existing actors, so if you wish to use any previously
    defined remote functions or actors after calling ray.shutdown(), then you
    need to redefine them. If they were defined in an imported module, then you
    will need to reload the module.
    """
    ray.worker.shutdown()


def show_in_dashboard(message, key="", dtype="text"):
    """Display message in dashboard.

    Display message for the current task or actor in the dashboard.
    For example, this can be used to display the status of a long-running
    computation.

    Args:
        message (str): Message to be displayed.
        key (str): The key name for the message. Multiple message under
            different keys will be displayed at the same time. Messages
            under the same key will be overridden.
        data_type (str): The type of message for rendering. One of the
            following: text, html.
    """
    worker = ray.worker.global_worker
    worker.check_connected()

    acceptable_dtypes = {"text", "html"}
    assert dtype in acceptable_dtypes, (
        f"dtype accepts only: {acceptable_dtypes}")

    message_wrapped = {"message": message, "dtype": dtype}
    message_encoded = json.dumps(message_wrapped).encode()

    worker.core_worker.set_webui_display(key.encode(), message_encoded)


def _make_decorator(num_returns=None,
                    num_cpus=None,
                    num_gpus=None,
                    memory=None,
                    object_store_memory=None,
                    resources=None,
                    accelerator_type=None,
                    max_calls=None,
                    max_retries=None,
                    max_restarts=None,
                    max_task_retries=None,
                    worker=None):
    def decorator(function_or_class):
        if (inspect.isfunction(function_or_class)
                or is_cython(function_or_class)):
            # Set the remote function default resources.
            if max_restarts is not None:
                raise ValueError("The keyword 'max_restarts' is not "
                                 "allowed for remote functions.")
            if max_task_retries is not None:
                raise ValueError("The keyword 'max_task_retries' is not "
                                 "allowed for remote functions.")
            if num_returns is not None and (not isinstance(num_returns, int)
                                            or num_returns < 0):
                raise ValueError(
                    "The keyword 'num_returns' only accepts 0 or a"
                    " positive integer")
            if max_retries is not None and (not isinstance(max_retries, int)
                                            or max_retries < -1):
                raise ValueError(
                    "The keyword 'max_retries' only accepts 0, -1 or a"
                    " positive integer")
            if max_calls is not None and (not isinstance(max_calls, int)
                                          or max_calls < 0):
                raise ValueError(
                    "The keyword 'max_calls' only accepts 0 or a positive"
                    " integer")
            return ray.remote_function.RemoteFunction(
                Language.PYTHON, function_or_class, None, num_cpus, num_gpus,
                memory, object_store_memory, resources, accelerator_type,
                num_returns, max_calls, max_retries)

        if inspect.isclass(function_or_class):
            if num_returns is not None:
                raise TypeError("The keyword 'num_returns' is not "
                                "allowed for actors.")
            if max_calls is not None:
                raise TypeError("The keyword 'max_calls' is not "
                                "allowed for actors.")
            if max_restarts is not None and (not isinstance(max_restarts, int)
                                             or max_restarts < -1):
                raise ValueError(
                    "The keyword 'max_restarts' only accepts -1, 0 or a"
                    " positive integer")
            if max_task_retries is not None and (not isinstance(
                    max_task_retries, int) or max_task_retries < -1):
                raise ValueError(
                    "The keyword 'max_task_retries' only accepts -1, 0 or a"
                    " positive integer")
            return ray.actor.make_actor(function_or_class, num_cpus, num_gpus,
                                        memory, object_store_memory, resources,
                                        accelerator_type, max_restarts,
                                        max_task_retries)

        raise TypeError("The @ray.remote decorator must be applied to "
                        "either a function or to a class.")

    return decorator


@client_mode_hook
def remote(*args, **kwargs):
    """Defines a remote function or an actor class.

    This can be used with no arguments to define a remote function or actor as
    follows:

    .. code-block:: python

        @ray.remote
        def f():
            return 1

        @ray.remote
        class Foo:
            def method(self):
                return 1

    It can also be used with specific keyword arguments as follows:

    .. code-block:: python

        @ray.remote(num_gpus=1, max_calls=1, num_returns=2)
        def f():
            return 1, 2

        @ray.remote(num_cpus=2, resources={"CustomResource": 1})
        class Foo:
            def method(self):
                return 1

    Remote task and actor objects returned by @ray.remote can also be
    dynamically modified with the same arguments as above using
    ``.options()`` as follows:

    .. code-block:: python

        @ray.remote(num_gpus=1, max_calls=1, num_returns=2)
        def f():
            return 1, 2
        g = f.options(num_gpus=2, max_calls=None)

        @ray.remote(num_cpus=2, resources={"CustomResource": 1})
        class Foo:
            def method(self):
                return 1
        Bar = Foo.options(num_cpus=1, resources=None)

    Running remote actors will be terminated when the actor handle to them
    in Python is deleted, which will cause them to complete any outstanding
    work and then shut down. If you want to kill them immediately, you can
    also call ``ray.kill(actor)``.

    Args:
        num_returns (int): This is only for *remote functions*. It specifies
            the number of object refs returned by
            the remote function invocation.
        num_cpus (float): The quantity of CPU cores to reserve
            for this task or for the lifetime of the actor.
        num_gpus (int): The quantity of GPUs to reserve
            for this task or for the lifetime of the actor.
        resources (Dict[str, float]): The quantity of various custom resources
            to reserve for this task or for the lifetime of the actor.
            This is a dictionary mapping strings (resource names) to floats.
        accelerator_type: If specified, requires that the task or actor run
            on a node with the specified type of accelerator.
            See `ray.accelerators` for accelerator types.
        max_calls (int): Only for *remote functions*. This specifies the
            maximum number of times that a given worker can execute
            the given remote function before it must exit
            (this can be used to address memory leaks in third-party
            libraries or to reclaim resources that cannot easily be
            released, e.g., GPU memory that was acquired by TensorFlow).
            By default this is infinite.
        max_restarts (int): Only for *actors*. This specifies the maximum
            number of times that the actor should be restarted when it dies
            unexpectedly. The minimum valid value is 0 (default),
            which indicates that the actor doesn't need to be restarted.
            A value of -1 indicates that an actor should be restarted
            indefinitely.
        max_task_retries (int): Only for *actors*. How many times to
            retry an actor task if the task fails due to a system error,
            e.g., the actor has died. If set to -1, the system will
            retry the failed task until the task succeeds, or the actor
            has reached its max_restarts limit. If set to `n > 0`, the
            system will retry the failed task up to n times, after which the
            task will throw a `RayActorError` exception upon :obj:`ray.get`.
            Note that Python exceptions are not considered system errors
            and will not trigger retries.
        max_retries (int): Only for *remote functions*. This specifies
            the maximum number of times that the remote function
            should be rerun when the worker process executing it
            crashes unexpectedly. The minimum valid value is 0,
            the default is 4 (default), and a value of -1 indicates
            infinite retries.
        override_environment_variables (Dict[str, str]): This specifies
            environment variables to override for the actor or task.  The
            overrides are propagated to all child actors and tasks.  This
            is a dictionary mapping variable names to their values.  Existing
            variables can be overridden, new ones can be created, and an
            existing variable can be unset by setting it to an empty string.

    """
    worker = ray.worker.global_worker

    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        # This is the case where the decorator is just @ray.remote.
        return _make_decorator(worker=worker)(args[0])

    # Parse the keyword arguments from the decorator.
    error_string = ("The @ray.remote decorator must be applied either "
                    "with no arguments and no parentheses, for example "
                    "'@ray.remote', or it must be applied using some of "
                    "the arguments 'num_returns', 'num_cpus', 'num_gpus', "
                    "'memory', 'object_store_memory', 'resources', "
                    "'max_calls', or 'max_restarts', like "
                    "'@ray.remote(num_returns=2, "
                    "resources={\"CustomResource\": 1})'.")
    assert len(args) == 0 and len(kwargs) > 0, error_string
    for key in kwargs:
        assert key in [
            "num_returns",
            "num_cpus",
            "num_gpus",
            "memory",
            "object_store_memory",
            "resources",
            "accelerator_type",
            "max_calls",
            "max_restarts",
            "max_task_retries",
            "max_retries",
        ], error_string

    num_cpus = kwargs["num_cpus"] if "num_cpus" in kwargs else None
    num_gpus = kwargs["num_gpus"] if "num_gpus" in kwargs else None
    resources = kwargs.get("resources")
    if not isinstance(resources, dict) and resources is not None:
        raise TypeError("The 'resources' keyword argument must be a "
                        f"dictionary, but received type {type(resources)}.")
    if resources is not None:
        assert "CPU" not in resources, "Use the 'num_cpus' argument."
        assert "GPU" not in resources, "Use the 'num_gpus' argument."

    accelerator_type = kwargs.get("accelerator_type")

    # Handle other arguments.
    num_returns = kwargs.get("num_returns")
    max_calls = kwargs.get("max_calls")
    max_restarts = kwargs.get("max_restarts")
    max_task_retries = kwargs.get("max_task_retries")
    memory = kwargs.get("memory")
    object_store_memory = kwargs.get("object_store_memory")
    max_retries = kwargs.get("max_retries")

    return _make_decorator(
        num_returns=num_returns,
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        memory=memory,
        object_store_memory=object_store_memory,
        resources=resources,
        accelerator_type=accelerator_type,
        max_calls=max_calls,
        max_restarts=max_restarts,
        max_task_retries=max_task_retries,
        max_retries=max_retries,
        worker=worker)
