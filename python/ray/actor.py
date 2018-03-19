from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import hashlib
import inspect
import json
import traceback

import ray.cloudpickle as pickle
import ray.local_scheduler
import ray.signature as signature
import ray.worker
from ray.utils import (FunctionProperties, random_string, is_cython,
                       push_error_to_driver)


def random_actor_id():
    return ray.local_scheduler.ObjectID(random_string())


def random_actor_class_id():
    return random_string()


def compute_actor_handle_id(actor_handle_id, num_forks):
    """Deterministically comopute an actor handle ID.

    A new actor handle ID is generated when it is forked from another actor
    handle. The new handle ID is computed as hash(old_handle_id || num_forks).

    Args:
        actor_handle_id (common.ObjectID): The original actor handle ID.
        num_forks: The number of times the original actor handle has been
                   forked so far.

    Returns:
        An object ID for the new actor handle.
    """
    handle_id_hash = hashlib.sha1()
    handle_id_hash.update(actor_handle_id.id())
    handle_id_hash.update(str(num_forks).encode("ascii"))
    handle_id = handle_id_hash.digest()
    assert len(handle_id) == 20
    return ray.local_scheduler.ObjectID(handle_id)


def compute_actor_creation_function_id(class_id):
    """Compute the function ID for an actor creation task.

    Args:
        class_id: The ID of the actor class.

    Returns:
        The function ID of the actor creation event.
    """
    return ray.local_scheduler.ObjectID(class_id)


def compute_actor_method_function_id(class_name, attr):
    """Get the function ID corresponding to an actor method.

    Args:
        class_name (str): The class name of the actor.
        attr (str): The attribute name of the method.

    Returns:
        Function ID corresponding to the method.
    """
    function_id_hash = hashlib.sha1()
    function_id_hash.update(class_name)
    function_id_hash.update(attr.encode("ascii"))
    function_id = function_id_hash.digest()
    assert len(function_id) == 20
    return ray.local_scheduler.ObjectID(function_id)


def set_actor_checkpoint(worker, actor_id, checkpoint_index, checkpoint,
                         frontier):
    """Set the most recent checkpoint associated with a given actor ID.

    Args:
        worker: The worker to use to get the checkpoint.
        actor_id: The actor ID of the actor to get the checkpoint for.
        checkpoint_index: The number of tasks included in the checkpoint.
        checkpoint: The state object to save.
        frontier: The task frontier at the time of the checkpoint.
    """
    actor_key = b"Actor:" + actor_id
    worker.redis_client.hmset(
        actor_key, {
            "checkpoint_index": checkpoint_index,
            "checkpoint": checkpoint,
            "frontier": frontier,
        })


def get_actor_checkpoint(worker, actor_id):
    """Get the most recent checkpoint associated with a given actor ID.

    Args:
        worker: The worker to use to get the checkpoint.
        actor_id: The actor ID of the actor to get the checkpoint for.

    Returns:
        If a checkpoint exists, this returns a tuple of the number of tasks
            included in the checkpoint, the saved checkpoint state, and the
            task frontier at the time of the checkpoint. If no checkpoint
            exists, all objects are set to None.  The checkpoint index is the .
            executed on the actor before the checkpoint was made.
    """
    actor_key = b"Actor:" + actor_id
    checkpoint_index, checkpoint, frontier = worker.redis_client.hmget(
        actor_key, ["checkpoint_index", "checkpoint", "frontier"])
    if checkpoint_index is not None:
        checkpoint_index = int(checkpoint_index)
    return checkpoint_index, checkpoint, frontier


def save_and_log_checkpoint(worker, actor):
    """Save a checkpoint on the actor and log any errors.

    Args:
        worker: The worker to use to log errors.
        actor: The actor to checkpoint.
        checkpoint_index: The number of tasks that have executed so far.
    """
    try:
        actor.__ray_checkpoint__()
    except Exception:
        traceback_str = ray.utils.format_error_message(
            traceback.format_exc())
        # Log the error message.
        ray.utils.push_error_to_driver(
            worker.redis_client,
            "checkpoint",
            traceback_str,
            driver_id=worker.task_driver_id.id(),
            data={"actor_class": actor.__class__.__name__,
                  "function_name": actor.__ray_checkpoint__.__name__})


def restore_and_log_checkpoint(worker, actor):
    """Restore an actor from a checkpoint and log any errors.

    Args:
        worker: The worker to use to log errors.
        actor: The actor to restore.
    """
    checkpoint_resumed = False
    try:
        checkpoint_resumed = actor.__ray_checkpoint_restore__()
    except Exception:
        traceback_str = ray.utils.format_error_message(
            traceback.format_exc())
        # Log the error message.
        ray.utils.push_error_to_driver(
            worker.redis_client,
            "checkpoint",
            traceback_str,
            driver_id=worker.task_driver_id.id(),
            data={
                "actor_class": actor.__class__.__name__,
                "function_name":
                actor.__ray_checkpoint_restore__.__name__})
    return checkpoint_resumed


def make_actor_method_executor(worker, method_name, method, actor_imported):
    """Make an executor that wraps a user-defined actor method.

    The wrapped method updates the worker's internal state and performs any
    necessary checkpointing operations.

    Args:
        worker (Worker): The worker that is executing the actor.
        method_name (str): The name of the actor method.
        method (instancemethod): The actor method to wrap. This should be a
            method defined on the actor class and should therefore take an
            instance of the actor as the first argument.
        actor_imported (bool): Whether the actor has been imported.
            Checkpointing operations will not be run if this is set to False.

    Returns:
        A function that executes the given actor method on the worker's stored
            instance of the actor. The function also updates the worker's
            internal state to record the executed method.
    """

    def actor_method_executor(dummy_return_id, actor, *args):
        # Update the actor's task counter to reflect the task we're about to
        # execute.
        worker.actor_task_counter += 1

        # If this is the first task to execute on the actor, try to resume from
        # a checkpoint.
        if actor_imported and worker.actor_task_counter == 1:
            checkpoint_resumed = restore_and_log_checkpoint(worker, actor)
            if checkpoint_resumed:
                # NOTE(swang): Since we did not actually execute the __init__
                # method, this will put None as the return value. If the
                # __init__ method is supposed to return multiple values, an
                # exception will be logged.
                return

        # Determine whether we should checkpoint the actor.
        checkpointing_on = (actor_imported and
                            worker.actor_checkpoint_interval > 0)
        # We should checkpoint the actor if user checkpointing is on, we've
        # executed checkpoint_interval tasks since the last checkpoint, and the
        # method we're about to execute is not a checkpoint.
        save_checkpoint = (checkpointing_on and
                           (worker.actor_task_counter %
                            worker.actor_checkpoint_interval == 0 and
                            method_name != "__ray_checkpoint__"))

        # Execute the assigned method and save a checkpoint if necessary.
        try:
            method_returns = method(actor, *args)
        except Exception:
            # Save the checkpoint before allowing the method exception to be
            # thrown.
            if save_checkpoint:
                save_and_log_checkpoint(worker, actor)
            raise
        else:
            # Save the checkpoint before returning the method's return values.
            if save_checkpoint:
                save_and_log_checkpoint(worker, actor)
            return method_returns

    return actor_method_executor


def fetch_and_register_actor(actor_class_key, resources, worker):
    """Import an actor.

    This will be called by the worker's import thread when the worker receives
    the actor_class export, assuming that the worker is an actor for that
    class.

    Args:
        actor_class_key: The key in Redis to use to fetch the actor.
        resources: The resources required for this actor's lifetime.
        worker: The worker to use.
    """
    actor_id_str = worker.actor_id
    (driver_id, class_id, class_name,
     module, pickled_class, checkpoint_interval,
     actor_method_names,
     actor_method_num_return_vals) = worker.redis_client.hmget(
         actor_class_key, ["driver_id", "class_id", "class_name", "module",
                           "class", "checkpoint_interval",
                           "actor_method_names",
                           "actor_method_num_return_vals"])

    actor_name = class_name.decode("ascii")
    module = module.decode("ascii")
    checkpoint_interval = int(checkpoint_interval)
    actor_method_names = json.loads(actor_method_names.decode("ascii"))
    actor_method_num_return_vals = json.loads(
        actor_method_num_return_vals.decode("ascii"))

    # Create a temporary actor with some temporary methods so that if the actor
    # fails to be unpickled, the temporary actor can be used (just to produce
    # error messages and to prevent the driver from hanging).
    class TemporaryActor(object):
        pass
    worker.actors[actor_id_str] = TemporaryActor()
    worker.actor_checkpoint_interval = checkpoint_interval

    def temporary_actor_method(*xs):
        raise Exception("The actor with name {} failed to be imported, and so "
                        "cannot execute this method".format(actor_name))
    # Register the actor method signatures.
    register_actor_signatures(worker, driver_id, class_id, class_name,
                              actor_method_names, actor_method_num_return_vals)
    # Register the actor method executors.
    for actor_method_name in actor_method_names:
        function_id = compute_actor_method_function_id(class_name,
                                                       actor_method_name).id()
        temporary_executor = make_actor_method_executor(worker,
                                                        actor_method_name,
                                                        temporary_actor_method,
                                                        actor_imported=False)
        worker.functions[driver_id][function_id] = (actor_method_name,
                                                    temporary_executor)
        worker.num_task_executions[driver_id][function_id] = 0

    try:
        unpickled_class = pickle.loads(pickled_class)
        worker.actor_class = unpickled_class
    except Exception:
        # If an exception was thrown when the actor was imported, we record the
        # traceback and notify the scheduler of the failure.
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        # Log the error message.
        push_error_to_driver(worker.redis_client, "register_actor_signatures",
                             traceback_str, driver_id,
                             data={"actor_id": actor_id_str})
        # TODO(rkn): In the future, it might make sense to have the worker exit
        # here. However, currently that would lead to hanging if someone calls
        # ray.get on a method invoked on the actor.
    else:
        # TODO(pcm): Why is the below line necessary?
        unpickled_class.__module__ = module
        worker.actors[actor_id_str] = unpickled_class.__new__(unpickled_class)
        actor_methods = inspect.getmembers(
            unpickled_class, predicate=(lambda x: (inspect.isfunction(x) or
                                                   inspect.ismethod(x) or
                                                   is_cython(x))))
        for actor_method_name, actor_method in actor_methods:
            function_id = compute_actor_method_function_id(
                class_name, actor_method_name).id()
            executor = make_actor_method_executor(worker, actor_method_name,
                                                  actor_method,
                                                  actor_imported=True)
            worker.functions[driver_id][function_id] = (actor_method_name,
                                                        executor)
            # We do not set worker.function_properties[driver_id][function_id]
            # because we currently do need the actor worker to submit new tasks
            # for the actor.


def register_actor_signatures(worker, driver_id, class_id, class_name,
                              actor_method_names,
                              actor_method_num_return_vals,
                              actor_creation_resources=None,
                              actor_method_cpus=None):
    """Register an actor's method signatures in the worker.

    Args:
        worker: The worker to register the signatures on.
        driver_id: The ID of the driver that this actor is associated with.
        class_id: The ID of the actor class.
        class_name: The name of the actor class.
        actor_method_names: The names of the methods to register.
        actor_method_num_return_vals: A list of the number of return values for
            each of the actor's methods.
        actor_creation_resources: The resources required by the actor creation
            task.
        actor_method_cpus: The number of CPUs required by each actor method.
    """
    assert len(actor_method_names) == len(actor_method_num_return_vals)
    for actor_method_name, num_return_vals in zip(
            actor_method_names, actor_method_num_return_vals):
        # TODO(rkn): When we create a second actor, we are probably overwriting
        # the values from the first actor here. This may or may not be a
        # problem.
        function_id = compute_actor_method_function_id(class_name,
                                                       actor_method_name).id()
        worker.function_properties[driver_id][function_id] = (
            # The extra return value is an actor dummy object.
            # In the cases where actor_method_cpus is None, that value should
            # never be used.
            FunctionProperties(num_return_vals=num_return_vals + 1,
                               resources={"CPU": actor_method_cpus},
                               max_calls=0))

    if actor_creation_resources is not None:
        # Also register the actor creation task.
        function_id = compute_actor_creation_function_id(class_id)
        worker.function_properties[driver_id][function_id.id()] = (
            # The extra return value is an actor dummy object.
            FunctionProperties(num_return_vals=0 + 1,
                               resources=actor_creation_resources,
                               max_calls=0))


def publish_actor_class_to_key(key, actor_class_info, worker):
    """Push an actor class definition to Redis.

    The is factored out as a separate function because it is also called
    on cached actor class definitions when a worker connects for the first
    time.

    Args:
        key: The key to store the actor class info at.
        actor_class_info: Information about the actor class.
        worker: The worker to use to connect to Redis.
    """
    # We set the driver ID here because it may not have been available when the
    # actor class was defined.
    actor_class_info["driver_id"] = worker.task_driver_id.id()
    worker.redis_client.hmset(key, actor_class_info)
    worker.redis_client.rpush("Exports", key)


def export_actor_class(class_id, Class, actor_method_names,
                       actor_method_num_return_vals,
                       checkpoint_interval, worker):
    key = b"ActorClass:" + class_id
    actor_class_info = {
        "class_name": Class.__name__,
        "module": Class.__module__,
        "class": pickle.dumps(Class),
        "checkpoint_interval": checkpoint_interval,
        "actor_method_names": json.dumps(list(actor_method_names)),
        "actor_method_num_return_vals": json.dumps(
            actor_method_num_return_vals)}

    if worker.mode is None:
        # This means that 'ray.init()' has not been called yet and so we must
        # cache the actor class definition and export it when 'ray.init()' is
        # called.
        assert worker.cached_remote_functions_and_actors is not None
        worker.cached_remote_functions_and_actors.append(
            ("actor", (key, actor_class_info)))
        # This caching code path is currently not used because we only export
        # actor class definitions lazily when we instantiate the actor for the
        # first time.
        assert False, "This should be unreachable."
    else:
        publish_actor_class_to_key(key, actor_class_info, worker)
    # TODO(rkn): Currently we allow actor classes to be defined within tasks.
    # I tried to disable this, but it may be necessary because of
    # https://github.com/ray-project/ray/issues/1146.


def export_actor(actor_id, class_id, class_name, actor_method_names,
                 actor_method_num_return_vals, actor_creation_resources,
                 actor_method_cpus, worker):
    """Export an actor to redis.

    Args:
        actor_id (common.ObjectID): The ID of the actor.
        class_id (str): A random ID for the actor class.
        class_name (str): The actor class name.
        actor_method_names (list): A list of the names of this actor's methods.
        actor_method_num_return_vals: A list of the number of return values for
            each of the actor's methods.
        actor_creation_resources: A dictionary mapping resource name to the
            quantity of that resource required by the actor.
        actor_method_cpus: The number of CPUs required by actor methods.
    """
    ray.worker.check_main_thread()
    if worker.mode is None:
        raise Exception("Actors cannot be created before Ray has been "
                        "started. You can start Ray with 'ray.init()'.")

    driver_id = worker.task_driver_id.id()
    register_actor_signatures(
        worker, driver_id, class_id, class_name, actor_method_names,
        actor_method_num_return_vals,
        actor_creation_resources=actor_creation_resources,
        actor_method_cpus=actor_method_cpus)

    args = [class_id]
    function_id = compute_actor_creation_function_id(class_id)
    return worker.submit_task(function_id, args, actor_creation_id=actor_id)[0]


def method(*args, **kwargs):
    assert len(args) == 0
    assert len(kwargs) == 1
    assert "num_return_vals" in kwargs
    num_return_vals = kwargs["num_return_vals"]

    def annotate_method(method):
        method.__ray_num_return_vals__ = num_return_vals
        return method

    return annotate_method


# Create objects to wrap method invocations. This is done so that we can
# invoke methods with actor.method.remote() instead of actor.method().
class ActorMethod(object):
    def __init__(self, actor, method_name):
        self._actor = actor
        self._method_name = method_name

    def __call__(self, *args, **kwargs):
        raise Exception("Actor methods cannot be called directly. Instead "
                        "of running 'object.{}()', try "
                        "'object.{}.remote()'."
                        .format(self._method_name, self._method_name))

    def remote(self, *args, **kwargs):
        return self._actor._actor_method_call(
            self._method_name, args=args, kwargs=kwargs,
            dependency=self._actor._ray_actor_cursor)


class ActorHandleWrapper(object):
    """A wrapper for the contents of an ActorHandle.

    This is essentially just a dictionary, but it is used so that the recipient
    can tell that an argument is an ActorHandle.
    """
    def __init__(self, actor_id, class_id, actor_handle_id, actor_cursor,
                 actor_counter, actor_method_names,
                 actor_method_num_return_vals, method_signatures,
                 checkpoint_interval, class_name,
                 actor_creation_dummy_object_id,
                 actor_creation_resources, actor_method_cpus):
        # TODO(rkn): Some of these fields are probably not necessary. We should
        # strip out the unnecessary fields to keep actor handles lightweight.
        self.actor_id = actor_id
        self.class_id = class_id
        self.actor_handle_id = actor_handle_id
        self.actor_cursor = actor_cursor
        self.actor_counter = actor_counter
        self.actor_method_names = actor_method_names
        self.actor_method_num_return_vals = actor_method_num_return_vals
        # TODO(swang): Fetch this information from Redis so that we don't have
        # to fall back to pickle.
        self.method_signatures = method_signatures
        self.checkpoint_interval = checkpoint_interval
        self.class_name = class_name
        self.actor_creation_dummy_object_id = actor_creation_dummy_object_id
        self.actor_creation_resources = actor_creation_resources
        self.actor_method_cpus = actor_method_cpus


def wrap_actor_handle(actor_handle):
    """Wrap the ActorHandle to store the fields.

    Args:
        actor_handle: The ActorHandle instance to wrap.

    Returns:
        An ActorHandleWrapper instance that stores the ActorHandle's fields.
    """
    wrapper = ActorHandleWrapper(
        actor_handle._ray_actor_id,
        actor_handle._ray_class_id,
        compute_actor_handle_id(actor_handle._ray_actor_handle_id,
                                actor_handle._ray_actor_forks),
        actor_handle._ray_actor_cursor,
        0,  # Reset the actor counter.
        actor_handle._ray_actor_method_names,
        actor_handle._ray_actor_method_num_return_vals,
        actor_handle._ray_method_signatures,
        actor_handle._ray_checkpoint_interval,
        actor_handle._ray_class_name,
        actor_handle._ray_actor_creation_dummy_object_id,
        actor_handle._ray_actor_creation_resources,
        actor_handle._ray_actor_method_cpus)
    actor_handle._ray_actor_forks += 1
    return wrapper


def unwrap_actor_handle(worker, wrapper):
    """Make an ActorHandle from the stored fields.

    Args:
        worker: The worker that is unwrapping the actor handle.
        wrapper: An ActorHandleWrapper instance to unwrap.

    Returns:
        The unwrapped ActorHandle instance.
    """
    driver_id = worker.task_driver_id.id()
    register_actor_signatures(worker, driver_id, wrapper.class_id,
                              wrapper.class_name, wrapper.actor_method_names,
                              wrapper.actor_method_num_return_vals,
                              wrapper.actor_creation_resources,
                              wrapper.actor_method_cpus)

    actor_handle_class = make_actor_handle_class(wrapper.class_name)
    actor_object = actor_handle_class.__new__(actor_handle_class)
    actor_object._manual_init(
        wrapper.actor_id,
        wrapper.class_id,
        wrapper.actor_handle_id,
        wrapper.actor_cursor,
        wrapper.actor_counter,
        wrapper.actor_method_names,
        wrapper.actor_method_num_return_vals,
        wrapper.method_signatures,
        wrapper.checkpoint_interval,
        wrapper.actor_creation_dummy_object_id,
        wrapper.actor_creation_resources,
        wrapper.actor_method_cpus)
    return actor_object


class ActorHandleParent(object):
    """This is the parent class of all ActorHandle classes.

    This enables us to identify actor handles by checking if an object obj
    satisfies isinstance(obj, ActorHandleParent).
    """
    pass


def make_actor_handle_class(class_name):
    class ActorHandle(ActorHandleParent):
        def __init__(self, *args, **kwargs):
            raise Exception("Actor classes cannot be instantiated directly. "
                            "Instead of running '{}()', try '{}.remote()'."
                            .format(class_name, class_name))

        @classmethod
        def remote(cls, *args, **kwargs):
            raise NotImplementedError("The classmethod remote() can only be "
                                      "called on the original Class.")

        def _manual_init(self, actor_id, class_id, actor_handle_id,
                         actor_cursor, actor_counter, actor_method_names,
                         actor_method_num_return_vals, method_signatures,
                         checkpoint_interval, actor_creation_dummy_object_id,
                         actor_creation_resources, actor_method_cpus):
            self._ray_actor_id = actor_id
            self._ray_class_id = class_id
            self._ray_actor_handle_id = actor_handle_id
            self._ray_actor_cursor = actor_cursor
            self._ray_actor_counter = actor_counter
            self._ray_actor_method_names = actor_method_names
            self._ray_actor_method_num_return_vals = (
                actor_method_num_return_vals)
            self._ray_method_signatures = method_signatures
            self._ray_checkpoint_interval = checkpoint_interval
            self._ray_class_name = class_name
            self._ray_actor_forks = 0
            self._ray_actor_creation_dummy_object_id = (
                actor_creation_dummy_object_id)
            self._ray_actor_creation_resources = actor_creation_resources
            self._ray_actor_method_cpus = actor_method_cpus

        def _actor_method_call(self, method_name, args=None, kwargs=None,
                               dependency=None):
            """Method execution stub for an actor handle.

            This is the function that executes when
            `actor.method_name.remote(*args, **kwargs)` is called. Instead of
            executing locally, the method is packaged as a task and scheduled
            to the remote actor instance.

            Args:
                self: The local actor handle.
                method_name: The name of the actor method to execute.
                args: A list of arguments for the actor method.
                kwargs: A dictionary of keyword arguments for the actor method.
                dependency: The object ID that this method is dependent on.
                    Defaults to None, for no dependencies. Most tasks should
                    pass in the dummy object returned by the preceding task.
                    Some tasks, such as checkpoint and terminate methods, have
                    no dependencies.

            Returns:
                object_ids: A list of object IDs returned by the remote actor
                    method.
            """
            ray.worker.check_connected()
            ray.worker.check_main_thread()
            function_signature = self._ray_method_signatures[method_name]
            if args is None:
                args = []
            if kwargs is None:
                kwargs = {}
            args = signature.extend_args(function_signature, args, kwargs)

            # Execute functions locally if Ray is run in PYTHON_MODE
            # Copy args to prevent the function from mutating them.
            if ray.worker.global_worker.mode == ray.PYTHON_MODE:
                return getattr(
                    ray.worker.global_worker.actors[self._ray_actor_id],
                    method_name)(*copy.deepcopy(args))

            # Add the execution dependency.
            if dependency is None:
                execution_dependencies = []
            else:
                execution_dependencies = [dependency]

            is_actor_checkpoint_method = (method_name == "__ray_checkpoint__")

            function_id = compute_actor_method_function_id(
                self._ray_class_name, method_name)
            object_ids = ray.worker.global_worker.submit_task(
                function_id, args, actor_id=self._ray_actor_id,
                actor_handle_id=self._ray_actor_handle_id,
                actor_counter=self._ray_actor_counter,
                is_actor_checkpoint_method=is_actor_checkpoint_method,
                actor_creation_dummy_object_id=(
                    self._ray_actor_creation_dummy_object_id),
                execution_dependencies=execution_dependencies)
            # Update the actor counter and cursor to reflect the most recent
            # invocation.
            self._ray_actor_counter += 1
            self._ray_actor_cursor = object_ids.pop()

            # The last object returned is the dummy object that should be
            # passed in to the next actor method. Do not return it to the user.
            if len(object_ids) == 1:
                return object_ids[0]
            elif len(object_ids) > 1:
                return object_ids

        # Make tab completion work.
        def __dir__(self):
            return self._ray_actor_method_names

        def __getattribute__(self, attr):
            try:
                # Check whether this is an actor method.
                actor_method_names = object.__getattribute__(
                    self, "_ray_actor_method_names")
                if attr in actor_method_names:
                    # We create the ActorMethod on the fly here so that the
                    # ActorHandle doesn't need a reference to the ActorMethod.
                    # The ActorMethod has a reference to the ActorHandle and
                    # this was causing cyclic references which were prevent
                    # object deallocation from behaving in a predictable
                    # manner.
                    actor_method_cls = ActorMethod
                    return actor_method_cls(self, attr)
            except AttributeError:
                pass

            # If the requested attribute is not a registered method, fall back
            # to default __getattribute__.
            return object.__getattribute__(self, attr)

        def __repr__(self):
            return "Actor(" + self._ray_actor_id.hex() + ")"

        def __reduce__(self):
            raise Exception("Actor objects cannot be pickled.")

        def __del__(self):
            """Kill the worker that is running this actor."""
            # TODO(swang): Also clean up forked actor handles.
            # Kill the worker if this is the original actor handle, created
            # with Class.remote().
            if (ray.worker.global_worker.connected and
                    self._ray_actor_handle_id.id() == ray.worker.NIL_ACTOR_ID):
                # TODO(rkn): Should we be passing in the actor cursor as a
                # dependency here?
                self._actor_method_call("__ray_terminate__",
                                        args=[self._ray_actor_id.id()])

    return ActorHandle


def actor_handle_from_class(Class, class_id, actor_creation_resources,
                            checkpoint_interval, actor_method_cpus):
    class_name = Class.__name__.encode("ascii")
    actor_handle_class = make_actor_handle_class(class_name)
    exported = []

    class ActorHandle(actor_handle_class):

        @classmethod
        def remote(cls, *args, **kwargs):
            if ray.worker.global_worker.mode is None:
                raise Exception("Actors cannot be created before ray.init() "
                                "has been called.")

            actor_id = random_actor_id()
            # The ID for this instance of ActorHandle. These should be unique
            # across instances with the same _ray_actor_id.
            actor_handle_id = ray.local_scheduler.ObjectID(
                ray.worker.NIL_ACTOR_ID)
            # The actor cursor is a dummy object representing the most recent
            # actor method invocation. For each subsequent method invocation,
            # the current cursor should be added as a dependency, and then
            # updated to reflect the new invocation.
            actor_cursor = None
            # The number of actor method invocations that we've called so far.
            actor_counter = 0
            # Get the actor methods of the given class.
            actor_methods = inspect.getmembers(
                Class, predicate=(lambda x: (inspect.isfunction(x) or
                                             inspect.ismethod(x) or
                                             is_cython(x))))
            # Extract the signatures of each of the methods. This will be used
            # to catch some errors if the methods are called with inappropriate
            # arguments.
            method_signatures = dict()
            for k, v in actor_methods:
                # Print a warning message if the method signature is not
                # supported. We don't raise an exception because if the actor
                # inherits from a class that has a method whose signature we
                # don't support, we there may not be much the user can do about
                # it.
                signature.check_signature_supported(v, warn=True)
                method_signatures[k] = signature.extract_signature(
                    v, ignore_first=True)

            actor_method_names = [method_name for method_name, _ in
                                  actor_methods]
            actor_method_num_return_vals = []
            for _, method in actor_methods:
                if hasattr(method, "__ray_num_return_vals__"):
                    actor_method_num_return_vals.append(
                        method.__ray_num_return_vals__)
                else:
                    actor_method_num_return_vals.append(1)
            # Do not export the actor class or the actor if run in PYTHON_MODE
            # Instead, instantiate the actor locally and add it to
            # global_worker's dictionary
            if ray.worker.global_worker.mode == ray.PYTHON_MODE:
                ray.worker.global_worker.actors[actor_id] = (
                    Class.__new__(Class))
            else:
                # Export the actor.
                if not exported:
                    export_actor_class(class_id, Class, actor_method_names,
                                       actor_method_num_return_vals,
                                       checkpoint_interval,
                                       ray.worker.global_worker)
                    exported.append(0)
                actor_cursor = export_actor(actor_id, class_id, class_name,
                                            actor_method_names,
                                            actor_method_num_return_vals,
                                            actor_creation_resources,
                                            actor_method_cpus,
                                            ray.worker.global_worker)

            # Instantiate the actor handle.
            actor_object = cls.__new__(cls)
            actor_object._manual_init(actor_id, class_id, actor_handle_id,
                                      actor_cursor, actor_counter,
                                      actor_method_names,
                                      actor_method_num_return_vals,
                                      method_signatures, checkpoint_interval,
                                      actor_cursor, actor_creation_resources,
                                      actor_method_cpus)

            # Call __init__ as a remote function.
            if "__init__" in actor_object._ray_actor_method_names:
                actor_object._actor_method_call("__init__", args=args,
                                                kwargs=kwargs,
                                                dependency=actor_cursor)
            else:
                print("WARNING: this object has no __init__ method.")

            return actor_object

    return ActorHandle


def make_actor(cls, resources, checkpoint_interval, actor_method_cpus):
    if checkpoint_interval == 0:
        raise Exception("checkpoint_interval must be greater than 0.")

    # Modify the class to have an additional method that will be used for
    # terminating the worker.
    class Class(cls):
        def __ray_terminate__(self, actor_id):
            # Record that this actor has been removed so that if this node
            # dies later, the actor won't be recreated. Alternatively, we could
            # remove the actor key from Redis here.
            ray.worker.global_worker.redis_client.hset(b"Actor:" + actor_id,
                                                       "removed", True)
            # Disconnect the worker from the local scheduler. The point of this
            # is so that when the worker kills itself below, the local
            # scheduler won't push an error message to the driver.
            ray.worker.global_worker.local_scheduler_client.disconnect()
            import os
            os._exit(0)

        def __ray_save_checkpoint__(self):
            if hasattr(self, "__ray_save__"):
                object_to_serialize = self.__ray_save__()
            else:
                object_to_serialize = self
            return pickle.dumps(object_to_serialize)

        @classmethod
        def __ray_restore_from_checkpoint__(cls, pickled_checkpoint):
            checkpoint = pickle.loads(pickled_checkpoint)
            if hasattr(cls, "__ray_restore__"):
                actor_object = cls.__new__(cls)
                actor_object.__ray_restore__(checkpoint)
            else:
                # TODO(rkn): It's possible that this will cause problems. When
                # you unpickle the same object twice, the two objects will not
                # have the same class.
                actor_object = checkpoint
            return actor_object

        def __ray_checkpoint__(self):
            """Save a checkpoint.

            This task saves the current state of the actor, the current task
            frontier according to the local scheduler, and the checkpoint index
            (number of tasks executed so far).
            """
            worker = ray.worker.global_worker
            checkpoint_index = worker.actor_task_counter
            # Get the state to save.
            checkpoint = self.__ray_save_checkpoint__()
            # Get the current task frontier, per actor handle.
            # NOTE(swang): This only includes actor handles that the local
            # scheduler has seen. Handle IDs for which no task has yet reached
            # the local scheduler will not be included, and may not be runnable
            # on checkpoint resumption.
            actor_id = ray.local_scheduler.ObjectID(worker.actor_id)
            frontier = worker.local_scheduler_client.get_actor_frontier(
                actor_id)
            # Save the checkpoint in Redis. TODO(rkn): Checkpoints
            # should not be stored in Redis. Fix this.
            set_actor_checkpoint(worker, worker.actor_id, checkpoint_index,
                                 checkpoint, frontier)

        def __ray_checkpoint_restore__(self):
            """Restore a checkpoint.

            This task looks for a saved checkpoint and if found, restores the
            state of the actor, the task frontier in the local scheduler, and
            the checkpoint index (number of tasks executed so far).

            Returns:
                A bool indicating whether a checkpoint was resumed.
            """
            worker = ray.worker.global_worker
            # Get the most recent checkpoint stored, if any.
            checkpoint_index, checkpoint, frontier = get_actor_checkpoint(
                worker, worker.actor_id)
            # Try to resume from the checkpoint.
            checkpoint_resumed = False
            if checkpoint_index is not None:
                # Load the actor state from the checkpoint.
                worker.actors[worker.actor_id] = (
                    worker.actor_class.__ray_restore_from_checkpoint__(
                        checkpoint))
                # Set the number of tasks executed so far.
                worker.actor_task_counter = checkpoint_index
                # Set the actor frontier in the local scheduler.
                worker.local_scheduler_client.set_actor_frontier(frontier)
                checkpoint_resumed = True

            return checkpoint_resumed

    Class.__module__ = cls.__module__
    Class.__name__ = cls.__name__

    class_id = random_actor_class_id()

    return actor_handle_from_class(Class, class_id, resources,
                                   checkpoint_interval, actor_method_cpus)


ray.worker.global_worker.fetch_and_register_actor = fetch_and_register_actor
ray.worker.global_worker.make_actor = make_actor
