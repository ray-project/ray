from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import hashlib
import inspect
import traceback

import ray.cloudpickle as pickle
from ray.function_manager import FunctionManager
import ray.local_scheduler
import ray.ray_constants as ray_constants
import ray.signature as signature
import ray.worker
from ray.utils import (
    _random_string,
    is_cython,
)

DEFAULT_ACTOR_METHOD_NUM_RETURN_VALS = 1


def compute_actor_handle_id(actor_handle_id, num_forks):
    """Deterministically compute an actor handle ID.

    A new actor handle ID is generated when it is forked from another actor
    handle. The new handle ID is computed as hash(old_handle_id || num_forks).

    Args:
        actor_handle_id (common.ObjectID): The original actor handle ID.
        num_forks: The number of times the original actor handle has been
                   forked so far.

    Returns:
        An ID for the new actor handle.
    """
    handle_id_hash = hashlib.sha1()
    handle_id_hash.update(actor_handle_id.id())
    handle_id_hash.update(str(num_forks).encode("ascii"))
    handle_id = handle_id_hash.digest()
    assert len(handle_id) == ray_constants.ID_SIZE
    return ray.ObjectID(handle_id)


def compute_actor_handle_id_non_forked(actor_id, actor_handle_id,
                                       current_task_id):
    """Deterministically compute an actor handle ID in the non-forked case.

    This code path is used whenever an actor handle is pickled and unpickled
    (for example, if a remote function closes over an actor handle). Then,
    whenever the actor handle is used, a new actor handle ID will be generated
    on the fly as a deterministic function of the actor ID, the previous actor
    handle ID and the current task ID.

    TODO(rkn): It may be possible to cause problems by closing over multiple
    actor handles in a remote function, which then get unpickled and give rise
    to the same actor handle IDs.

    Args:
        actor_id: The actor ID.
        actor_handle_id: The original actor handle ID.
        num_forks: The number of times the original actor handle has been
           forked so far.

    Returns:
        An ID for the new actor handle.
    """
    handle_id_hash = hashlib.sha1()
    handle_id_hash.update(actor_id.id())
    handle_id_hash.update(actor_handle_id.id())
    handle_id_hash.update(current_task_id.id())
    handle_id = handle_id_hash.digest()
    assert len(handle_id) == ray_constants.ID_SIZE
    return ray.ObjectID(handle_id)


def compute_actor_creation_function_id(class_id):
    """Compute the function ID for an actor creation task.

    Args:
        class_id: The ID of the actor class.

    Returns:
        The function ID of the actor creation event.
    """
    return ray.ObjectID(class_id)


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
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        # Log the error message.
        ray.utils.push_error_to_driver(
            worker,
            ray_constants.CHECKPOINT_PUSH_ERROR,
            traceback_str,
            driver_id=worker.task_driver_id.id(),
            data={
                "actor_class": actor.__class__.__name__,
                "function_name": actor.__ray_checkpoint__.__name__
            })


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
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        # Log the error message.
        ray.utils.push_error_to_driver(
            worker,
            ray_constants.CHECKPOINT_PUSH_ERROR,
            traceback_str,
            driver_id=worker.task_driver_id.id(),
            data={
                "actor_class": actor.__class__.__name__,
                "function_name": actor.__ray_checkpoint_restore__.__name__
            })
    return checkpoint_resumed


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


def method(*args, **kwargs):
    """Annotate an actor method.

    .. code-block:: python

        @ray.remote
        class Foo(object):
            @ray.method(num_return_vals=2)
            def bar(self):
                return 1, 2

        f = Foo.remote()

        _, _ = f.bar.remote()

    Args:
        num_return_vals: The number of object IDs that should be returned by
            invocations of this actor method.
    """
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
    def __init__(self, actor, method_name, num_return_vals):
        self._actor = actor
        self._method_name = method_name
        self._num_return_vals = num_return_vals

    def __call__(self, *args, **kwargs):
        raise Exception("Actor methods cannot be called directly. Instead "
                        "of running 'object.{}()', try "
                        "'object.{}.remote()'.".format(self._method_name,
                                                       self._method_name))

    def remote(self, *args, **kwargs):
        return self._submit(args, kwargs)

    def _submit(self, args, kwargs, num_return_vals=None):
        if num_return_vals is None:
            num_return_vals = self._num_return_vals

        return self._actor._actor_method_call(
            self._method_name,
            args=args,
            kwargs=kwargs,
            num_return_vals=num_return_vals,
            dependency=self._actor._ray_actor_cursor)


class ActorClass(object):
    """An actor class.

    This is a decorated class. It can be used to create actors.

    Attributes:
        _modified_class: The original class that was decorated (with some
            additional methods added like __ray_terminate__).
        _class_id: The ID of this actor class.
        _class_name: The name of this class.
        _checkpoint_interval: The interval at which to checkpoint actor state.
        _num_cpus: The default number of CPUs required by the actor creation
            task.
        _num_gpus: The default number of GPUs required by the actor creation
            task.
        _resources: The default resources required by the actor creation task.
        _actor_method_cpus: The number of CPUs required by actor method tasks.
        _exported: True if the actor class has been exported and false
            otherwise.
        _actor_methods: The actor methods.
        _method_signatures: The signatures of the methods.
        _actor_method_names: The names of the actor methods.
        _actor_method_num_return_vals: The default number of return values for
            each actor method.
    """

    def __init__(self, modified_class, class_id, checkpoint_interval, num_cpus,
                 num_gpus, resources, actor_method_cpus):
        self._modified_class = modified_class
        self._class_id = class_id
        self._class_name = modified_class.__name__
        self._checkpoint_interval = checkpoint_interval
        self._num_cpus = num_cpus
        self._num_gpus = num_gpus
        self._resources = resources
        self._actor_method_cpus = actor_method_cpus
        self._exported = False

        # Get the actor methods of the given class.
        def pred(x):
            return (inspect.isfunction(x) or inspect.ismethod(x)
                    or is_cython(x))

        self._actor_methods = inspect.getmembers(
            self._modified_class, predicate=pred)
        # Extract the signatures of each of the methods. This will be used
        # to catch some errors if the methods are called with inappropriate
        # arguments.
        self._method_signatures = {}
        self._actor_method_num_return_vals = {}
        for method_name, method in self._actor_methods:
            # Print a warning message if the method signature is not
            # supported. We don't raise an exception because if the actor
            # inherits from a class that has a method whose signature we
            # don't support, there may not be much the user can do about it.
            signature.check_signature_supported(method, warn=True)
            self._method_signatures[method_name] = signature.extract_signature(
                method,
                ignore_first=not FunctionManager.is_classmethod(method))

            # Set the default number of return values for this method.
            if hasattr(method, "__ray_num_return_vals__"):
                self._actor_method_num_return_vals[method_name] = (
                    method.__ray_num_return_vals__)
            else:
                self._actor_method_num_return_vals[method_name] = (
                    DEFAULT_ACTOR_METHOD_NUM_RETURN_VALS)

        self._actor_method_names = [
            method_name for method_name, _ in self._actor_methods
        ]

    def __call__(self, *args, **kwargs):
        raise Exception("Actors methods cannot be instantiated directly. "
                        "Instead of running '{}()', try '{}.remote()'.".format(
                            self._class_name, self._class_name))

    def remote(self, *args, **kwargs):
        """Create an actor.

        Args:
            args: These arguments are forwarded directly to the actor
                constructor.
            kwargs: These arguments are forwarded directly to the actor
                constructor.

        Returns:
            A handle to the newly created actor.
        """
        return self._submit(args=args, kwargs=kwargs)

    def _submit(self,
                args,
                kwargs,
                num_cpus=None,
                num_gpus=None,
                resources=None):
        """Create an actor.

        This method allows more flexibility than the remote method because
        resource requirements can be specified and override the defaults in the
        decorator.

        Args:
            args: The arguments to forward to the actor constructor.
            kwargs: The keyword arguments to forward to the actor constructor.
            num_cpus: The number of CPUs required by the actor creation task.
            num_gpus: The number of GPUs required by the actor creation task.
            resources: The custom resources required by the actor creation
                task.

        Returns:
            A handle to the newly created actor.
        """
        worker = ray.worker.get_global_worker()
        if worker.mode is None:
            raise Exception("Actors cannot be created before ray.init() "
                            "has been called.")

        actor_id = ray.ObjectID(_random_string())
        # The actor cursor is a dummy object representing the most recent
        # actor method invocation. For each subsequent method invocation,
        # the current cursor should be added as a dependency, and then
        # updated to reflect the new invocation.
        actor_cursor = None

        # Do not export the actor class or the actor if run in LOCAL_MODE
        # Instead, instantiate the actor locally and add it to the worker's
        # dictionary
        if worker.mode == ray.LOCAL_MODE:
            worker.actors[actor_id] = self._modified_class.__new__(
                self._modified_class)
        else:
            # Export the actor.
            if not self._exported:
                worker.function_manager.export_actor_class(
                    self._class_id, self._modified_class,
                    self._actor_method_names, self._checkpoint_interval)
                self._exported = True

            resources = ray.utils.resources_from_resource_arguments(
                self._num_cpus, self._num_gpus, self._resources, num_cpus,
                num_gpus, resources)

            creation_args = [self._class_id]
            function_id = compute_actor_creation_function_id(self._class_id)
            [actor_cursor] = worker.submit_task(
                function_id,
                creation_args,
                actor_creation_id=actor_id,
                num_return_vals=1,
                resources=resources)

        # We initialize the actor counter at 1 to account for the actor
        # creation task.
        actor_counter = 1
        actor_handle = ActorHandle(
            actor_id, self._class_name, actor_cursor, actor_counter,
            self._actor_method_names, self._method_signatures,
            self._actor_method_num_return_vals, actor_cursor,
            self._actor_method_cpus, worker.task_driver_id)

        # Call __init__ as a remote function.
        if "__init__" in actor_handle._ray_actor_method_names:
            actor_handle.__init__.remote(*args, **kwargs)
        else:
            if len(args) != 0 or len(kwargs) != 0:
                raise Exception("Arguments cannot be passed to the actor "
                                "constructor because this actor class has no "
                                "__init__ method.")

        return actor_handle

    @property
    def class_id(self):
        return self._class_id


class ActorHandle(object):
    """A handle to an actor.

    The fields in this class are prefixed with _ray_ to hide them from the user
    and to avoid collision with actor method names.

    An ActorHandle can be created in three ways. First, by calling .remote() on
    an ActorClass. Second, by passing an actor handle into a task (forking the
    ActorHandle). Third, by directly serializing the ActorHandle (e.g., with
    cloudpickle).

    Attributes:
        _ray_actor_id: The ID of the corresponding actor.
        _ray_actor_handle_id: The ID of this handle. If this is the "original"
            handle for an actor (as opposed to one created by passing another
            handle into a task), then this ID must be NIL_ID. If this
            ActorHandle was created by forking an existing ActorHandle, then
            this ID must be computed deterministically via
            compute_actor_handle_id. If this ActorHandle was created by an
            out-of-band mechanism (e.g., pickling), then this must be None (in
            this case, a new actor handle ID will be generated on the fly every
            time a method is invoked).
        _ray_actor_cursor: The actor cursor is a dummy object representing the
            most recent actor method invocation. For each subsequent method
            invocation, the current cursor should be added as a dependency, and
            then updated to reflect the new invocation.
        _ray_actor_counter: The number of actor method invocations that we've
            called so far.
        _ray_actor_method_names: The names of the actor methods.
        _ray_method_signatures: The signatures of the actor methods.
        _ray_method_num_return_vals: The default number of return values for
            each method.
        _ray_class_name: The name of the actor class.
        _ray_actor_forks: The number of times this handle has been forked.
        _ray_actor_creation_dummy_object_id: The dummy object ID from the actor
            creation task.
        _ray_actor_method_cpus: The number of CPUs required by actor methods.
        _ray_original_handle: True if this is the original actor handle for a
            given actor. If this is true, then the actor will be destroyed when
            this handle goes out of scope.
        _ray_actor_driver_id: The driver ID of the job that created the actor
            (it is possible that this ActorHandle exists on a driver with a
            different driver ID).
        _ray_previous_actor_handle_id: If this actor handle is not an original
            handle, (e.g., it was created by forking or pickling), then
            this is the ID of the handle that this handle was created from.
            Otherwise, this is None.
    """

    def __init__(self,
                 actor_id,
                 class_name,
                 actor_cursor,
                 actor_counter,
                 actor_method_names,
                 method_signatures,
                 method_num_return_vals,
                 actor_creation_dummy_object_id,
                 actor_method_cpus,
                 actor_driver_id,
                 actor_handle_id=None,
                 previous_actor_handle_id=None):
        # False if this actor handle was created by forking or pickling. True
        # if it was created by the _serialization_helper function.
        self._ray_original_handle = previous_actor_handle_id is None

        self._ray_actor_id = actor_id
        if self._ray_original_handle:
            self._ray_actor_handle_id = ray.ObjectID(
                ray.worker.NIL_ACTOR_HANDLE_ID)
        else:
            self._ray_actor_handle_id = actor_handle_id
        self._ray_actor_cursor = actor_cursor
        self._ray_actor_counter = actor_counter
        self._ray_actor_method_names = actor_method_names
        self._ray_method_signatures = method_signatures
        self._ray_method_num_return_vals = method_num_return_vals
        self._ray_class_name = class_name
        self._ray_actor_forks = 0
        self._ray_actor_creation_dummy_object_id = (
            actor_creation_dummy_object_id)
        self._ray_actor_method_cpus = actor_method_cpus
        self._ray_actor_driver_id = actor_driver_id
        self._ray_previous_actor_handle_id = previous_actor_handle_id

    def _actor_method_call(self,
                           method_name,
                           args=None,
                           kwargs=None,
                           num_return_vals=None,
                           dependency=None):
        """Method execution stub for an actor handle.

        This is the function that executes when
        `actor.method_name.remote(*args, **kwargs)` is called. Instead of
        executing locally, the method is packaged as a task and scheduled
        to the remote actor instance.

        Args:
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
        worker = ray.worker.get_global_worker()

        worker.check_connected()

        function_signature = self._ray_method_signatures[method_name]
        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}
        args = signature.extend_args(function_signature, args, kwargs)

        # Execute functions locally if Ray is run in LOCAL_MODE
        # Copy args to prevent the function from mutating them.
        if worker.mode == ray.LOCAL_MODE:
            return getattr(worker.actors[self._ray_actor_id],
                           method_name)(*copy.deepcopy(args))

        # Add the execution dependency.
        if dependency is None:
            execution_dependencies = []
        else:
            execution_dependencies = [dependency]

        is_actor_checkpoint_method = (method_name == "__ray_checkpoint__")

        if self._ray_actor_handle_id is None:
            actor_handle_id = compute_actor_handle_id_non_forked(
                self._ray_actor_id, self._ray_previous_actor_handle_id,
                worker.current_task_id)
        else:
            actor_handle_id = self._ray_actor_handle_id

        function_id = FunctionManager.compute_actor_method_function_id(
            self._ray_class_name, method_name)
        object_ids = worker.submit_task(
            function_id,
            args,
            actor_id=self._ray_actor_id,
            actor_handle_id=actor_handle_id,
            actor_counter=self._ray_actor_counter,
            is_actor_checkpoint_method=is_actor_checkpoint_method,
            actor_creation_dummy_object_id=(
                self._ray_actor_creation_dummy_object_id),
            execution_dependencies=execution_dependencies,
            # We add one for the dummy return ID.
            num_return_vals=num_return_vals + 1,
            resources={"CPU": self._ray_actor_method_cpus},
            driver_id=self._ray_actor_driver_id)
        # Update the actor counter and cursor to reflect the most recent
        # invocation.
        self._ray_actor_counter += 1
        # The last object returned is the dummy object that should be
        # passed in to the next actor method. Do not return it to the user.
        self._ray_actor_cursor = object_ids.pop()

        if len(object_ids) == 1:
            object_ids = object_ids[0]
        elif len(object_ids) == 0:
            object_ids = None

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
                return ActorMethod(self, attr,
                                   self._ray_method_num_return_vals[attr])
        except AttributeError:
            pass

        # If the requested attribute is not a registered method, fall back
        # to default __getattribute__.
        return object.__getattribute__(self, attr)

    def __repr__(self):
        return "Actor({}, {})".format(self._ray_class_name,
                                      self._ray_actor_id.hex())

    def __del__(self):
        """Kill the worker that is running this actor."""
        # TODO(swang): Also clean up forked actor handles.
        # Kill the worker if this is the original actor handle, created
        # with Class.remote(). TODO(rkn): Even without passing handles around,
        # this is not the right policy. the actor should be alive as long as
        # there are ANY handles in scope in the process that created the actor,
        # not just the first one.
        worker = ray.worker.get_global_worker()
        if worker.connected and self._ray_original_handle:
            # TODO(rkn): Should we be passing in the actor cursor as a
            # dependency here?
            self.__ray_terminate__.remote()

    @property
    def _actor_id(self):
        return self._ray_actor_id

    @property
    def _actor_handle_id(self):
        return self._ray_actor_handle_id

    def _serialization_helper(self, ray_forking):
        """This is defined in order to make pickling work.

        Args:
            ray_forking: True if this is being called because Ray is forking
                the actor handle and false if it is being called by pickling.

        Returns:
            A dictionary of the information needed to reconstruct the object.
        """
        state = {
            "actor_id": self._ray_actor_id.id(),
            "class_name": self._ray_class_name,
            "actor_forks": self._ray_actor_forks,
            "actor_cursor": self._ray_actor_cursor.id()
            if self._ray_actor_cursor is not None else None,
            "actor_counter": 0,  # Reset the actor counter.
            "actor_method_names": self._ray_actor_method_names,
            "method_signatures": self._ray_method_signatures,
            "method_num_return_vals": self._ray_method_num_return_vals,
            "actor_creation_dummy_object_id": self.
            _ray_actor_creation_dummy_object_id.id()
            if self._ray_actor_creation_dummy_object_id is not None else None,
            "actor_method_cpus": self._ray_actor_method_cpus,
            "actor_driver_id": self._ray_actor_driver_id.id(),
            "previous_actor_handle_id": self._ray_actor_handle_id.id()
            if self._ray_actor_handle_id else None,
            "ray_forking": ray_forking
        }

        if ray_forking:
            self._ray_actor_forks += 1

        return state

    def _deserialization_helper(self, state, ray_forking):
        """This is defined in order to make pickling work.

        Args:
            state: The serialized state of the actor handle.
            ray_forking: True if this is being called because Ray is forking
                the actor handle and false if it is being called by pickling.
        """
        worker = ray.worker.get_global_worker()
        worker.check_connected()

        if state["ray_forking"]:
            actor_handle_id = compute_actor_handle_id(
                ray.ObjectID(state["previous_actor_handle_id"]),
                state["actor_forks"])
        else:
            actor_handle_id = None

        # This is the driver ID of the driver that owns the actor, not
        # necessarily the driver that owns this actor handle.
        actor_driver_id = ray.ObjectID(state["actor_driver_id"])

        self.__init__(
            ray.ObjectID(state["actor_id"]),
            state["class_name"],
            ray.ObjectID(state["actor_cursor"])
            if state["actor_cursor"] is not None else None,
            state["actor_counter"],
            state["actor_method_names"],
            state["method_signatures"],
            state["method_num_return_vals"],
            ray.ObjectID(state["actor_creation_dummy_object_id"])
            if state["actor_creation_dummy_object_id"] is not None else None,
            state["actor_method_cpus"],
            actor_driver_id,
            actor_handle_id=actor_handle_id,
            previous_actor_handle_id=ray.ObjectID(
                state["previous_actor_handle_id"]))

    def __getstate__(self):
        """This code path is used by pickling but not by Ray forking."""
        return self._serialization_helper(False)

    def __setstate__(self, state):
        """This code path is used by pickling but not by Ray forking."""
        return self._deserialization_helper(state, False)


def make_actor(cls, num_cpus, num_gpus, resources, actor_method_cpus,
               checkpoint_interval):
    if checkpoint_interval is None:
        checkpoint_interval = -1

    if checkpoint_interval == 0:
        raise Exception("checkpoint_interval must be greater than 0.")

    # Modify the class to have an additional method that will be used for
    # terminating the worker.
    class Class(cls):
        def __ray_terminate__(self):
            worker = ray.worker.get_global_worker()
            if worker.mode != ray.LOCAL_MODE:
                # Disconnect the worker from the local scheduler. The point of
                # this is so that when the worker kills itself below, the local
                # scheduler won't push an error message to the driver.
                worker.local_scheduler_client.disconnect()
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
            actor_id = ray.ObjectID(worker.actor_id)
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

    class_id = _random_string()

    return ActorClass(Class, class_id, checkpoint_interval, num_cpus, num_gpus,
                      resources, actor_method_cpus)


ray.worker.global_worker.make_actor = make_actor
