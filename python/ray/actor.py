from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import cloudpickle as pickle
import copy
import hashlib
import inspect
import json
import numpy as np
import traceback

import pyarrow.plasma as plasma
import ray.local_scheduler
import ray.signature as signature
import ray.worker
from ray.utils import (binary_to_hex, FunctionProperties, random_string,
                       release_gpus_in_use, select_local_scheduler)


def random_actor_id():
    return ray.local_scheduler.ObjectID(random_string())


def random_actor_class_id():
    return random_string()


def get_actor_method_function_id(attr):
    """Get the function ID corresponding to an actor method.

    Args:
        attr (str): The attribute name of the method.

    Returns:
        Function ID corresponding to the method.
    """
    function_id_hash = hashlib.sha1()
    function_id_hash.update(attr.encode("ascii"))
    function_id = function_id_hash.digest()
    assert len(function_id) == 20
    return ray.local_scheduler.ObjectID(function_id)


def get_checkpoint_indices(worker, actor_id):
    """Get the checkpoint indices associated with a given actor ID.

    Args:
        worker: The worker to use to get the checkpoint indices.
        actor_id: The actor ID of the actor to get the checkpoint indices for.

    Returns:
        The indices of existing checkpoints as a list of integers.
    """
    actor_key = b"Actor:" + actor_id
    checkpoint_indices = []
    for key in worker.redis_client.hkeys(actor_key):
        if key.startswith(b"checkpoint_"):
            index = int(key[len(b"checkpoint_"):])
            checkpoint_indices.append(index)
    return checkpoint_indices


def get_actor_checkpoint(worker, actor_id):
    """Get the most recent checkpoint associated with a given actor ID.

    Args:
        worker: The worker to use to get the checkpoint.
        actor_id: The actor ID of the actor to get the checkpoint for.

    Returns:
        If a checkpoint exists, this returns a tuple of the checkpoint index
            and the checkpoint. Otherwise it returns (-1, None). The checkpoint
            index is the actor counter of the last task that was executed on
            the actor before the checkpoint was made.
    """
    checkpoint_indices = get_checkpoint_indices(worker, actor_id)
    if len(checkpoint_indices) == 0:
        return -1, None
    else:
        actor_key = b"Actor:" + actor_id
        checkpoint_index = max(checkpoint_indices)
        checkpoint = worker.redis_client.hget(
            actor_key, "checkpoint_{}".format(checkpoint_index))
        return checkpoint_index, checkpoint


def put_dummy_object(worker, dummy_object_id):
    """Put a dummy actor object into the local object store.

    This registers a dummy object ID in the local store with an empty numpy
    array as the value. The resulting object is pinned to the store by storing
    it to the worker's state.

    For actors, dummy objects are used to store the stateful dependencies
    between consecutive method calls. This function should be called for every
    actor method execution that updates the actor's internal state.

    Args:
        worker: The worker to use to perform the put.
        dummy_object_id: The object ID of the dummy object.
    """
    # Add the dummy output for actor tasks. TODO(swang): We use
    # a numpy array as a hack to pin the object in the object
    # store. Once we allow object pinning in the store, we may
    # use `None`.
    dummy_object = np.zeros(1)
    worker.put_object(dummy_object_id, dummy_object)
    # Keep the dummy output in scope for the lifetime of the
    # actor, to prevent eviction from the object store.
    dummy_object = worker.get_object([dummy_object_id])
    dummy_object = dummy_object[0]
    worker.actor_pinned_objects[dummy_object_id] = dummy_object


def is_checkpoint_task(task_counter, checkpoint_interval):
    if checkpoint_interval <= 0:
        return False
    return (task_counter % checkpoint_interval == 0)


def make_actor_method_executor(worker, method_name, method):
    """Make an executor that wraps a user-defined actor method.

    The executor wraps the method to update the worker's internal state. If the
    task is a success, the dummy object returned is added to the object store,
    to signal that the following task can run, and the worker's task counter is
    updated to match the executed task. Else, the executor reports failure to
    the local scheduler so that the task counter does not get updated.

    Args:
        worker (Worker): The worker that is executing the actor.
        method_name (str): The name of the actor method.
        method (instancemethod): The actor method to wrap. This should be a
            method defined on the actor class and should therefore take an
            instance of the actor as the first argument.

    Returns:
        A function that executes the given actor method on the worker's stored
            instance of the actor. The function also updates the worker's
            internal state to record the executed method.
    """

    def actor_method_executor(dummy_return_id, task_counter, actor,
                              *args):
        # An actor task's dependency on the previous task is represented by
        # a dummy argument. Remove this argument before invocation.
        args = args[:-1]
        if method_name == "__ray_checkpoint__":
            # Execute the checkpoint task.
            actor_checkpoint_failed, error = method(actor, *args)
            # If the checkpoint was successfully loaded, put the dummy object
            # and update the actor's task counter, so that the task following
            # the checkpoint can run.
            if not actor_checkpoint_failed:
                put_dummy_object(worker, dummy_return_id)
                worker.actor_task_counter = task_counter + 1
                # Once the actor has resumed from a checkpoint, it counts as
                # loaded.
                worker.actor_loaded = True
            # Report to the local scheduler whether this task succeeded in
            # loading the checkpoint.
            worker.actor_checkpoint_failed = actor_checkpoint_failed
            # If there was an exception during the checkpoint method, re-raise
            # it after updating the actor's internal state.
            if error is not None:
                raise error
            return None
        else:
            # Update the worker's internal state before executing the method in
            # case the method throws an exception.
            put_dummy_object(worker, dummy_return_id)
            worker.actor_task_counter = task_counter + 1
            # Once the actor executes a task, it counts as loaded.
            worker.actor_loaded = True
            # Execute the actor method.
            return method(actor, *args)
    return actor_method_executor


def fetch_and_register_actor(actor_class_key, worker):
    """Import an actor.

    This will be called by the worker's import thread when the worker receives
    the actor_class export, assuming that the worker is an actor for that
    class.
    """
    actor_id_str = worker.actor_id
    (driver_id, class_id, class_name,
     module, pickled_class, checkpoint_interval,
     actor_method_names) = worker.redis_client.hmget(
         actor_class_key, ["driver_id", "class_id", "class_name", "module",
                           "class", "checkpoint_interval",
                           "actor_method_names"])

    actor_name = class_name.decode("ascii")
    module = module.decode("ascii")
    checkpoint_interval = int(checkpoint_interval)
    actor_method_names = json.loads(actor_method_names.decode("ascii"))

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
    for actor_method_name in actor_method_names:
        function_id = get_actor_method_function_id(actor_method_name).id()
        temporary_executor = make_actor_method_executor(worker,
                                                        actor_method_name,
                                                        temporary_actor_method)
        worker.functions[driver_id][function_id] = (actor_method_name,
                                                    temporary_executor)
        worker.function_properties[driver_id][function_id] = (
            FunctionProperties(num_return_vals=2,
                               num_cpus=1,
                               num_gpus=0,
                               num_custom_resource=0,
                               max_calls=0))
        worker.num_task_executions[driver_id][function_id] = 0

    try:
        unpickled_class = pickle.loads(pickled_class)
        worker.actor_class = unpickled_class
    except Exception:
        # If an exception was thrown when the actor was imported, we record the
        # traceback and notify the scheduler of the failure.
        traceback_str = ray.worker.format_error_message(traceback.format_exc())
        # Log the error message.
        worker.push_error_to_driver(driver_id, "register_actor", traceback_str,
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
                                                   inspect.ismethod(x))))
        for actor_method_name, actor_method in actor_methods:
            function_id = get_actor_method_function_id(actor_method_name).id()
            executor = make_actor_method_executor(worker, actor_method_name,
                                                  actor_method)
            worker.functions[driver_id][function_id] = (actor_method_name,
                                                        executor)
            # We do not set worker.function_properties[driver_id][function_id]
            # because we currently do need the actor worker to submit new tasks
            # for the actor.

        # Store some extra information that will be used when the actor exits
        # to release GPU resources.
        worker.driver_id = binary_to_hex(driver_id)
        local_scheduler_id = worker.redis_client.hget(
            b"Actor:" + actor_id_str, "local_scheduler_id")
        worker.local_scheduler_id = binary_to_hex(local_scheduler_id)


def export_actor_class(class_id, Class, actor_method_names,
                       checkpoint_interval, worker):
    if worker.mode is None:
        raise Exception("Actors cannot be created before Ray has been "
                        "started. You can start Ray with 'ray.init()'.")
    key = b"ActorClass:" + class_id
    d = {"driver_id": worker.task_driver_id.id(),
         "class_name": Class.__name__,
         "module": Class.__module__,
         "class": pickle.dumps(Class),
         "checkpoint_interval": checkpoint_interval,
         "actor_method_names": json.dumps(list(actor_method_names))}
    worker.redis_client.hmset(key, d)
    worker.redis_client.rpush("Exports", key)


def export_actor(actor_id, class_id, actor_method_names, num_cpus, num_gpus,
                 worker):
    """Export an actor to redis.

    Args:
        actor_id: The ID of the actor.
        actor_method_names (list): A list of the names of this actor's methods.
        num_cpus (int): The number of CPUs that this actor requires.
        num_gpus (int): The number of GPUs that this actor requires.
    """
    ray.worker.check_main_thread()
    if worker.mode is None:
        raise Exception("Actors cannot be created before Ray has been "
                        "started. You can start Ray with 'ray.init()'.")
    key = b"Actor:" + actor_id.id()

    # For now, all actor methods have 1 return value.
    driver_id = worker.task_driver_id.id()
    for actor_method_name in actor_method_names:
        # TODO(rkn): When we create a second actor, we are probably overwriting
        # the values from the first actor here. This may or may not be a
        # problem.
        function_id = get_actor_method_function_id(actor_method_name).id()
        worker.function_properties[driver_id][function_id] = (
            FunctionProperties(num_return_vals=2,
                               num_cpus=1,
                               num_gpus=0,
                               num_custom_resource=0,
                               max_calls=0))

    # Select a local scheduler for the actor.
    local_scheduler_id = select_local_scheduler(
        worker.task_driver_id.id(), ray.global_state.local_schedulers(),
        num_gpus, worker.redis_client)
    assert local_scheduler_id is not None

    # We must put the actor information in Redis before publishing the actor
    # notification so that when the newly created actor attempts to fetch the
    # information from Redis, it is already there.
    worker.redis_client.hmset(key, {"class_id": class_id,
                                    "driver_id": driver_id,
                                    "local_scheduler_id": local_scheduler_id,
                                    "num_gpus": num_gpus,
                                    "removed": False})

    # TODO(rkn): There is actually no guarantee that the local scheduler that
    # we are publishing to has already subscribed to the actor_notifications
    # channel. Therefore, this message may be missed and the workload will
    # hang. This is a bug.
    ray.utils.publish_actor_creation(actor_id.id(), driver_id,
                                     local_scheduler_id, False,
                                     worker.redis_client)


def make_actor(cls, num_cpus, num_gpus, checkpoint_interval):
    # Add one to the checkpoint interval since we will insert a mock task for
    # every checkpoint.
    checkpoint_interval += 1

    # Modify the class to have an additional method that will be used for
    # terminating the worker.
    class Class(cls):
        def __ray_terminate__(self, actor_id):
            # Record that this actor has been removed so that if this node
            # dies later, the actor won't be recreated. Alternatively, we could
            # remove the actor key from Redis here.
            ray.worker.global_worker.redis_client.hset(b"Actor:" + actor_id,
                                                       "removed", True)
            # Release the GPUs that this worker was using.
            if len(ray.get_gpu_ids()) > 0:
                release_gpus_in_use(
                    ray.worker.global_worker.driver_id,
                    ray.worker.global_worker.local_scheduler_id,
                    ray.get_gpu_ids(),
                    ray.worker.global_worker.redis_client)
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

        def __ray_checkpoint__(self, task_counter, previous_object_id):
            """Save or resume a stored checkpoint.

            This task checkpoints the current state of the actor. If the actor
            has not yet executed to `task_counter`, then the task instead
            attempts to resume from a saved checkpoint that matches
            `task_counter`. If the most recently saved checkpoint is earlier
            than `task_counter`, the task requests reconstruction of the tasks
            that executed since the previous checkpoint and before
            `task_counter`.

            Args:
                self: An instance of the actor class.
                task_counter: The index assigned to this checkpoint method.
                previous_object_id: The dummy object returned by the task that
                    immediately precedes this checkpoint.

            Returns:
                A bool representing whether the checkpoint was successfully
                    loaded (whether the actor can safely execute the next task)
                    and an Exception instance, if one was thrown.
            """
            worker = ray.worker.global_worker
            previous_object_id = previous_object_id[0]
            plasma_id = plasma.ObjectID(previous_object_id.id())

            # Initialize the return values. `actor_checkpoint_failed` will be
            # set to True if we fail to load the checkpoint. `error` will be
            # set to the Exception, if one is thrown.
            actor_checkpoint_failed = False
            error_to_return = None

            # Save or resume the checkpoint.
            if worker.actor_loaded:
                # The actor has loaded, so we are running the normal execution.
                # Save the checkpoint.
                print("Saving actor checkpoint. actor_counter = {}."
                      .format(task_counter))
                actor_key = b"Actor:" + worker.actor_id

                try:
                    checkpoint = worker.actors[
                        worker.actor_id].__ray_save_checkpoint__()
                    # Save the checkpoint in Redis. TODO(rkn): Checkpoints
                    # should not be stored in Redis. Fix this.
                    worker.redis_client.hset(
                        actor_key,
                        "checkpoint_{}".format(task_counter),
                        checkpoint)
                    # Remove the previous checkpoints if there is one.
                    checkpoint_indices = get_checkpoint_indices(
                        worker, worker.actor_id)
                    for index in checkpoint_indices:
                        if index < task_counter:
                            worker.redis_client.hdel(
                                actor_key, "checkpoint_{}".format(index))
                # An exception was thrown. Save the error.
                except Exception as error:
                    # Checkpoint saves should not block execution on the actor,
                    # so we still consider the task successful.
                    error_to_return = error
            else:
                # The actor has not yet loaded. Try loading it from the most
                # recent checkpoint.
                checkpoint_index, checkpoint = get_actor_checkpoint(
                    worker, worker.actor_id)
                if checkpoint_index == task_counter:
                    # The checkpoint matches ours. Resume the actor instance.
                    try:
                        actor = (worker.actor_class.
                                 __ray_restore_from_checkpoint__(checkpoint))
                        worker.actors[worker.actor_id] = actor
                    # An exception was thrown. Save the error.
                    except Exception as error:
                        # We could not resume the checkpoint, so count the task
                        # as failed.
                        actor_checkpoint_failed = True
                        error_to_return = error
                else:
                    # We cannot resume a mismatching checkpoint, so count the
                    # task as failed.
                    actor_checkpoint_failed = True

            # Fall back to lineage reconstruction if we were unable to load the
            # checkpoint.
            if actor_checkpoint_failed:
                worker.local_scheduler_client.reconstruct_object(
                    plasma_id.binary())
                worker.local_scheduler_client.notify_unblocked()

            return actor_checkpoint_failed, error_to_return

    Class.__module__ = cls.__module__
    Class.__name__ = cls.__name__

    class_id = random_actor_class_id()
    # The list exported will have length 0 if the class has not been exported
    # yet, and length one if it has. This is just implementing a bool, but we
    # don't use a bool because we need to modify it inside of the ActorHandle
    # constructor.
    exported = []

    # Create objects to wrap method invocations. This is done so that we can
    # invoke methods with actor.method.remote() instead of actor.method().
    class ActorMethod(object):
        def __init__(self, actor, method_name):
            self.actor = actor
            self.method_name = method_name

        def __call__(self, *args, **kwargs):
            raise Exception("Actor methods cannot be called directly. Instead "
                            "of running 'object.{}()', try "
                            "'object.{}.remote()'."
                            .format(self.method_name, self.method_name))

        def remote(self, *args, **kwargs):
            return self.actor._actor_method_call(
                self.method_name, args=args, kwargs=kwargs,
                dependency=self.actor._ray_actor_cursor)

    # Checkpoint methods do not take in the state of the previous actor method
    # as an explicit data dependency.
    class CheckpointMethod(ActorMethod):
        def remote(self):
            # A checkpoint's arguments are the current task counter and the
            # object ID of the preceding task. The latter is an implicit data
            # dependency, since the checkpoint method can run at any time.
            args = [self.actor._ray_actor_counter,
                    [self.actor._ray_actor_cursor]]
            return self.actor._actor_method_call(self.method_name, args=args)

    class ActorHandle(object):
        def __init__(self, *args, **kwargs):
            raise Exception("Actor classes cannot be instantiated directly. "
                            "Instead of running '{}()', try '{}.remote()'."
                            .format(Class.__name__, Class.__name__))

        @classmethod
        def remote(cls, *args, **kwargs):
            actor_object = cls.__new__(cls)
            actor_object._manual_init(*args, **kwargs)
            return actor_object

        def _manual_init(self, *args, **kwargs):
            self._ray_actor_id = random_actor_id()
            # The number of actor method invocations that we've called so far.
            self._ray_actor_counter = 0
            # The actor cursor is a dummy object representing the most recent
            # actor method invocation. For each subsequent method invocation,
            # the current cursor should be added as a dependency, and then
            # updated to reflect the new invocation.
            self._ray_actor_cursor = None
            ray_actor_methods = inspect.getmembers(
                Class, predicate=(lambda x: (inspect.isfunction(x) or
                                             inspect.ismethod(x))))
            self._ray_actor_methods = {}
            for actor_method_name, actor_method in ray_actor_methods:
                self._ray_actor_methods[actor_method_name] = actor_method
            # Extract the signatures of each of the methods. This will be used
            # to catch some errors if the methods are called with inappropriate
            # arguments.
            self._ray_method_signatures = dict()
            for k, v in self._ray_actor_methods.items():
                # Print a warning message if the method signature is not
                # supported. We don't raise an exception because if the actor
                # inherits from a class that has a method whose signature we
                # don't support, we there may not be much the user can do about
                # it.
                signature.check_signature_supported(v, warn=True)
                self._ray_method_signatures[k] = signature.extract_signature(
                    v, ignore_first=True)

            # Do not export the actor class or the actor if run in PYTHON_MODE
            # Instead, instantiate the actor locally and add it to
            # global_worker's dictionary
            if ray.worker.global_worker.mode == ray.PYTHON_MODE:
                ray.worker.global_worker.actors[self._ray_actor_id] = (
                    Class.__new__(Class))
            else:
                # Export the actor class if it has not been exported yet.
                if len(exported) == 0:
                    export_actor_class(class_id, Class,
                                       self._ray_actor_methods.keys(),
                                       checkpoint_interval,
                                       ray.worker.global_worker)
                    exported.append(0)
                # Export the actor.
                export_actor(self._ray_actor_id, class_id,
                             self._ray_actor_methods.keys(), num_cpus,
                             num_gpus, ray.worker.global_worker)

            # Call __init__ as a remote function.
            if "__init__" in self._ray_actor_methods.keys():
                self._actor_method_call("__init__", args=args, kwargs=kwargs)
            else:
                print("WARNING: this object has no __init__ method.")

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

            # Add the dummy argument that represents dependency on a preceding
            # task.
            args.append(dependency)

            actor_counter = self._ray_actor_counter
            # Mark checkpoint methods with a negative task counter.
            if is_checkpoint_task(actor_counter, checkpoint_interval):
                actor_counter = self._ray_actor_counter * -1

            function_id = get_actor_method_function_id(method_name)
            object_ids = ray.worker.global_worker.submit_task(
                function_id, args, actor_id=self._ray_actor_id,
                actor_counter=actor_counter)
            # Update the actor counter and cursor to reflect the most recent
            # invocation.
            self._ray_actor_counter += 1
            self._ray_actor_cursor = object_ids.pop()

            # Submit a checkpoint task if necessary.
            if is_checkpoint_task(self._ray_actor_counter,
                                  checkpoint_interval):
                self.__ray_checkpoint__.remote()

            # The last object returned is the dummy object that should be
            # passed in to the next actor method. Do not return it to the user.
            if len(object_ids) == 1:
                return object_ids[0]
            elif len(object_ids) > 1:
                return object_ids

        # Make tab completion work.
        def __dir__(self):
            return self._ray_actor_methods

        def __getattribute__(self, attr):
            # The following is needed so we can still access
            # self.actor_methods.
            if attr in ["_manual_init", "_ray_actor_id", "_ray_actor_counter",
                        "_ray_actor_cursor", "_ray_actor_methods",
                        "_actor_method_invokers", "_ray_method_signatures",
                        "_actor_method_call"]:
                return object.__getattribute__(self, attr)
            if attr in self._ray_actor_methods.keys():
                # We create the ActorMethod on the fly here so that the
                # ActorHandle doesn't need a reference to the ActorMethod. The
                # ActorMethod has a reference to the ActorHandle and this was
                # causing cyclic references which were prevent object
                # deallocation from behaving in a predictable manner.
                if attr == "__ray_checkpoint__":
                    actor_method_cls = CheckpointMethod
                else:
                    actor_method_cls = ActorMethod
                return actor_method_cls(self, attr)
            else:
                # There is no method with this name, so raise an exception.
                raise AttributeError("'{}' Actor object has no attribute '{}'"
                                     .format(Class, attr))

        def __repr__(self):
            return "Actor(" + self._ray_actor_id.hex() + ")"

        def __reduce__(self):
            raise Exception("Actor objects cannot be pickled.")

        def __del__(self):
            """Kill the worker that is running this actor."""
            if ray.worker.global_worker.connected:
                self._actor_method_call("__ray_terminate__",
                                        args=[self._ray_actor_id.id()])

    return ActorHandle


ray.worker.global_worker.fetch_and_register_actor = fetch_and_register_actor
ray.worker.global_worker.make_actor = make_actor
