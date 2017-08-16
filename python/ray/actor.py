from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import cloudpickle as pickle
import hashlib
import inspect
import json
import time
import traceback

import ray.local_scheduler
import ray.signature as signature
import ray.worker
from ray.utils import (FunctionProperties, hex_to_binary, random_string,
                       select_local_scheduler)


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


def get_actor_checkpoint(actor_id, worker):
    """Get the most recent checkpoint associated with a given actor ID.

    Args:
        actor_id: The actor ID of the actor to get the checkpoint for.
        worker: The worker to use to get the checkpoint.

    Returns:
        If a checkpoint exists, this returns a tuple of the checkpoint index
            and the checkpoint. Otherwise it returns (-1, None). The checkpoint
            index is the actor counter of the last task that was executed on
            the actor before the checkpoint was made.
    """
    # Get all of the keys associated with checkpoints for this actor.
    actor_key = b"Actor:" + actor_id
    checkpoint_indices = [int(key[len(b"checkpoint_"):])
                          for key in worker.redis_client.hkeys(actor_key)
                          if key.startswith(b"checkpoint_")]
    if len(checkpoint_indices) == 0:
        return -1, None
    most_recent_checkpoint_index = max(checkpoint_indices)
    # Get the most recent checkpoint.
    checkpoint = worker.redis_client.hget(
        actor_key, "checkpoint_{}".format(most_recent_checkpoint_index))
    return most_recent_checkpoint_index, checkpoint


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
        worker.functions[driver_id][function_id] = (actor_method_name,
                                                    temporary_actor_method)
        worker.function_properties[driver_id][function_id] = (
            FunctionProperties(num_return_vals=1,
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
    else:
        # TODO(pcm): Why is the below line necessary?
        unpickled_class.__module__ = module
        worker.actors[actor_id_str] = unpickled_class.__new__(unpickled_class)
        for (k, v) in inspect.getmembers(
            unpickled_class, predicate=(lambda x: (inspect.isfunction(x) or
                                                   inspect.ismethod(x)))):
            function_id = get_actor_method_function_id(k).id()
            worker.functions[driver_id][function_id] = (k, v)
            # We do not set worker.function_properties[driver_id][function_id]
            # because we currently do need the actor worker to submit new tasks
            # for the actor.


def export_actor_class(class_id, Class, actor_method_names,
                       checkpoint_interval, worker):
    if worker.mode is None:
        raise NotImplemented("TODO(pcm): Cache actors")
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
            FunctionProperties(num_return_vals=1,
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


def reconstruct_actor_state(actor_id, worker):
    """Reconstruct the state of an actor that is being reconstructed.

    Args:
        actor_id: The ID of the actor being reconstructed.
        worker: The worker object that is running the actor.
    """
    # Get the most recent actor checkpoint.
    checkpoint_index, checkpoint = get_actor_checkpoint(actor_id, worker)
    if checkpoint is not None:
        print("Loading actor state from checkpoint {}"
              .format(checkpoint_index))
        # Wait for the actor to have been defined.
        while not hasattr(worker, "actor_class"):
            time.sleep(0.001)
        # TODO(rkn): Restoring from the checkpoint may fail, so this should be
        # in a try-except block and we should give a good error message.
        worker.actors[actor_id] = (
            worker.actor_class.__ray_restore_from_checkpoint__(checkpoint))

    # TODO(rkn): This call is expensive. It'd be nice to find a way to get only
    # the tasks that are relevant to this actor.
    tasks = ray.global_state.task_table()

    def hex_to_object_id(hex_id):
        return ray.local_scheduler.ObjectID(hex_to_binary(hex_id))

    relevant_tasks = []

    # Loop over the task table and keep the tasks that are relevant to this
    # actor.
    for _, task_info in tasks.items():
        task_spec_info = task_info["TaskSpec"]
        if hex_to_binary(task_spec_info["ActorID"]) == actor_id:
            relevant_tasks.append(task_spec_info)

    # Sort the tasks by actor ID.
    relevant_tasks.sort(key=lambda task: task["ActorCounter"])
    for i in range(len(relevant_tasks)):
        assert relevant_tasks[i]["ActorCounter"] == i

    # This is a mini replica of the worker's main_loop. This will loop over all
    # of the tasks that this actor is supposed to rerun. For each task, the
    # worker will submit the task to the local scheduler, retrieve the task
    # from the local scheduler, and execute the task.
    for task_spec_info in relevant_tasks:
        # Create a task spec out of the dictionary of info. This isn't
        # necessary. It is strictly for the purposes of checking that the task
        # we get back from the local scheduler is identical to the one we
        # submit.
        task_spec = ray.local_scheduler.Task(
            hex_to_object_id(task_spec_info["DriverID"]),
            hex_to_object_id(task_spec_info["FunctionID"]),
            task_spec_info["Args"],
            len(task_spec_info["ReturnObjectIDs"]),
            hex_to_object_id(task_spec_info["ParentTaskID"]),
            task_spec_info["ParentCounter"],
            hex_to_object_id(task_spec_info["ActorID"]),
            task_spec_info["ActorCounter"],
            [task_spec_info["RequiredResources"]["CPUs"],
             task_spec_info["RequiredResources"]["GPUs"],
             task_spec_info["RequiredResources"]["CustomResource"]])

        # Verify that the return object IDs are the same as they were the
        # first time.
        assert task_spec_info["ReturnObjectIDs"] == task_spec.returns()

        # We need to wait for the actor to be imported and for the functions to
        # be defined before we can submit the task.
        worker._wait_for_function(hex_to_binary(task_spec_info["FunctionID"]),
                                  hex_to_binary(task_spec_info["DriverID"]))

        # Set some additional state. During normal operation
        # (non-reconstruction) this state would already be set because tasks
        # are only submitted from drivers or from workers that are in the
        # middle of executing other tasks.
        worker.task_driver_id = ray.local_scheduler.ObjectID(
            hex_to_binary(task_spec_info["DriverID"]))
        worker.current_task_id = ray.local_scheduler.ObjectID(
            hex_to_binary(task_spec_info["ParentTaskID"]))
        worker.task_index = task_spec_info["ParentCounter"]

        # Submit the task to the local scheduler. This is important so that the
        # local scheduler does bookkeeping about this actor's resource
        # utilization and things like that. It's also important for updating
        # some state on the worker.
        if task_spec_info["ActorCounter"] > checkpoint_index:
            worker.submit_task(
                hex_to_object_id(task_spec_info["FunctionID"]),
                task_spec_info["Args"],
                actor_id=hex_to_object_id(task_spec_info["ActorID"]))
        else:
            # Pass in a dummy task with no arguments to avoid having to
            # unnecessarily reconstruct past arguments.
            worker.submit_task(
                hex_to_object_id(task_spec_info["FunctionID"]),
                [],
                actor_id=hex_to_object_id(task_spec_info["ActorID"]))

        # Clear the extra state that we set.
        del worker.task_driver_id
        del worker.current_task_id
        del worker.task_index

        # Get the task from the local scheduler.
        retrieved_task = worker._get_next_task_from_local_scheduler()

        # If the task happened before the most recent checkpoint, ignore it.
        # Otherwise, execute it.
        if retrieved_task.actor_counter() > checkpoint_index:
            # Assert that the retrieved task is the same as the constructed
            # task.
            assert (ray.local_scheduler.task_to_string(task_spec) ==
                    ray.local_scheduler.task_to_string(retrieved_task))
            # Wait for the task to be ready and then execute it.
            worker._wait_for_and_process_task(retrieved_task)

    # Enter the main loop to receive and process tasks.
    worker.main_loop()


def make_actor(cls, num_cpus, num_gpus, checkpoint_interval):
    # Modify the class to have an additional method that will be used for
    # terminating the worker.
    class Class(cls):
        def __ray_terminate__(self, actor_id):
            # Record that this actor has been removed so that if this node
            # dies later, the actor won't be recreated. Alternatively, we could
            # remove the actor key from Redis here.
            ray.worker.global_worker.redis_client.hset(b"Actor:" + actor_id,
                                                       "removed", True)
            # Disconnect the worker from he local scheduler. The point of this
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
                actor_object = pickle.loads(checkpoint)
            return actor_object

    Class.__module__ = cls.__module__
    Class.__name__ = cls.__name__

    class_id = random_actor_class_id()
    # The list exported will have length 0 if the class has not been exported
    # yet, and length one if it has. This is just implementing a bool, but we
    # don't use a bool because we need to modify it inside of the NewClass
    # constructor.
    exported = []

    # The function actor_method_call gets called if somebody tries to call a
    # method on their local actor stub object.
    def actor_method_call(actor_id, attr, function_signature, *args, **kwargs):
        ray.worker.check_connected()
        ray.worker.check_main_thread()
        args = signature.extend_args(function_signature, args, kwargs)

        function_id = get_actor_method_function_id(attr)
        object_ids = ray.worker.global_worker.submit_task(function_id, args,
                                                          actor_id=actor_id)
        if len(object_ids) == 1:
            return object_ids[0]
        elif len(object_ids) > 1:
            return object_ids

    class ActorMethod(object):
        def __init__(self, method_name, actor_id, method_signature):
            self.method_name = method_name
            self.actor_id = actor_id
            self.method_signature = method_signature

        def __call__(self, *args, **kwargs):
            raise Exception("Actor methods cannot be called directly. Instead "
                            "of running 'object.{}()', try "
                            "'object.{}.remote()'."
                            .format(self.method_name, self.method_name))

        def remote(self, *args, **kwargs):
            return actor_method_call(self.actor_id, self.method_name,
                                     self.method_signature, *args, **kwargs)

    class NewClass(object):
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
            self._ray_actor_methods = {
                k: v for (k, v) in inspect.getmembers(
                    Class, predicate=(lambda x: (inspect.isfunction(x) or
                                                 inspect.ismethod(x))))}
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

            # Create objects to wrap method invocations. This is done so that
            # we can invoke methods with actor.method.remote() instead of
            # actor.method().
            self._actor_method_invokers = dict()
            for k, v in self._ray_actor_methods.items():
                self._actor_method_invokers[k] = ActorMethod(
                    k, self._ray_actor_id, self._ray_method_signatures[k])

            # Export the actor class if it has not been exported yet.
            if len(exported) == 0:
                export_actor_class(class_id, Class,
                                   self._ray_actor_methods.keys(),
                                   checkpoint_interval,
                                   ray.worker.global_worker)
                exported.append(0)
            # Export the actor.
            export_actor(self._ray_actor_id, class_id,
                         self._ray_actor_methods.keys(), num_cpus, num_gpus,
                         ray.worker.global_worker)
            # Call __init__ as a remote function.
            if "__init__" in self._ray_actor_methods.keys():
                actor_method_call(self._ray_actor_id, "__init__",
                                  self._ray_method_signatures["__init__"],
                                  *args, **kwargs)
            else:
                print("WARNING: this object has no __init__ method.")

        # Make tab completion work.
        def __dir__(self):
            return self._ray_actor_methods

        def __getattribute__(self, attr):
            # The following is needed so we can still access
            # self.actor_methods.
            if attr in ["_manual_init", "_ray_actor_id", "_ray_actor_methods",
                        "_actor_method_invokers", "_ray_method_signatures"]:
                return object.__getattribute__(self, attr)
            if attr in self._ray_actor_methods.keys():
                return self._actor_method_invokers[attr]
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
                actor_method_call(
                    self._ray_actor_id, "__ray_terminate__",
                    self._ray_method_signatures["__ray_terminate__"],
                    self._ray_actor_id.id())

    return NewClass


ray.worker.global_worker.fetch_and_register_actor = fetch_and_register_actor
ray.worker.global_worker.make_actor = make_actor
