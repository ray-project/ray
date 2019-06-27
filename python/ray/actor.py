from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import hashlib
import inspect
import logging
import six
import sys
import threading

from abc import ABCMeta, abstractmethod
from collections import namedtuple

from ray.function_manager import FunctionDescriptor
import ray.ray_constants as ray_constants
import ray.signature as signature
import ray.worker
from ray import (ObjectID, ActorID, ActorHandleID, ActorClassID, TaskID)

logger = logging.getLogger(__name__)


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
    assert isinstance(actor_handle_id, ActorHandleID)
    handle_id_hash = hashlib.sha1()
    handle_id_hash.update(actor_handle_id.binary())
    handle_id_hash.update(str(num_forks).encode("ascii"))
    handle_id = handle_id_hash.digest()
    return ActorHandleID(handle_id)


def compute_actor_handle_id_non_forked(actor_handle_id, current_task_id):
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
        actor_handle_id: The original actor handle ID.
        current_task_id: The ID of the task that is unpickling the handle.

    Returns:
        An ID for the new actor handle.
    """
    assert isinstance(actor_handle_id, ActorHandleID)
    assert isinstance(current_task_id, TaskID)
    handle_id_hash = hashlib.sha1()
    handle_id_hash.update(actor_handle_id.binary())
    handle_id_hash.update(current_task_id.binary())
    handle_id = handle_id_hash.digest()
    return ActorHandleID(handle_id)


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
    """A class used to invoke an actor method.

    Note: This class is instantiated only while the actor method is being
    invoked (so that it doesn't keep a reference to the actor handle and
    prevent it from going out of scope).

    Attributes:
        _actor: A handle to the actor.
        _method_name: The name of the actor method.
        _num_return_vals: The default number of return values that the method
            invocation should return.
        _decorator: An optional decorator that should be applied to the actor
            method invocation (as opposed to the actor method execution) before
            invoking the method. The decorator must return a function that
            takes in two arguments ("args" and "kwargs"). In most cases, it
            should call the function that was passed into the decorator and
            return the resulting ObjectIDs. For an example, see
            "test_decorated_method" in "python/ray/tests/test_actor.py".
    """

    def __init__(self, actor, method_name, num_return_vals, decorator=None):
        self._actor = actor
        self._method_name = method_name
        self._num_return_vals = num_return_vals
        # This is a decorator that is used to wrap the function invocation (as
        # opposed to the function execution). The decorator must return a
        # function that takes in two arguments ("args" and "kwargs"). In most
        # cases, it should call the function that was passed into the decorator
        # and return the resulting ObjectIDs.
        self._decorator = decorator

    def __call__(self, *args, **kwargs):
        raise Exception("Actor methods cannot be called directly. Instead "
                        "of running 'object.{}()', try "
                        "'object.{}.remote()'.".format(self._method_name,
                                                       self._method_name))

    def remote(self, *args, **kwargs):
        return self._remote(args, kwargs)

    def _remote(self, args=None, kwargs=None, num_return_vals=None):
        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}
        if num_return_vals is None:
            num_return_vals = self._num_return_vals

        def invocation(args, kwargs):
            return self._actor._actor_method_call(
                self._method_name,
                args=args,
                kwargs=kwargs,
                num_return_vals=num_return_vals)

        # Apply the decorator if there is one.
        if self._decorator is not None:
            invocation = self._decorator(invocation)

        return invocation(args, kwargs)


class ActorClass(object):
    """An actor class.

    This is a decorated class. It can be used to create actors.

    Attributes:
        _modified_class: The original class that was decorated (with some
            additional methods added like __ray_terminate__).
        _class_id: The ID of this actor class.
        _class_name: The name of this class.
        _num_cpus: The default number of CPUs required by the actor creation
            task.
        _num_gpus: The default number of GPUs required by the actor creation
            task.
        _resources: The default resources required by the actor creation task.
        _actor_method_cpus: The number of CPUs required by actor method tasks.
        _last_job_id_exported_for: The ID of the job of the last Ray
            session during which this actor class definition was exported. This
            is an imperfect mechanism used to determine if we need to export
            the remote function again. It is imperfect in the sense that the
            actor class definition could be exported multiple times by
            different workers.
        _actor_methods: The actor methods.
        _method_decorators: Optional decorators that should be applied to the
            method invocation function before invoking the actor methods. These
            can be set by attaching the attribute
            "__ray_invocation_decorator__" to the actor method.
        _method_signatures: The signatures of the methods.
        _actor_method_names: The names of the actor methods.
        _actor_method_num_return_vals: The default number of return values for
            each actor method.
    """

    def __init__(self, modified_class, class_id, max_reconstructions, num_cpus,
                 num_gpus, resources):
        self._modified_class = modified_class
        self._class_id = class_id
        self._class_name = modified_class.__name__
        self._max_reconstructions = max_reconstructions
        self._num_cpus = num_cpus
        self._num_gpus = num_gpus
        self._resources = resources
        self._last_job_id_exported_for = None

        self._actor_methods = inspect.getmembers(
            self._modified_class, ray.utils.is_function_or_method)
        self._actor_method_names = [
            method_name for method_name, _ in self._actor_methods
        ]

        constructor_name = "__init__"
        if constructor_name not in self._actor_method_names:
            # Add __init__ if it does not exist.
            # Actor creation will be executed with __init__ together.

            # Assign an __init__ function will avoid many checks later on.
            def __init__(self):
                pass

            self._modified_class.__init__ = __init__
            self._actor_method_names.append(constructor_name)
            self._actor_methods.append((constructor_name, __init__))

        # Extract the signatures of each of the methods. This will be used
        # to catch some errors if the methods are called with inappropriate
        # arguments.
        self._method_decorators = {}
        self._method_signatures = {}
        self._actor_method_num_return_vals = {}
        for method_name, method in self._actor_methods:
            # Print a warning message if the method signature is not
            # supported. We don't raise an exception because if the actor
            # inherits from a class that has a method whose signature we
            # don't support, there may not be much the user can do about it.
            signature.check_signature_supported(method, warn=True)
            self._method_signatures[method_name] = signature.extract_signature(
                method, ignore_first=not ray.utils.is_class_method(method))
            # Set the default number of return values for this method.
            if hasattr(method, "__ray_num_return_vals__"):
                self._actor_method_num_return_vals[method_name] = (
                    method.__ray_num_return_vals__)
            else:
                self._actor_method_num_return_vals[method_name] = (
                    ray_constants.DEFAULT_ACTOR_METHOD_NUM_RETURN_VALS)

            if hasattr(method, "__ray_invocation_decorator__"):
                self._method_decorators[method_name] = (
                    method.__ray_invocation_decorator__)

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
        return self._remote(args=args, kwargs=kwargs)

    def _remote(self,
                args=None,
                kwargs=None,
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
        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}

        worker = ray.worker.get_global_worker()
        if worker.mode is None:
            raise Exception("Actors cannot be created before ray.init() "
                            "has been called.")

        actor_id = ActorID.from_random()
        # The actor cursor is a dummy object representing the most recent
        # actor method invocation. For each subsequent method invocation,
        # the current cursor should be added as a dependency, and then
        # updated to reflect the new invocation.
        actor_cursor = None

        # Set the actor's default resources if not already set. First three
        # conditions are to check that no resources were specified in the
        # decorator. Last three conditions are to check that no resources were
        # specified when _remote() was called.
        if (self._num_cpus is None and self._num_gpus is None
                and self._resources is None and num_cpus is None
                and num_gpus is None and resources is None):
            # In the default case, actors acquire no resources for
            # their lifetime, and actor methods will require 1 CPU.
            cpus_to_use = ray_constants.DEFAULT_ACTOR_CREATION_CPU_SIMPLE
            actor_method_cpu = ray_constants.DEFAULT_ACTOR_METHOD_CPU_SIMPLE
        else:
            # If any resources are specified (here or in decorator), then
            # all resources are acquired for the actor's lifetime and no
            # resources are associated with methods.
            cpus_to_use = (ray_constants.DEFAULT_ACTOR_CREATION_CPU_SPECIFIED
                           if self._num_cpus is None else self._num_cpus)
            actor_method_cpu = ray_constants.DEFAULT_ACTOR_METHOD_CPU_SPECIFIED

        # Do not export the actor class or the actor if run in LOCAL_MODE
        # Instead, instantiate the actor locally and add it to the worker's
        # dictionary
        if worker.mode == ray.LOCAL_MODE:
            worker.actors[actor_id] = self._modified_class(
                *copy.deepcopy(args), **copy.deepcopy(kwargs))
        else:
            # Export the actor.
            if (self._last_job_id_exported_for is None or
                    self._last_job_id_exported_for != worker.current_job_id):
                # If this actor class was exported in a previous session, we
                # need to export this function again, because current GCS
                # doesn't have it.
                self._last_job_id_exported_for = worker.current_job_id
                worker.function_actor_manager.export_actor_class(
                    self._modified_class, self._actor_method_names)

            resources = ray.utils.resources_from_resource_arguments(
                cpus_to_use, self._num_gpus, self._resources, num_cpus,
                num_gpus, resources)

            # If the actor methods require CPU resources, then set the required
            # placement resources. If actor_placement_resources is empty, then
            # the required placement resources will be the same as resources.
            actor_placement_resources = {}
            assert actor_method_cpu in [0, 1]
            if actor_method_cpu == 1:
                actor_placement_resources = resources.copy()
                actor_placement_resources["CPU"] += 1

            function_name = "__init__"
            function_signature = self._method_signatures[function_name]
            creation_args = signature.extend_args(function_signature, args,
                                                  kwargs)
            function_descriptor = FunctionDescriptor(
                self._modified_class.__module__, function_name,
                self._modified_class.__name__)
            [actor_cursor] = worker.submit_task(
                function_descriptor,
                creation_args,
                actor_creation_id=actor_id,
                max_actor_reconstructions=self._max_reconstructions,
                num_return_vals=1,
                resources=resources,
                placement_resources=actor_placement_resources)
            assert isinstance(actor_cursor, ObjectID)

        actor_handle = ActorHandle(
            actor_id, self._modified_class.__module__, self._class_name,
            actor_cursor, self._actor_method_names, self._method_decorators,
            self._method_signatures, self._actor_method_num_return_vals,
            actor_cursor, actor_method_cpu, worker.current_job_id)
        # We increment the actor counter by 1 to account for the actor creation
        # task.
        actor_handle._ray_actor_counter += 1

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
        _ray_module_name: The module name of this actor.
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
        _ray_method_decorators: Optional decorators for the function
            invocation. This can be used to change the behavior on the
            invocation side, whereas a regular decorator can be used to change
            the behavior on the execution side.
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
        _ray_actor_job_id: The ID of the job that created the actor
            (it is possible that this ActorHandle exists on a job with a
            different job ID).
        _ray_new_actor_handles: The new actor handles that were created from
            this handle since the last task on this handle was submitted. This
            is used to garbage-collect dummy objects that are no longer
            necessary in the backend.
    """

    def __init__(self,
                 actor_id,
                 module_name,
                 class_name,
                 actor_cursor,
                 actor_method_names,
                 method_decorators,
                 method_signatures,
                 method_num_return_vals,
                 actor_creation_dummy_object_id,
                 actor_method_cpus,
                 actor_job_id,
                 actor_handle_id=None):
        assert isinstance(actor_id, ActorID)
        assert isinstance(actor_job_id, ray.JobID)
        self._ray_actor_id = actor_id
        self._ray_module_name = module_name
        # False if this actor handle was created by forking or pickling. True
        # if it was created by the _serialization_helper function.
        self._ray_original_handle = actor_handle_id is None
        if self._ray_original_handle:
            self._ray_actor_handle_id = ActorHandleID.nil()
        else:
            assert isinstance(actor_handle_id, ActorHandleID)
            self._ray_actor_handle_id = actor_handle_id
        self._ray_actor_cursor = actor_cursor
        self._ray_actor_counter = 0
        self._ray_actor_method_names = actor_method_names
        self._ray_method_decorators = method_decorators
        self._ray_method_signatures = method_signatures
        self._ray_method_num_return_vals = method_num_return_vals
        self._ray_class_name = class_name
        self._ray_actor_forks = 0
        self._ray_actor_creation_dummy_object_id = (
            actor_creation_dummy_object_id)
        self._ray_actor_method_cpus = actor_method_cpus
        self._ray_actor_job_id = actor_job_id
        self._ray_new_actor_handles = []
        self._ray_actor_lock = threading.Lock()

    def _actor_method_call(self,
                           method_name,
                           args=None,
                           kwargs=None,
                           num_return_vals=None):
        """Method execution stub for an actor handle.

        This is the function that executes when
        `actor.method_name.remote(*args, **kwargs)` is called. Instead of
        executing locally, the method is packaged as a task and scheduled
        to the remote actor instance.

        Args:
            method_name: The name of the actor method to execute.
            args: A list of arguments for the actor method.
            kwargs: A dictionary of keyword arguments for the actor method.
            num_return_vals (int): The number of return values for the method.

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

        function_descriptor = FunctionDescriptor(
            self._ray_module_name, method_name, self._ray_class_name)

        if worker.mode == ray.LOCAL_MODE:
            function = getattr(worker.actors[self._ray_actor_id], method_name)
            object_ids = worker.local_mode_manager.execute(
                function, function_descriptor, args, num_return_vals)
        else:
            with self._ray_actor_lock:
                object_ids = worker.submit_task(
                    function_descriptor,
                    args,
                    actor_id=self._ray_actor_id,
                    actor_handle_id=self._ray_actor_handle_id,
                    actor_counter=self._ray_actor_counter,
                    actor_creation_dummy_object_id=(
                        self._ray_actor_creation_dummy_object_id),
                    execution_dependencies=[self._ray_actor_cursor],
                    new_actor_handles=self._ray_new_actor_handles,
                    # We add one for the dummy return ID.
                    num_return_vals=num_return_vals + 1,
                    resources={"CPU": self._ray_actor_method_cpus},
                    placement_resources={},
                    job_id=self._ray_actor_job_id,
                )
                # Update the actor counter and cursor to reflect the most recent
                # invocation.
                self._ray_actor_counter += 1
                # The last object returned is the dummy object that should be
                # passed in to the next actor method. Do not return it to the user.
                self._ray_actor_cursor = object_ids.pop()
                # We have notified the backend of the new actor handles to expect
                # since the last task was submitted, so clear the list.
                self._ray_new_actor_handles = []

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
                return ActorMethod(
                    self,
                    attr,
                    self._ray_method_num_return_vals[attr],
                    decorator=self._ray_method_decorators.get(attr))
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
        if (worker.mode == ray.worker.SCRIPT_MODE
                and self._ray_actor_job_id.binary() != worker.worker_id):
            # If the worker is a driver and driver id has changed because
            # Ray was shut down re-initialized, the actor is already cleaned up
            # and we don't need to send `__ray_terminate__` again.
            logger.warning(
                "Actor is garbage collected in the wrong driver." +
                " Actor id = %s, class name = %s.", self._ray_actor_id,
                self._ray_class_name)
            return
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
        if ray_forking:
            actor_handle_id = compute_actor_handle_id(
                self._ray_actor_handle_id, self._ray_actor_forks)
        else:
            actor_handle_id = self._ray_actor_handle_id

        # Note: _ray_actor_cursor and _ray_actor_creation_dummy_object_id
        # could be None.
        state = {
            "actor_id": self._ray_actor_id,
            "actor_handle_id": actor_handle_id,
            "module_name": self._ray_module_name,
            "class_name": self._ray_class_name,
            "actor_cursor": self._ray_actor_cursor,
            "actor_method_names": self._ray_actor_method_names,
            "method_decorators": self._ray_method_decorators,
            "method_signatures": self._ray_method_signatures,
            "method_num_return_vals": self._ray_method_num_return_vals,
            # Actors in local mode don't have dummy objects.
            "actor_creation_dummy_object_id": self.
            _ray_actor_creation_dummy_object_id,
            "actor_method_cpus": self._ray_actor_method_cpus,
            "actor_job_id": self._ray_actor_job_id,
            "ray_forking": ray_forking
        }

        if ray_forking:
            self._ray_actor_forks += 1
            new_actor_handle_id = actor_handle_id
        else:
            # The execution dependency for a pickled actor handle is never safe
            # to release, since it could be unpickled and submit another
            # dependent task at any time. Therefore, we notify the backend of a
            # random handle ID that will never actually be used.
            new_actor_handle_id = ActorHandleID.from_random()
        # Notify the backend to expect this new actor handle. The backend will
        # not release the cursor for any new handles until the first task for
        # each of the new handles is submitted.
        # NOTE(swang): There is currently no garbage collection for actor
        # handles until the actor itself is removed.
        self._ray_new_actor_handles.append(new_actor_handle_id)

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
            actor_handle_id = state["actor_handle_id"]
        else:
            # Right now, if the actor handle has been pickled, we create a
            # temporary actor handle id for invocations.
            # TODO(pcm): This still leads to a lot of actor handles being
            # created, there should be a better way to handle pickled
            # actor handles.
            # TODO(swang): Accessing the worker's current task ID is not
            # thread-safe.
            # TODO(swang): Unpickling the same actor handle twice in the same
            # task will break the application, and unpickling it twice in the
            # same actor is likely a performance bug. We should consider
            # logging a warning in these cases.
            actor_handle_id = compute_actor_handle_id_non_forked(
                state["actor_handle_id"], worker.current_task_id)

        self.__init__(
            state["actor_id"],
            state["module_name"],
            state["class_name"],
            state["actor_cursor"],
            state["actor_method_names"],
            state["method_decorators"],
            state["method_signatures"],
            state["method_num_return_vals"],
            state["actor_creation_dummy_object_id"],
            state["actor_method_cpus"],
            # This is the ID of the job that owns the actor, not
            # necessarily the job that owns this actor handle.
            state["actor_job_id"],
            actor_handle_id=actor_handle_id)

    def __getstate__(self):
        """This code path is used by pickling but not by Ray forking."""
        return self._serialization_helper(False)

    def __setstate__(self, state):
        """This code path is used by pickling but not by Ray forking."""
        return self._deserialization_helper(state, False)


def make_actor(cls, num_cpus, num_gpus, resources, max_reconstructions):
    # Give an error if cls is an old-style class.
    if not issubclass(cls, object):
        raise TypeError(
            "The @ray.remote decorator cannot be applied to old-style "
            "classes. In Python 2, you must declare the class with "
            "'class ClassName(object):' instead of 'class ClassName:'.")

    if issubclass(cls, Checkpointable) and inspect.isabstract(cls):
        raise TypeError(
            "A checkpointable actor class should implement all abstract "
            "methods in the `Checkpointable` interface.")

    if max_reconstructions is None:
        max_reconstructions = 0

    if not (ray_constants.NO_RECONSTRUCTION <= max_reconstructions <=
            ray_constants.INFINITE_RECONSTRUCTION):
        raise Exception("max_reconstructions must be in range [%d, %d]." %
                        (ray_constants.NO_RECONSTRUCTION,
                         ray_constants.INFINITE_RECONSTRUCTION))

    # Modify the class to have an additional method that will be used for
    # terminating the worker.
    class Class(cls):
        def __ray_terminate__(self):
            worker = ray.worker.get_global_worker()
            if worker.mode != ray.LOCAL_MODE:
                ray.actor.exit_actor()

        def __ray_checkpoint__(self):
            """Save a checkpoint.

            This task saves the current state of the actor, the current task
            frontier according to the raylet, and the checkpoint index
            (number of tasks executed so far).
            """
            worker = ray.worker.global_worker
            if not isinstance(self, ray.actor.Checkpointable):
                raise Exception(
                    "__ray_checkpoint__.remote() may only be called on actors "
                    "that implement ray.actor.Checkpointable")
            return worker._save_actor_checkpoint()

    Class.__module__ = cls.__module__
    Class.__name__ = cls.__name__

    class_id = ActorClassID.from_random()

    return ActorClass(Class, class_id, max_reconstructions, num_cpus, num_gpus,
                      resources)


def exit_actor():
    """Intentionally exit the current actor.

    This function is used to disconnect an actor and exit the worker.

    Raises:
        Exception: An exception is raised if this is a driver or this
            worker is not an actor.
    """
    worker = ray.worker.global_worker
    if worker.mode == ray.WORKER_MODE and not worker.actor_id.is_nil():
        # Disconnect the worker from the raylet. The point of
        # this is so that when the worker kills itself below, the
        # raylet won't push an error message to the driver.
        worker.raylet_client.disconnect()
        ray.disconnect()
        # Disconnect global state from GCS.
        ray.state.state.disconnect()
        sys.exit(0)
        assert False, "This process should have terminated."
    else:
        raise Exception("exit_actor called on a non-actor worker.")


ray.worker.global_worker.make_actor = make_actor

CheckpointContext = namedtuple(
    "CheckpointContext",
    [
        # Actor's ID.
        "actor_id",
        # Number of tasks executed since last checkpoint.
        "num_tasks_since_last_checkpoint",
        # Time elapsed since last checkpoint, in milliseconds.
        "time_elapsed_ms_since_last_checkpoint",
    ],
)
"""A namedtuple that contains information about actor's last checkpoint."""

Checkpoint = namedtuple(
    "Checkpoint",
    [
        # ID of this checkpoint.
        "checkpoint_id",
        # The timestamp at which this checkpoint was saved,
        # represented as milliseconds elapsed since Unix epoch.
        "timestamp",
    ],
)
"""A namedtuple that represents a checkpoint."""


class Checkpointable(six.with_metaclass(ABCMeta, object)):
    """An interface that indicates an actor can be checkpointed."""

    @abstractmethod
    def should_checkpoint(self, checkpoint_context):
        """Whether this actor needs to be checkpointed.

        This method will be called after every task. You should implement this
        callback to decide whether this actor needs to be checkpointed at this
        time, based on the checkpoint context, or any other factors.

        Args:
            checkpoint_context: A namedtuple that contains info about last
                checkpoint.

        Returns:
            A boolean value that indicates whether this actor needs to be
            checkpointed.
        """
        pass

    @abstractmethod
    def save_checkpoint(self, actor_id, checkpoint_id):
        """Save a checkpoint to persistent storage.

        If `should_checkpoint` returns true, this method will be called. You
        should implement this callback to save actor's checkpoint and the given
        checkpoint id to persistent storage.

        Args:
            actor_id: Actor's ID.
            checkpoint_id: ID of this checkpoint. You should save it together
                with actor's checkpoint data. And it will be used by the
                `load_checkpoint` method.
        Returns:
            None.
        """
        pass

    @abstractmethod
    def load_checkpoint(self, actor_id, available_checkpoints):
        """Load actor's previous checkpoint, and restore actor's state.

        This method will be called when an actor is reconstructed, after
        actor's constructor.
        If the actor needs to restore from previous checkpoint, this function
        should restore actor's state and return the checkpoint ID. Otherwise,
        it should do nothing and return None.
        Note, this method must return one of the checkpoint IDs in the
        `available_checkpoints` list, or None. Otherwise, an exception will be
        raised.

        Args:
            actor_id: Actor's ID.
            available_checkpoints: A list of `Checkpoint` namedtuples that
                contains all available checkpoint IDs and their timestamps,
                sorted by timestamp in descending order.
        Returns:
            The ID of the checkpoint from which the actor was resumed, or None
            if the actor should restart from the beginning.
        """
        pass

    @abstractmethod
    def checkpoint_expired(self, actor_id, checkpoint_id):
        """Delete an expired checkpoint.

        This method will be called when an checkpoint is expired. You should
        implement this method to delete your application checkpoint data.
        Note, the maximum number of checkpoints kept in the backend can be
        configured at `RayConfig.num_actor_checkpoints_to_keep`.

        Args:
            actor_id: ID of the actor.
            checkpoint_id: ID of the checkpoint that has expired.
        Returns:
            None.
        """
        pass


def get_checkpoints_for_actor(actor_id):
    """Get the available checkpoints for the given actor ID, return a list
    sorted by checkpoint timestamp in descending order.
    """
    checkpoint_info = ray.state.state.actor_checkpoint_info(actor_id)
    if checkpoint_info is None:
        return []
    checkpoints = [
        Checkpoint(checkpoint_id, timestamp) for checkpoint_id, timestamp in
        zip(checkpoint_info["CheckpointIds"], checkpoint_info["Timestamps"])
    ]
    return sorted(
        checkpoints,
        key=lambda checkpoint: checkpoint.timestamp,
        reverse=True,
    )
