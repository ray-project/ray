import inspect
import logging
import weakref

from abc import ABCMeta, abstractmethod
from collections import namedtuple
import ray.ray_constants as ray_constants
import ray._raylet
import ray.signature as signature
import ray.worker
from ray import ActorClassID, Language
from ray._raylet import PythonFunctionDescriptor, gcs_actor_service_enabled
from ray import cross_language

logger = logging.getLogger(__name__)


def method(*args, **kwargs):
    """Annotate an actor method.

    .. code-block:: python

        @ray.remote
        class Foo:
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
class ActorMethod:
    """A class used to invoke an actor method.

    Note: This class only keeps a weak ref to the actor, unless it has been
    passed to a remote function. This avoids delays in GC of the actor.

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

    def __init__(self,
                 actor,
                 method_name,
                 num_return_vals,
                 decorator=None,
                 hardref=False):
        self._actor_ref = weakref.ref(actor)
        self._method_name = method_name
        self._num_return_vals = num_return_vals
        # This is a decorator that is used to wrap the function invocation (as
        # opposed to the function execution). The decorator must return a
        # function that takes in two arguments ("args" and "kwargs"). In most
        # cases, it should call the function that was passed into the decorator
        # and return the resulting ObjectIDs.
        self._decorator = decorator

        # Acquire a hard ref to the actor, this is useful mainly when passing
        # actor method handles to remote functions.
        if hardref:
            self._actor_hard_ref = actor
        else:
            self._actor_hard_ref = None

    def __call__(self, *args, **kwargs):
        raise TypeError("Actor methods cannot be called directly. Instead "
                        "of running 'object.{}()', try "
                        "'object.{}.remote()'.".format(self._method_name,
                                                       self._method_name))

    def remote(self, *args, **kwargs):
        return self._remote(args, kwargs)

    def _remote(self, args=None, kwargs=None, num_return_vals=None):
        if num_return_vals is None:
            num_return_vals = self._num_return_vals

        def invocation(args, kwargs):
            actor = self._actor_hard_ref or self._actor_ref()
            if actor is None:
                raise RuntimeError("Lost reference to actor")
            return actor._actor_method_call(
                self._method_name,
                args=args,
                kwargs=kwargs,
                num_return_vals=num_return_vals)

        # Apply the decorator if there is one.
        if self._decorator is not None:
            invocation = self._decorator(invocation)

        return invocation(args, kwargs)

    def __getstate__(self):
        return {
            "actor": self._actor_ref(),
            "method_name": self._method_name,
            "num_return_vals": self._num_return_vals,
            "decorator": self._decorator,
        }

    def __setstate__(self, state):
        self.__init__(
            state["actor"],
            state["method_name"],
            state["num_return_vals"],
            state["decorator"],
            hardref=True)


class ActorClassMethodMetadata(object):
    """Metadata for all methods in an actor class. This data can be cached.

    Attributes:
        methods: The actor methods.
        decorators: Optional decorators that should be applied to the
            method invocation function before invoking the actor methods. These
            can be set by attaching the attribute
            "__ray_invocation_decorator__" to the actor method.
        signatures: The signatures of the methods.
        num_return_vals: The default number of return values for
            each actor method.
    """

    _cache = {}  # This cache will be cleared in ray.disconnect()

    def __init__(self):
        class_name = type(self).__name__
        raise TypeError("{} can not be constructed directly, "
                        "instead of running '{}()', try '{}.create()'".format(
                            class_name, class_name, class_name))

    @classmethod
    def reset_cache(cls):
        cls._cache.clear()

    @classmethod
    def create(cls, modified_class, actor_creation_function_descriptor):
        # Try to create an instance from cache.
        cached_meta = cls._cache.get(actor_creation_function_descriptor)
        if cached_meta is not None:
            return cached_meta

        # Create an instance without __init__ called.
        self = cls.__new__(cls)

        actor_methods = inspect.getmembers(modified_class,
                                           ray.utils.is_function_or_method)
        self.methods = dict(actor_methods)

        # Extract the signatures of each of the methods. This will be used
        # to catch some errors if the methods are called with inappropriate
        # arguments.
        self.decorators = {}
        self.signatures = {}
        self.num_return_vals = {}
        for method_name, method in actor_methods:
            # Whether or not this method requires binding of its first
            # argument. For class and static methods, we do not want to bind
            # the first argument, but we do for instance methods
            is_bound = (ray.utils.is_class_method(method)
                        or ray.utils.is_static_method(modified_class,
                                                      method_name))

            # Print a warning message if the method signature is not
            # supported. We don't raise an exception because if the actor
            # inherits from a class that has a method whose signature we
            # don't support, there may not be much the user can do about it.
            self.signatures[method_name] = signature.extract_signature(
                method, ignore_first=not is_bound)
            # Set the default number of return values for this method.
            if hasattr(method, "__ray_num_return_vals__"):
                self.num_return_vals[method_name] = (
                    method.__ray_num_return_vals__)
            else:
                self.num_return_vals[method_name] = (
                    ray_constants.DEFAULT_ACTOR_METHOD_NUM_RETURN_VALS)

            if hasattr(method, "__ray_invocation_decorator__"):
                self.decorators[method_name] = (
                    method.__ray_invocation_decorator__)

        # Update cache.
        cls._cache[actor_creation_function_descriptor] = self
        return self


class ActorClassMetadata:
    """Metadata for an actor class.

    Attributes:
        language: The actor language, e.g. Python, Java.
        modified_class: The original class that was decorated (with some
            additional methods added like __ray_terminate__).
        actor_creation_function_descriptor: The function descriptor for
            the actor creation task.
        class_id: The ID of this actor class.
        class_name: The name of this class.
        num_cpus: The default number of CPUs required by the actor creation
            task.
        num_gpus: The default number of GPUs required by the actor creation
            task.
        memory: The heap memory quota for this actor.
        object_store_memory: The object store memory quota for this actor.
        resources: The default resources required by the actor creation task.
        last_export_session_and_job: A pair of the last exported session
            and job to help us to know whether this function was exported.
            This is an imperfect mechanism used to determine if we need to
            export the remote function again. It is imperfect in the sense that
            the actor class definition could be exported multiple times by
            different workers.
        method_meta: The actor method metadata.
    """

    def __init__(self, language, modified_class,
                 actor_creation_function_descriptor, class_id, max_restarts,
                 max_task_retries, num_cpus, num_gpus, memory,
                 object_store_memory, resources):
        self.language = language
        self.modified_class = modified_class
        self.actor_creation_function_descriptor = \
            actor_creation_function_descriptor
        self.class_name = actor_creation_function_descriptor.class_name
        self.is_cross_language = language != Language.PYTHON
        self.class_id = class_id
        self.max_restarts = max_restarts
        self.max_task_retries = max_task_retries
        self.num_cpus = num_cpus
        self.num_gpus = num_gpus
        self.memory = memory
        self.object_store_memory = object_store_memory
        self.resources = resources
        self.last_export_session_and_job = None
        self.method_meta = ActorClassMethodMetadata.create(
            modified_class, actor_creation_function_descriptor)


class ActorClass:
    """An actor class.

    This is a decorated class. It can be used to create actors.

    Attributes:
        __ray_metadata__: Contains metadata for the actor.
    """

    def __init__(cls, name, bases, attr):
        """Prevents users from directly inheriting from an ActorClass.

        This will be called when a class is defined with an ActorClass object
        as one of its base classes. To intentionally construct an ActorClass,
        use the '_ray_from_modified_class' classmethod.

        Raises:
            TypeError: Always.
        """
        for base in bases:
            if isinstance(base, ActorClass):
                raise TypeError("Attempted to define subclass '{}' of actor "
                                "class '{}'. Inheriting from actor classes is "
                                "not currently supported. You can instead "
                                "inherit from a non-actor base class and make "
                                "the derived class an actor class (with "
                                "@ray.remote).".format(
                                    name, base.__ray_metadata__.class_name))

        # This shouldn't be reached because one of the base classes must be
        # an actor class if this was meant to be subclassed.
        assert False, ("ActorClass.__init__ should not be called. Please use "
                       "the @ray.remote decorator instead.")

    def __call__(self, *args, **kwargs):
        """Prevents users from directly instantiating an ActorClass.

        This will be called instead of __init__ when 'ActorClass()' is executed
        because an is an object rather than a metaobject. To properly
        instantiated a remote actor, use 'ActorClass.remote()'.

        Raises:
            Exception: Always.
        """
        raise TypeError("Actors cannot be instantiated directly. "
                        "Instead of '{}()', use '{}.remote()'.".format(
                            self.__ray_metadata__.class_name,
                            self.__ray_metadata__.class_name))

    @classmethod
    def _ray_from_modified_class(cls, modified_class, class_id, max_restarts,
                                 max_task_retries, num_cpus, num_gpus, memory,
                                 object_store_memory, resources):
        for attribute in [
                "remote", "_remote", "_ray_from_modified_class",
                "_ray_from_function_descriptor"
        ]:
            if hasattr(modified_class, attribute):
                logger.warning("Creating an actor from class {} overwrites "
                               "attribute {} of that class".format(
                                   modified_class.__name__, attribute))

        # Make sure the actor class we are constructing inherits from the
        # original class so it retains all class properties.
        class DerivedActorClass(cls, modified_class):
            pass

        name = "ActorClass({})".format(modified_class.__name__)
        DerivedActorClass.__module__ = modified_class.__module__
        DerivedActorClass.__name__ = name
        DerivedActorClass.__qualname__ = name
        # Construct the base object.
        self = DerivedActorClass.__new__(DerivedActorClass)
        # Actor creation function descriptor.
        actor_creation_function_descriptor = \
            PythonFunctionDescriptor.from_class(
                modified_class.__ray_actor_class__)

        self.__ray_metadata__ = ActorClassMetadata(
            Language.PYTHON, modified_class,
            actor_creation_function_descriptor, class_id, max_restarts,
            max_task_retries, num_cpus, num_gpus, memory, object_store_memory,
            resources)

        return self

    @classmethod
    def _ray_from_function_descriptor(
            cls, language, actor_creation_function_descriptor, max_restarts,
            max_task_retries, num_cpus, num_gpus, memory, object_store_memory,
            resources):
        self = ActorClass.__new__(ActorClass)

        self.__ray_metadata__ = ActorClassMetadata(
            language, None, actor_creation_function_descriptor, None,
            max_restarts, max_task_retries, num_cpus, num_gpus, memory,
            object_store_memory, resources)

        return self

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

    def options(self, **options):
        """Convenience method for creating an actor with options.

        Same arguments as Actor._remote(), but returns a wrapped actor class
        that a non-underscore .remote() can be called on.

        Examples:
            # The following two calls are equivalent.
            >>> Actor._remote(num_cpus=4, max_concurrency=8, args=[x, y])
            >>> Actor.options(num_cpus=4, max_concurrency=8).remote(x, y)
        """

        actor_cls = self

        class ActorOptionWrapper:
            def remote(self, *args, **kwargs):
                return actor_cls._remote(args=args, kwargs=kwargs, **options)

        return ActorOptionWrapper()

    def _remote(self,
                args=None,
                kwargs=None,
                num_cpus=None,
                num_gpus=None,
                memory=None,
                object_store_memory=None,
                resources=None,
                is_direct_call=None,
                max_concurrency=None,
                max_restarts=None,
                max_task_retries=None,
                name=None,
                detached=False):
        """Create an actor.

        This method allows more flexibility than the remote method because
        resource requirements can be specified and override the defaults in the
        decorator.

        Args:
            args: The arguments to forward to the actor constructor.
            kwargs: The keyword arguments to forward to the actor constructor.
            num_cpus: The number of CPUs required by the actor creation task.
            num_gpus: The number of GPUs required by the actor creation task.
            memory: Restrict the heap memory usage of this actor.
            object_store_memory: Restrict the object store memory used by
                this actor when creating objects.
            resources: The custom resources required by the actor creation
                task.
            is_direct_call: Use direct actor calls.
            max_concurrency: The max number of concurrent calls to allow for
                this actor. This only works with direct actor calls. The max
                concurrency defaults to 1 for threaded execution, and 1000 for
                asyncio execution. Note that the execution order is not
                guaranteed when max_concurrency > 1.
            name: The globally unique name for the actor.
            detached: DEPRECATED.

        Returns:
            A handle to the newly created actor.
        """
        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}
        if is_direct_call is not None and not is_direct_call:
            raise ValueError("Non-direct call actors are no longer supported.")

        meta = self.__ray_metadata__
        actor_has_async_methods = len(
            inspect.getmembers(
                meta.modified_class,
                predicate=inspect.iscoroutinefunction)) > 0
        is_asyncio = actor_has_async_methods

        if max_concurrency is None:
            if is_asyncio:
                max_concurrency = 1000
            else:
                max_concurrency = 1

        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")

        worker = ray.worker.global_worker
        if worker.mode is None:
            raise RuntimeError("Actors cannot be created before ray.init() "
                               "has been called.")

        if detached:
            logger.warning("The detached flag is deprecated. To create a "
                           "detached actor, use the name parameter.")

        if name is not None:
            if not isinstance(name, str):
                raise TypeError("name must be None or a string, "
                                "got: '{}'.".format(type(name)))
            if name == "":
                raise ValueError("Actor name cannot be an empty string.")

        # Check whether the name is already taken.
        # TODO(edoakes): this check has a race condition because two drivers
        # could pass the check and then create the same named actor. We should
        # instead check this when we create the actor, but that's currently an
        # async call.
        if name is not None:
            try:
                ray.get_actor(name)
            except ValueError:  # Name is not taken.
                pass
            else:
                raise ValueError(
                    "The name {name} is already taken. Please use "
                    "a different name or get the existing actor using "
                    "ray.get_actor('{name}')".format(name=name))
            detached = True
        else:
            detached = False

        # Set the actor's default resources if not already set. First three
        # conditions are to check that no resources were specified in the
        # decorator. Last three conditions are to check that no resources were
        # specified when _remote() was called.
        if (meta.num_cpus is None and meta.num_gpus is None
                and meta.resources is None and num_cpus is None
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
                           if meta.num_cpus is None else meta.num_cpus)
            actor_method_cpu = ray_constants.DEFAULT_ACTOR_METHOD_CPU_SPECIFIED

        # LOCAL_MODE cannot handle cross_language
        if worker.mode == ray.LOCAL_MODE:
            assert not meta.is_cross_language, \
                "Cross language ActorClass cannot be executed locally."

        # Export the actor.
        if not meta.is_cross_language and (meta.last_export_session_and_job !=
                                           worker.current_session_and_job):
            # If this actor class was not exported in this session and job,
            # we need to export this function again, because current GCS
            # doesn't have it.
            meta.last_export_session_and_job = (worker.current_session_and_job)
            # After serialize / deserialize modified class, the __module__
            # of modified class will be ray.cloudpickle.cloudpickle.
            # So, here pass actor_creation_function_descriptor to make
            # sure export actor class correct.
            worker.function_actor_manager.export_actor_class(
                meta.modified_class, meta.actor_creation_function_descriptor,
                meta.method_meta.methods.keys())

        resources = ray.utils.resources_from_resource_arguments(
            cpus_to_use, meta.num_gpus, meta.memory, meta.object_store_memory,
            meta.resources, num_cpus, num_gpus, memory, object_store_memory,
            resources)

        # If the actor methods require CPU resources, then set the required
        # placement resources. If actor_placement_resources is empty, then
        # the required placement resources will be the same as resources.
        actor_placement_resources = {}
        assert actor_method_cpu in [0, 1]
        if actor_method_cpu == 1:
            actor_placement_resources = resources.copy()
            actor_placement_resources["CPU"] += 1
        if meta.is_cross_language:
            creation_args = cross_language.format_args(worker, args, kwargs)
        else:
            function_signature = meta.method_meta.signatures["__init__"]
            creation_args = signature.flatten_args(function_signature, args,
                                                   kwargs)
        actor_id = worker.core_worker.create_actor(
            meta.language,
            meta.actor_creation_function_descriptor,
            creation_args,
            max_restarts or meta.max_restarts,
            max_task_retries or meta.max_task_retries,
            resources,
            actor_placement_resources,
            max_concurrency,
            detached,
            name if name is not None else "",
            is_asyncio,
            # Store actor_method_cpu in actor handle's extension data.
            extension_data=str(actor_method_cpu))

        actor_handle = ActorHandle(
            meta.language,
            actor_id,
            meta.method_meta.decorators,
            meta.method_meta.signatures,
            meta.method_meta.num_return_vals,
            actor_method_cpu,
            meta.actor_creation_function_descriptor,
            worker.current_session_and_job,
            original_handle=True)

        if name is not None and not gcs_actor_service_enabled():
            ray.util.named_actors._register_actor(name, actor_handle)

        return actor_handle


class ActorHandle:
    """A handle to an actor.

    The fields in this class are prefixed with _ray_ to hide them from the user
    and to avoid collision with actor method names.

    An ActorHandle can be created in three ways. First, by calling .remote() on
    an ActorClass. Second, by passing an actor handle into a task (forking the
    ActorHandle). Third, by directly serializing the ActorHandle (e.g., with
    cloudpickle).

    Attributes:
        _ray_actor_language: The actor language.
        _ray_actor_id: Actor ID.
        _ray_method_decorators: Optional decorators for the function
            invocation. This can be used to change the behavior on the
            invocation side, whereas a regular decorator can be used to change
            the behavior on the execution side.
        _ray_method_signatures: The signatures of the actor methods.
        _ray_method_num_return_vals: The default number of return values for
            each method.
        _ray_actor_method_cpus: The number of CPUs required by actor methods.
        _ray_original_handle: True if this is the original actor handle for a
            given actor. If this is true, then the actor will be destroyed when
            this handle goes out of scope.
        _ray_is_cross_language: Whether this actor is cross language.
        _ray_actor_creation_function_descriptor: The function descriptor
            of the actor creation task.
    """

    def __init__(self,
                 language,
                 actor_id,
                 method_decorators,
                 method_signatures,
                 method_num_return_vals,
                 actor_method_cpus,
                 actor_creation_function_descriptor,
                 session_and_job,
                 original_handle=False):
        self._ray_actor_language = language
        self._ray_actor_id = actor_id
        self._ray_original_handle = original_handle
        self._ray_method_decorators = method_decorators
        self._ray_method_signatures = method_signatures
        self._ray_method_num_return_vals = method_num_return_vals
        self._ray_actor_method_cpus = actor_method_cpus
        self._ray_session_and_job = session_and_job
        self._ray_is_cross_language = language != Language.PYTHON
        self._ray_actor_creation_function_descriptor = \
            actor_creation_function_descriptor
        self._ray_function_descriptor = {}

        if not self._ray_is_cross_language:
            assert isinstance(actor_creation_function_descriptor,
                              PythonFunctionDescriptor)
            module_name = actor_creation_function_descriptor.module_name
            class_name = actor_creation_function_descriptor.class_name
            for method_name in self._ray_method_signatures.keys():
                function_descriptor = PythonFunctionDescriptor(
                    module_name, method_name, class_name)
                self._ray_function_descriptor[
                    method_name] = function_descriptor
                method = ActorMethod(
                    self,
                    method_name,
                    self._ray_method_num_return_vals[method_name],
                    decorator=self._ray_method_decorators.get(method_name))
                setattr(self, method_name, method)

    def __del__(self):
        # Mark that this actor handle has gone out of scope. Once all actor
        # handles are out of scope, the actor will exit.
        worker = ray.worker.global_worker
        if worker.connected and hasattr(worker, "core_worker"):
            worker.core_worker.remove_actor_handle_reference(
                self._ray_actor_id)

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
        worker = ray.worker.global_worker

        args = args or []
        kwargs = kwargs or {}
        if self._ray_is_cross_language:
            list_args = cross_language.format_args(worker, args, kwargs)
            function_descriptor = \
                cross_language.get_function_descriptor_for_actor_method(
                    self._ray_actor_language,
                    self._ray_actor_creation_function_descriptor, method_name)
        else:
            function_signature = self._ray_method_signatures[method_name]

            if not args and not kwargs and not function_signature:
                list_args = []
            else:
                list_args = signature.flatten_args(function_signature, args,
                                                   kwargs)
            function_descriptor = self._ray_function_descriptor[method_name]

        if worker.mode == ray.LOCAL_MODE:
            assert not self._ray_is_cross_language,\
                "Cross language remote actor method " \
                "cannot be executed locally."

        object_ids = worker.core_worker.submit_actor_task(
            self._ray_actor_language, self._ray_actor_id, function_descriptor,
            list_args, num_return_vals, self._ray_actor_method_cpus)

        if len(object_ids) == 1:
            object_ids = object_ids[0]
        elif len(object_ids) == 0:
            object_ids = None

        return object_ids

    def __getattr__(self, item):
        if not self._ray_is_cross_language:
            raise AttributeError("'{}' object has no attribute '{}'".format(
                type(self).__name__, item))
        if item in ["__ray_terminate__", "__ray_checkpoint__"]:

            class FakeActorMethod(object):
                def __call__(self, *args, **kwargs):
                    raise TypeError(
                        "Actor methods cannot be called directly. Instead "
                        "of running 'object.{}()', try 'object.{}.remote()'.".
                        format(item, item))

                def remote(self, *args, **kwargs):
                    logger.warning(
                        "Actor method {} is not supported by cross language."
                        .format(item))

            return FakeActorMethod()

        return ActorMethod(
            self,
            item,
            ray_constants.
            # Currently, we use default num returns
            DEFAULT_ACTOR_METHOD_NUM_RETURN_VALS,
            # Currently, cross-lang actor method not support decorator
            decorator=None)

    # Make tab completion work.
    def __dir__(self):
        return self._ray_method_signatures.keys()

    def __repr__(self):
        return "Actor({}, {})".format(
            self._ray_actor_creation_function_descriptor.class_name,
            self._actor_id.hex())

    @property
    def _actor_id(self):
        return self._ray_actor_id

    def _serialization_helper(self):
        """This is defined in order to make pickling work.

        Returns:
            A dictionary of the information needed to reconstruct the object.
        """
        worker = ray.worker.global_worker
        worker.check_connected()

        if hasattr(worker, "core_worker"):
            # Non-local mode
            state = worker.core_worker.serialize_actor_handle(
                self._ray_actor_id)
        else:
            # Local mode
            state = ({
                "actor_language": self._ray_actor_language,
                "actor_id": self._ray_actor_id,
                "method_decorators": self._ray_method_decorators,
                "method_signatures": self._ray_method_signatures,
                "method_num_return_vals": self._ray_method_num_return_vals,
                "actor_method_cpus": self._ray_actor_method_cpus,
                "actor_creation_function_descriptor": self.
                _ray_actor_creation_function_descriptor,
            }, None)

        return state

    @classmethod
    def _deserialization_helper(cls, state, outer_object_id=None):
        """This is defined in order to make pickling work.

        Args:
            state: The serialized state of the actor handle.
            outer_object_id: The ObjectID that the serialized actor handle was
                contained in, if any. This is used for counting references to
                the actor handle.

        """
        worker = ray.worker.global_worker
        worker.check_connected()

        if hasattr(worker, "core_worker"):
            # Non-local mode
            return worker.core_worker.deserialize_and_register_actor_handle(
                state, outer_object_id)
        else:
            # Local mode
            return cls(
                # TODO(swang): Accessing the worker's current task ID is not
                # thread-safe.
                state["actor_language"],
                state["actor_id"],
                state["method_decorators"],
                state["method_signatures"],
                state["method_num_return_vals"],
                state["actor_method_cpus"],
                state["actor_creation_function_descriptor"],
                worker.current_session_and_job)

    def __reduce__(self):
        """This code path is used by pickling but not by Ray forking."""
        state = self._serialization_helper()
        return ActorHandle._deserialization_helper, (state)


def modify_class(cls):
    # cls has been modified.
    if hasattr(cls, "__ray_actor_class__"):
        return cls

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

    # Modify the class to have an additional method that will be used for
    # terminating the worker.
    class Class(cls):
        __ray_actor_class__ = cls  # The original actor class

        def __ray_terminate__(self):
            worker = ray.worker.global_worker
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
                raise TypeError(
                    "__ray_checkpoint__.remote() may only be called on actors "
                    "that implement ray.actor.Checkpointable")
            return worker._save_actor_checkpoint()

    Class.__module__ = cls.__module__
    Class.__name__ = cls.__name__

    if not ray.utils.is_function_or_method(getattr(Class, "__init__", None)):
        # Add __init__ if it does not exist.
        # Actor creation will be executed with __init__ together.

        # Assign an __init__ function will avoid many checks later on.
        def __init__(self):
            pass

        Class.__init__ = __init__

    return Class


def make_actor(cls, num_cpus, num_gpus, memory, object_store_memory, resources,
               max_restarts, max_task_retries):
    Class = modify_class(cls)

    if max_restarts is None:
        max_restarts = 0
    if max_task_retries is None:
        max_task_retries = 0

    infinite_restart = max_restarts == -1
    if not infinite_restart:
        if max_restarts < 0:
            raise ValueError("max_restarts must be an integer >= -1 "
                             "-1 indicates infinite restarts")
        else:
            # Make sure we don't pass too big of an int to C++, causing
            # an overflow.
            max_restarts = min(max_restarts, ray_constants.MAX_INT64_VALUE)

    if max_restarts == 0 and max_task_retries != 0:
        raise ValueError(
            "max_task_retries cannot be set if max_restarts is 0.")

    return ActorClass._ray_from_modified_class(
        Class, ActorClassID.from_random(), max_restarts, max_task_retries,
        num_cpus, num_gpus, memory, object_store_memory, resources)


def exit_actor():
    """Intentionally exit the current actor.

    This function is used to disconnect an actor and exit the worker.

    Raises:
        Exception: An exception is raised if this is a driver or this
            worker is not an actor.
    """
    worker = ray.worker.global_worker
    if worker.mode == ray.WORKER_MODE and not worker.actor_id.is_nil():
        # Intentionally disconnect the core worker from the raylet so the
        # raylet won't push an error message to the driver.
        ray.disconnect()
        # Disconnect global state from GCS.
        ray.state.state.disconnect()
        # Set a flag to indicate this is an intentional actor exit. This
        # reduces log verbosity.
        exit = SystemExit(0)
        exit.is_ray_terminate = True
        raise exit
        assert False, "This process should have terminated."
    else:
        raise TypeError("exit_actor called on a non-actor worker.")


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


class Checkpointable(metaclass=ABCMeta):
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

        This method will be called when an actor is restarted, after
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
