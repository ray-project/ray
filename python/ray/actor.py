import inspect
import logging
import weakref
from typing import Any, Dict, List, Optional

import ray._private.ray_constants as ray_constants
import ray._private.signature as signature
import ray._private.worker
import ray._raylet
from ray import ActorClassID, Language, cross_language
from ray._private import ray_option_utils
from ray._private.client_mode_hook import (
    client_mode_convert_actor,
    client_mode_hook,
    client_mode_should_convert,
)
from ray._private.inspect_util import (
    is_class_method,
    is_function_or_method,
    is_static_method,
)
from ray._private.utils import get_runtime_env_info, parse_runtime_env
from ray._raylet import PythonFunctionDescriptor
from ray.exceptions import AsyncioActorExit
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.util.placement_group import _configure_placement_group_based_on_context
from ray.util.scheduling_strategies import (
    PlacementGroupSchedulingStrategy,
    SchedulingStrategyT,
)
from ray.util.tracing.tracing_helper import (
    _inject_tracing_into_class,
    _tracing_actor_creation,
    _tracing_actor_method_invocation,
)

logger = logging.getLogger(__name__)

# Hook to call with (actor, resources, strategy) on each local actor creation.
_actor_launch_hook = None


@PublicAPI
@client_mode_hook(auto_init=False)
def method(*args, **kwargs):
    """Annotate an actor method.

    .. code-block:: python

        @ray.remote
        class Foo:
            @ray.method(num_returns=2)
            def bar(self):
                return 1, 2

        f = Foo.remote()

        _, _ = f.bar.remote()

    Args:
        num_returns: The number of object refs that should be returned by
            invocations of this actor method.
    """
    valid_kwargs = ["num_returns", "concurrency_group"]
    error_string = (
        "The @ray.method decorator must be applied using at least one of "
        f"the arguments in the list {valid_kwargs}, for example "
        "'@ray.method(num_returns=2)'."
    )
    assert len(args) == 0 and len(kwargs) > 0, error_string
    for key in kwargs:
        key_error_string = (
            f"Unexpected keyword argument to @ray.method: '{key}'. The "
            f"supported keyword arguments are {valid_kwargs}"
        )
        assert key in valid_kwargs, key_error_string

    def annotate_method(method):
        if "num_returns" in kwargs:
            method.__ray_num_returns__ = kwargs["num_returns"]
        if "concurrency_group" in kwargs:
            method.__ray_concurrency_group__ = kwargs["concurrency_group"]
        return method

    return annotate_method


# Create objects to wrap method invocations. This is done so that we can
# invoke methods with actor.method.remote() instead of actor.method().
@PublicAPI
class ActorMethod:
    """A class used to invoke an actor method.

    Note: This class only keeps a weak ref to the actor, unless it has been
    passed to a remote function. This avoids delays in GC of the actor.

    Attributes:
        _actor_ref: A weakref handle to the actor.
        _method_name: The name of the actor method.
        _num_returns: The default number of return values that the method
            invocation should return.
        _decorator: An optional decorator that should be applied to the actor
            method invocation (as opposed to the actor method execution) before
            invoking the method. The decorator must return a function that
            takes in two arguments ("args" and "kwargs"). In most cases, it
            should call the function that was passed into the decorator and
            return the resulting ObjectRefs. For an example, see
            "test_decorated_method" in "python/ray/tests/test_actor.py".
    """

    def __init__(self, actor, method_name, num_returns, decorator=None, hardref=False):
        self._actor_ref = weakref.ref(actor)
        self._method_name = method_name
        self._num_returns = num_returns
        # This is a decorator that is used to wrap the function invocation (as
        # opposed to the function execution). The decorator must return a
        # function that takes in two arguments ("args" and "kwargs"). In most
        # cases, it should call the function that was passed into the decorator
        # and return the resulting ObjectRefs.
        self._decorator = decorator

        # Acquire a hard ref to the actor, this is useful mainly when passing
        # actor method handles to remote functions.
        if hardref:
            self._actor_hard_ref = actor
        else:
            self._actor_hard_ref = None

    def __call__(self, *args, **kwargs):
        raise TypeError(
            "Actor methods cannot be called directly. Instead "
            f"of running 'object.{self._method_name}()', try "
            f"'object.{self._method_name}.remote()'."
        )

    def remote(self, *args, **kwargs):
        return self._remote(args, kwargs)

    def options(self, **options):
        """Convenience method for executing an actor method call with options.

        Same arguments as func._remote(), but returns a wrapped function
        that a non-underscore .remote() can be called on.

        Examples:
            # The following two calls are equivalent.
            >>> actor.my_method._remote(args=[x, y], name="foo", num_returns=2)
            >>> actor.my_method.options(name="foo", num_returns=2).remote(x, y)
        """

        func_cls = self

        class FuncWrapper:
            def remote(self, *args, **kwargs):
                return func_cls._remote(args=args, kwargs=kwargs, **options)

        return FuncWrapper()

    @_tracing_actor_method_invocation
    def _remote(
        self, args=None, kwargs=None, name="", num_returns=None, concurrency_group=None
    ):
        if num_returns is None:
            num_returns = self._num_returns

        def invocation(args, kwargs):
            actor = self._actor_hard_ref or self._actor_ref()
            if actor is None:
                raise RuntimeError("Lost reference to actor")
            return actor._actor_method_call(
                self._method_name,
                args=args,
                kwargs=kwargs,
                name=name,
                num_returns=num_returns,
                concurrency_group_name=concurrency_group,
            )

        # Apply the decorator if there is one.
        if self._decorator is not None:
            invocation = self._decorator(invocation)

        return invocation(args, kwargs)

    def __getstate__(self):
        return {
            "actor": self._actor_ref(),
            "method_name": self._method_name,
            "num_returns": self._num_returns,
            "decorator": self._decorator,
        }

    def __setstate__(self, state):
        self.__init__(
            state["actor"],
            state["method_name"],
            state["num_returns"],
            state["decorator"],
            hardref=True,
        )


class _ActorClassMethodMetadata(object):
    """Metadata for all methods in an actor class. This data can be cached.

    Attributes:
        methods: The actor methods.
        decorators: Optional decorators that should be applied to the
            method invocation function before invoking the actor methods. These
            can be set by attaching the attribute
            "__ray_invocation_decorator__" to the actor method.
        signatures: The signatures of the methods.
        num_returns: The default number of return values for
            each actor method.
    """

    _cache = {}  # This cache will be cleared in ray._private.worker.disconnect()

    def __init__(self):
        class_name = type(self).__name__
        raise TypeError(
            f"{class_name} can not be constructed directly, "
            f"instead of running '{class_name}()', "
            f"try '{class_name}.create()'"
        )

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

        actor_methods = inspect.getmembers(modified_class, is_function_or_method)
        self.methods = dict(actor_methods)

        # Extract the signatures of each of the methods. This will be used
        # to catch some errors if the methods are called with inappropriate
        # arguments.
        self.decorators = {}
        self.signatures = {}
        self.num_returns = {}
        self.concurrency_group_for_methods = {}

        for method_name, method in actor_methods:
            # Whether or not this method requires binding of its first
            # argument. For class and static methods, we do not want to bind
            # the first argument, but we do for instance methods
            method = inspect.unwrap(method)
            is_bound = is_class_method(method) or is_static_method(
                modified_class, method_name
            )

            # Print a warning message if the method signature is not
            # supported. We don't raise an exception because if the actor
            # inherits from a class that has a method whose signature we
            # don't support, there may not be much the user can do about it.
            self.signatures[method_name] = signature.extract_signature(
                method, ignore_first=not is_bound
            )
            # Set the default number of return values for this method.
            if hasattr(method, "__ray_num_returns__"):
                self.num_returns[method_name] = method.__ray_num_returns__
            else:
                self.num_returns[
                    method_name
                ] = ray_constants.DEFAULT_ACTOR_METHOD_NUM_RETURN_VALS

            if hasattr(method, "__ray_invocation_decorator__"):
                self.decorators[method_name] = method.__ray_invocation_decorator__

            if hasattr(method, "__ray_concurrency_group__"):
                self.concurrency_group_for_methods[
                    method_name
                ] = method.__ray_concurrency_group__

        # Update cache.
        cls._cache[actor_creation_function_descriptor] = self
        return self


class _ActorClassMetadata:
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
        accelerator_type: The specified type of accelerator required for the
            node on which this actor runs.
        runtime_env: The runtime environment for this actor.
        scheduling_strategy: Strategy about how to schedule this actor.
        last_export_session_and_job: A pair of the last exported session
            and job to help us to know whether this function was exported.
            This is an imperfect mechanism used to determine if we need to
            export the remote function again. It is imperfect in the sense that
            the actor class definition could be exported multiple times by
            different workers.
        method_meta: The actor method metadata.
    """

    def __init__(
        self,
        language,
        modified_class,
        actor_creation_function_descriptor,
        class_id,
        max_restarts,
        max_task_retries,
        num_cpus,
        num_gpus,
        memory,
        object_store_memory,
        resources,
        accelerator_type,
        runtime_env,
        concurrency_groups,
        scheduling_strategy: SchedulingStrategyT,
    ):
        self.language = language
        self.modified_class = modified_class
        self.actor_creation_function_descriptor = actor_creation_function_descriptor
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
        self.accelerator_type = accelerator_type
        self.runtime_env = runtime_env
        self.concurrency_groups = concurrency_groups
        self.scheduling_strategy = scheduling_strategy
        self.last_export_session_and_job = None
        self.method_meta = _ActorClassMethodMetadata.create(
            modified_class, actor_creation_function_descriptor
        )


@PublicAPI
class ActorClassInheritanceException(TypeError):
    pass


def _process_option_dict(actor_options):
    _filled_options = {}
    arg_names = set(inspect.getfullargspec(_ActorClassMetadata.__init__)[0])
    for k, v in ray_option_utils.actor_options.items():
        if k in arg_names:
            _filled_options[k] = actor_options.get(k, v.default_value)
    _filled_options["runtime_env"] = parse_runtime_env(_filled_options["runtime_env"])
    return _filled_options


@PublicAPI
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
            ActorClassInheritanceException: When ActorClass is inherited.
            AssertionError: If ActorClassInheritanceException is not raised i.e.,
                            conditions for raising it are not met in any
                            iteration of the loop.
            TypeError: In all other cases.
        """
        for base in bases:
            if isinstance(base, ActorClass):
                raise ActorClassInheritanceException(
                    f"Attempted to define subclass '{name}' of actor "
                    f"class '{base.__ray_metadata__.class_name}'. "
                    "Inheriting from actor classes is "
                    "not currently supported. You can instead "
                    "inherit from a non-actor base class and make "
                    "the derived class an actor class (with "
                    "@ray.remote)."
                )

        # This shouldn't be reached because one of the base classes must be
        # an actor class if this was meant to be subclassed.
        assert False, (
            "ActorClass.__init__ should not be called. Please use "
            "the @ray.remote decorator instead."
        )

    def __call__(self, *args, **kwargs):
        """Prevents users from directly instantiating an ActorClass.

        This will be called instead of __init__ when 'ActorClass()' is executed
        because an is an object rather than a metaobject. To properly
        instantiated a remote actor, use 'ActorClass.remote()'.

        Raises:
            Exception: Always.
        """
        raise TypeError(
            "Actors cannot be instantiated directly. "
            f"Instead of '{self.__ray_metadata__.class_name}()', "
            f"use '{self.__ray_metadata__.class_name}.remote()'."
        )

    @classmethod
    def _ray_from_modified_class(
        cls,
        modified_class,
        class_id,
        actor_options,
    ):
        for attribute in [
            "remote",
            "_remote",
            "_ray_from_modified_class",
            "_ray_from_function_descriptor",
        ]:
            if hasattr(modified_class, attribute):
                logger.warning(
                    "Creating an actor from class "
                    f"{modified_class.__name__} overwrites "
                    f"attribute {attribute} of that class"
                )

        # Make sure the actor class we are constructing inherits from the
        # original class so it retains all class properties.
        class DerivedActorClass(cls, modified_class):
            def __init__(self, *args, **kwargs):
                try:
                    cls.__init__(self, *args, **kwargs)
                except Exception as e:
                    # Delegate call to modified_class.__init__ only
                    # if the exception raised by cls.__init__ is
                    # TypeError and not ActorClassInheritanceException(TypeError).
                    # In all other cases proceed with raise e.
                    if isinstance(e, TypeError) and not isinstance(
                        e, ActorClassInheritanceException
                    ):
                        modified_class.__init__(self, *args, **kwargs)
                    else:
                        raise e

        name = f"ActorClass({modified_class.__name__})"
        DerivedActorClass.__module__ = modified_class.__module__
        DerivedActorClass.__name__ = name
        DerivedActorClass.__qualname__ = name
        # Construct the base object.
        self = DerivedActorClass.__new__(DerivedActorClass)
        # Actor creation function descriptor.
        actor_creation_function_descriptor = PythonFunctionDescriptor.from_class(
            modified_class.__ray_actor_class__
        )

        self.__ray_metadata__ = _ActorClassMetadata(
            Language.PYTHON,
            modified_class,
            actor_creation_function_descriptor,
            class_id,
            **_process_option_dict(actor_options),
        )
        self._default_options = actor_options
        if "runtime_env" in self._default_options:
            self._default_options["runtime_env"] = self.__ray_metadata__.runtime_env

        return self

    @classmethod
    def _ray_from_function_descriptor(
        cls,
        language,
        actor_creation_function_descriptor,
        actor_options,
    ):
        self = ActorClass.__new__(ActorClass)
        self.__ray_metadata__ = _ActorClassMetadata(
            language,
            None,
            actor_creation_function_descriptor,
            None,
            **_process_option_dict(actor_options),
        )
        self._default_options = actor_options
        if "runtime_env" in self._default_options:
            self._default_options["runtime_env"] = self.__ray_metadata__.runtime_env
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
        return self._remote(args=args, kwargs=kwargs, **self._default_options)

    def options(self, **actor_options):
        """Configures and overrides the actor instantiation parameters.

        The arguments are the same as those that can be passed
        to :obj:`ray.remote`.

        Examples:

        .. code-block:: python

            @ray.remote(num_cpus=2, resources={"CustomResource": 1})
            class Foo:
                def method(self):
                    return 1
            # Class Foo will require 1 cpu instead of 2.
            # It will also require no custom resources.
            Bar = Foo.options(num_cpus=1, resources=None)
        """

        actor_cls = self

        # override original options
        default_options = self._default_options.copy()
        # "concurrency_groups" could not be used in ".options()",
        # we should remove it before merging options from '@ray.remote'.
        default_options.pop("concurrency_groups", None)
        updated_options = ray_option_utils.update_options(
            default_options, actor_options
        )
        ray_option_utils.validate_actor_options(updated_options, in_options=True)

        # only update runtime_env when ".options()" specifies new runtime_env
        if "runtime_env" in actor_options:
            updated_options["runtime_env"] = parse_runtime_env(
                updated_options["runtime_env"]
            )

        class ActorOptionWrapper:
            def remote(self, *args, **kwargs):
                return actor_cls._remote(args=args, kwargs=kwargs, **updated_options)

            @DeveloperAPI
            def bind(self, *args, **kwargs):
                """
                For Ray DAG building that creates static graph from decorated
                class or functions.
                """
                from ray.dag.class_node import ClassNode

                return ClassNode(
                    actor_cls.__ray_metadata__.modified_class,
                    args,
                    kwargs,
                    updated_options,
                )

        return ActorOptionWrapper()

    @_tracing_actor_creation
    def _remote(self, args=None, kwargs=None, **actor_options):
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
            max_concurrency: The max number of concurrent calls to allow for
                this actor. This only works with direct actor calls. The max
                concurrency defaults to 1 for threaded execution, and 1000 for
                asyncio execution. Note that the execution order is not
                guaranteed when max_concurrency > 1.
            name: The globally unique name for the actor, which can be used
                to retrieve the actor via ray.get_actor(name) as long as the
                actor is still alive.
            namespace: Override the namespace to use for the actor. By default,
                actors are created in an anonymous namespace. The actor can
                be retrieved via ray.get_actor(name=name, namespace=namespace).
            lifetime: Either `None`, which defaults to the actor will fate
                share with its creator and will be deleted once its refcount
                drops to zero, or "detached", which means the actor will live
                as a global object independent of the creator.
            placement_group: (This has been deprecated, please use
                `PlacementGroupSchedulingStrategy` scheduling_strategy)
                the placement group this actor belongs to,
                or None if it doesn't belong to any group. Setting to "default"
                autodetects the placement group based on the current setting of
                placement_group_capture_child_tasks.
            placement_group_bundle_index: (This has been deprecated, please use
                `PlacementGroupSchedulingStrategy` scheduling_strategy)
                the index of the bundle
                if the actor belongs to a placement group, which may be -1 to
                specify any available bundle.
            placement_group_capture_child_tasks: (This has been deprecated,
                please use `PlacementGroupSchedulingStrategy`
                scheduling_strategy)
                Whether or not children tasks
                of this actor should implicitly use the same placement group
                as its parent. It is False by default.
            runtime_env (Dict[str, Any]): Specifies the runtime environment for
                this actor or task and its children (see
                :ref:`runtime-environments` for details).
            max_pending_calls: Set the max number of pending calls
                allowed on the actor handle. When this value is exceeded,
                PendingCallsLimitExceeded will be raised for further tasks.
                Note that this limit is counted per handle. -1 means that the
                number of pending calls is unlimited.
            scheduling_strategy: Strategy about how to schedule this actor.

        Returns:
            A handle to the newly created actor.
        """
        name = actor_options.get("name")
        namespace = actor_options.get("namespace")
        if name is not None:
            if not isinstance(name, str):
                raise TypeError(f"name must be None or a string, got: '{type(name)}'.")
            elif name == "":
                raise ValueError("Actor name cannot be an empty string.")
        if namespace is not None:
            ray._private.utils.validate_namespace(namespace)

        # Handle the get-or-create case.
        if actor_options.get("get_if_exists"):
            try:
                return ray.get_actor(name, namespace=namespace)
            except ValueError:
                # Attempt to create it (may race with other attempts).
                updated_options = actor_options.copy()
                updated_options["get_if_exists"] = False  # prevent infinite loop
                try:
                    return self._remote(args, kwargs, **updated_options)
                except ValueError:
                    # We lost the creation race, ignore.
                    pass
                return ray.get_actor(name, namespace=namespace)

        # We pop the "concurrency_groups" coming from "@ray.remote" here. We no longer
        # need it in "_remote()".
        actor_options.pop("concurrency_groups", None)

        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}
        meta = self.__ray_metadata__
        actor_has_async_methods = (
            len(
                inspect.getmembers(
                    meta.modified_class, predicate=inspect.iscoroutinefunction
                )
            )
            > 0
        )
        is_asyncio = actor_has_async_methods

        if actor_options.get("max_concurrency") is None:
            actor_options["max_concurrency"] = 1000 if is_asyncio else 1

        if client_mode_should_convert(auto_init=True):
            return client_mode_convert_actor(self, args, kwargs, **actor_options)

        # fill actor required options
        for k, v in ray_option_utils.actor_options.items():
            actor_options[k] = actor_options.get(k, v.default_value)
        # "concurrency_groups" already takes effects and should not apply again.
        # Remove the default value here.
        actor_options.pop("concurrency_groups", None)

        # TODO(suquark): cleanup these fields
        max_concurrency = actor_options["max_concurrency"]
        lifetime = actor_options["lifetime"]
        runtime_env = actor_options["runtime_env"]
        placement_group = actor_options["placement_group"]
        placement_group_bundle_index = actor_options["placement_group_bundle_index"]
        placement_group_capture_child_tasks = actor_options[
            "placement_group_capture_child_tasks"
        ]
        scheduling_strategy = actor_options["scheduling_strategy"]
        max_restarts = actor_options["max_restarts"]
        max_task_retries = actor_options["max_task_retries"]
        max_pending_calls = actor_options["max_pending_calls"]

        worker = ray._private.worker.global_worker
        worker.check_connected()

        # Check whether the name is already taken.
        # TODO(edoakes): this check has a race condition because two drivers
        # could pass the check and then create the same named actor. We should
        # instead check this when we create the actor, but that's currently an
        # async call.
        if name is not None:
            try:
                ray.get_actor(name, namespace=namespace)
            except ValueError:  # Name is not taken.
                pass
            else:
                raise ValueError(
                    f"The name {name} (namespace={namespace}) is already "
                    "taken. Please use "
                    "a different name or get the existing actor using "
                    f"ray.get_actor('{name}', namespace='{namespace}')"
                )

        if lifetime is None:
            detached = None
        elif lifetime == "detached":
            detached = True
        elif lifetime == "non_detached":
            detached = False
        else:
            raise ValueError(
                "actor `lifetime` argument must be one of 'detached', "
                "'non_detached' and 'None'."
            )

        # LOCAL_MODE cannot handle cross_language
        if worker.mode == ray.LOCAL_MODE:
            assert (
                not meta.is_cross_language
            ), "Cross language ActorClass cannot be executed locally."

        # Export the actor.
        if not meta.is_cross_language and (
            meta.last_export_session_and_job != worker.current_session_and_job
        ):
            # If this actor class was not exported in this session and job,
            # we need to export this function again, because current GCS
            # doesn't have it.
            meta.last_export_session_and_job = worker.current_session_and_job
            # After serialize / deserialize modified class, the __module__
            # of modified class will be ray.cloudpickle.cloudpickle.
            # So, here pass actor_creation_function_descriptor to make
            # sure export actor class correct.
            worker.function_actor_manager.export_actor_class(
                meta.modified_class,
                meta.actor_creation_function_descriptor,
                meta.method_meta.methods.keys(),
            )

        resources = ray._private.utils.resources_from_ray_options(actor_options)
        # Set the actor's default resources if not already set. First three
        # conditions are to check that no resources were specified in the
        # decorator. Last three conditions are to check that no resources were
        # specified when _remote() was called.
        # TODO(suquark): In the original code, memory is not considered as resources,
        # when deciding the default CPUs. It is strange, but we keep the original
        # semantics in case that it breaks user applications & tests.
        if not set(resources.keys()).difference({"memory", "object_store_memory"}):
            # In the default case, actors acquire no resources for
            # their lifetime, and actor methods will require 1 CPU.
            resources.setdefault("CPU", ray_constants.DEFAULT_ACTOR_CREATION_CPU_SIMPLE)
            actor_method_cpu = ray_constants.DEFAULT_ACTOR_METHOD_CPU_SIMPLE
        else:
            # If any resources are specified (here or in decorator), then
            # all resources are acquired for the actor's lifetime and no
            # resources are associated with methods.
            resources.setdefault(
                "CPU", ray_constants.DEFAULT_ACTOR_CREATION_CPU_SPECIFIED
            )
            actor_method_cpu = ray_constants.DEFAULT_ACTOR_METHOD_CPU_SPECIFIED

        # If the actor methods require CPU resources, then set the required
        # placement resources. If actor_placement_resources is empty, then
        # the required placement resources will be the same as resources.
        actor_placement_resources = {}
        assert actor_method_cpu in [0, 1]
        if actor_method_cpu == 1:
            actor_placement_resources = resources.copy()
            actor_placement_resources["CPU"] += 1
        if meta.is_cross_language:
            creation_args = cross_language._format_args(worker, args, kwargs)
        else:
            function_signature = meta.method_meta.signatures["__init__"]
            creation_args = signature.flatten_args(function_signature, args, kwargs)

        if scheduling_strategy is None or isinstance(
            scheduling_strategy, PlacementGroupSchedulingStrategy
        ):
            # TODO(jjyao) Clean this up once the
            # placement_group option is removed.
            # We should also consider pushing this logic down to c++
            # so that it can be reused by all languages.
            if isinstance(scheduling_strategy, PlacementGroupSchedulingStrategy):
                placement_group = scheduling_strategy.placement_group
                placement_group_bundle_index = (
                    scheduling_strategy.placement_group_bundle_index
                )
                placement_group_capture_child_tasks = (
                    scheduling_strategy.placement_group_capture_child_tasks
                )

            if placement_group_capture_child_tasks is None:
                placement_group_capture_child_tasks = (
                    worker.should_capture_child_tasks_in_placement_group
                )
            placement_group = _configure_placement_group_based_on_context(
                placement_group_capture_child_tasks,
                placement_group_bundle_index,
                resources,
                actor_placement_resources,
                meta.class_name,
                placement_group=placement_group,
            )
            if not placement_group.is_empty:
                scheduling_strategy = PlacementGroupSchedulingStrategy(
                    placement_group,
                    placement_group_bundle_index,
                    placement_group_capture_child_tasks,
                )
            else:
                scheduling_strategy = "DEFAULT"

        serialized_runtime_env_info = None
        if runtime_env is not None:
            serialized_runtime_env_info = get_runtime_env_info(
                runtime_env,
                is_job_runtime_env=False,
                serialize=True,
            )

        concurrency_groups_dict = {}
        if meta.concurrency_groups is None:
            meta.concurrency_groups = []
        for cg_name in meta.concurrency_groups:
            concurrency_groups_dict[cg_name] = {
                "name": cg_name,
                "max_concurrency": meta.concurrency_groups[cg_name],
                "function_descriptors": [],
            }

        # Update methods
        for method_name in meta.method_meta.concurrency_group_for_methods:
            cg_name = meta.method_meta.concurrency_group_for_methods[method_name]
            assert cg_name in concurrency_groups_dict

            module_name = meta.actor_creation_function_descriptor.module_name
            class_name = meta.actor_creation_function_descriptor.class_name
            concurrency_groups_dict[cg_name]["function_descriptors"].append(
                PythonFunctionDescriptor(module_name, method_name, class_name)
            )

        # Update the creation descriptor based on number of arguments
        if meta.is_cross_language:
            func_name = "<init>"
            if meta.language == Language.CPP:
                func_name = meta.actor_creation_function_descriptor.function_name
            meta.actor_creation_function_descriptor = (
                cross_language._get_function_descriptor_for_actor_method(
                    meta.language,
                    meta.actor_creation_function_descriptor,
                    func_name,
                    str(len(args) + len(kwargs)),
                )
            )

        actor_id = worker.core_worker.create_actor(
            meta.language,
            meta.actor_creation_function_descriptor,
            creation_args,
            max_restarts,
            max_task_retries,
            resources,
            actor_placement_resources,
            max_concurrency,
            detached,
            name if name is not None else "",
            namespace if namespace is not None else "",
            is_asyncio,
            # Store actor_method_cpu in actor handle's extension data.
            extension_data=str(actor_method_cpu),
            serialized_runtime_env_info=serialized_runtime_env_info or "{}",
            concurrency_groups_dict=concurrency_groups_dict or dict(),
            max_pending_calls=max_pending_calls,
            scheduling_strategy=scheduling_strategy,
        )

        if _actor_launch_hook:
            _actor_launch_hook(
                meta.actor_creation_function_descriptor, resources, scheduling_strategy
            )

        actor_handle = ActorHandle(
            meta.language,
            actor_id,
            meta.method_meta.decorators,
            meta.method_meta.signatures,
            meta.method_meta.num_returns,
            actor_method_cpu,
            meta.actor_creation_function_descriptor,
            worker.current_session_and_job,
            original_handle=True,
        )

        return actor_handle

    @DeveloperAPI
    def bind(self, *args, **kwargs):
        """
        For Ray DAG building that creates static graph from decorated
        class or functions.
        """
        from ray.dag.class_node import ClassNode

        return ClassNode(
            self.__ray_metadata__.modified_class, args, kwargs, self._default_options
        )


@PublicAPI
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
        _ray_method_num_returns: The default number of return values for
            each method.
        _ray_actor_method_cpus: The number of CPUs required by actor methods.
        _ray_original_handle: True if this is the original actor handle for a
            given actor. If this is true, then the actor will be destroyed when
            this handle goes out of scope.
        _ray_is_cross_language: Whether this actor is cross language.
        _ray_actor_creation_function_descriptor: The function descriptor
            of the actor creation task.
    """

    def __init__(
        self,
        language,
        actor_id,
        method_decorators,
        method_signatures,
        method_num_returns,
        actor_method_cpus,
        actor_creation_function_descriptor,
        session_and_job,
        original_handle=False,
    ):
        self._ray_actor_language = language
        self._ray_actor_id = actor_id
        self._ray_original_handle = original_handle
        self._ray_method_decorators = method_decorators
        self._ray_method_signatures = method_signatures
        self._ray_method_num_returns = method_num_returns
        self._ray_actor_method_cpus = actor_method_cpus
        self._ray_session_and_job = session_and_job
        self._ray_is_cross_language = language != Language.PYTHON
        self._ray_actor_creation_function_descriptor = (
            actor_creation_function_descriptor
        )
        self._ray_function_descriptor = {}

        if not self._ray_is_cross_language:
            assert isinstance(
                actor_creation_function_descriptor, PythonFunctionDescriptor
            )
            module_name = actor_creation_function_descriptor.module_name
            class_name = actor_creation_function_descriptor.class_name
            for method_name in self._ray_method_signatures.keys():
                function_descriptor = PythonFunctionDescriptor(
                    module_name, method_name, class_name
                )
                self._ray_function_descriptor[method_name] = function_descriptor
                method = ActorMethod(
                    self,
                    method_name,
                    self._ray_method_num_returns[method_name],
                    decorator=self._ray_method_decorators.get(method_name),
                )
                setattr(self, method_name, method)

    def __del__(self):
        # Mark that this actor handle has gone out of scope. Once all actor
        # handles are out of scope, the actor will exit.
        if ray._private.worker:
            worker = ray._private.worker.global_worker
            if worker.connected and hasattr(worker, "core_worker"):
                worker.core_worker.remove_actor_handle_reference(self._ray_actor_id)

    def _actor_method_call(
        self,
        method_name: str,
        args: List[Any] = None,
        kwargs: Dict[str, Any] = None,
        name: str = "",
        num_returns: Optional[int] = None,
        concurrency_group_name: Optional[str] = None,
    ):
        """Method execution stub for an actor handle.

        This is the function that executes when
        `actor.method_name.remote(*args, **kwargs)` is called. Instead of
        executing locally, the method is packaged as a task and scheduled
        to the remote actor instance.

        Args:
            method_name: The name of the actor method to execute.
            args: A list of arguments for the actor method.
            kwargs: A dictionary of keyword arguments for the actor method.
            name: The name to give the actor method call task.
            num_returns: The number of return values for the method.

        Returns:
            object_refs: A list of object refs returned by the remote actor
                method.
        """
        worker = ray._private.worker.global_worker

        args = args or []
        kwargs = kwargs or {}
        if self._ray_is_cross_language:
            list_args = cross_language._format_args(worker, args, kwargs)
            function_descriptor = cross_language._get_function_descriptor_for_actor_method(  # noqa: E501
                self._ray_actor_language,
                self._ray_actor_creation_function_descriptor,
                method_name,
                # The signature for xlang should be "{length_of_arguments}" to handle
                # overloaded methods.
                signature=str(len(args) + len(kwargs)),
            )
        else:
            function_signature = self._ray_method_signatures[method_name]

            if not args and not kwargs and not function_signature:
                list_args = []
            else:
                list_args = signature.flatten_args(function_signature, args, kwargs)
            function_descriptor = self._ray_function_descriptor[method_name]

        if worker.mode == ray.LOCAL_MODE:
            assert (
                not self._ray_is_cross_language
            ), "Cross language remote actor method cannot be executed locally."

        object_refs = worker.core_worker.submit_actor_task(
            self._ray_actor_language,
            self._ray_actor_id,
            function_descriptor,
            list_args,
            name,
            num_returns,
            self._ray_actor_method_cpus,
            concurrency_group_name if concurrency_group_name is not None else b"",
        )

        if len(object_refs) == 1:
            object_refs = object_refs[0]
        elif len(object_refs) == 0:
            object_refs = None

        return object_refs

    def __getattr__(self, item):
        if not self._ray_is_cross_language:
            raise AttributeError(
                f"'{type(self).__name__}' object has " f"no attribute '{item}'"
            )
        if item in ["__ray_terminate__", "__ray_checkpoint__"]:

            class FakeActorMethod(object):
                def __call__(self, *args, **kwargs):
                    raise TypeError(
                        "Actor methods cannot be called directly. Instead "
                        "of running 'object.{}()', try 'object.{}.remote()'.".format(
                            item, item
                        )
                    )

                def remote(self, *args, **kwargs):
                    logger.warning(
                        f"Actor method {item} is not supported by cross language."
                    )

            return FakeActorMethod()

        return ActorMethod(
            self,
            item,
            ray_constants.
            # Currently, we use default num returns
            DEFAULT_ACTOR_METHOD_NUM_RETURN_VALS,
            # Currently, cross-lang actor method not support decorator
            decorator=None,
        )

    # Make tab completion work.
    def __dir__(self):
        return self._ray_method_signatures.keys()

    def __repr__(self):
        return (
            "Actor("
            f"{self._ray_actor_creation_function_descriptor.class_name}, "
            f"{self._actor_id.hex()})"
        )

    @property
    def _actor_id(self):
        return self._ray_actor_id

    def _serialization_helper(self):
        """This is defined in order to make pickling work.

        Returns:
            A dictionary of the information needed to reconstruct the object.
        """
        worker = ray._private.worker.global_worker
        worker.check_connected()

        if hasattr(worker, "core_worker"):
            # Non-local mode
            state = worker.core_worker.serialize_actor_handle(self._ray_actor_id)
        else:
            # Local mode
            state = (
                {
                    "actor_language": self._ray_actor_language,
                    "actor_id": self._ray_actor_id,
                    "method_decorators": self._ray_method_decorators,
                    "method_signatures": self._ray_method_signatures,
                    "method_num_returns": self._ray_method_num_returns,
                    "actor_method_cpus": self._ray_actor_method_cpus,
                    "actor_creation_function_descriptor": self._ray_actor_creation_function_descriptor,  # noqa: E501
                },
                None,
            )

        return state

    @classmethod
    def _deserialization_helper(cls, state, outer_object_ref=None):
        """This is defined in order to make pickling work.

        Args:
            state: The serialized state of the actor handle.
            outer_object_ref: The ObjectRef that the serialized actor handle
                was contained in, if any. This is used for counting references
                to the actor handle.

        """
        worker = ray._private.worker.global_worker
        worker.check_connected()

        if hasattr(worker, "core_worker"):
            # Non-local mode
            return worker.core_worker.deserialize_and_register_actor_handle(
                state, outer_object_ref
            )
        else:
            # Local mode
            return cls(
                # TODO(swang): Accessing the worker's current task ID is not
                # thread-safe.
                state["actor_language"],
                state["actor_id"],
                state["method_decorators"],
                state["method_signatures"],
                state["method_num_returns"],
                state["actor_method_cpus"],
                state["actor_creation_function_descriptor"],
                worker.current_session_and_job,
            )

    def __reduce__(self):
        """This code path is used by pickling but not by Ray forking."""
        state = self._serialization_helper()
        return ActorHandle._deserialization_helper, state


def _modify_class(cls):
    # cls has been modified.
    if hasattr(cls, "__ray_actor_class__"):
        return cls

    # Give an error if cls is an old-style class.
    if not issubclass(cls, object):
        raise TypeError(
            "The @ray.remote decorator cannot be applied to old-style "
            "classes. In Python 2, you must declare the class with "
            "'class ClassName(object):' instead of 'class ClassName:'."
        )

    # Modify the class to have an additional method that will be used for
    # terminating the worker.
    class Class(cls):
        __ray_actor_class__ = cls  # The original actor class

        def __ray_terminate__(self):
            worker = ray._private.worker.global_worker
            if worker.mode != ray.LOCAL_MODE:
                ray.actor.exit_actor()

    Class.__module__ = cls.__module__
    Class.__name__ = cls.__name__

    if not is_function_or_method(getattr(Class, "__init__", None)):
        # Add __init__ if it does not exist.
        # Actor creation will be executed with __init__ together.

        # Assign an __init__ function will avoid many checks later on.
        def __init__(self):
            pass

        Class.__init__ = __init__

    return Class


def _make_actor(cls, actor_options):
    Class = _modify_class(cls)
    _inject_tracing_into_class(Class)

    if "max_restarts" in actor_options:
        if actor_options["max_restarts"] != -1:  # -1 represents infinite restart
            # Make sure we don't pass too big of an int to C++, causing
            # an overflow.
            actor_options["max_restarts"] = min(
                actor_options["max_restarts"], ray_constants.MAX_INT64_VALUE
            )

    return ActorClass._ray_from_modified_class(
        Class,
        ActorClassID.from_random(),
        actor_options,
    )


@PublicAPI
def exit_actor():
    """Intentionally exit the current actor.

    This function is used to disconnect an actor and exit the worker.
    Any ``atexit`` handlers installed in the actor will be run.

    Raises:
        Exception: An exception is raised if this is a driver or this
            worker is not an actor.
    """
    worker = ray._private.worker.global_worker
    if worker.mode == ray.WORKER_MODE and not worker.actor_id.is_nil():
        # Intentionally disconnect the core worker from the raylet so the
        # raylet won't push an error message to the driver.
        ray._private.worker.disconnect()
        # Disconnect global state from GCS.
        ray._private.state.state.disconnect()

        # In asyncio actor mode, we can't raise SystemExit because it will just
        # quit the asycnio event loop thread, not the main thread. Instead, we
        # raise a custom error to the main thread to tell it to exit.
        if worker.core_worker.current_actor_is_asyncio():
            raise AsyncioActorExit()

        # Set a flag to indicate this is an intentional actor exit. This
        # reduces log verbosity.
        exit = SystemExit(0)
        exit.is_ray_terminate = True
        exit.ray_terminate_msg = "exit_actor() is called."
        raise exit
        assert False, "This process should have terminated."
    else:
        raise TypeError("exit_actor called on a non-actor worker.")
