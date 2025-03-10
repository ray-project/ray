import inspect
import logging
import weakref
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import ray._private.ray_constants as ray_constants
import ray._private.signature as signature
import ray._private.worker
import ray._raylet
from ray import ActorClassID, Language, cross_language
from ray._private import ray_option_utils
from ray._private.async_compat import has_async_methods
from ray._private.auto_init_hook import wrap_auto_init
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
from ray._private.ray_option_utils import _warn_if_using_deprecated_placement_group
from ray._private.utils import get_runtime_env_info, parse_runtime_env
from ray._raylet import (
    STREAMING_GENERATOR_RETURN,
    ObjectRefGenerator,
    PythonFunctionDescriptor,
    raise_sys_exit_with_custom_error_message,
)
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
@client_mode_hook
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
    valid_kwargs = [
        "num_returns",
        "concurrency_group",
        "max_task_retries",
        "retry_exceptions",
        "_generator_backpressure_num_objects",
        "enable_task_events",
    ]
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
        if "max_task_retries" in kwargs:
            method.__ray_max_task_retries__ = kwargs["max_task_retries"]
        if "retry_exceptions" in kwargs:
            method.__ray_retry_exceptions__ = kwargs["retry_exceptions"]
        if "concurrency_group" in kwargs:
            method.__ray_concurrency_group__ = kwargs["concurrency_group"]
        if "_generator_backpressure_num_objects" in kwargs:
            method.__ray_generator_backpressure_num_objects__ = kwargs[
                "_generator_backpressure_num_objects"
            ]
        if "enable_task_events" in kwargs and kwargs["enable_task_events"] is not None:
            method.__ray_enable_task_events__ = kwargs["enable_task_events"]
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
            invocation should return. If None is given, it uses
            DEFAULT_ACTOR_METHOD_NUM_RETURN_VALS for a normal actor task
            and "streaming" for a generator task (when `is_generator` is True).
        _max_task_retries: Number of retries on method failure.
        _retry_exceptions: Boolean of whether you want to retry all user-raised
            exceptions, or a list of allowlist exceptions to retry.
        _is_generator: True if a given method is a Python generator.
        _generator_backpressure_num_objects: Generator-only config.
            If a number of unconsumed objects reach this threshold,
            a actor task stop pausing.
        enable_task_events: True if task events is enabled, i.e., task events from
            the actor should be reported. Defaults to True.
        _signature: The signature of the actor method. It is None only when cross
            language feature is used.
        _decorator: An optional decorator that should be applied to the actor
            method invocation (as opposed to the actor method execution) before
            invoking the method. The decorator must return a function that
            takes in two arguments ("args" and "kwargs"). In most cases, it
            should call the function that was passed into the decorator and
            return the resulting ObjectRefs. For an example, see
            "test_decorated_method" in "python/ray/tests/test_actor.py".
    """

    def __init__(
        self,
        actor,
        method_name,
        num_returns: Optional[Union[int, Literal["streaming"]]],
        max_task_retries: int,
        retry_exceptions: Union[bool, list, tuple],
        is_generator: bool,
        generator_backpressure_num_objects: int,
        enable_task_events: bool,
        decorator=None,
        signature: Optional[List[inspect.Parameter]] = None,
        hardref=False,
    ):
        self._actor_ref = weakref.ref(actor)
        self._method_name = method_name
        self._num_returns = num_returns

        # Default case.
        if self._num_returns is None:
            if is_generator:
                self._num_returns = "streaming"
            else:
                self._num_returns = ray_constants.DEFAULT_ACTOR_METHOD_NUM_RETURN_VALS

        self._max_task_retries = max_task_retries
        self._retry_exceptions = retry_exceptions
        self._is_generator = is_generator
        self._generator_backpressure_num_objects = generator_backpressure_num_objects
        self._enable_task_events = enable_task_events
        self._signature = signature
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

    @DeveloperAPI
    def bind(self, *args, **kwargs):
        """
        Bind arguments to the actor method for Ray DAG building.
        """
        return self._bind(args, kwargs)

    def _get_callee_info(self):
        """
        get the callee info of the actor method
        this is needed for the insight monitor to record the call
        """
        callee_func = self._method_name
        actor = self._actor_ref()
        callee_class = None
        if actor is not None:
            callee_class = (
                actor._ray_actor_creation_function_descriptor.class_name
                + ":"
                + actor._ray_actor_id.hex()
            )
        return callee_class, callee_func

    def remote(self, *args, **kwargs):
        from ray.util.insight import record_control_flow

        callee_class, callee_func = self._get_callee_info()
        # report the call info to the insight monitor
        record_control_flow(callee_class, callee_func)
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

            @DeveloperAPI
            def bind(self, *args, **kwargs):
                return func_cls._bind(args=args, kwargs=kwargs, **options)

        return FuncWrapper()

    @wrap_auto_init
    @_tracing_actor_method_invocation
    def _bind(
        self,
        args=None,
        kwargs=None,
        name="",
        num_returns=None,
        concurrency_group=None,
        _generator_backpressure_num_objects=None,
    ) -> Union["ray.dag.ClassMethodNode", Tuple["ray.dag.ClassMethodNode", ...]]:
        from ray.dag.class_node import (
            BIND_INDEX_KEY,
            IS_CLASS_METHOD_OUTPUT_KEY,
            PARENT_CLASS_NODE_KEY,
            PREV_CLASS_METHOD_CALL_KEY,
            ClassMethodNode,
        )

        # TODO(sang): unify option passing
        options = {
            "name": name,
            "num_returns": num_returns,
            "concurrency_group": concurrency_group,
            "_generator_backpressure_num_objects": _generator_backpressure_num_objects,
        }

        actor = self._actor_ref()
        if actor is None:
            # Ref is GC'ed. It happens when the actor handle is GC'ed
            # when bind is called.
            raise RuntimeError("Lost reference to actor")

        other_args_to_resolve = {
            PARENT_CLASS_NODE_KEY: actor,
            PREV_CLASS_METHOD_CALL_KEY: None,
            BIND_INDEX_KEY: actor._ray_dag_bind_index,
        }
        actor._ray_dag_bind_index += 1

        assert (
            self._signature is not None
        ), "self._signature should be set for .bind API."
        try:
            signature.validate_args(self._signature, args, kwargs)
        except TypeError as e:
            signature_copy = self._signature.copy()
            if len(signature_copy) > 0 and signature_copy[-1].name == "_ray_trace_ctx":
                # Remove the trace context arg for readability.
                signature_copy.pop(-1)
            signature_copy = inspect.Signature(parameters=signature_copy)
            raise TypeError(
                f"{str(e)}. The function `{self._method_name}` has a signature "
                f"`{signature_copy}`, but the given arguments to `bind` doesn't "
                f"match. args: {args}. kwargs: {kwargs}."
            ) from None

        node = ClassMethodNode(
            self._method_name,
            args,
            kwargs,
            options,
            other_args_to_resolve=other_args_to_resolve,
        )

        if node.num_returns > 1:
            output_nodes: List[ClassMethodNode] = []
            for i in range(node.num_returns):
                output_node = ClassMethodNode(
                    f"return_idx_{i}",
                    (node, i),
                    dict(),
                    dict(),
                    {IS_CLASS_METHOD_OUTPUT_KEY: True, PARENT_CLASS_NODE_KEY: actor},
                )
                output_nodes.append(output_node)
            return tuple(output_nodes)
        else:
            return node

    @wrap_auto_init
    @_tracing_actor_method_invocation
    def _remote(
        self,
        args=None,
        kwargs=None,
        name="",
        num_returns=None,
        max_task_retries=None,
        retry_exceptions=None,
        concurrency_group=None,
        _generator_backpressure_num_objects=None,
        enable_task_events=None,
    ):
        if num_returns is None:
            num_returns = self._num_returns
        if max_task_retries is None:
            max_task_retries = self._max_task_retries
        if max_task_retries is None:
            max_task_retries = 0
        if retry_exceptions is None:
            retry_exceptions = self._retry_exceptions
        if enable_task_events is None:
            enable_task_events = self._enable_task_events
        if _generator_backpressure_num_objects is None:
            _generator_backpressure_num_objects = (
                self._generator_backpressure_num_objects
            )

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
                max_task_retries=max_task_retries,
                retry_exceptions=retry_exceptions,
                concurrency_group_name=concurrency_group,
                generator_backpressure_num_objects=(
                    _generator_backpressure_num_objects
                ),
                enable_task_events=enable_task_events,
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
            "max_task_retries": self._max_task_retries,
            "retry_exceptions": self._retry_exceptions,
            "decorator": self._decorator,
            "is_generator": self._is_generator,
            "generator_backpressure_num_objects": self._generator_backpressure_num_objects,  # noqa
            "enable_task_events": self._enable_task_events,
        }

    def __setstate__(self, state):
        self.__init__(
            state["actor"],
            state["method_name"],
            state["num_returns"],
            state["max_task_retries"],
            state["retry_exceptions"],
            state["is_generator"],
            state["generator_backpressure_num_objects"],
            state["enable_task_events"],
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
        max_task_retries: Number of retries on method failure.
        retry_exceptions: Boolean of whether you want to retry all user-raised
            exceptions, or a list of allowlist exceptions to retry, for each method.
        enable_task_events: True if tracing is enabled, i.e., task events from
            the actor should be reported. Defaults to True.
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
        self.max_task_retries = {}
        self.retry_exceptions = {}
        self.method_is_generator = {}
        self.enable_task_events = {}
        self.generator_backpressure_num_objects = {}
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
                self.num_returns[method_name] = None

            # Only contains entries from `@ray.method(max_task_retries=...)`
            # Ray may not populate the others with max_task_retries here because you may
            # have set in `actor.method.options(max_task_retries=...)`. So Ray always
            # stores max_task_retries both from the method and from the actor, and
            # favors the former.
            if hasattr(method, "__ray_max_task_retries__"):
                self.max_task_retries[method_name] = method.__ray_max_task_retries__

            if hasattr(method, "__ray_retry_exceptions__"):
                self.retry_exceptions[method_name] = method.__ray_retry_exceptions__

            if hasattr(method, "__ray_invocation_decorator__"):
                self.decorators[method_name] = method.__ray_invocation_decorator__

            if hasattr(method, "__ray_concurrency_group__"):
                self.concurrency_group_for_methods[
                    method_name
                ] = method.__ray_concurrency_group__

            if hasattr(method, "__ray_enable_task_events__"):
                self.enable_task_events[method_name] = method.__ray_enable_task_events__

            is_generator = inspect.isgeneratorfunction(
                method
            ) or inspect.isasyncgenfunction(method)
            self.method_is_generator[method_name] = is_generator

            if hasattr(method, "__ray_generator_backpressure_num_objects__"):
                self.generator_backpressure_num_objects[
                    method_name
                ] = method.__ray_generator_backpressure_num_objects__

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
        resources: The default resources required by the actor creation task.
        accelerator_type: The specified type of accelerator required for the
            node on which this actor runs.
            See :ref:`accelerator types <accelerator_types>`.
        runtime_env: The runtime environment for this actor.
        scheduling_strategy: Strategy about how to schedule this actor.
        last_export_cluster_and_job: A pair of the last exported cluster
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
        self.last_export_cluster_and_job = None
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

        Args:
            num_cpus: The quantity of CPU cores to reserve
                for this task or for the lifetime of the actor.
            num_gpus: The quantity of GPUs to reserve
                for this task or for the lifetime of the actor.
            resources (Dict[str, float]): The quantity of various custom resources
                to reserve for this task or for the lifetime of the actor.
                This is a dictionary mapping strings (resource names) to floats.
            accelerator_type: If specified, requires that the task or actor run
                on a node with the specified type of accelerator.
                See :ref:`accelerator types <accelerator_types>`.
            memory: The heap memory request in bytes for this task/actor,
                rounded down to the nearest integer.
            object_store_memory: The object store memory request for actors only.
            max_restarts: This specifies the maximum
                number of times that the actor should be restarted when it dies
                unexpectedly. The minimum valid value is 0 (default),
                which indicates that the actor doesn't need to be restarted.
                A value of -1 indicates that an actor should be restarted
                indefinitely.
            max_task_retries: How many times to
                retry an actor task if the task fails due to a runtime error,
                e.g., the actor has died. If set to -1, the system will
                retry the failed task until the task succeeds, or the actor
                has reached its max_restarts limit. If set to `n > 0`, the
                system will retry the failed task up to n times, after which the
                task will throw a `RayActorError` exception upon :obj:`ray.get`.
                Note that Python exceptions may trigger retries *only if*
                `retry_exceptions` is set for the method, in that case when
                `max_task_retries` runs out the task will rethrow the exception from
                the task. You can override this number with the method's
                `max_task_retries` option in `@ray.method` decorator or in `.option()`.
            max_pending_calls: Set the max number of pending calls
                allowed on the actor handle. When this value is exceeded,
                PendingCallsLimitExceeded will be raised for further tasks.
                Note that this limit is counted per handle. -1 means that the
                number of pending calls is unlimited.
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
            runtime_env (Dict[str, Any]): Specifies the runtime environment for
                this actor or task and its children. See
                :ref:`runtime-environments` for detailed documentation.
            scheduling_strategy: Strategy about how to
                schedule a remote function or actor. Possible values are
                None: ray will figure out the scheduling strategy to use, it
                will either be the PlacementGroupSchedulingStrategy using parent's
                placement group if parent has one and has
                placement_group_capture_child_tasks set to true,
                or "DEFAULT";
                "DEFAULT": default hybrid scheduling;
                "SPREAD": best effort spread scheduling;
                `PlacementGroupSchedulingStrategy`:
                placement group based scheduling;
                `NodeAffinitySchedulingStrategy`:
                node id based affinity scheduling.
            _metadata: Extended options for Ray libraries. For example,
                _metadata={"workflows.io/options": <workflow options>} for
                Ray workflows.
            enable_task_events: True if tracing is enabled, i.e., task events from
                the actor should be reported. Defaults to True.

        Examples:

        .. code-block:: python

            @ray.remote(num_cpus=2, resources={"CustomResource": 1})
            class Foo:
                def method(self):
                    return 1
            # Class Bar will require 1 cpu instead of 2.
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

    @wrap_auto_init
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
            enable_task_events: True if tracing is enabled, i.e., task events from
                the actor should be reported. Defaults to True.
            _labels: The key-value labels of the actor.

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
        is_asyncio = has_async_methods(meta.modified_class)

        if actor_options.get("max_concurrency") is None:
            actor_options["max_concurrency"] = (
                ray_constants.DEFAULT_MAX_CONCURRENCY_ASYNC
                if is_asyncio
                else ray_constants.DEFAULT_MAX_CONCURRENCY_THREADED
            )

        if client_mode_should_convert():
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

        # Override enable_task_events to default for actor if not specified (i.e. None)
        enable_task_events = actor_options.get("enable_task_events")

        if scheduling_strategy is None or not isinstance(
            scheduling_strategy, PlacementGroupSchedulingStrategy
        ):
            _warn_if_using_deprecated_placement_group(actor_options, 3)

        worker = ray._private.worker.global_worker
        worker.check_connected()

        if worker.mode != ray._private.worker.WORKER_MODE:
            from ray._private.usage import usage_lib

            usage_lib.record_library_usage("core")

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
            meta.last_export_cluster_and_job != worker.current_cluster_and_job
        ):
            # If this actor class was not exported in this cluster and job,
            # we need to export this function again, because current GCS
            # doesn't have it.

            # After serialize / deserialize modified class, the __module__
            # of modified class will be ray.cloudpickle.cloudpickle.
            # So, here pass actor_creation_function_descriptor to make
            # sure export actor class correct.
            worker.function_actor_manager.export_actor_class(
                meta.modified_class,
                meta.actor_creation_function_descriptor,
                meta.method_meta.methods.keys(),
            )
            meta.last_export_cluster_and_job = worker.current_cluster_and_job

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
            enable_task_events=enable_task_events,
            labels=actor_options.get("_labels"),
        )

        if _actor_launch_hook:
            _actor_launch_hook(
                meta.actor_creation_function_descriptor, resources, scheduling_strategy
            )

        actor_handle = ActorHandle(
            meta.language,
            actor_id,
            max_task_retries,
            enable_task_events,
            meta.method_meta.method_is_generator,
            meta.method_meta.decorators,
            meta.method_meta.signatures,
            meta.method_meta.num_returns,
            meta.method_meta.max_task_retries,
            meta.method_meta.retry_exceptions,
            meta.method_meta.generator_backpressure_num_objects,
            meta.method_meta.enable_task_events,
            actor_method_cpu,
            meta.actor_creation_function_descriptor,
            worker.current_cluster_and_job,
            original_handle=True,
        )

        callee_class, callee_func = self._get_callee_info(actor_handle)
        from ray.util.insight import record_control_flow

        # report the call info to the insight monitor
        record_control_flow(callee_class, callee_func)

        return actor_handle

    def _get_callee_info(self, actor_handle):
        """
        get the callee info of the actor method
        this is needed for the insight monitor to record the call
        """
        callee_func = "__init__"
        callee_class = None
        if actor_handle is not None:
            callee_class = (
                actor_handle._ray_actor_creation_function_descriptor.class_name
                + ":"
                + actor_handle._ray_actor_id.hex()
            )
        return callee_class, callee_func

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
        _ray_enable_task_events: The default value of whether task events is
            enabled, i.e., task events from the actor should be reported.
        _ray_method_is_generator: Map of method name -> if it is a generator
            method.
        _ray_method_decorators: Optional decorators for the function
            invocation. This can be used to change the behavior on the
            invocation side, whereas a regular decorator can be used to change
            the behavior on the execution side.
        _ray_method_signatures: The signatures of the actor methods.
        _ray_method_max_task_retries: Max number of retries on method failure.
        _ray_method_num_returns: The default number of return values for
            each method.
        _ray_method_retry_exceptions: The default value of boolean of whether you want
            to retry all user-raised exceptions, or a list of allowlist exceptions to
            retry.
        _ray_method_generator_backpressure_num_objects: Generator-only
            config. The max number of objects to generate before it
            starts pausing a generator.
        _ray_method_enable_task_events: The value of whether task
            tracing is enabled for the actor methods. This overrides the
            actor's default value (`_ray_enable_task_events`).
        _ray_actor_method_cpus: The number of CPUs required by actor methods.
        _ray_original_handle: True if this is the original actor handle for a
            given actor. If this is true, then the actor will be destroyed when
            this handle goes out of scope.
        _ray_weak_ref: True means that this handle does not count towards the
            distributed ref count for the actor, i.e. the actor may be GCed
            while this handle is still in scope. This is set to True if the
            handle was created by getting an actor by name or by getting the
            self handle. It is set to False if this is the original handle or
            if it was created by passing the original handle through task args
            and returns.
        _ray_is_cross_language: Whether this actor is cross language.
        _ray_actor_creation_function_descriptor: The function descriptor
            of the actor creation task.
    """

    def __init__(
        self,
        language,
        actor_id,
        max_task_retries: Optional[int],
        enable_task_events: bool,
        method_is_generator: Dict[str, bool],
        method_decorators,
        method_signatures,
        method_num_returns: Dict[str, Union[int, Literal["streaming"]]],
        method_max_task_retries: Dict[str, int],
        method_retry_exceptions: Dict[str, Union[bool, list, tuple]],
        method_generator_backpressure_num_objects: Dict[str, int],
        method_enable_task_events: Dict[str, bool],
        actor_method_cpus: int,
        actor_creation_function_descriptor,
        cluster_and_job,
        original_handle=False,
        weak_ref: bool = False,
    ):
        self._ray_actor_language = language
        self._ray_actor_id = actor_id
        self._ray_max_task_retries = max_task_retries
        self._ray_original_handle = original_handle
        self._ray_weak_ref = weak_ref
        self._ray_enable_task_events = enable_task_events

        self._ray_method_is_generator = method_is_generator
        self._ray_method_decorators = method_decorators
        self._ray_method_signatures = method_signatures
        self._ray_method_num_returns = method_num_returns
        self._ray_method_max_task_retries = method_max_task_retries
        self._ray_method_retry_exceptions = method_retry_exceptions
        self._ray_method_generator_backpressure_num_objects = (
            method_generator_backpressure_num_objects
        )
        self._ray_method_enable_task_events = method_enable_task_events
        self._ray_actor_method_cpus = actor_method_cpus
        self._ray_cluster_and_job = cluster_and_job
        self._ray_is_cross_language = language != Language.PYTHON
        self._ray_actor_creation_function_descriptor = (
            actor_creation_function_descriptor
        )
        self._ray_function_descriptor = {}
        # This is incremented each time `bind()` is called on an actor handle
        # (in Ray DAGs), therefore capturing the bind order of the actor methods.
        # TODO: this does not work properly if the caller has two copies of the
        # same actor handle, and needs to be fixed.
        self._ray_dag_bind_index = 0

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
                    self._ray_method_max_task_retries.get(
                        method_name, self._ray_max_task_retries
                    )
                    or 0,  # never None
                    self._ray_method_retry_exceptions.get(method_name),
                    self._ray_method_is_generator[method_name],
                    self._ray_method_generator_backpressure_num_objects.get(
                        method_name
                    ),  # noqa
                    self._ray_method_enable_task_events.get(
                        method_name,
                        self._ray_enable_task_events,  # Use actor's default value
                    ),
                    decorator=self._ray_method_decorators.get(method_name),
                    signature=self._ray_method_signatures[method_name],
                )
                setattr(self, method_name, method)

    def __del__(self):
        # Weak references don't count towards the distributed ref count, so no
        # need to decrement the ref count.
        if self._ray_weak_ref:
            return

        try:
            # Mark that this actor handle has gone out of scope. Once all actor
            # handles are out of scope, the actor will exit.
            if ray._private.worker:
                worker = ray._private.worker.global_worker
                if worker.connected and hasattr(worker, "core_worker"):
                    worker.core_worker.remove_actor_handle_reference(self._ray_actor_id)
        except AttributeError:
            # Suppress the attribtue error which is caused by
            # python destruction ordering issue.
            # It only happen when python exits.
            pass

    def _actor_method_call(
        self,
        method_name: str,
        args: List[Any] = None,
        kwargs: Dict[str, Any] = None,
        name: str = "",
        num_returns: Optional[Union[int, Literal["streaming"]]] = None,
        max_task_retries: int = None,
        retry_exceptions: Union[bool, list, tuple] = None,
        concurrency_group_name: Optional[str] = None,
        generator_backpressure_num_objects: Optional[int] = None,
        enable_task_events: Optional[bool] = None,
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
            max_task_retries: Number of retries when method fails.
            retry_exceptions: Boolean of whether you want to retry all user-raised
                exceptions, or a list of allowlist exceptions to retry.
            enable_task_events: True if tracing is enabled, i.e., task events from
                the actor should be reported.

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

        if num_returns == "dynamic":
            num_returns = -1
        elif num_returns == "streaming":
            # TODO(sang): This is a temporary private API.
            # Remove it when we migrate to the streaming generator.
            num_returns = ray._raylet.STREAMING_GENERATOR_RETURN

        retry_exception_allowlist = None
        if retry_exceptions is None:
            retry_exceptions = False
        elif isinstance(retry_exceptions, (list, tuple)):
            retry_exception_allowlist = tuple(retry_exceptions)
            retry_exceptions = True
        assert isinstance(
            retry_exceptions, bool
        ), "retry_exceptions can either be \
            boolean or list/tuple of exception types."

        if generator_backpressure_num_objects is None:
            generator_backpressure_num_objects = -1

        object_refs = worker.core_worker.submit_actor_task(
            self._ray_actor_language,
            self._ray_actor_id,
            function_descriptor,
            list_args,
            name,
            num_returns,
            max_task_retries,
            retry_exceptions,
            retry_exception_allowlist,
            self._ray_actor_method_cpus,
            concurrency_group_name if concurrency_group_name is not None else b"",
            generator_backpressure_num_objects,
            enable_task_events,
        )

        if num_returns == STREAMING_GENERATOR_RETURN:
            # Streaming generator will return a single ref
            # that is for the generator task.
            assert len(object_refs) == 1
            generator_ref = object_refs[0]
            return ObjectRefGenerator(generator_ref, worker)
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
        if item in ["__ray_terminate__"]:

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
            self,  # actor
            item,  # method_name
            ray_constants.DEFAULT_ACTOR_METHOD_NUM_RETURN_VALS,
            0,  # max_task_retries
            False,  # retry_exceptions
            False,  # is_generator
            self._ray_method_generator_backpressure_num_objects.get(item, -1),
            self._ray_enable_task_events,  # enable_task_events
            # Currently, cross-lang actor method not support decorator
            decorator=None,
            signature=None,
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

    def __hash__(self):
        return hash(self._actor_id)

    def __eq__(self, __value):
        return hash(self) == hash(__value)

    @property
    def _actor_id(self):
        return self._ray_actor_id

    def _get_local_state(self):
        """Get the local actor state.

        NOTE: this method only returns accurate actor state
        after a first actor method call is made against
        this actor handle due to https://github.com/ray-project/ray/pull/24600.

        Returns:
           ActorTableData.ActorState or None if the state is unknown.
        """
        worker = ray._private.worker.global_worker
        worker.check_connected()
        return worker.core_worker.get_local_actor_state(self._ray_actor_id)

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
                    "max_task_retries": self._ray_max_task_retries,
                    "enable_task_events": self._enable_task_events,
                    "method_is_generator": self._ray_method_is_generator,
                    "method_decorators": self._ray_method_decorators,
                    "method_signatures": self._ray_method_signatures,
                    "method_num_returns": self._ray_method_num_returns,
                    "method_max_task_retries": self._ray_method_max_task_retries,
                    "method_retry_exceptions": self._ray_method_retry_exceptions,
                    "method_generator_backpressure_num_objects": (
                        self._ray_method_generator_backpressure_num_objects
                    ),
                    "method_enable_task_events": self._ray_method_enable_task_events,
                    "actor_method_cpus": self._ray_actor_method_cpus,
                    "actor_creation_function_descriptor": self._ray_actor_creation_function_descriptor,  # noqa: E501
                },
                None,
            )

        return (*state, self._ray_weak_ref)

    @classmethod
    def _deserialization_helper(cls, state, weak_ref: bool, outer_object_ref=None):
        """This is defined in order to make pickling work.

        Args:
            state: The serialized state of the actor handle.
            outer_object_ref: The ObjectRef that the serialized actor handle
                was contained in, if any. This is used for counting references
                to the actor handle.
            weak_ref: Whether this was serialized from an actor handle with a
                weak ref to the actor.

        """
        worker = ray._private.worker.global_worker
        worker.check_connected()

        if hasattr(worker, "core_worker"):
            # Non-local mode
            return worker.core_worker.deserialize_and_register_actor_handle(
                state,
                outer_object_ref,
                weak_ref,
            )
        else:
            # Local mode
            assert worker.current_cluster_and_job == state["current_cluster_and_job"]
            return cls(
                # TODO(swang): Accessing the worker's current task ID is not
                # thread-safe.
                state["actor_language"],
                state["actor_id"],
                state["max_task_retries"],
                state["enable_task_events"],
                state["method_is_generator"],
                state["method_decorators"],
                state["method_signatures"],
                state["method_num_returns"],
                state["method_max_task_retries"],
                state["method_retry_exceptions"],
                state["method_generator_backpressure_num_objects"],
                state["method_enable_task_events"],
                state["actor_method_cpus"],
                state["actor_creation_function_descriptor"],
                state["current_cluster_and_job"],
            )

    def __reduce__(self):
        """This code path is used by pickling but not by Ray forking."""
        (serialized, _, weak_ref) = self._serialization_helper()
        # There is no outer object ref when the actor handle is
        # deserialized out-of-band using pickle.
        return ActorHandle._deserialization_helper, (serialized, weak_ref, None)


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

    # Modify the class to have additional default methods.
    class Class(cls):
        __ray_actor_class__ = cls  # The original actor class

        def __ray_ready__(self):
            return True

        def __ray_call__(self, fn, *args, **kwargs):
            return fn(self, *args, **kwargs)

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

    This API can be used only inside an actor. Use ray.kill
    API if you'd like to kill an actor using actor handle.

    When the API is called, the actor raises an exception and exits.
    Any queued methods will fail. Any ``atexit``
    handlers installed in the actor will be run.

    Raises:
        TypeError: An exception is raised if this is a driver or this
            worker is not an actor.
    """
    worker = ray._private.worker.global_worker
    if worker.mode == ray.WORKER_MODE and not worker.actor_id.is_nil():
        # In asyncio actor mode, we can't raise SystemExit because it will just
        # quit the asycnio event loop thread, not the main thread. Instead, we
        # raise a custom error to the main thread to tell it to exit.
        if worker.core_worker.current_actor_is_asyncio():
            raise AsyncioActorExit()

        # Set a flag to indicate this is an intentional actor exit. This
        # reduces log verbosity.
        raise_sys_exit_with_custom_error_message("exit_actor() is called.")
    else:
        raise TypeError(
            "exit_actor API is called on a non-actor worker, "
            f"{worker.mode}. Call this API inside an actor methods"
            "if you'd like to exit the actor gracefully."
        )
