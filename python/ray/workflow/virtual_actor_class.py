import abc
import functools
import inspect
import logging
from typing import TYPE_CHECKING, Any, Tuple, Dict, Callable
import uuid
import json
import weakref
import ray
from ray.util.inspect import is_function_or_method, is_class_method, is_static_method
from ray._private import signature

from ray.workflow.common import (
    slugify,
    WorkflowData,
    Workflow,
    WorkflowRef,
    StepType,
    WorkflowStepRuntimeOptions,
    validate_user_metadata,
)
from ray.workflow import serialization_context
from ray.workflow.storage import Storage, get_global_storage
from ray.workflow.workflow_storage import WorkflowStorage
from ray.workflow.recovery import get_latest_output
from ray.workflow.workflow_access import get_or_create_management_actor
from ray.workflow import workflow_context
from ray.workflow.step_executor import execute_workflow

if TYPE_CHECKING:
    from ray import ObjectRef

logger = logging.getLogger(__name__)


class VirtualActorNotInitializedError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(message)


class ActorMethodBase(metaclass=abc.ABCMeta):
    def run(self, *args, **kwargs) -> Any:
        """Execute the actor method.

        Returns:
            A future object represents the result.
        """
        return ray.get(self.run_async(*args, **kwargs))

    @abc.abstractmethod
    def run_async(self, *args, **kwargs) -> "ObjectRef":
        """Execute the actor method asynchronously.

        Returns:
            A future object represents the result.
        """


# Create objects to wrap method invocations. This is done so that we can
# invoke methods with actor.method.run() instead of actor.method().
class ActorMethod(ActorMethodBase):
    """A class used to invoke an actor method.

    Note: This class only keeps a weak ref to the actor, unless it has been
    passed to a remote function. This avoids delays in GC of the actor.

    Attributes:
        _actor_ref: A weakref handle to the actor.
        _method_name: The name of the actor method.
    """

    def __init__(self, actor, method_name, method_helper: "_VirtualActorMethodHelper"):
        self._actor_ref = weakref.ref(actor)
        self._method_name = method_name
        self._method_helper = method_helper

    def __call__(self, *args, **kwargs):
        raise TypeError(
            "Actor methods cannot be called directly. Instead "
            f"of running 'object.{self._method_name}()', try "
            f"'object.{self._method_name}.remote()'."
        )

    def run_async(self, *args, **kwargs) -> "ObjectRef":
        """Execute the actor method asynchronously.

        Returns:
            A future object represents the result.
        """
        return self._run(args, kwargs)

    def options(
        self,
        *,
        max_retries: int = 0,
        catch_exceptions: bool = False,
        name: str = None,
        metadata: Dict[str, Any] = None,
        **ray_options,
    ) -> ActorMethodBase:
        """This function set how the actor method is going to be executed.

        Args:
            max_retries: num of retries the step for an application
                level error.
            catch_exceptions: Whether the user want to take care of the
                failure manually.
                If it's set to be true, (Optional[R], Optional[E]) will be
                returned.
                If it's false, the normal result will be returned.
            name: The name of this step, which will be used to
                generate the step_id of the step. The name will be used
                directly as the step id if possible, otherwise deduplicated by
                appending .N suffixes.
            metadata: metadata to add to the step.
            **ray_options: All parameters in this fields will be passed
                to ray remote function options.

        Returns:
            The actor method itself.
        """

        method_helper = self._method_helper.options(
            max_retries=max_retries,
            catch_exceptions=catch_exceptions,
            name=name,
            metadata=metadata,
            **ray_options,
        )
        return ActorMethod(self._actor_ref(), self._method_name, method_helper)

    def _run(self, args: Tuple[Any], kwargs: Dict[str, Any]) -> "ObjectRef":
        actor = self._actor_ref()
        if actor is None:
            raise RuntimeError(
                "Lost reference to actor. One common cause is "
                "doing 'workflow.get_actor(id).method.run()', "
                "in this case the actor instance is released "
                "by Python before calling the method. Try "
                "'actor = workflow.get_actor(id); "
                "actor.method.run()' instead."
            )
        try:
            return actor._actor_method_call(
                self._method_helper, args=args, kwargs=kwargs
            )
        except TypeError as exc:  # capture a friendlier stacktrace
            raise TypeError(
                "Invalid input arguments for virtual actor "
                f"method '{self._method_name}': {str(exc)}"
            ) from None

    def __getstate__(self):
        return {
            "actor": self._actor_ref(),
            "method_name": self._method_name,
            "method_helper": self._method_helper,
        }

    def __setstate__(self, state):
        self.__init__(state["actor"], state["method_name"], state["method_helper"])


def __getstate(instance):
    if hasattr(instance, "__getstate__"):
        state = instance.__getstate__()
    else:
        try:
            state = json.dumps(instance.__dict__)
        except TypeError as e:
            raise ValueError(
                "The virtual actor contains fields that can't be "
                "converted to JSON. Please define `__getstate__` "
                "and `__setstate__` explicitly:"
                f" {instance.__dict__}"
            ) from e
    return state


def __setstate(instance, v):
    if hasattr(instance, "__setstate__"):
        return instance.__setstate__(v)
    else:
        instance.__dict__ = json.loads(v)


class _VirtualActorMethodHelper:
    """This is a helper class for managing options and creating workflow steps
    from raw class methods."""

    def __init__(
        self,
        original_class,
        method: Callable,
        method_name: str,
        runtime_options: WorkflowStepRuntimeOptions,
    ):
        self._original_class = original_class
        self._original_method = method
        # Extract the signature of the method. This will be used
        # to catch some errors if the methods are called with inappropriate
        # arguments.

        # Whether or not this method requires binding of its first
        # argument. For class and static methods, we do not want to bind
        # the first argument, but we do for instance methods
        method = inspect.unwrap(method)
        is_bound = is_class_method(method) or is_static_method(
            original_class, method_name
        )

        # Print a warning message if the method signature is not
        # supported. We don't raise an exception because if the actor
        # inherits from a class that has a method whose signature we
        # don't support, there may not be much the user can do about it.
        self._signature = signature.extract_signature(method, ignore_first=not is_bound)

        self._method = method
        self._method_name = method_name
        self._options = runtime_options
        self._name = None
        self._user_metadata = {}

        # attach properties to the original function, so we can create a
        # workflow step with the original function inside a virtual actor.
        self._original_method.step = self.step
        self._original_method.options = self.options

    @property
    def readonly(self) -> bool:
        return self._options.step_type == StepType.READONLY_ACTOR_METHOD

    def step(self, *args, **kwargs):
        flattened_args = signature.flatten_args(self._signature, args, kwargs)
        actor_id = workflow_context.get_current_workflow_id()
        if not self.readonly:
            if self._method_name == "__init__":
                state_ref = None
            else:
                ws = WorkflowStorage(actor_id, get_global_storage())
                state_ref = WorkflowRef(ws.get_entrypoint_step_id())
            # This is a hack to insert a positional argument.
            flattened_args = [signature.DUMMY_TYPE, state_ref] + flattened_args
        workflow_inputs = serialization_context.make_workflow_inputs(flattened_args)

        if self.readonly:
            _actor_method = _wrap_readonly_actor_method(
                actor_id, self._original_class, self._method_name
            )
        else:
            _actor_method = _wrap_actor_method(self._original_class, self._method_name)
        workflow_data = WorkflowData(
            func_body=_actor_method,
            inputs=workflow_inputs,
            name=self._name,
            step_options=self._options,
            user_metadata=self._user_metadata,
        )
        wf = Workflow(workflow_data)
        return wf

    def options(
        self,
        *,
        max_retries=0,
        catch_exceptions=False,
        name=None,
        metadata=None,
        **ray_options,
    ) -> "_VirtualActorMethodHelper":
        validate_user_metadata(metadata)
        options = WorkflowStepRuntimeOptions.make(
            step_type=self._options.step_type,
            catch_exceptions=catch_exceptions
            if catch_exceptions is not None
            else self._options.catch_exceptions,
            max_retries=max_retries
            if max_retries is not None
            else self._options.max_retries,
            ray_options={
                **self._options.ray_options,
                **(ray_options if ray_options is not None else {}),
            },
        )
        _self = _VirtualActorMethodHelper(
            self._original_class,
            self._original_method,
            self._method_name,
            runtime_options=options,
        )
        _self._name = name if name is not None else self._name
        _self._user_metadata = {
            **self._user_metadata,
            **(metadata if metadata is not None else {}),
        }
        return _self

    def __call__(self, *args, **kwargs):
        return self._original_method(*args, **kwargs)


class VirtualActorMetadata:
    """Recording the metadata of a virtual actor class, including
    the signatures of its methods etc."""

    def __init__(self, original_class: type):
        actor_methods = inspect.getmembers(original_class, is_function_or_method)

        self.cls = original_class
        self.module = original_class.__module__
        self.name = original_class.__name__
        self.qualname = original_class.__qualname__
        self.methods = {}
        for method_name, method in actor_methods:
            self._readonly = getattr(method, "__virtual_actor_readonly__", False)
            if self._readonly:
                step_type = StepType.READONLY_ACTOR_METHOD
            else:
                step_type = StepType.ACTOR_METHOD
            options = WorkflowStepRuntimeOptions.make(step_type=step_type)
            self.methods[method_name] = _VirtualActorMethodHelper(
                original_class, method, method_name, runtime_options=options
            )

    def generate_random_actor_id(self) -> str:
        """Generate random actor ID."""
        return f"{slugify(self.qualname)}.{uuid.uuid4()}"


class VirtualActorClassBase(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_or_create(self, actor_id: str, *args, **kwargs) -> "VirtualActor":
        """Get or create a virtual actor.

        Args:
            actor_id: The ID of the actor.
            args: These arguments are forwarded directly to the actor
                constructor.
            kwargs: These arguments are forwarded directly to the actor
                constructor.

        Returns:
            A handle to the newly created actor.
        """


class VirtualActorClass(VirtualActorClassBase):
    """The virtual actor class used to create a virtual actor."""

    _metadata: VirtualActorMetadata

    def __init__(self):
        # This shouldn't be reached because one of the base classes must be
        # an actor class if this was meant to be subclassed.
        assert False, (
            "VirtualActorClass.__init__ should not be called. Please use "
            "the @workflow.actor decorator instead."
        )

    def __call__(self, *args, **kwargs):
        """Prevents users from directly instantiating an ActorClass.

        This will be called instead of __init__ when 'VirtualActorClass()' is
        executed because an is an object rather than a metaobject. To properly
        instantiated a virtual actor, use 'VirtualActorClass.create()' or
        'VirtualActorClass.get()'.

        Raises:
            Exception: Always.
        """
        raise TypeError(
            "Actors cannot be instantiated directly. "
            f"Instead of '{self._metadata.name}()', "
            f"use '{self._metadata.name}.create()'."
        )

    @classmethod
    def _from_class(cls, base_class: type) -> "VirtualActorClass":
        """Construct the virtual actor class from a base class."""
        # TODO(suquark): we may use more complex name for private functions
        # to avoid collision with user-defined functions.
        for attribute in [
            "create",
            "_create",
            "option",
            "_construct",
            "_from_class",
        ]:
            if hasattr(base_class, attribute):
                logger.warning(
                    "Creating an actor from class "
                    f"{base_class.__name__} overwrites "
                    f"attribute {attribute} of that class"
                )

        if not is_function_or_method(getattr(base_class, "__init__", None)):
            # Add __init__ if it does not exist.
            # Actor creation will be executed with __init__ together.

            # Assign an __init__ function will avoid many checks later on.
            def __init__(self):
                pass

            base_class.__init__ = __init__

        # Make sure the actor class we are constructing inherits from the
        # original class so it retains all class properties.
        class DerivedActorClass(cls, base_class):
            pass

        metadata = VirtualActorMetadata(base_class)
        has_getstate = "__getstate__" in metadata.methods
        has_setstate = "__setstate__" in metadata.methods

        if not has_getstate and not has_setstate:
            # This is OK since we'll use default one defined
            pass
        elif not has_getstate:
            raise ValueError("The class does not have '__getstate__' method")
        elif not has_setstate:
            raise ValueError("The class does not have '__setstate__' method")

        DerivedActorClass.__module__ = metadata.module
        name = f"VirtualActorClass({metadata.name})"
        DerivedActorClass.__name__ = name
        DerivedActorClass.__qualname__ = name
        # Construct the base object.

        self = DerivedActorClass.__new__(DerivedActorClass)

        self._metadata = metadata
        return self

    def get_or_create(self, actor_id: str, *args, **kwargs) -> "VirtualActor":
        """Create an actor. See `VirtualActorClassBase.create()`."""
        return self._get_or_create(
            actor_id, args=args, kwargs=kwargs, storage=get_global_storage()
        )

    # TODO(suquark): support num_cpu etc in options
    def options(self) -> VirtualActorClassBase:
        """Configures and overrides the actor instantiation parameters."""

        actor_cls = self

        class ActorOptionWrapper(VirtualActorClassBase):
            def get_or_create(self, actor_id: str, *args, **kwargs):
                return actor_cls._get_or_create(
                    actor_id, args=args, kwargs=kwargs, storage=get_global_storage()
                )

        return ActorOptionWrapper()

    def _get_or_create(
        self, actor_id: str, args, kwargs, storage: Storage
    ) -> "VirtualActor":
        """Create a new virtual actor"""
        try:
            return get_actor(actor_id, storage)
        except Exception:
            instance = self._construct(actor_id, storage)
            instance._create(args, kwargs)
            return instance

    def _construct(self, actor_id: str, storage: Storage) -> "VirtualActor":
        """Construct a blank virtual actor."""
        return VirtualActor(self._metadata, actor_id, storage)

    def __reduce__(self):
        return decorate_actor, (self._metadata.cls,)


def _wrap_readonly_actor_method(actor_id: str, cls: type, method_name: str):
    # generate better step names
    @functools.wraps(getattr(cls, method_name))
    def _readonly_actor_method(*args, **kwargs):
        storage = get_global_storage()
        instance = cls.__new__(cls)
        try:
            state = get_latest_output(actor_id, storage)
        except Exception as e:
            raise VirtualActorNotInitializedError(
                f"Virtual actor '{actor_id}' has not been initialized. "
                "We cannot get the latest state for the "
                "readonly virtual actor."
            ) from e
        __setstate(instance, state)
        method = getattr(instance, method_name)
        return method(*args, **kwargs)

    return _readonly_actor_method


def _wrap_actor_method(cls: type, method_name: str):
    @ray.workflow.step
    def deref(*args):
        return args

    @functools.wraps(getattr(cls, method_name))
    def _actor_method(state, *args, **kwargs):
        instance = cls.__new__(cls)
        if method_name != "__init__":
            __setstate(instance, state)
        method = getattr(instance, method_name)
        output = method(*args, **kwargs)
        if isinstance(output, Workflow):
            if output.data.step_options.step_type == StepType.FUNCTION:
                next_step = deref.step(__getstate(instance), output)
                next_step.data.step_options.step_type = StepType.ACTOR_METHOD
                return next_step, None
            return __getstate(instance), output
        return __getstate(instance), output

    return _actor_method


class VirtualActor:
    """The instance of a virtual actor class."""

    def __init__(self, metadata: VirtualActorMetadata, actor_id: str, storage: Storage):
        self._metadata = metadata
        self._actor_id = actor_id
        self._storage = storage

    def _create(self, args: Tuple[Any], kwargs: Dict[str, Any]):
        workflow_storage = WorkflowStorage(self._actor_id, self._storage)
        workflow_storage.save_actor_class_body(self._metadata.cls)
        method_helper = self._metadata.methods["__init__"]
        ref = self._actor_method_call(method_helper, args, kwargs)
        workflow_manager = get_or_create_management_actor()
        # keep the ref in a list to prevent dereference
        ray.get(workflow_manager.init_actor.remote(self._actor_id, [ref]))

    @property
    def actor_id(self) -> str:
        """The actor ID of the virtual actor."""
        return self._actor_id

    def ready(self) -> "ObjectRef":
        """Return a future. If 'ray.get()' runs successfully, then the actor
        is fully initialized."""
        # TODO(suquark): should ray.get(xxx.ready()) always be true?
        workflow_manager = get_or_create_management_actor()
        return ray.get(workflow_manager.actor_ready.remote(self._actor_id))

    def __getattr__(self, method_name):
        if method_name in self._metadata.methods:
            return ActorMethod(self, method_name, self._metadata.methods[method_name])
        raise AttributeError(f"No method with name '{method_name}'")

    def _actor_method_call(
        self, method_helper: _VirtualActorMethodHelper, args, kwargs
    ) -> "ObjectRef":
        with workflow_context.workflow_step_context(
            self._actor_id, self._storage.storage_url
        ):
            wf = method_helper.step(*args, **kwargs)
            if method_helper.readonly:
                return execute_workflow(wf).volatile_output.ref
            else:
                return wf.run_async(self._actor_id)


def decorate_actor(cls: type):
    """Decorate and convert a class to virtual actor class."""
    return VirtualActorClass._from_class(cls)


def get_actor(actor_id: str, storage: Storage) -> VirtualActor:
    """Get an virtual actor.

    Args:
        actor_id: The ID of the actor.
        storage: The storage of the actor.

    Returns:
        A virtual actor.
    """
    ws = WorkflowStorage(actor_id, storage)
    cls = ws.load_actor_class_body()
    v_cls = VirtualActorClass._from_class(cls)
    return v_cls._construct(actor_id, storage)
