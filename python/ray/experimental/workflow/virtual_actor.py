import abc
import inspect
import logging
from typing import Optional, List, TYPE_CHECKING, Any, Tuple, Dict
import uuid
import weakref

import ray
from ray.util.inspect import (is_function_or_method, is_class_method,
                              is_static_method)
from ray._private import signature
from ray.experimental.workflow.common import slugify, actor_id_to_workflow_id
from ray.experimental.workflow.storage import Storage, get_global_storage
from ray.experimental.workflow.workflow_storage import WorkflowStorage
from ray.experimental.workflow.recovery import get_latest_output
from ray.experimental.workflow.workflow_access import (
    get_or_create_management_actor)
from ray.experimental.workflow.step_function import WorkflowStepFunction

if TYPE_CHECKING:
    from ray import ObjectRef

logger = logging.getLogger(__name__)


class VirtualActorNotInitializedError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(message)


@ray.remote
def _readonly_method_executor(actor_id: str, storage: Storage, cls: type,
                              method_name: str, flattened_args: List):
    workflow_id = actor_id_to_workflow_id(actor_id)
    instance = cls.__new__(cls)
    try:
        state = get_latest_output(workflow_id, storage)
    except Exception as e:
        raise VirtualActorNotInitializedError(
            f"Virtual actor '{actor_id}' has not been initialized. "
            "We cannot get the latest state for the "
            "readonly virtual actor.") from e
    instance.__setstate__(state)
    args, kwargs = signature.recover_args(flattened_args)
    method = getattr(instance, method_name)
    return method(*args, **kwargs)


# TODO(suquark): This is just a temporary solution. A virtual actor writer
# should take place of this solution later.
@WorkflowStepFunction
def _virtual_actor_init(cls: type, flattened_args: List) -> Any:
    instance = cls.__new__(cls)
    args, kwargs = signature.recover_args(flattened_args)
    instance.__init__(*args, **kwargs)
    return instance.__getstate__()


class ActorMethodBase(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def run(self, *args, **kwargs) -> "ObjectRef":
        """Execute the actor method.

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

    def __init__(self, actor, method_name):
        self._actor_ref = weakref.ref(actor)
        self._method_name = method_name

    def __call__(self, *args, **kwargs):
        raise TypeError("Actor methods cannot be called directly. Instead "
                        f"of running 'object.{self._method_name}()', try "
                        f"'object.{self._method_name}.remote()'.")

    def run(self, *args, **kwargs) -> "ObjectRef":
        """Execute the actor method.

        Returns:
            A future object represents the result.
        """
        return self._run(args, kwargs)

    def options(self, **options) -> ActorMethodBase:
        """Convenience method for executing an actor method call with options.

        Same arguments as func._run(), but returns a wrapped function
        that a non-underscore .run() can be called on.

        Examples:
            # The following two calls are equivalent.
            >>> actor.my_method._run(args=[x, y], readonly=False)
            >>> actor.my_method.options(readonly=False).run(x, y)
        """

        func_cls = self

        class FuncWrapper(ActorMethodBase):
            def run(self, *args, **kwargs):
                return func_cls._run(args=args, kwargs=kwargs, **options)

        return FuncWrapper()

    def _run(self,
             args: Tuple[Any],
             kwargs: Dict[str, Any],
             readonly: Optional[bool] = None) -> "ObjectRef":
        actor = self._actor_ref()
        if actor is None:
            raise RuntimeError("Lost reference to actor")
        try:
            return actor._actor_method_call(
                self._method_name, args=args, kwargs=kwargs, readonly=readonly)
        except TypeError as exc:  # capture a friendlier stacktrace
            raise TypeError(
                "Invalid input arguments for virtual actor "
                f"method '{self._method_name}': {str(exc)}") from None

    def __getstate__(self):
        return {
            "actor": self._actor_ref(),
            "method_name": self._method_name,
        }

    def __setstate__(self, state):
        self.__init__(state["actor"], state["method_name"])


class VirtualActorMetadata:
    """Recording the metadata of a virtual actor class, including
    the signatures of its methods etc."""

    def __init__(self, original_class: type):
        actor_methods = inspect.getmembers(original_class,
                                           is_function_or_method)
        self.cls = original_class
        self.module = original_class.__module__
        self.name = original_class.__name__
        self.qualname = original_class.__qualname__
        self.methods = dict(actor_methods)

        # Extract the signatures of each of the methods. This will be used
        # to catch some errors if the methods are called with inappropriate
        # arguments.
        self.signatures = {}
        for method_name, method in actor_methods:
            # Whether or not this method requires binding of its first
            # argument. For class and static methods, we do not want to bind
            # the first argument, but we do for instance methods
            method = inspect.unwrap(method)
            is_bound = (is_class_method(method)
                        or is_static_method(original_class, method_name))

            # Print a warning message if the method signature is not
            # supported. We don't raise an exception because if the actor
            # inherits from a class that has a method whose signature we
            # don't support, there may not be much the user can do about it.
            self.signatures[method_name] = signature.extract_signature(
                method, ignore_first=not is_bound)

    def generate_random_actor_id(self) -> str:
        """Generate random actor ID."""
        return f"{slugify(self.qualname)}.{uuid.uuid4()}"

    def flatten_args(self, method_name: str, args: Tuple[Any],
                     kwargs: Dict[str, Any]) -> List[Any]:
        """Check and flatten arguments of the actor method.

        Args:
            method_name: The name of the actor method in the actor class.
            args: Positional arguments.
            kwargs: Keywords arguments.

        Returns:
            Flattened arguments.
        """
        return signature.flatten_args(self.signatures[method_name], args,
                                      kwargs)


class VirtualActorClassBase(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def create(self, *args, **kwargs) -> "VirtualActor":
        """Create a virtual actor.

        Args:
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
            "the @workflow.actor decorator instead.")

    def __call__(self, *args, **kwargs):
        """Prevents users from directly instantiating an ActorClass.

        This will be called instead of __init__ when 'VirtualActorClass()' is
        executed because an is an object rather than a metaobject. To properly
        instantiated a virtual actor, use 'VirtualActorClass.create()' or
        'VirtualActorClass.get()'.

        Raises:
            Exception: Always.
        """
        raise TypeError("Actors cannot be instantiated directly. "
                        f"Instead of '{self._metadata.name}()', "
                        f"use '{self._metadata.name}.create()'.")

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
                logger.warning("Creating an actor from class "
                               f"{base_class.__name__} overwrites "
                               f"attribute {attribute} of that class")

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
        if "__getstate__" not in metadata.methods:
            raise ValueError("The class does not have '__getstate__' method")
        if "__setstate__" not in metadata.methods:
            raise ValueError("The class does not have '__setstate__' method")

        DerivedActorClass.__module__ = metadata.module
        name = f"VirtualActorClass({metadata.name})"
        DerivedActorClass.__name__ = name
        DerivedActorClass.__qualname__ = name
        # Construct the base object.
        self = DerivedActorClass.__new__(DerivedActorClass)

        self._metadata = metadata
        return self

    def create(self, *args, **kwargs) -> "VirtualActor":
        """Create an actor. See `VirtualActorClassBase.create()`."""
        return self._create(
            args=args,
            kwargs=kwargs,
            actor_id=None,
            storage=None,
            readonly=False)

    # TODO(suquark): support num_cpu etc in options
    def options(self,
                actor_id: str,
                storage: Optional[Storage] = None,
                readonly: bool = False) -> VirtualActorClassBase:
        """Configures and overrides the actor instantiation parameters.

        Examples:

        .. code-block:: python

            @workflow.actor
            class Foo:
                def method(self):
                    return 1
            # By default class Foo will not be running in the readonly mode,
            # and will use a randomly generated 'actor_id'.
            # You can specify 'readonly' and 'actor_id' by
            Bar = Foo.options(actor_id="my_actor_id", readonly=True)
        """

        actor_cls = self

        class ActorOptionWrapper(VirtualActorClassBase):
            def create(self, *args, **kwargs):
                return actor_cls._create(
                    args=args,
                    kwargs=kwargs,
                    actor_id=actor_id,
                    storage=storage,
                    readonly=readonly)

        return ActorOptionWrapper()

    def _create(self, args, kwargs, actor_id: Optional[str],
                storage: Optional[Storage], readonly: bool) -> "VirtualActor":
        """Create a new virtual actor"""
        if actor_id is None:
            actor_id = self._metadata.generate_random_actor_id()
        if storage is None:
            storage = get_global_storage()
        instance = self._construct(actor_id, storage, readonly)
        instance._create(args, kwargs)
        return instance

    def _construct(self, actor_id: str, storage: Storage,
                   readonly: bool) -> "VirtualActor":
        """Construct a blank virtual actor."""
        return VirtualActor(self._metadata, actor_id, storage, readonly)


class VirtualActor:
    """The instance of a virtual actor class."""

    def __init__(self, metadata: VirtualActorMetadata, actor_id: str,
                 storage: Storage, readonly: bool):
        self._metadata = metadata
        self._actor_id = actor_id
        self._storage = storage
        self._readonly = readonly
        for method_name in metadata.signatures:
            # TODO(suquark): Maybe we should avoid overriding class fields.
            # However, we did not do it in ActorHandle.
            method = ActorMethod(self, method_name)
            setattr(self, method_name, method)

    def _create(self, args: Tuple[Any], kwargs: Dict[str, Any]):
        workflow_storage = WorkflowStorage(self.workflow_id, self._storage)
        workflow_storage.save_actor_class_body(self._metadata.cls)
        # TODO(suquark): This is just a temporary solution.
        # A virtual actor writer should take place of this solution later.
        from ray.experimental.workflow import run
        arg_list = self._metadata.flatten_args("__init__", args, kwargs)
        init_step = _virtual_actor_init.step(self._metadata.cls, arg_list)
        init_step._step_id = self._metadata.cls.__init__.__name__
        ref = run(
            init_step, storage=self._storage, workflow_id=self.workflow_id)
        workflow_manager = get_or_create_management_actor()
        # keep the ref in a list to prevent dereference
        ray.get(workflow_manager.init_actor.remote(self._actor_id, [ref]))

    @property
    def actor_id(self) -> str:
        """The actor ID of the virtual actor."""
        return self._actor_id

    @property
    def workflow_id(self) -> str:
        """The workflow ID associated with the actor."""
        return actor_id_to_workflow_id(self._actor_id)

    @property
    def readonly(self) -> bool:
        """If the actor is readonly or not."""
        return self._readonly

    def ready(self) -> "ObjectRef":
        """Return a future. If 'ray.get()' it successfully, then the actor
        is fully initialized."""
        # TODO(suquark): should ray.get(xxx.ready()) always be true?
        actor = get_or_create_management_actor()
        return ray.get(
            actor.actor_ready.remote(self._actor_id,
                                     self._storage.storage_url))

    def __getattr__(self, item):
        if item in self._methods_metadata.signatures:
            return ActorMethod(self, item)
        raise AttributeError(f"No method with name '{item}'")

    def _actor_method_call(self, method_name: str, args, kwargs,
                           readonly: Optional[bool]) -> "ObjectRef":
        flatten_args = self._metadata.flatten_args(method_name, args, kwargs)
        if readonly is None:
            # inherit the class setting by default
            readonly = self._readonly
        if readonly:
            cls = self._metadata.cls
            return _readonly_method_executor.remote(
                self._actor_id, self._storage, cls, method_name, flatten_args)
        raise NotImplementedError("Virtual actor writer mode has not been "
                                  "supported yet.")


def decorate_actor(cls: type):
    """Decorate and convert a class to virtual actor class."""
    return VirtualActorClass._from_class(cls)


def get_actor(actor_id: str, storage: Storage, readonly) -> VirtualActor:
    """Get an virtual actor.

    Args:
        actor_id: The ID of the actor.
        storage: The storage of the actor.
        readonly: Turn the actor into readonly actor or not.

    Returns:
        A virtual actor.
    """
    ws = WorkflowStorage(actor_id_to_workflow_id(actor_id), storage)
    cls = ws.load_actor_class_body()
    v_cls = VirtualActorClass._from_class(cls)
    return v_cls._construct(actor_id, storage, readonly=readonly)
