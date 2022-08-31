import functools
from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Union

from ray.autoscaler._private.cli_logger import cli_logger


class EventSequence:
    """Extensible class for building custom events."""

    @property
    @abstractmethod
    def state(self) -> str:
        raise NotImplementedError("State must be implemented by a sub-class")

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError("Event name must be implemented by a sub-class")

    @property
    @abstractmethod
    def value(self) -> int:
        raise NotImplementedError("Sequence value must be implemented by a sub-class")


class StateEvent(Enum):
    """Events to track with an associated state identifier."""

    @property
    def state(self) -> str:
        raise NotImplementedError("State must be implemented by a sub-class")


class CreateClusterEvent(StateEvent):
    """Events to track in ray.autoscaler.sdk.create_or_update_cluster.

    Attributes:
        up_started : Invoked at the beginning of create_or_update_cluster.
        ssh_keypair_downloaded : Invoked when the ssh keypair is downloaded.
        cluster_booting_started : Invoked when when the cluster booting starts.
        acquiring_new_head_node : Invoked before the head node is acquired.
        head_node_acquired : Invoked after the head node is acquired.
        ssh_control_acquired : Invoked when the node is being updated.
        run_initialization_cmd : Invoked before all initialization
            commands are called and again before each initialization command.
        run_setup_cmd : Invoked before all setup commands are
            called and again before each setup command.
        start_ray_runtime : Invoked before ray start commands are run.
        start_ray_runtime_completed : Invoked after ray start commands
            are run.
        cluster_booting_completed : Invoked after cluster booting
            is completed.
    """

    @property
    def state(self) -> str:
        return "DISPATCHED"

    up_started = auto()
    ssh_keypair_downloaded = auto()
    cluster_booting_started = auto()
    acquiring_new_head_node = auto()
    head_node_acquired = auto()
    ssh_control_acquired = auto()
    run_initialization_cmd = auto()
    run_setup_cmd = auto()
    start_ray_runtime = auto()
    start_ray_runtime_completed = auto()
    cluster_booting_completed = auto()


RayEvent = Union[EventSequence, StateEvent]


class _EventSystem:
    """Event system that handles storing and calling callbacks for events.

    Attributes:
        callback_map (Dict[str, List[Callable]]) : Stores list of callbacks
            for events when registered.
    """

    def __init__(self):
        self.callback_map = {}

    def add_callback_handler(
        self,
        event: RayEvent,
        callback: Union[Callable[[Dict], None], List[Callable[[Dict], None]]],
        *args,
        **kwargs,
    ):
        """Stores callback handler for event.

        Args:
            event: Event that callback should be called on. See
                CreateClusterEvent for details on the events available to be
                registered against.
            callback (Callable[[Dict], None]): Callable object that is invoked
                when specified event occurs.
        """
        callback = functools.partial(callback, *args, **kwargs)

        self.callback_map.setdefault(event, []).extend(
            [callback] if type(callback) is not list else callback
        )

    def execute_callback(
        self, event: RayEvent, event_data: Optional[Dict[str, Any]] = None
    ):
        """Executes all callbacks for event.

        Args:
            event: Event that is invoked. See CreateClusterEvent
                for details on the available events.
            event_data (Dict[str, Any]): Argument that is passed to each
                callable object stored for this particular event.
        """
        if event_data is None:
            event_data = {}

        event_data["event_name"] = event
        if event in self.callback_map:
            for callback in self.callback_map[event]:
                callback(event_data)

    def clear_callbacks_for_event(self, event: str):
        """Clears stored callable objects for event.

        Args:
            event: Event that has callable objects stored in map.
                See CreateClusterEvent for details on the available events.
        """
        if event in self.callback_map:
            del self.callback_map[event]


class EventCallbackHandler:
    def __init__(self, handler: Callable, *args, **kwargs):
        """A helper class containing a callable object with its associated args and kwargs.
        Args:
            handler: A callable object.
            *args: Arguments to be passed into the callable.
            **kwargs: Keyword arguments to be passed into the callable.
        """
        self._handler = handler
        self._args = args
        self._kwargs = kwargs

    @property
    def handler(self):
        return self._handler

    @property
    def args(self):
        return self._args

    @property
    def kwargs(self):
        return self._kwargs


class EventPublisher(ABC):
    """Event publisher abstract class for publishing event notifications."""

    def __init__(self, events_config: Dict[str, Any]):
        """Constructor for event publisher.
        Args:
            events_config: A dict loaded from the autoscaler YAML config.
        """
        self.validate_config(events_config)
        self._config = events_config
        self._event_base_params = _event_base_params = events_config.get(
            "parameters", {}
        )
        self.validate_event_base_params(_event_base_params)
        self._uri = events_config["notification_uri"]

    def add_callback(self, event: RayEvent):
        """Adds a callback handler for a given event.
        Args:
            event: The event to invoke the callback handler for
        """
        callback_handlers: List[EventCallbackHandler] = self.get_callback_handlers()
        for cb in callback_handlers:
            global_event_system.add_callback_handler(
                event, cb.handler, *cb.args, **cb.kwargs
            )
            cli_logger.info(
                f"Added callback handler {cb.handler.__name__} for event {event.name}"
            )

    def get_callback_handlers(self) -> List[EventCallbackHandler]:
        raise NotImplementedError(
            "Event publisher sub-classes must provide their own callback handlers."
        )

    @staticmethod
    def publish(event: RayEvent, event_data: Optional[Dict[str, Any]] = None):
        global_event_system.execute_callback(event, event_data)

    @abstractmethod
    def validate_config(self, events_config: Dict[str, Any]):
        raise NotImplementedError("Method is not implemented")

    @abstractmethod
    def validate_event_base_params(self, params_config: Dict[str, Any]):
        raise NotImplementedError("Method is not implemented")

    @property
    def config(self) -> Dict[str, Any]:
        """Configuration for event notifications.
        Holds outgoing event base parameters, the notification URI,
            and other additional metadata.

        Returns: configuration dict
        """
        return self._config

    @property
    def event_base_params(self) -> Dict[str, Any]:
        """Base parameters injected into every outgoing event notification.

        Returns: parameters dict
        """
        return self._event_base_params

    @property
    def uri(self) -> str:
        """URI endpoint for outgoing event notifications.

        Returns: uri string
        """
        return self._uri


global_event_system = _EventSystem()
