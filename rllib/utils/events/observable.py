import collections
from typing import Any, Callable, Dict, List, Optional, Union

from ray.rllib.utils import force_list
from ray.rllib.utils.typing import EventName


class Observable:
    """An Observable that can trigger events other objects are subscribed to.

    Examples:
        >>> class Observer:
        ...     def __init__(self):
        ...         self.sum = 0
        ...
        ...     def callback(self, num):
        ...         self.sum += num
        ...         print(self.sum)

        >>> observable = Observable()
        >>> observer = Observer()

        >>> observable.subscribe_to("on_train_start", observer.callback)
        >>> observable.trigger("on_train_start", 5)
        >>> 5
        >>> observable.trigger("on_train_start", 3)
        >>> 8
    """

    def __init__(self):
        # Keep a  a list of callbacks indexed by event name.
        self.subscribers: Dict[str, List[Callable]] = \
            collections.defaultdict(list)

    def subscribe_to(self, event: EventName, callbacks: Union[Callable, List[Callable]]) -> None:
        """Binds a callback to an event (str) on this Observable.

        The callback may be a function or a bound method of some observer
        object. From here on, if the event gets triggered via
        `[Observable].trigger_event()`, the callback will be called.

        Args:
            event (EventName): The name of the event to subscribe to.
            callback (Union[Callable, List[Callable]]): The callable(s) to be
                called whenever the event is triggered.
        """
        callbacks = force_list(callbacks)
        for callback in callbacks:
            if not callable(callback):
                raise ValueError(f"`callback` ({callback}) must be a callable!")
            self.subscribers[event].append(callback)

    def trigger_event(self, event: EventName, *args, **kwargs) -> Optional[Any]:
        """Triggers an event and specifies the callbacks' args/kwargs.

        Args:
            event (EventName): The name of the event to be triggered.
            args (Any): The *args to be passed to all subscribed callables.

        Keyword Args:
            kwargs (Any): The **kwargs to be passed to all subscribed callables.
        """
        # Make sure there are any listeners for this specific event.
        if event not in self.subscribers:
            return

        suggestions = []
        # Call each listener with the given args/kwargs.
        for callback in self.subscribers[event]:
            ret = callback(self, *args, **kwargs)
            if ret is not None:
                suggestions.append(ret)
        # If this is a "suggest_..." event, make sure there is only one
        # suggestion and return that one (or None).
        if event.startswith("suggest_"):
            if len(suggestions) > 1:
                raise ValueError(
                    "More than one suggestion received for event "
                    f"{event}! Only exactly one suggestion allowed to be "
                    f"returned in total from all subscribers.")
            return suggestions[0] if len(suggestions) == 1 else None

    def unsubscribe_from(self, event: EventName, callback: Callable) -> None:
        """Unsubscribes from an event.

        Args:
            event (EventName): The event to be unsubscribed from.
            callback (Callable): The callable that is to be unsubscribed
                from `event`. This callable will no longer be called when
                `event` is triggered.
        """
        # Remove observer (callback) from the subscribers' list.
        if event in self.subscribers:
            self.subscribers[event].remove(callback)
