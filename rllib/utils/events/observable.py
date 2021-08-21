import collections
from typing import Callable, Dict, List

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

    def subscribe_to(self, event: EventName, callback: Callable) -> None:
        """Binds a callback to an event (str) on this Observable.

        The callback may be a function or a bound method of some observer
        object. From here on, if the event gets triggered via
        `[Observable].trigger_event()`, the callback will be called.

        Args:
            event (EventName): The name of the event to subscribe to.
            callback (Callable): The callable to be called whenever the event
                is triggered.
        """
        self.subscribers[event].append(callback)

    # TODO: good debugging: warn if a registered event doesn't get triggered for a long time?
    def trigger_event(self, event: EventName, *args, **kwargs) -> None:
        """Triggers an event and specifies the callbacks' args/kwargs.

        Args:
            event (EventName): The name of the event to be triggered.
            args (Any): The *args to be passed to all subscribed callables.

        Keyword Args:
            kwargs (Any): The **kwargs to be passed to all subscribed callables.
        """
        # Make sure there are any listeners for this specific event.
        if event in self.subscribers:
            # Call each listener with the given args/kwargs.
            for callback in self.subscribers[event]:
                callback(*args, **kwargs)

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
