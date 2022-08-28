import time
from threading import RLock
from typing import Any, Callable, Dict, List


class EventSummarizer:
    """Utility that aggregates related log messages to reduce log spam."""

    def __init__(self):
        self.events_by_key: Dict[str, int] = {}
        # Messages to send in next summary batch.
        self.messages_to_send: List[str] = []
        # Tracks TTL of messages. A message will not be re-sent once it is
        # added here, until its TTL expires.
        self.throttled_messages: Dict[str, float] = {}

        # Event summarizer is used by the main thread and
        # by node launcher child threads.
        self.lock = RLock()

    def add(
        self, template: str, *, quantity: Any, aggregate: Callable[[Any, Any], Any]
    ) -> None:
        """Add a log message, which will be combined by template.

        Args:
            template: Format string with one placeholder for quantity.
            quantity: Quantity to aggregate.
            aggregate: Aggregation function used to combine the
                quantities. The result is inserted into the template to
                produce the final log message.
        """
        with self.lock:
            # Enforce proper sentence structure.
            if not template.endswith("."):
                template += "."
            if template in self.events_by_key:
                self.events_by_key[template] = aggregate(
                    self.events_by_key[template], quantity
                )
            else:
                self.events_by_key[template] = quantity

    def add_once_per_interval(self, message: str, key: str, interval_s: int):
        """Add a log message, which is throttled once per interval by a key.

        Args:
            message: The message to log.
            key: The key to use to deduplicate the message.
            interval_s: Throttling interval in seconds.
        """
        with self.lock:
            if key not in self.throttled_messages:
                self.throttled_messages[key] = time.time() + interval_s
                self.messages_to_send.append(message)

    def summary(self) -> List[str]:
        """Generate the aggregated log summary of all added events."""
        with self.lock:
            out = []
            for template, quantity in self.events_by_key.items():
                out.append(template.format(quantity))
            out.extend(self.messages_to_send)
        return out

    def clear(self) -> None:
        """Clear the events added."""
        with self.lock:
            self.events_by_key.clear()
            self.messages_to_send.clear()
            # Expire any messages that have reached their TTL. This allows them
            # to be sent again.
            for k, t in list(self.throttled_messages.items()):
                if time.time() > t:
                    del self.throttled_messages[k]
