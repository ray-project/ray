from typing import Any, Callable, Dict, List
import time


class EventSummarizer:
    """Utility that aggregates related log messages to reduce log spam."""

    def __init__(self):
        self.events_by_key: Dict[str, int] = {}
        self.messages: List[str] = []
        # Tracks TTL of messages. A message will not be re-posted once it is
        # added here, until its TTL expires.
        self.key_ttl: Dict[str, float] = {}

    def add(self, template: str, *, quantity: Any,
            aggregate: Callable[[Any, Any], Any]) -> None:
        """Add a log message, which will be combined by template.

        Args:
            template (str): Format string with one placeholder for quantity.
            quantity (Any): Quantity to aggregate.
            aggregate (func): Aggregation function used to combine the
                quantities. The result is inserted into the template to
                produce the final log message.
        """
        # Enforce proper sentence structure.
        if not template.endswith("."):
            template += "."
        if template in self.events_by_key:
            self.events_by_key[template] = aggregate(
                self.events_by_key[template], quantity)
        else:
            self.events_by_key[template] = quantity

    def add_once_per_interval(self, message: str, key: str, interval_s: int):
        """Add a log message, which is throttled once per interval by a key.

        Args:
            message (str): The message to log.
            key (str): The key to use to deduplicate the message.
            interval_s (int): Throttling interval in seconds.
        """
        if key not in self.key_ttl:
            self.key_ttl[key] = time.time() + interval_s
            self.messages.append(message)

    def summary(self) -> List[str]:
        """Generate the aggregated log summary of all added events."""
        out = []
        for template, quantity in self.events_by_key.items():
            out.append(template.format(quantity))
        out.extend(self.messages)
        return out

    def clear(self) -> None:
        """Clear the events added."""
        self.events_by_key.clear()
        self.messages.clear()
        # Expire any messages that have reached their TTL. This allows them
        # to be posted again.
        for k, t in list(self.key_ttl.items()):
            if time.time() > t:
                del self.key_ttl[k]
