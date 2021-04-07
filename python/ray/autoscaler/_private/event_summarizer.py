from typing import Any, Callable, Dict, List


class EventSummarizer:
    """Utility that aggregates related log messages to reduce log spam."""

    def __init__(self):
        self.events_by_key: Dict[str, int] = {}

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

    def summary(self) -> List[str]:
        """Generate the aggregated log summary of all added events."""
        out = []
        for template, quantity in self.events_by_key.items():
            out.append(template.format(quantity))
        return out

    def clear(self) -> None:
        """Clear the events added."""
        self.events_by_key.clear()
