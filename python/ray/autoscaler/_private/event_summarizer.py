from typing import Any, Callable, Dict, List


class EventSummarizer:
    def __init__(self):
        self.events_by_key: Dict[str, int] = {}

    def add(self, template: str, *, quantity: int,
            aggregate: Callable[[Any, Any], Any]) -> None:
        if template in self.events_by_key:
            self.events_by_key[template] = aggregate(
                self.events_by_key[template], quantity)
        else:
            self.events_by_key[template] = quantity

    def summary(self) -> List[str]:
        out = []
        for template, quantity in self.events_by_key.items():
            out.append(template.format(quantity))
        return out

    def clear(self) -> None:
        self.events_by_key.clear()
