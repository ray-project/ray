# python/ray/data/accumulator.py
from typing import Any, Dict, Generic, TypeVar
from abc import ABC, abstractmethod
import ray

T = TypeVar("T")


# Base Classes
class Accumulator(ABC, Generic[T]):
    """Stateless accumulator logic without any actor references"""

    @abstractmethod
    def update(self, *args) -> None:
        pass

    @abstractmethod
    def get(self) -> T:
        pass

    @abstractmethod
    def merge(self, state: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def state(self) -> Dict[str, Any]:
        pass


class AccumulatorManager:
    """Fixed implementation with proper actor creation"""

    def __init__(self, accumulator_cls):
        self.actor = AccumulatorActor.remote(accumulator_cls)  # Corrected line

    def update(self, *args):
        ray.get(self.actor.update.remote(*args))

    def get(self):
        return ray.get(self.actor.get.remote())

    def reset(self):
        ray.get(self.actor.reset.remote())


@ray.remote  # Single decorator
class AccumulatorActor:
    """Actor class with proper initialization"""

    def __init__(self, accumulator_cls):
        self.accumulator = accumulator_cls()

    def update(self, *args):
        self.accumulator.update(*args)

    def get(self):
        return self.accumulator.get()

    def reset(self):
        self.accumulator.reset()

    def merge(self, state):
        self.accumulator.merge(state)


# Concrete Implementations
class SumAccumulator(Accumulator[float]):
    def __init__(self):
        self._sum = 0.0

    def update(self, value: float):
        self._sum += value

    def get(self) -> float:
        return self._sum

    def merge(self, state: Dict[str, Any]):
        self._sum += state["sum"]

    def state(self) -> Dict[str, Any]:
        return {"sum": self._sum}


class CountAccumulator(Accumulator[int]):
    def __init__(self):
        self._count = 0

    def update(self, value: int = 1):
        self._count += value

    def get(self) -> int:
        return self._count

    def merge(self, state: Dict[str, Any]):
        self._count += state["count"]

    def state(self) -> Dict[str, Any]:
        return {"count": self._count}


class AverageAccumulator(Accumulator[float]):
    def __init__(self):
        self._sum = 0.0
        self._count = 0

    def update(self, value: float):
        self._sum += value
        self._count += 1

    def get(self) -> float:
        return self._sum / self._count if self._count > 0 else 0.0

    def merge(self, state: Dict[str, Any]):
        self._sum += state["sum"]
        self._count += state["count"]

    def state(self) -> Dict[str, Any]:
        return {"sum": self._sum, "count": self._count}


class CollectionAccumulator(Accumulator[list]):
    def __init__(self):
        self._items = []

    def update(self, value: Any):
        self._items.append(value)

    def get(self) -> list:
        return self._items

    def merge(self, state: Dict[str, Any]):
        self._items.extend(state["items"])

    def state(self) -> Dict[str, Any]:
        return {"items": self._items.copy()}
