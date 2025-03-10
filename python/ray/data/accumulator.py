# python/ray/data/accumulator.py
from typing import Any, Generic, TypeVar
from abc import ABC, abstractmethod
import ray

T = TypeVar("T")


class BaseAccumulator(ABC, Generic[T]):
    """Stateless accumulator logic without any actor references"""

    @abstractmethod
    def update(self, *args) -> None:
        pass

    @abstractmethod
    def get(self) -> T:
        pass

    @abstractmethod
    def reset(self) -> None:
        pass


class Accumulator:
    """Manages a stateful accumulator instance through a Ray actor.

    This provides thread-safe distributed access to accumulator operations
    (update, get, reset) by wrapping them in actor method calls.

    Args:
        accumulator_cls: The accumulator class to instantiate in the actor.
    """

    @ray.remote
    class AccumulatorActor:
        """Ray actor that owns and manages an accumulator instance."""

        def __init__(self, accumulator_cls):
            """Initialize with a fresh accumulator instance."""
            self.accumulator = accumulator_cls()

        def _update(self, *args) -> None:
            """Update the accumulator state."""
            self.accumulator.update(*args)

        def _get(self):
            """Get the current accumulator value."""
            return self.accumulator.get()

        def _reset(self) -> None:
            """Reset the accumulator to initial state."""
            self.accumulator.reset()

    def __init__(self, accumulator_cls):
        self.actor = self.AccumulatorActor.remote(accumulator_cls)

    def update(self, *args) -> None:
        """Update the accumulator state (blocking)."""
        ray.get(self.actor._update.remote(*args))

    def get(self):
        """Get the current accumulator value (blocking)."""
        return ray.get(self.actor._get.remote())

    def reset(self) -> None:
        """Reset the accumulator to initial state (blocking)."""
        ray.get(self.actor._reset.remote())
