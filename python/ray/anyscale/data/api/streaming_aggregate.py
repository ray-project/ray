from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class StreamingAggFn(ABC):
    """User-defined aggregate function for the streaming aggregate operator."""

    @abstractmethod
    def init_state(self, key: str) -> Dict[str, Any]:
        """Initialize the state of the given key.

        Args:
            key: The key to initialize the state for.

        Returns:
            A dict that represents the initial state of the given key.
        """
        ...

    @abstractmethod
    def aggregate_row(
        self, key: str, state: Dict[str, Any], row: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], bool]:
        """Aggregate a new row to the key state.

        Args:
            key: The key to aggregate on.
            state: The current state of the key.
            row: The new row to aggregate.

        Returns:
            The new state, followed by a boolean indicating whether the state is final.
        """
        ...
