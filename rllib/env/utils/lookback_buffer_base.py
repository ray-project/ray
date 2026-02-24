"""Abstract base class for lookback buffers."""
from abc import ABC, abstractmethod
from typing import Any, Dict


class BaseLookbackBuffer(ABC):
    """Abstract base class defining the public contract for lookback buffers.

    Both :class:`InfiniteLookbackBuffer` and
    :class:`PreAllocatedLookbackBuffer` implement this interface so they are
    interchangeable drop-in replacements for each other.
    """

    @abstractmethod
    def append(self, item) -> None:
        """Appends the given item to the end of this buffer."""

    @abstractmethod
    def extend(self, items) -> None:
        """Appends all items in `items` to the end of this buffer."""

    @abstractmethod
    def concat(self, other: "BaseLookbackBuffer") -> None:
        """Concatenates the data of `other` (w/o its lookback) to self."""

    @abstractmethod
    def pop(self, index: int = -1) -> None:
        """Removes the item at `index` from this buffer (does NOT return it)."""

    @abstractmethod
    def finalize(self) -> None:
        """Converts / trims internal storage to batch (numpy) format."""

    @abstractmethod
    def get(
        self,
        indices=None,
        *,
        neg_index_as_lookback: bool = False,
        fill=None,
        one_hot_discrete: bool = False,
        _ignore_last_ts: bool = False,
        _add_last_ts_value=None,
    ) -> Any:
        """Returns data, based on the given args, from this buffer."""

    @abstractmethod
    def set(
        self,
        new_data,
        *,
        at_indices=None,
        neg_index_as_lookback: bool = False,
    ) -> None:
        """Overwrites all or some of the data in this buffer."""

    @abstractmethod
    def __len__(self) -> int:
        """Return the length of our data, excluding the lookback buffer."""

    @abstractmethod
    def __getitem__(self, item):
        """Support squared bracket syntax, e.g. buffer[:5]."""

    @abstractmethod
    def __setitem__(self, key, value):
        """Support squared bracket assignment, e.g. buffer[0] = x."""

    @abstractmethod
    def __repr__(self) -> str:
        ...

    @abstractmethod
    def __add__(self, other):
        ...

    @abstractmethod
    def __eq__(self, other) -> bool:
        ...

    @abstractmethod
    def len_incl_lookback(self) -> int:
        """Return the total data length including the lookback buffer."""

    @abstractmethod
    def get_state(self) -> Dict[str, Any]:
        """Returns the pickable state of this buffer."""

    @classmethod
    @abstractmethod
    def from_state(cls, state: Dict[str, Any]) -> "BaseLookbackBuffer":
        """Creates a new buffer instance from a state dict."""
