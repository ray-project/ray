import threading
import time
from abc import ABCMeta, abstractmethod
from collections import deque
from typing import Any, Dict, List, Optional, Union

from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class StatsBase(metaclass=ABCMeta):
    """A base class for Stats.

    Stats are meant to be used to log values to and then aggregate them in a tree.

    Therefore, we log to stats in two different ways:
    - On a leaf component, we log values directly by pushing.
    - On a non-leaf component, we only aggregate incoming values.

    Additionally, we pay special respect to Stats that live at the root of the tree.
    These may have a different behaviour (example: a lifetime sum).

    Note the tight coupling between StatsBase and MetricsLogger.
    """

    # In order to restore from a checkpoint, we need to know the class of the Stats object.
    # This is set in the subclass.
    stats_cls_identifier: str = None

    def __init__(
        self,
        is_root: bool = False,
        is_leaf: bool = True,
    ):
        """Initializes a StatsBase object.

        Args:
            is_root: If True, the Stats object is a root stats object.
            is_leaf: If True, the Stats object is a leaf stats object.

        Note: A stats object can be both root and leaf at the same time.
        Note: A stats object can also be neither root nor leaf ("intermediate" stats that only aggregate from other stats but are not at the root).
        """
        self.is_root = is_root
        self.is_leaf = is_leaf
        # Used to keep track of start times when using the `with` context manager.
        # This helps us measure times with threads in parallel.
        self._start_times = {}
        # For non-leaf stats (root or intermediate), track the latest merged values
        # This is overwritten on each merge operation
        if not self.is_leaf:
            self.latest_merged: Union[List[Any], Any] = None

        assert (
            self.stats_cls_identifier is not None
        ), "stats_cls_identifier must be set in the subclass"

    @property
    def has_throughputs(self) -> bool:
        """Returns True if the Stats object has throughput tracking enabled.

        Some Stats classes may have throughput tracking enabled, such as SumStats.
        """
        return False

    @OverrideToImplementCustomLogic
    def initialize_throughput_reference_time(self, time: float) -> None:
        """Sets the reference time for this Stats object.

        This is important because the component that tracks the time
        between reduce cycles is not Stats, but MetricsLogger.

        Args:
            time: The time to establish as the reference time for this Stats object.
        """
        if self.has_throughputs:
            raise ValueError(
                "initialize_throughput_reference_time must be overridden for stats objects that have throughputs."
            )

    @abstractmethod
    def __len__(self) -> int:
        """Returns the length of the internal values list."""
        ...

    def __float__(self):
        value = self.peek(compile=True)
        if isinstance(value, (list, tuple, deque)):
            raise ValueError(f"Can not convert {self} to float.")
        return float(value)

    def __int__(self):
        value = self.peek(compile=True)
        if isinstance(value, (list, tuple, deque)):
            raise ValueError(f"Can not convert {self} to int.")
        return int(value)

    def __eq__(self, other):
        return float(self) == float(other)

    def __le__(self, other):
        return float(self) <= float(other)

    def __ge__(self, other):
        return float(self) >= float(other)

    def __lt__(self, other):
        return float(self) < float(other)

    def __gt__(self, other):
        return float(self) > float(other)

    def __add__(self, other):
        return float(self) + float(other)

    def __sub__(self, other):
        return float(self) - float(other)

    def __mul__(self, other):
        return float(self) * float(other)

    def __format__(self, fmt):
        return f"{float(self):{fmt}}"

    def __enter__(self) -> "StatsBase":
        """Called when entering a context (with which users can measure a time delta).

        Returns:
            This stats instance.
        """
        thread_id = threading.get_ident()
        self._start_times[thread_id] = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_value, tb) -> None:
        """Called when exiting a context (with which users can measure a time delta).

        This pushes the time delta since __enter__ to this Stats object.
        """
        thread_id = threading.get_ident()
        assert self._start_times[thread_id] is not None
        time_delta_s = time.perf_counter() - self._start_times[thread_id]
        self.push(time_delta_s)

        del self._start_times[thread_id]

    @classmethod
    def from_state(cls, state: Dict[str, Any]) -> "StatsBase":
        """Creates a stats object from a state dictionary.

        Any implementation of this should call this base classe's
        `stats_object.set_state()` to set the state of the stats object.

        Args:
            state: The state to set after instantiation.
        """
        init_args = cls._get_init_args(state=state)
        stats = cls(**init_args)
        stats.set_state(state)
        return stats

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def clone(
        self,
        init_overrides: Optional[Dict[str, Any]] = None,
    ) -> "StatsBase":
        """Returns a new stats object with the same settings as `self`.

        Args:
            init_overrides: Optional dict of initialization arguments to override. Can be used to change is_root, is_leaf, etc.

        Returns:
            A new stats object similar to `self` but missing internal values.
        """
        init_args = self.__class__._get_init_args(stats_object=self)
        if init_overrides:
            init_args.update(init_overrides)
        new_stats = self.__class__(**init_args)
        return new_stats

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def get_state(self) -> Dict[str, Any]:
        """Returns the state of the stats object."""
        state = {
            "stats_cls_identifier": self.stats_cls_identifier,
            "is_root": self.is_root,
            "is_leaf": self.is_leaf,
        }
        if not self.is_leaf:
            state["latest_merged"] = self.latest_merged
        return state

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def set_state(self, state: Dict[str, Any]) -> None:
        """Sets the state of the stats object.

        Args:
            state: The state to set on this StatsBase object.
        """

        # Handle legacy state that uses old attribute names
        self.is_root = state["is_root"]
        self.is_leaf = state["is_leaf"]
        # Prevent setting a state with a different stats class identifier
        assert self.stats_cls_identifier == state["stats_cls_identifier"]
        if not self.is_leaf:
            # Handle legacy state that doesn't have latest_merged
            self.latest_merged = state["latest_merged"]

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @staticmethod
    def _get_init_args(stats_object=None, state=None) -> Dict[str, Any]:
        """Returns the initialization arguments for this Stats object."""
        if state is not None:
            # Handle legacy state that uses old attribute names
            is_root = state["is_root"]
            is_leaf = state["is_leaf"]
            return {
                "is_root": is_root,
                "is_leaf": is_leaf,
            }
        elif stats_object is not None:
            return {
                "is_root": stats_object.is_root,
                "is_leaf": stats_object.is_leaf,
            }
        else:
            raise ValueError("Either stats_object or state must be provided")

    @abstractmethod
    def __repr__(self) -> str:
        ...

    @abstractmethod
    def merge(self, incoming_stats: List["StatsBase"]) -> None:
        """Merges StatsBase objects.

        Args:
            incoming_stats: The list of StatsBase objects to merge.
        """

    @abstractmethod
    def push(self, value: Any) -> None:
        """Pushes a value into this Stats object.

        Args:
            value: The value to push. Can be of any type.
                GPU tensors are moved to CPU memory.
        """
        assert (
            self.is_leaf
        ), "Cannot push values to non-leaf Stats. Non-leaf Stats can only receive values via merge()."

    @abstractmethod
    def peek(
        self, compile: bool = True, latest_merged_only: bool = False
    ) -> Union[Any, List[Any]]:
        """Returns the result of reducing the internal values list.

        Note that this method does NOT alter the internal values list in this process.
        Thus, users can call this method to get an accurate look at the reduced value(s)
        given the current internal values list.

        Args:
            compile: If True, the result is compiled into a single value if possible.
            latest_merged_only: If True, only considers the latest merged values.
                This parameter only works on aggregation stats objects (is_leaf=False).
                When enabled, peek() will only use the values from the most recent merge operation.

        Returns:
            The result of reducing the internal values list on CPU memory.
        """

    @abstractmethod
    def reduce(self, compile: bool = True) -> Union[Any, "StatsBase"]:
        """Reduces the internal values.

        This method should NOT be called directly by users.
        It can be used as a hook to prepare the stats object for sending it to the root metrics logger and starting a new 'reduce cycle'.

        The reduction logic depends on the implementation of the subclass.
        Meaning that some classes may reduce to a single value, while others do not or don't even contain values.

        Args:
            compile: If True, the result is compiled into a single value if possible.
                If False, the result is a Stats object similar to itself, but with the internal values reduced.
        Returns:
            The reduced value or a Stats object similar to itself, but with the internal values reduced.
        """
