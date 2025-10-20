from collections import deque
import threading
import time
from typing import Any, Dict, List, Union
from abc import ABCMeta, abstractmethod
from ray.util.annotations import DeveloperAPI


from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)


@DeveloperAPI
class StatsBase(metaclass=ABCMeta):
    """A base class for Stats.

    Stats are meant to be used in conjunction with the MetricsLogger class.
    """

    # In order to restore from a checkpoint, we need to know the class of the Stats object.
    # This is set in the subclass.
    stats_cls_identifier: str = None

    def __init__(
        self,
        is_root_stats: bool = False,
        clear_on_reduce: bool = True,
        reduce_at_root: bool = False,
    ):
        self._is_root_stats = is_root_stats
        self._clear_on_reduce = clear_on_reduce
        self._reduce_at_root = reduce_at_root
        # Used to keep track of start times when using the `with` context manager.
        # This helps us measure times with threads in parallel.
        self._start_times = {}

        assert (
            self.stats_cls_identifier is not None
        ), "stats_cls_identifier must be set in the subclass"

    @property
    def has_throughputs(self) -> bool:
        """Returns True if the Stats object has throughput tracking enabled.

        Some Stats classes may have throughput tracking enabled, such as SumStats.
        """
        return False

    def initialize_throughput_reference_time(self, time: float) -> None:
        """If the Stats object has throughput tracking enabled, this method is called by the MetricsLogger to set the initial time for throughput calculation."""
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
            raise ValueError(f"Value {value} is a list, tuple, or deque, not a scalar")
        return float(value)

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
            This stats instance (self), unless another thread has already entered (and
            not exited yet), in which case a copy of `self` is returned. This way, the
            second thread(s) cannot mess with the original stats's (self) time-measuring.
            This also means that only the first thread to __enter__ actually logs into
            `self` and the following threads' measurements are discarded (logged into
            a non-referenced shim-Stats object, which will simply be garbage collected).
        """
        # In case another thread already is measuring this Stats (timing), simply ignore
        # the "enter request" and return a clone of `self`.
        thread_id = threading.get_ident()
        self._start_times[thread_id] = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_value, tb) -> None:
        """Called when exiting a context (with which users can measure a time delta)."""
        thread_id = threading.get_ident()
        assert self._start_times[thread_id] is not None
        time_delta_s = time.perf_counter() - self._start_times[thread_id]
        self.push(time_delta_s)

        del self._start_times[thread_id]

    @classmethod
    def from_state(cls, state: Dict[str, Any]) -> "StatsBase":
        """Creates a stats object from a state dictionary

        Any implementation of this should call this base classe's `stats_object.set_state()` to set the state of the stats object.
        """
        init_args = cls._get_init_args(state=state)
        stats = cls(**init_args)
        stats.set_state(state)
        return stats

    @classmethod
    def similar_to(
        cls,
        other: "StatsBase",
    ) -> "StatsBase":
        """Returns a new stats object that's similar to `other`.

        "Similar" here means it has the exact same settings (reduce, window, ema_coeff,
        etc..). The initial values of the returned stats are empty by default, but
        can be set as well.

        Args:
            other: The other stats object to return a similar new stats equivalent for.
            init_value: The initial value to already push into the returned stats.

        Returns:
            A new stats object similar to `other`, with the exact same settings and
            maybe a custom initial value (if provided; otherwise empty).
        """
        return cls(**cls._get_init_args(stats_object=other))

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def get_state(self) -> Dict[str, Any]:
        """Returns the state of the stats object."""
        return {
            "stats_cls_identifier": self.stats_cls_identifier,
            "_is_root_stats": self._is_root_stats,
            "_clear_on_reduce": self._clear_on_reduce,
        }

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def set_state(self, state: Dict[str, Any]) -> None:
        """Sets the state of the stats object."""

        self._is_root_stats = state["_is_root_stats"]
        self._clear_on_reduce = state["_clear_on_reduce"]
        # Prevent setting a state with a different stats class identifier
        assert self.stats_cls_identifier == state["stats_cls_identifier"]

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @staticmethod
    def _get_init_args(stats_object=None, state=None) -> Dict[str, Any]:
        """Returns the initialization arguments for this Stats object."""
        if state is not None:
            return {
                "is_root_stats": state["_is_root_stats"],
                "clear_on_reduce": state["_clear_on_reduce"],
            }
        elif stats_object is not None:
            return {
                "is_root_stats": stats_object._is_root_stats,
                "clear_on_reduce": stats_object._clear_on_reduce,
            }
        else:
            raise ValueError("Either stats_object or state must be provided")

    @abstractmethod
    def __repr__(self) -> str:
        ...

    @abstractmethod
    def merge(self, incoming_stats: List["StatsBase"], replace=True):
        """Merges StatsBase objects.

        Args:
            incoming_stats: The list of StatsBase objects to merge.
            replace: If True, replace internal items with the result of the merge.

        Returns:
            The merged StatsBase object.
        """
        ...

    @abstractmethod
    def push(self, value: Any) -> None:
        """Pushes a value into this Stats object.

        Args:
            value: The value to push. Can be of any type.
                GPU tensors are moved to CPU memory.

        Returns:
            None
        """
        ...

    @abstractmethod
    def peek(self, compile: bool = True) -> Union[Any, List[Any]]:
        """Returns the result of reducing the internal values list.

        Note that this method does NOT alter the internal values list in this process.
        Thus, users can call this method to get an accurate look at the reduced value(s)
        given the current internal values list.

        Args:
            compile: If True, the result is compiled into a single value if possible.

        Returns:
            The result of reducing the internal values list on CPU memory.
        """
        ...

    @abstractmethod
    def reduce(self, compile: bool = True) -> Union[Any, "StatsBase"]:
        """Reduces the internal values.

        This method should NOT be called directly by users.
        It can be used as a hook to prepare the stats object for sending it to the root metrics logger and starting a new 'reduce cycle'.

        The reduction logic depends on the implementation of the subclass.
        Meaning that some classes may reduce to a single value, while others do not or don't even contain values.

        If `reduce_at_root` is True, costly reductions like calculating a mean over multiple values will be performed at the root logger.
        This is useful for cases where we don't want to spend CPU time in parallel components, e.g. Learner Actors.

        Args:
            compile: If True, the result is compiled into a single value if possible.
                If False, the result is a Stats object similar to itself, but with the internal values reduced.
        Returns:
            The reduced value or a Stats object similar to itself, but with the internal values reduced.
        """
        ...
