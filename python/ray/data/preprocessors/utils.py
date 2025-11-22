import hashlib
from collections import deque
from typing import Any, Callable, Deque, Dict, List, Optional, Union

from ray.data import Dataset
from ray.data.aggregate import AggregateFnV2
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
def simple_split_tokenizer(value: str) -> List[str]:
    """Tokenize a string using a split on spaces."""
    return value.split(" ")


@DeveloperAPI
def simple_hash(value: object, num_features: int) -> int:
    """Deterministically hash a value into the integer space."""
    encoded_value = str(value).encode()
    hashed_value = hashlib.sha1(encoded_value)
    hashed_value_int = int(hashed_value.hexdigest(), 16)
    return hashed_value_int % num_features


class BaseStatSpec:
    """Encapsulates a statistical computation with optional post-processing."""

    def __init__(
        self,
        *,
        stat_fn: Union[AggregateFnV2, Callable],
        post_process_fn: Callable = lambda x: x,
        post_key_fn: Callable[[str], str],
    ):
        self.stat_fn = stat_fn
        self.post_process_fn = post_process_fn
        self.post_key_fn = post_key_fn


class AggregateStatSpec(BaseStatSpec):
    """Represents an AggregateFnV2 spec for a single column."""

    def __init__(
        self,
        *,
        aggregator_fn: Union[AggregateFnV2, Callable[[str], AggregateFnV2]],
        post_process_fn: Callable = lambda x: x,
        post_key_fn: Callable[[str], str],
        column: Optional[str] = None,
    ):
        super().__init__(
            stat_fn=aggregator_fn,
            post_process_fn=post_process_fn,
            post_key_fn=post_key_fn,
        )
        self.column = column


class CallableStatSpec(BaseStatSpec):
    """Represents a user-defined stat function that operates outside Dataset.aggregate."""

    def __init__(
        self,
        *,
        stat_fn: Callable,
        stat_key_fn: Optional[Callable[[str], str]],
        post_key_fn: Optional[Callable[[str], str]],
        post_process_fn: Callable = lambda x: x,
        columns: List[str],
    ):
        super().__init__(
            stat_fn=stat_fn, post_process_fn=post_process_fn, post_key_fn=post_key_fn
        )
        self.columns = columns
        self.stat_key_fn = stat_key_fn


class StatComputationPlan:
    """
    Encapsulates a set of aggregators (AggregateFnV2) and legacy stat functions
    to compute statistics over a Ray dataset.

    Supports two types of aggregations:
    1. AggregateFnV2-based aggregators, which are batch-executed using `Dataset.aggregate(...)`.
    2. Callable-based stat functions, executed sequentially (legacy use case).
    """

    def __init__(self):
        self._aggregators: Deque[BaseStatSpec] = deque()

    def reset(self):
        self._aggregators.clear()

    def add_aggregator(
        self,
        *,
        aggregator_fn: Callable[[str], AggregateFnV2],
        post_process_fn: Callable = lambda x: x,
        post_key_fn: Optional[Callable[[str], str]] = None,
        columns: List[str],
    ) -> None:
        """
        Registers an AggregateFnV2 factory for one or more columns.

        Args:
            aggregator_fn: A callable (typically a lambda or class) that accepts a column name and returns an instance of AggregateFnV2.
            post_process_fn: Function to post-process the aggregated result.
            post_key_fn: Optional key generator to use to save aggregation results after post-processing.
            columns: List of column names to aggregate.
        """
        for column in columns:
            agg_instance = aggregator_fn(column)
            self._aggregators.append(
                AggregateStatSpec(
                    aggregator_fn=agg_instance,
                    post_process_fn=post_process_fn,
                    post_key_fn=post_key_fn,
                    column=column,
                )
            )

    def add_callable_stat(
        self,
        *,
        stat_fn: Callable[[], Any],
        post_process_fn: Callable = lambda x: x,
        stat_key_fn: Callable[[str], str],
        post_key_fn: Optional[Callable[[str], str]] = None,
        columns: List[str],
    ) -> None:
        """
        Registers a custom stat function to be run sequentially.

        This supports legacy use cases where arbitrary callables are needed
        and cannot be run via Dataset.aggregate().

        Args:
            stat_fn: A zero-argument callable that returns the stat.
            post_process_fn: Function to apply to the result.
            stat_key_fn:
            post_key_fn:
            columns:
        """
        self._aggregators.append(
            CallableStatSpec(
                stat_fn=stat_fn,
                post_process_fn=post_process_fn,
                columns=columns,
                stat_key_fn=stat_key_fn,
                post_key_fn=post_key_fn or stat_key_fn,
            )
        )

    def compute(self, dataset: Dataset) -> Dict[str, Any]:
        """
        Executes all registered aggregators and stat functions.

        AggregateFnV2-based aggregators are batched and executed via Dataset.aggregate().
        Callable-based stat functions are run sequentially.

        Args:
            dataset: The Ray Dataset to compute statistics on.

        Returns:
            A dictionary of computed statistics.
        """
        stats = {}
        # Run batched aggregators (AggregateFnV2)
        aggregators = self._get_aggregate_fn_list()
        if aggregators:
            raw_result = dataset.aggregate(*aggregators)
            for spec in self._get_aggregate_specs():
                stat_key = spec.stat_fn.name
                post_key = (
                    spec.post_key_fn(spec.column)
                    if spec.post_key_fn is not None
                    else stat_key
                )
                stats[post_key] = spec.post_process_fn(raw_result[stat_key])

        # Run sequential stat functions
        for spec in self._get_custom_stat_fn_specs():
            result = spec.stat_fn(spec.stat_key_fn)
            for col in spec.columns:
                stat_key = spec.stat_key_fn(col)
                post_key = spec.post_key_fn(col)
                stats[post_key] = spec.post_process_fn(result[stat_key])

        return stats

    def _get_aggregate_fn_list(self) -> List[AggregateFnV2]:
        return [
            spec.stat_fn
            for spec in self._aggregators
            if isinstance(spec, AggregateStatSpec)
        ]

    def _get_aggregate_specs(self) -> List[AggregateStatSpec]:
        return [
            spec for spec in self._aggregators if isinstance(spec, AggregateStatSpec)
        ]

    def _get_custom_stat_fn_specs(self) -> List[CallableStatSpec]:
        return [
            spec for spec in self._aggregators if isinstance(spec, CallableStatSpec)
        ]

    def has_custom_stat_fn(self):
        return len(self._get_custom_stat_fn_specs()) > 0

    def __iter__(self):
        """
        Iterates over all AggregatorSpecs.
        """
        return iter(self._get_aggregate_specs())


def make_post_processor(base_fn, callbacks: List[Callable]):
    """
    Wraps a base post-processing function with a sequence of callback functions.
    Useful when multiple post-processing steps need to be applied in order.
    """

    def wrapper(result):
        processed = base_fn(result)
        for cb in callbacks:
            processed = cb(processed)
        return processed

    return wrapper
