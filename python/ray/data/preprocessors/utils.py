import hashlib
from collections import Counter
from typing import Callable, List, Optional

import numpy as np
import pandas as pd
import pandas.api.types

from ray.data.aggregate import AggregateFnV2
from ray.data.block import Block, BlockAccessor
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


class ValueCounter(AggregateFnV2):
    """Counts the number of times each value appears in a column."""

    def __init__(
        self,
        on: str,
        alias_name: Optional[str] = None,
        encode_lists: bool = True,
    ):
        self._encode_lists = encode_lists
        super().__init__(
            alias_name if alias_name else f"value_counter({str(on)})",
            on=on,
            ignore_nulls=False,
            zero_factory=lambda: Counter(),
        )

    def aggregate_block(self, block: Block) -> Counter:

        block_acc = BlockAccessor.for_block(block)
        df = block_acc.to_pandas()
        col_series = df[self._target_col_name]

        def _is_series_composed_of_lists(series: pd.Series) -> bool:
            # we assume that all elements are a list here
            first_not_none_element = next(
                (element for element in series if element is not None), None
            )
            return pandas.api.types.is_object_dtype(series.dtype) and isinstance(
                first_not_none_element, (list, np.ndarray)
            )

        def get_pd_value_counts_per_column(col_series: pd.Series):

            # special handling for lists
            if _is_series_composed_of_lists(col_series):
                if self._encode_lists:
                    counter = Counter()

                    def update_counter(element):
                        counter.update(element)
                        return element

                    col_series.map(update_counter)
                    return counter
                else:
                    # convert to tuples to make lists hashable
                    col_series = col_series.map(lambda x: tuple(x))
            series_counts = col_series.value_counts(dropna=False).to_dict()
            return Counter(series_counts)

        return get_pd_value_counts_per_column(col_series)

    def combine(self, current_accumulator: Counter, new: Counter) -> Counter:
        current_accumulator_dict = {}
        new_accumulator_dict = {}

        # Counter.update fails with ValueError if any of values in Counter are None
        for item, count in current_accumulator.items():
            if count is not None:
                current_accumulator_dict[item] = count

        for item, count in new.items():
            if count is not None:
                new_accumulator_dict[item] = count

        updated_counter = Counter(current_accumulator_dict)
        updated_counter.update(new_accumulator_dict)
        return updated_counter


class TokenCounter(AggregateFnV2):
    """Counts the number of tokens in a column."""

    def __init__(
        self,
        on: str,
        alias_name: Optional[str] = None,
        tokenization_fn: Optional[Callable[[str], List[str]]] = None,
    ):
        # Initialize with a list accumulator [null_count, total_count]
        super().__init__(
            alias_name if alias_name else f"token_counter({str(on)})",
            on=on,
            ignore_nulls=False,  # Include nulls for this calculation
            zero_factory=lambda: Counter(),  # Our AggType is a Counter
        )
        self.tokenization_fn = tokenization_fn or simple_split_tokenizer

    def aggregate_block(self, block: Block) -> Counter:
        # Use BlockAccessor to work with the block
        block_acc = BlockAccessor.for_block(block)

        df = block_acc.to_pandas()
        token_series = df[self._target_col_name].apply(self.tokenization_fn)
        tokens = token_series.sum()
        return Counter(tokens)

    def combine(self, current_accumulator: Counter, new: Counter) -> Counter:
        current_accumulator_dict = {}
        new_accumulator_dict = {}

        # Counter.update fails with ValueError if any of values in Counter are None
        for item, count in current_accumulator.items():
            if count is not None:
                current_accumulator_dict[item] = count

        for item, count in new.items():
            if count is not None:
                new_accumulator_dict[item] = count

        updated_counter = Counter(current_accumulator_dict)
        updated_counter.update(new_accumulator_dict)
        return updated_counter
