import hashlib
from typing import List

from ray.util.annotations import DeveloperAPI
from ray.data.aggregate import AggregateFnV2
from ray.data.block import BlockAccessor, Block
from typing import List, Optional
from collections import Counter
import pandas.api.types
import pandas as pd
import numpy as np


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

            return Counter(col_series.value_counts(dropna=False).to_dict())

        return get_pd_value_counts_per_column(col_series)

    def combine(self, current_accumulator: Counter, new: Counter) -> Counter:
        return current_accumulator + new
