"""Internal utilities and base class for KFold-style splitters.

This module contains the hash-based implementation used by several
KFold splitters (KFold, GroupedKFold, ...). The symbols in this module are
internal and not part of the public API.
"""
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

import pandas as pd

from ray.data import Dataset
from ray.train.cross_validation.splitter import (
    Splitter,
    _build_folds_from_column,
    _get_noncolliding_column,
)


def _compute_fold_ids(
    key_frame: pd.DataFrame, seed: Optional[int], n_splits: int
) -> pd.Series:
    """Module-level implementation for computing fold ids from a key frame.

    Accepts a pre-extracted `key_frame` (DataFrame) so the function is pure
    and easy to test. Returns an int32 Series with fold ids in [0, n_splits).
    """
    key_frame = key_frame.astype("string").fillna("")

    hashes = pd.util.hash_pandas_object(
        key_frame,
        index=False,
    ).astype("uint64")

    if seed is not None:
        hashes = pd.util.hash_pandas_object(
            pd.DataFrame(
                {
                    "seed": pd.Series(seed, index=hashes.index),
                    "key_hash": hashes,
                }
            ),
            index=False,
        ).astype("uint64")

    return (hashes % n_splits).astype("int32")


class _HashBasedKFoldSplitter(Splitter, ABC):
    """Internal base class for hash-based deterministic KFold splitting.

    Implements a stateless fold assignment strategy using hashing.

    Subclasses define the input columns used to compute the hash.
    """

    def __init__(self, n_splits: int, seed: Optional[int] = None):
        """Initialize a hash-based KFold splitter.

        Args:
            n_splits: Number of folds to create.
            seed: Optional seed incorporated into the hash for reproducible
                but varying fold assignments across runs.

        Notes:
            This base class implements a stateless fold assignment using a
            hash of a key frame (provided by subclasses via
            ``_get_key_frame``). By default the splitter keeps the pipeline
            streaming (it does not materialize the dataset after adding the
            fold column) to avoid unbounded memory/disk pressure on large
            datasets. Callers that prefer to avoid recomputation and can
            afford materialization should materialize externally before
            calling ``split()``.
        """

        super().__init__(n_splits)
        self._seed = seed

    @abstractmethod
    def _get_key_frame(self, batch: pd.DataFrame) -> pd.DataFrame:
        """Return the DataFrame used as the key for hashing for a batch.

        Subclasses must implement this to select/transform the columns
        that should be hashed to compute fold ids.
        """
        raise NotImplementedError

    def split(self, dataset: Dataset) -> List[Tuple[Dataset, Dataset]]:
        get_key_frame = self._get_key_frame
        seed = self._seed
        n_splits = self._n_splits

        # Use a collision-free column name in case the dataset already has a "fold_id" column.
        existing_columns = set(dataset.schema().names)
        fold_column = _get_noncolliding_column(existing_columns, "fold_id")

        def add_fold_column(batch: pd.DataFrame) -> pd.DataFrame:
            batch[fold_column] = _compute_fold_ids(get_key_frame(batch), seed, n_splits)
            return batch

        # We intentionally keep the pipeline streaming here (no materialize).
        # Materializing the dataset after adding a fold column would avoid
        # re-running the `map_batches` work during the 2*n_splits filter
        # passes performed by `_build_folds_from_column`, but it can cause
        # excessive memory use for very large datasets.
        dataset_with_fold_id = dataset.map_batches(
            add_fold_column, batch_format="pandas"
        )

        return _build_folds_from_column(dataset_with_fold_id, fold_column, n_splits)
