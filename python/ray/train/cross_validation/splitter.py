"""Base classes and utilities for cross-validation splitters.

This module defines the abstract `Splitter` base class and helper
functions used across different splitter implementations (KFold,
StratifiedKFold, TimeSeries, etc.). Helpers in this module are
considered internal unless explicitly exported.
"""

from abc import ABC, abstractmethod
from typing import Iterable, List, Tuple

from ray.data import Dataset
from ray.data.expressions import col
from ray.util.annotations import PublicAPI


@PublicAPI
class Splitter(ABC):
    def __init__(self, n_splits: int):
        if not isinstance(n_splits, int) or n_splits < 2:
            raise ValueError("n_splits must be an integer >= 2")
        self._n_splits = n_splits

    @abstractmethod
    def split(self, dataset: Dataset) -> List:
        """Partition ``dataset`` into a list of ``(train, val)`` dataset pairs.

        Implementations must return an iterable of tuples where each tuple
        contains the training dataset and the corresponding validation
        dataset for a single fold.

        Args:
            dataset: A :class:`ray.data.Dataset` containing the full set of
                examples to be partitioned. Implementations may assume the
                dataset is lazily-evaluated and should materialize it if
                needed.

        Returns:
            A list of ``(train, val)`` tuples where each element is a
            :class:`ray.data.Dataset` representing the training and
            validation split for a fold. The typical length of the list is
            ``self._n_splits``, although concrete splitters may impose
            additional constraints.

        Raises:
            ValueError: If the splitter configuration is invalid for the
                provided dataset (for example, missing required columns or
                insufficient samples for the requested number of splits).
        """
        raise NotImplementedError


def _get_noncolliding_column(existing_columns: Iterable[str], desired_name: str) -> str:
    """Return a non-colliding column name based on `desired_name`.

    If `desired_name` collides with `existing_columns`, append a numeric
    suffix: `desired_name`, `desired_name_1`, `desired_name_2`, ...
    This function is intended for internal use.
    """
    col_name = desired_name
    suffix = 0
    existing = set(existing_columns)
    while col_name in existing:
        suffix += 1
        col_name = f"{desired_name}_{suffix}"
    return col_name


def _build_folds_from_column(
    dataset_with_fold_id: Dataset, fold_column: str, n_splits: int
) -> List[Tuple[Dataset, Dataset]]:
    """Build (train, val) pairs from a dataset that already has a fold column.

    This centralizes the common filtering + drop logic used by KFold splitters.
    """
    folds: List[Tuple[Dataset, Dataset]] = []
    for i in range(n_splits):
        val = dataset_with_fold_id.filter(expr=(col(fold_column) == i)).drop_columns(
            [fold_column]
        )

        train = dataset_with_fold_id.filter(expr=(col(fold_column) != i)).drop_columns(
            [fold_column]
        )

        folds.append((train, val))

    return folds
