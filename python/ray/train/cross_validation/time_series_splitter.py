from typing import List, Optional, Tuple

from ray.data import Dataset
from ray.train.cross_validation.splitter import Splitter
from ray.util.annotations import PublicAPI


def _compute_fold_boundaries(
    n_samples: int,
    n_splits: int,
    val_size: Optional[int],
    gap: int,
    max_train_size: Optional[int],
) -> List[Tuple[int, int, int, int]]:
    """Module-level implementation for computing fold boundaries."""
    val_size_final = val_size if val_size is not None else n_samples // (n_splits + 1)

    # Verify there are enough rows for the given parameters.
    if n_splits >= n_samples:
        raise ValueError(
            f"The number of samples ({n_samples}) must be strictly greater "
            f"than the number of splits ({n_splits})."
        )

    min_train_rows = n_samples - n_splits * val_size_final - gap
    if min_train_rows < 1:
        raise ValueError(
            f"Not enough rows ({n_samples}) for n_splits={n_splits}, "
            f"val_size={val_size_final}, gap={gap}. "
            f"Need at least {n_splits * val_size_final + gap + 1} rows."
        )

    folds: List[Tuple[int, int, int, int]] = []
    for val_start in range(
        n_samples - n_splits * val_size_final, n_samples, val_size_final
    ):
        train_end = val_start - gap
        train_start = (
            max(0, train_end - max_train_size) if max_train_size is not None else 0
        )
        val_end = val_start + val_size_final
        folds.append((train_start, train_end, val_start, val_end))

    return folds


@PublicAPI
class TimeSeriesSplitter(Splitter):
    """Time Series Splitter for datasets where sample order matters (e.g. time series,
    logs, sensor readings).

    Produces ``n_splits`` (train, val) pairs where each val set is strictly later in
    time than its train set, preventing future information from leaking into training.
    The train set grows with each fold as the val window advances toward the end of the dataset.

    If ``time_column`` is provided the dataset is sorted by that column before
    splitting. Otherwise the dataset is assumed to already be in temporal order.

    .. warning::
        When sorting by ``time_column``, rows with identical timestamps have no
        guaranteed tie-breaker.
    """

    def __init__(
        self,
        n_splits: int,
        max_train_size: Optional[int] = None,
        val_size: Optional[int] = None,
        gap: int = 0,
        time_column: Optional[str] = None,
    ) -> None:
        """Create a ``TimeSeriesSplitter``.

        Args:
            n_splits: Number of folds.
            max_train_size: Maximum number of rows in the training set. If
                ``None``, all rows before the gap are used.
            val_size: Number of rows in each validation set. If ``None``,
                defaults to ``n_samples // (n_splits + 1)``.
            gap: Number of rows to skip between the end of the train set and
                the start of the val set. Useful to prevent leakage when there
                is temporal autocorrelation.
            time_column: Column to sort by before splitting. If ``None``, the
                dataset is assumed to already be in temporal order.
        """
        super().__init__(n_splits)
        if val_size is not None and val_size <= 0:
            raise ValueError("val_size must be a positive integer.")
        if gap < 0:
            raise ValueError("gap must be >= 0.")
        if max_train_size is not None and max_train_size <= 0:
            raise ValueError("max_train_size must be a positive integer.")
        self._time_column = time_column
        self._val_size = val_size
        self._gap = gap
        self._max_train_size = max_train_size

    def split(self, dataset: Dataset) -> List[Tuple[Dataset, Dataset]]:
        if self._time_column is not None:
            schema_cols = set(dataset.schema().names)
            if self._time_column not in schema_cols:
                raise ValueError(
                    f"time_column '{self._time_column}' not found in dataset. "
                    f"Available columns: {sorted(schema_cols)}"
                )
            dataset = dataset.sort(self._time_column)

        # Materialize so that all split_at_indices calls below see the same
        # row order. Without this, re-executing a lazy multi-block pipeline
        # with preserve_order=False can yield blocks in a different order each
        # time, causing splits to select the wrong rows.
        dataset = dataset.materialize()

        n_samples = dataset.count()

        fold_boundaries = _compute_fold_boundaries(
            n_samples,
            self._n_splits,
            self._val_size,
            self._gap,
            self._max_train_size,
        )

        folds = []
        for train_start, train_end, val_start, val_end in fold_boundaries:
            if train_start > 0:
                train = dataset.split_at_indices([train_start, train_end])[1]
            else:
                train = dataset.split_at_indices([train_end])[0]

            if val_end < n_samples:
                val = dataset.split_at_indices([val_start, val_end])[1]
            else:
                val = dataset.split_at_indices([val_start])[1]

            folds.append((train, val))

        return folds
