import logging
from typing import List, Tuple

import pandas as pd

from ray.data import Dataset
from ray.train.cross_validation.kfold._hashbased import (
    _HashBasedKFoldSplitter,
)
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI
class GroupedKFoldSplitter(_HashBasedKFoldSplitter):
    """Grouped KFold splitter that assigns rows to folds based on a hash of the
    values in specified grouping columns.

    Rows with identical values in the grouping columns will be assigned to the
    same fold, ensuring that related data (e.g. from the same user or session)
    is not split across training and validation sets. The hash is combined with
    an optional seed to allow for deterministic but different fold assignments across runs.

    WARNING: using floating-point columns as grouping keys can lead to non-deterministic
    fold assignments across platforms due to differences in float formatting and precision.
    To ensure deterministic splits, it is recommended to convert float columns to stable
    keys (e.g. by rounding, casting to integers, or normalizing/formatting as strings)
    before using them as grouping columns.
    """

    def __init__(
        self, n_splits: int, group_columns: List[str], seed: int | None = None
    ):
        """Create a GroupedKFoldSplitter.

        Args:
            n_splits: Number of folds to create.
            group_columns: List of column names whose identical values should
                be assigned to the same fold (e.g. user id, session id).
                Must be a non-empty list and the named columns must exist in
                the dataset passed to `split()`.
            seed: Optional int seed incorporated into the hash for
                deterministic but different fold assignments across runs.

        Raises:
            ValueError: If `group_columns` is empty.
        """

        super().__init__(n_splits, seed)

        if not group_columns:
            raise ValueError("group_columns must be a non-empty list")

        self._group_columns = group_columns

    def _get_key_frame(self, batch: pd.DataFrame) -> pd.DataFrame:
        float_cols = [c for c in self._group_columns if batch[c].dtype.kind == "f"]
        if float_cols:
            logger.warning(
                "GroupedKFoldSplitter: group_columns %s contain floating-point types. "
                "Using floats as grouping keys can produce non-deterministic fold "
                "assignments across platforms due to "
                "differences in float formatting and precision. To ensure "
                "deterministic splits, convert these columns to stable keys (e.g. "
                "round the floats, cast to integers, or normalize/format strings) ",
                float_cols,
            )
        return batch[self._group_columns]

    def split(self, dataset: Dataset) -> List[Tuple[Dataset, Dataset]]:
        # Validate that the configured group columns exist in the dataset schema
        schema_cols = set(dataset.schema().names)
        missing = set(self._group_columns) - schema_cols
        if missing:
            raise ValueError(
                f"group_columns not found in dataset: {missing}. "
                f"Available columns: {sorted(schema_cols)}"
            )

        return super().split(dataset)
