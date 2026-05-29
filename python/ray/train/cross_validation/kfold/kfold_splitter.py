from typing import Optional

import pandas as pd

from ray.train.cross_validation.kfold._hashbased import (
    _HashBasedKFoldSplitter,
)
from ray.util.annotations import PublicAPI


@PublicAPI
class KFoldSplitter(_HashBasedKFoldSplitter):
    """KFold splitter that assigns folds based on a hash of the full row content.

    WARNING: the default implementation intentionally excludes floating-point
    columns from the key frame to avoid non-deterministic hashing caused by
    platform/representation differences when converting floats to strings.

    If your intended key includes floating-point columns (and you accept the
    potential instability), subclass and override ``_get_key_frame`` to return
    the exact set of columns to include in the hash (for example, after
    applying a stable formatting to floats).

    Hashing the full row content can be expensive for datasets with large
    columns (e.g. text blobs, embeddings). If hashing cost or memory usage is
    a concern, consider one of the following alternatives:

    - Precompute a lightweight ``fold_id`` column outside of Ray and use
      filters to split by that column.

    - Use ``GroupedKFoldSplitter`` with a small key column that uniquely
      identifies grouping keys.

    - Subclass ``KFoldSplitter`` and override ``_get_key_frame`` to return
      a smaller set of ``key_columns`` to hash.

    """

    def __init__(self, n_splits: int, seed: Optional[int] = None) -> None:
        """Create a `KFoldSplitter`.

        Args:
            n_splits: Number of folds.
            seed: Optional int seed to incorporate into the hash for
                deterministic but different fold assignments between runs.
        """
        super().__init__(n_splits, seed)

    def _get_key_frame(self, batch: pd.DataFrame) -> pd.DataFrame:
        # Exclude float columns by default to avoid non-deterministic
        # string representations. Keep all other columns.
        non_float_cols = batch.select_dtypes(exclude=["float"]).columns

        # If excluding floats would drop all columns, normalize them
        # to ensure deterministic string serialization across platforms.
        if len(non_float_cols) == 0:
            return batch.round(6)

        return batch[non_float_cols]
