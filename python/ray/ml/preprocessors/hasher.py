import collections
from typing import List

import pandas as pd

from ray.ml.preprocessor import Preprocessor
import hashlib


class FeatureHasher(Preprocessor):
    """Hash the features of the specified columns.

    The created columns will have names in the format ``hash_{column_names}_{hash}``,
    e.g. ``hash_column1_column2_0``, ``hash_column1_column2_1``, ...

    Note: Currently sparse matrices are not supported.
    Therefore, It is recommended to **not** use a large ``num_features``.

    Args:
        columns: The columns of features that should be projected
                 onto a single hashed feature vector.
        num_features: The size of the hashed feature vector.
    """

    _is_fittable = False

    def __init__(self, columns: List[str], num_features: int):
        super().__init__()
        self.columns = columns
        # TODO(matt): Set default number of features.
        # This likely requires sparse matrix support to avoid explosion of columns.
        self.num_features = num_features

    def _transform_pandas(self, df: pd.DataFrame):
        # TODO(matt): Use sparse matrix for efficiency.
        joined_columns = "_".join(self.columns)

        def row_feature_hasher(row):
            hash_counts = collections.defaultdict(int)
            for column in self.columns:
                hashed_value = self._hash(row[column])
                hash_counts[hashed_value] = hash_counts[hashed_value] + 1
            return {
                f"hash_{joined_columns}_{i}": hash_counts[i]
                for i in range(self.num_features)
            }

        feature_columns = df.loc[:, self.columns].apply(
            row_feature_hasher, axis=1, result_type="expand"
        )
        df = df.join(feature_columns)

        # Drop original unhashed columns.
        df = df.drop(columns=self.columns)
        return df

    def _hash(self, value: object) -> int:
        encoded_value = str(value).encode()
        hashed_value = hashlib.sha1(encoded_value)
        hashed_value_int = int(hashed_value.hexdigest(), 16)
        return hashed_value_int % self.num_features

    def __repr__(self):
        return f"<Columns={self.columns} num_features={self.num_features} >"
