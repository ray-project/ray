import collections
from typing import List

import pandas as pd

from ray.data.preprocessor import Preprocessor

from ray.data.preprocessors.utils import simple_hash


class FeatureHasher(Preprocessor):
    """Hash the features of the specified columns.

    The created columns will have names in the format ``hash_{column_names}_{hash}``,
    e.g. ``hash_column1_column2_0``, ``hash_column1_column2_1``, ...

    Note: Currently sparse matrices are not supported.
    Therefore, it is recommended to **not** use a large ``num_features``.

    Args:
        columns: The columns of features that should be projected
                 onto a single hashed feature vector.
        num_features: The size of the hashed feature vector.
    """

    _is_fittable = False

    def __init__(self, columns: List[str], num_features: int):
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
                hashed_value = simple_hash(row[column], self.num_features)
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
        df.drop(columns=self.columns, inplace=True)
        return df

    def __repr__(self):
        return (
            f"FeatureHasher(columns={self.columns}, num_features={self.num_features})"
        )
