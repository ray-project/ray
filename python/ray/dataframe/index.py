from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas as pd


class Index(object):

    def __init__(self, idx, pandas_type):
        self.idx = idx
        self.pandas_type = pandas_type

    def __getitem__(self, item):
        return self.idx[item]

    def __len__(self):
        return len(self.idx)

    @classmethod
    def to_pandas(cls, index):
        """Convert a Ray Index object to a Pandas Index object.

        Args:
            index (ray.Index): A Ray Index object.

        Returns:
            A pandas Index object.
        """
        k = index.idx.keys()
        if index.pandas_type is pd.RangeIndex:
            return pd.RangeIndex(min(k), max(k)+1)
        else:
            return pd.Index(k)

    @classmethod
    def from_pandas(cls, pd_index, lengths):
        """Convert a Pandas Index object to a Ray Index object.

        Args:
            pd_index (pd.Index): A Pandas Index object.
            lengths ([int]): A list of lengths for the partitions.

        Returns:
            A Ray Index object.
        """
        dest_indices = [(i, j)
                        for i in range(len(lengths))
                        for j in range(lengths[i])]
        if len(pd_index) != len(dest_indices):
            raise ValueError(
              "Length of index given does not match current dataframe")

        return Index(
          {pd_index[i]: dest_indices[i] for i in range(len(dest_indices))},
          type(pd_index))
