from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas as pd


class Index(object):

    def __init__(self, idx):
        self.idx = idx

    @classmethod
    def to_pandas(indices):
        if isinstance(indices[0], pd.RangeIndex):
            merged = indices[0]
            for index in indices[1:]:
                merged = merged.union(index)
            return merged
        else:
            return indices[0].append(indices[1:])
