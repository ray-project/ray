from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np


def arrayify(s):
    if type(s) in [int, float, str, np.ndarray]:
        return s
    elif type(s) is list:
        # recursive call to convert LazyFrames to arrays
        return np.array([arrayify(x) for x in s])
    else:
        return np.array(s)


class SampleBatch(object):
    """Wrapper around a dictionary with string keys and array-like values.

    For example, {"obs": [1, 2, 3], "reward": [0, -1, 1]} is a batch of three
    samples, each with an "obs" and "reward" attribute.
    """

    def __init__(self, *args, **kwargs):
        """Constructs a sample batch (same params as dict constructor)."""

        self.data = dict(*args, **kwargs)
        lengths = []
        for k, v in self.data.copy().items():
            assert type(k) == str, self
            lengths.append(len(v))
        assert len(set(lengths)) == 1, "data columns must be same length"
        self.count = lengths[0]

    @staticmethod
    def concat_samples(samples):
        out = {}
        for k in samples[0].data.keys():
            out[k] = np.concatenate([s.data[k] for s in samples])
        return SampleBatch(out)

    def concat(self, other):
        """Returns a new SampleBatch with each data column concatenated.

        Examples:
            >>> b1 = SampleBatch({"a": [1, 2]})
            >>> b2 = SampleBatch({"a": [3, 4, 5]})
            >>> print(b1.concat(b2))
            {"a": [1, 2, 3, 4, 5]}
        """

        assert self.data.keys() == other.data.keys(), "must have same columns"
        out = {}
        for k in self.data.keys():
            out[k] = np.concatenate([self.data[k], other.data[k]])
        return SampleBatch(out)

    def rows(self):
        """Returns an iterator over data rows, i.e. dicts with column values.

        Examples:
            >>> batch = SampleBatch({"a": [1, 2, 3], "b": [4, 5, 6]})
            >>> for row in batch.rows():
                   print(row)
            {"a": 1, "b": 4}
            {"a": 2, "b": 5}
            {"a": 3, "b": 6}
        """

        for i in range(self.count):
            row = {}
            for k in self.data.keys():
                row[k] = self[k][i]
            yield row

    def columns(self, keys):
        """Returns a list of just the specified columns.

        Examples:
            >>> batch = SampleBatch({"a": [1], "b": [2], "c": [3]})
            >>> print(batch.columns(["a", "b"]))
            [[1], [2]]
        """

        out = []
        for k in keys:
            out.append(self.data[k])
        return out

    def shuffle(self):
        permutation = np.random.permutation(self.count)
        for key, val in self.data.items():
            self.data[key] = val[permutation]

    def __getitem__(self, key):
        return self.data[key]

    def __str__(self):
        return "SampleBatch({})".format(str(self.data))

    def __repr__(self):
        return "SampleBatch({})".format(str(self.data))
