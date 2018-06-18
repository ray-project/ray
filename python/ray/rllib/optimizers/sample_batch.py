from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import numpy as np


class SampleBatchBuilder(object):
    """Util to build a SampleBatch incrementally.

    For efficiency, SampleBatches hold values in column form (as arrays).
    However, it is useful to add data one row (dict) at a time.
    """

    def __init__(self):
        self.postprocessed = []
        self.buffers = collections.defaultdict(list)
        self.count = 0

    def add_values(self, **values):
        """Add the given dictionary (row) of values to this batch."""

        for k, v in values.items():
            self.buffers[k].append(v)
        self.count += 1

    def postprocess_batch_so_far(self, postprocessor):
        """Apply the given postprocessor to any unprocessed rows."""

        batch = postprocessor(self._build_buffers())
        self.postprocessed.append(batch)

    def build_and_reset(self, postprocessor):
        """Returns a sample batch including all previously added values.

        Any unprocessed rows will be first postprocessed with the given
        postprocessor. The internal state of this builder will be reset.
        """

        self.postprocess_batch_so_far(postprocessor)
        batch = SampleBatch.concat_samples(self.postprocessed)
        self.postprocessed = []
        self.count = 0
        return batch

    def _build_buffers(self):
        batch = SampleBatch({k: np.array(v) for k, v in self.buffers.items()})
        self.buffers.clear()
        return batch


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
        samples = [s for s in samples if s.count > 0]
        for k in samples[0].keys():
            out[k] = np.concatenate([s[k] for s in samples])
        return SampleBatch(out)

    def concat(self, other):
        """Returns a new SampleBatch with each data column concatenated.

        Examples:
            >>> b1 = SampleBatch({"a": [1, 2]})
            >>> b2 = SampleBatch({"a": [3, 4, 5]})
            >>> print(b1.concat(b2))
            {"a": [1, 2, 3, 4, 5]}
        """

        assert self.keys() == other.keys(), "must have same columns"
        out = {}
        for k in self.keys():
            out[k] = np.concatenate([self[k], other[k]])
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
            for k in self.keys():
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
            out.append(self[k])
        return out

    def shuffle(self):
        permutation = np.random.permutation(self.count)
        for key, val in self.items():
            self[key] = val[permutation]

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, item):
        self.data[key] = item

    def __str__(self):
        return "SampleBatch({})".format(str(self.data))

    def __repr__(self):
        return "SampleBatch({})".format(str(self.data))

    def keys(self):
        return self.data.keys()

    def items(self):
        return self.data.items()

    def __iter__(self):
        return self.data.__iter__()

    def __contains__(self, x):
        return x in self.data
