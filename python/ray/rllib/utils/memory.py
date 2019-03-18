from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np


def tf_aligned(size, dtype, align=64):
    """Returns an array of a given size that is 64-byte aligned.

    The returned array can be efficiently copied into GPU memory by TensorFlow.
    """

    n = size * dtype.itemsize
    a = np.empty(n + (align - 1), dtype=np.uint8)
    data_align = a.ctypes.data % align
    offset = 0 if data_align == 0 else (align - data_align)
    output = a[offset:offset + n].view(dtype)
    assert len(output) == size, len(output)
    return output


def concat_aligned(items):
    if len(items) == 0:
        return []
    elif len(items) == 1:
        return items[0]
    dtype = items[0].dtype
    if dtype in [np.float32, np.float64, np.int32, np.int64]:
        flat = tf_aligned(sum(s.size for s in items), dtype)
        batch_dim = sum(s.shape[0] for s in items)
        new_shape = (batch_dim, ) + items[0].shape[1:]
        out = flat.reshape(new_shape)
        assert out.ctypes.data % 64 == 0, out.ctypes.data
        np.concatenate(items, out=out)
        return out
    else:
        return np.concatenate(items)
