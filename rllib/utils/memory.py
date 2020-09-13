import numpy as np


def aligned_array(size, dtype, align=64):
    """Returns an array of a given size that is 64-byte aligned.

    The returned array can be efficiently copied into GPU memory by TensorFlow.
    """

    n = size * dtype.itemsize
    empty = np.empty(n + (align - 1), dtype=np.uint8)
    data_align = empty.ctypes.data % align
    offset = 0 if data_align == 0 else (align - data_align)
    if n == 0:
        # stop np from optimising out empty slice reference
        output = empty[offset:offset + 1][0:0].view(dtype)
    else:
        output = empty[offset:offset + n].view(dtype)

    assert len(output) == size, len(output)
    assert output.ctypes.data % align == 0, output.ctypes.data
    return output


def concat_aligned(items, time_major=None):
    """Concatenate arrays, ensuring the output is 64-byte aligned.

    We only align float arrays; other arrays are concatenated as normal.

    This should be used instead of np.concatenate() to improve performance
    when the output array is likely to be fed into TensorFlow.

    Args:
        items (List(np.ndarray)): The list of items to concatenate and align.
        time_major (bool): Whether the data in items is time-major, in which
            case, we will concatenate along axis=1.

    Returns:
        np.ndarray: The concat'd and aligned array.
    """

    if len(items) == 0:
        return []
    elif len(items) == 1:
        # we assume the input is aligned. In any case, it doesn't help
        # performance to force align it since that incurs a needless copy.
        return items[0]
    elif (isinstance(items[0], np.ndarray)
          and items[0].dtype in [np.float32, np.float64, np.uint8]):
        dtype = items[0].dtype
        flat = aligned_array(sum(s.size for s in items), dtype)
        if time_major is not None:
            if time_major is True:
                batch_dim = sum(s.shape[1] for s in items)
                new_shape = (
                    items[0].shape[0],
                    batch_dim,
                ) + items[0].shape[2:]
            else:
                batch_dim = sum(s.shape[0] for s in items)
                new_shape = (
                    batch_dim,
                    items[0].shape[1],
                ) + items[0].shape[2:]
        else:
            batch_dim = sum(s.shape[0] for s in items)
            new_shape = (batch_dim, ) + items[0].shape[1:]
        output = flat.reshape(new_shape)
        assert output.ctypes.data % 64 == 0, output.ctypes.data
        np.concatenate(items, out=output, axis=1 if time_major else 0)
        return output
    else:
        return np.concatenate(items, axis=1 if time_major else 0)
