from ray.air.util.tensor_extensions.arrow import concat_tensor_arrays

try:
    import pyarrow
except ImportError:
    pyarrow = None


def _is_pa_extension_type(pa_type: "pyarrow.lib.DataType") -> bool:
    """Whether the provided Arrow Table column is an extension array, using an Arrow
    extension type.
    """
    return isinstance(pa_type, pyarrow.ExtensionType)


def _concatenate_extension_column(
    ca: "pyarrow.ChunkedArray", ensure_copy: bool = False
) -> "pyarrow.Array":
    """Concatenate chunks of an extension column into a contiguous array.

    This concatenation is required for creating copies and for .take() to work on
    extension arrays.
    See https://issues.apache.org/jira/browse/ARROW-16503.

    Args:
        ca: The chunked array representing the extension column to be concatenated.
        ensure_copy: Skip copying when ensure_copy is False and there is exactly 1 chunk.
    """
    from ray.air.util.tensor_extensions.arrow import (
        get_arrow_extension_tensor_types,
    )

    if not _is_pa_extension_type(ca.type):
        raise ValueError("Chunked array isn't an extension array: {ca}")

    tensor_extension_types = get_arrow_extension_tensor_types()

    if ca.num_chunks == 0:
        # Create empty storage array.
        storage = pyarrow.array([], type=ca.type.storage_type)
    elif not ensure_copy and len(ca.chunks) == 1:
        # Skip copying
        return ca.chunks[0]
    elif isinstance(ca.type, tensor_extension_types):
        return concat_tensor_arrays(ca.chunks, ensure_copy)
    else:
        storage = pyarrow.concat_arrays([c.storage for c in ca.chunks])

    return ca.type.wrap_array(storage)
