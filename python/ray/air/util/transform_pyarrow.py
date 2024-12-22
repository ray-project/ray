try:
    import pyarrow
except ImportError:
    pyarrow = None


def _is_column_extension_type(ca: "pyarrow.ChunkedArray") -> bool:
    """Whether the provided Arrow Table column is an extension array, using an Arrow
    extension type.
    """
    return isinstance(ca.type, pyarrow.ExtensionType)


def _concatenate_extension_column(ca: "pyarrow.ChunkedArray") -> "pyarrow.Array":
    """Concatenate chunks of an extension column into a contiguous array.

    This concatenation is required for creating copies and for .take() to work on
    extension arrays.
    See https://issues.apache.org/jira/browse/ARROW-16503.
    """
    from ray.air.util.tensor_extensions.arrow import (
        ArrowTensorArray,
        get_arrow_extension_tensor_types,
    )

    if not _is_column_extension_type(ca):
        raise ValueError("Chunked array isn't an extension array: {ca}")

    tensor_extension_types = get_arrow_extension_tensor_types()

    if ca.num_chunks == 0:
        # Create empty storage array.
        storage = pyarrow.array([], type=ca.type.storage_type)
    elif isinstance(ca.type, tensor_extension_types):
        return ArrowTensorArray._concat_same_type(ca.chunks)
    else:
        storage = pyarrow.concat_arrays([c.storage for c in ca.chunks])

    return ca.type.__arrow_ext_class__().from_storage(ca.type, storage)
