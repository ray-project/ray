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
    if not _is_column_extension_type(ca):
        raise ValueError("Chunked array isn't an extension array: {ca}")

    if ca.num_chunks == 0:
        # No-op for no-chunk chunked arrays, since there's nothing to concatenate.
        return ca

    chunk = ca.chunk(0)
    return type(chunk).from_storage(
        chunk.type, pyarrow.concat_arrays([c.storage for c in ca.chunks])
    )
