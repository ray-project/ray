"""Helpers for download columns whose cells are lists of URIs.

A scalar download column holds one URI string per row; a list column (e.g. the
frame paths of one video) holds many. These helpers flatten a ``list<string>``
column into a single flat URI list so it runs through the same concurrent
downloader as the scalar path, then re-nest the downloaded bytes back into a
``list<binary>`` column with the original per-row shape and order.
"""

from typing import List, Optional, Tuple

import pyarrow as pa


def is_uri_list_column(arrow_type: "pa.DataType") -> bool:
    """Return whether ``arrow_type`` is a list of strings (a multi-URI column).

    Matches ``list`` / ``large_list`` / ``fixed_size_list`` of ``string`` or
    ``large_string``. Scalar string columns return ``False`` and stay on the
    unchanged single-URI-per-row download path.
    """
    if not (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        return False
    value_type = arrow_type.value_type
    return pa.types.is_string(value_type) or pa.types.is_large_string(value_type)


def first_inner_uri(column: "pa.ChunkedArray") -> Optional[str]:
    """Return the first non-null inner URI in a list<string> column, or ``None``.

    Used only to pick the download path (obstore vs PyArrow) from a URI's
    scheme, mirroring the scalar path's "look at the first URI" behavior. Scans
    the offset-free child values, so it never indexes an empty or null cell.
    """
    for chunk in column.iterchunks():
        for value in chunk.values:
            if value.is_valid:
                return value.as_py()
    return None


def flatten_uri_list(
    column: "pa.ChunkedArray",
) -> Tuple[List[Optional[str]], List[Optional[int]]]:
    """Flatten a list<string> URI column into a flat URI list + per-row lengths.

    Returns ``(flat_uris, row_lengths)``: ``flat_uris`` concatenates every row's
    URIs in order (null inner elements are kept as ``None`` so positions stay
    aligned with the downloaded bytes); ``row_lengths[i]`` is row ``i``'s URI
    count, or ``None`` for a null cell. Pair with :func:`renest_downloaded_bytes`.
    """
    flat_uris: List[Optional[str]] = []
    row_lengths: List[Optional[int]] = []
    for uris in column.to_pylist():
        if uris is None:
            row_lengths.append(None)
        else:
            row_lengths.append(len(uris))
            flat_uris.extend(uris)
    return flat_uris, row_lengths


def renest_downloaded_bytes(
    flat_bytes: List[Optional[bytes]], row_lengths: List[Optional[int]]
) -> "pa.Array":
    """Re-nest flat downloaded bytes into a ``list<binary>`` column.

    Inverse of :func:`flatten_uri_list`: slices ``flat_bytes`` back into one
    inner list per row using ``row_lengths`` (``None`` -> null cell, ``0`` ->
    empty list), preserving per-row length and order. Failed downloads stay
    ``None`` in place, matching the scalar path. Always returns ``list<binary>``
    (even for all-empty or all-null blocks) so output blocks concatenate without
    a ``list<null>`` type clash.
    """
    nested: List[Optional[List[Optional[bytes]]]] = []
    pos = 0
    for length in row_lengths:
        if length is None:
            nested.append(None)
            continue
        nested.append(flat_bytes[pos : pos + length])
        pos += length
    return pa.array(nested, type=pa.list_(pa.binary()))
