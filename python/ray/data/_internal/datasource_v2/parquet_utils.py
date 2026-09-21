"""Parquet helpers shared by the V2 Parquet datasource, scanner, reader and
footer indexer.

These used to live in the legacy
``ray.data._internal.datasource.parquet_datasource`` module. They moved here so
that ``datasource_v2`` does not depend on the legacy package; the legacy module
imports them from here instead. This module has no Ray Data imports of its own,
so it can be imported from anywhere without creating import cycles.
"""

from typing import TYPE_CHECKING, List, Optional

import pyarrow as pa

if TYPE_CHECKING:
    import pyarrow.dataset
    import pyarrow.parquet

# File extensions a Parquet datasource lists by default.
PARQUET_FILE_EXTENSIONS: List[str] = ["parquet"]

# Arrow's nested type chunking limit
# See: https://github.com/apache/arrow/issues/21526 (ARROW-5030)
_ARROW_CHUNK_LIMIT = 2 * 1024**3  # 2GB


def check_for_legacy_tensor_type(schema: pa.Schema) -> None:
    """Check for the legacy tensor extension type and raise an error if found.

    Ray Data uses an extension type to represent tensors in Arrow tables. Previously,
    the extension type extended `PyExtensionType`. However, this base type can expose
    users to arbitrary code execution. To prevent this, we don't load the type by
    default.
    """
    for name, type in zip(schema.names, schema.types):
        if isinstance(type, pa.UnknownExtensionType) and isinstance(
            type, pa.PyExtensionType
        ):
            raise RuntimeError(
                f"Ray Data couldn't infer the type of column '{name}' (got "
                f"`UnknownExtensionType` with pickled class ref "
                f"'{type.__arrow_ext_serialize__()}'). This might mean you're trying "
                f"to read data written with an older version of Ray. Reading data "
                f"written with older versions of Ray might expose you to arbitrary code "
                f"execution. To try reading the data anyway, "
                f"preset `RAY_DATA_AUTOLOAD_PYEXTENSIONTYPE=1` on *all* nodes."
                "To learn more, see https://github.com/ray-project/ray/issues/41314."
            )


def _has_susceptible_nested_types(schema: pa.Schema) -> bool:
    """Check if a schema contains nested column types wrapping variable-length
    leaves that are susceptible to Arrow's chunked array limitation (ARROW-5030).

    The error only occurs when a nested container (list, struct, map) contains
    a variable-length leaf (string, binary, and their large/view variants) whose
    data exceeds ~2GB in a single row group. Fixed-width leaves (int, float,
    bool, etc.) never trigger chunking.
    """
    # is_string_view / is_binary_view only exist in PyArrow >= 16.0
    _has_view_types = hasattr(pa.types, "is_string_view")

    def _is_variable_length(t):
        return (
            pa.types.is_string(t)
            or pa.types.is_binary(t)
            or pa.types.is_large_string(t)
            or pa.types.is_large_binary(t)
            or (_has_view_types and pa.types.is_string_view(t))
            or (_has_view_types and pa.types.is_binary_view(t))
        )

    def _is_nested(t):
        return (
            pa.types.is_list(t)
            or pa.types.is_large_list(t)
            or pa.types.is_struct(t)
            or pa.types.is_map(t)
            or pa.types.is_fixed_size_list(t)
        )

    def _nested_contains_variable_length(t):
        """Recursively check if a nested type contains a variable-length leaf."""
        if _is_variable_length(t):
            return True
        if (
            pa.types.is_list(t)
            or pa.types.is_large_list(t)
            or pa.types.is_fixed_size_list(t)
        ):
            return _nested_contains_variable_length(t.value_type)
        if pa.types.is_struct(t):
            return any(_nested_contains_variable_length(f.type) for f in t)
        if pa.types.is_map(t):
            return _nested_contains_variable_length(
                t.key_type
            ) or _nested_contains_variable_length(t.item_type)
        return False

    return any(
        _is_nested(field.type) and _nested_contains_variable_length(field.type)
        for field in schema
    )


def _row_group_uncompressed_size(
    rg_meta: "pyarrow.parquet.RowGroupMetaData",
    column_indices: Optional[List[int]] = None,
) -> int:
    """Total uncompressed byte size of columns in a row group.

    When *column_indices* is ``None`` all columns are summed, otherwise only
    the listed (leaf-level) column indices are included.

    NOTE: We intentionally avoid ``rg_meta.total_byte_size`` because it can
    return the *compressed* size for some files (apache/arrow#48138).
    """
    indices = range(rg_meta.num_columns) if column_indices is None else column_indices
    return sum(rg_meta.column(i).total_uncompressed_size for i in indices)


def _resolve_leaf_column_indices(
    metadata: "pyarrow.parquet.FileMetaData",
    columns: List[str],
) -> List[int]:
    """Map top-level column names to Parquet metadata leaf column indices.

    Parquet metadata enumerates *leaf* columns (nested types are flattened),
    and each leaf's ``path_in_schema`` starts with the top-level field name.
    """
    col_set = set(columns)
    return [
        i
        for i in range(metadata.num_columns)
        if metadata.row_group(0).column(i).path_in_schema.split(".")[0] in col_set
    ]


def _get_safe_batch_size_for_nested_types(
    pf: "pyarrow.parquet.ParquetFile",
    column_indices: Optional[List[int]] = None,
) -> int:
    """Compute a batch size that keeps each batch under Arrow's ~2GB nested-type
    chunking threshold.

    Uses Parquet row group metadata (uncompressed column sizes) to estimate
    bytes per row, then picks a batch size with a 50% safety margin.
    """
    safe_batch_size = pf.metadata.num_rows
    for rg_idx in range(pf.metadata.num_row_groups):
        rg_meta = pf.metadata.row_group(rg_idx)
        if rg_meta.num_rows == 0:
            continue
        uncompressed = _row_group_uncompressed_size(rg_meta, column_indices)
        if uncompressed == 0:
            continue
        bytes_per_row = uncompressed / rg_meta.num_rows
        rg_safe = max(int(_ARROW_CHUNK_LIMIT // bytes_per_row // 2), 1)
        safe_batch_size = min(safe_batch_size, rg_safe)
    return safe_batch_size


def _needs_nested_type_fallback(
    fragment: "pyarrow.dataset.ParquetFileFragment",
    columns: Optional[List[str]] = None,
) -> bool:
    """Check if a fragment requires the fallback reader for nested types.

    Returns True if the *requested* columns (or all columns when ``columns``
    is ``None``) contain nested types AND any row group has uncompressed data
    exceeding Arrow's ~2GB chunking threshold.
    This is a metadata-only check (no data read).
    """
    physical_schema = fragment.physical_schema
    if columns is not None:
        physical_schema = pa.schema(
            [
                physical_schema.field(c)
                for c in columns
                if physical_schema.get_field_index(c) != -1
            ]
        )
    if not _has_susceptible_nested_types(physical_schema):
        return False
    metadata = fragment.metadata
    column_indices = (
        _resolve_leaf_column_indices(metadata, columns)
        if columns is not None and metadata.num_row_groups > 0
        else None
    )
    # fragment.row_groups is non-None when the fragment is a subset of the
    # file (e.g. only row group 0).  Only inspect those row groups to avoid
    # falsely triggering the fallback because of a *different* large row
    # group elsewhere in the same file.
    if fragment.row_groups is not None:
        rg_indices = [rg.id for rg in fragment.row_groups]
    else:
        rg_indices = range(metadata.num_row_groups)
    return any(
        _row_group_uncompressed_size(metadata.row_group(rg_idx), column_indices)
        >= _ARROW_CHUNK_LIMIT
        for rg_idx in rg_indices
    )


def _resolve_read_columns(
    columns: Optional[List[str]],
    filter_expr: Optional["pyarrow.dataset.Expression"],
    filter_columns: Optional[List[str]],
) -> Optional[List[str]]:
    """Compute the union of projected and filter-referenced columns.

    When a filter references columns outside the projection, we must read
    the union so the filter can evaluate.  Returns ``None`` (meaning "all
    columns") when filter_columns is unknown.
    """
    if filter_expr is not None and columns is not None:
        if filter_columns is not None:
            return list(dict.fromkeys(columns + filter_columns))
        return None
    return columns
