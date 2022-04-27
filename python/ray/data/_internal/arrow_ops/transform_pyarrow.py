import collections
from typing import TYPE_CHECKING, List, Union, Optional

import numpy as np

try:
    import pyarrow as pa

    # This import is necessary to load the tensor extension type.
    from ray.data.extensions.tensor_extension import ArrowTensorType  # noqa
except ImportError:
    pa = None

if TYPE_CHECKING:
    from ray.data._internal.sort import SortKeyT


def sort(table: "pa.Table", key: "SortKeyT", descending: bool) -> "pa.Table":
    import pyarrow.compute as pac

    indices = pac.sort_indices(table, sort_keys=key)
    return take_table(table, indices)


def take_table(
    table: "pa.Table",
    indices: Union[List[int], "pa.Array", "pa.ChunkedArray"],
) -> "pa.Table":
    """Select rows from the table.

    This method is an alternative to pa.Table.take(), which breaks for
    extension arrays. This is exposed as a static method for easier use on
    intermediate tables, not underlying an ArrowBlockAccessor.
    """
    from ray.air.util.transform_pyarrow import (
        _is_column_extension_type,
        _concatenate_extension_column,
    )

    if any(_is_column_extension_type(col) for col in table.columns):
        new_cols = []
        for col in table.columns:
            if _is_column_extension_type(col):
                # .take() will concatenate internally, which currently breaks for
                # extension arrays.
                col = _concatenate_extension_column(col)
            new_cols.append(col.take(indices))
        table = pa.Table.from_arrays(new_cols, schema=table.schema)
    else:
        table = table.take(indices)
    return table


def _concatenate_chunked_arrays(arrs: "pa.ChunkedArray") -> "pa.ChunkedArray":
    """
    Concatenate provided chunked arrays into a single chunked array.
    """
    from ray.data.extensions import (
        ArrowTensorType,
        ArrowVariableShapedTensorType,
    )

    # Single flat list of chunks across all chunked arrays.
    chunks = []
    type_ = None
    for arr in arrs:
        if type_ is None:
            type_ = arr.type
        else:
            if isinstance(type_, (ArrowTensorType, ArrowVariableShapedTensorType)):
                raise ValueError(
                    "_concatenate_chunked_arrays should only be used on non-tensor "
                    f"extension types, but got a chunked array of type {type_}."
                )
            assert type_ == arr.type
        # Add chunks for this chunked array to flat chunk list.
        chunks.extend(arr.chunks)
    # Construct chunked array on flat list of chunks.
    return pa.chunked_array(chunks, type=type_)


def concat(blocks: List["pa.Table"]) -> "pa.Table":
    """Concatenate provided Arrow Tables into a single Arrow Table. This has special
    handling for:
     - extension types that pa.concat_tables does not yet support,
     - upcasting non-extension type column types,
     - promoting nulls.
    """
    from ray.data.extensions import (
        ArrowTensorArray,
        ArrowTensorType,
        ArrowVariableShapedTensorType,
    )

    if not blocks:
        # Short-circuit on empty list of blocks.
        return blocks

    if len(blocks) == 1:
        return blocks[0]

    schema = blocks[0].schema
    if any(isinstance(type_, pa.ExtensionType) for type_ in schema.types):
        # Custom handling for extension array columns.
        # TODO(Clark): Add upcasting for extension type path once extension type support
        # is added to Polars (and therefore our Polars integration).
        cols = []
        schema_tensor_field_overrides = {}
        for col_name in schema.names:
            col_chunked_arrays = []
            for block in blocks:
                col_chunked_arrays.append(block.column(col_name))
            if isinstance(
                schema.field(col_name).type,
                (ArrowTensorType, ArrowVariableShapedTensorType),
            ):
                # For our tensor extension types, manually construct a chunked array
                # containing chunks from all blocks. This is to handle
                # homogeneous-shaped block columns having different shapes across
                # blocks: if tensor element shapes differ across blocks, a
                # variable-shaped tensor array will be returned.
                col = ArrowTensorArray._chunk_tensor_arrays(
                    [chunk for ca in col_chunked_arrays for chunk in ca.chunks]
                )
                if schema.field(col_name).type != col.type:
                    # Ensure that the field's type is properly updated in the schema if
                    # a collection of homogeneous-shaped columns resulted in a
                    # variable-shaped tensor column once concatenated.
                    new_field = schema.field(col_name).with_type(col.type)
                    field_idx = schema.get_field_index(col_name)
                    schema_tensor_field_overrides[field_idx] = new_field
            else:
                col = _concatenate_chunked_arrays(col_chunked_arrays)
            cols.append(col)
        # Unify schemas.
        schemas = []
        for block in blocks:
            schema = block.schema
            # If concatenating uniform tensor columns results in a variable-shaped
            # tensor columns, override the column type for all blocks.
            if schema_tensor_field_overrides:
                for idx, field in schema_tensor_field_overrides.items():
                    schema = schema.set(idx, field)
            schemas.append(schema)
        # Let Arrow unify the schema of non-tensor extension type columns.
        schema = pa.unify_schemas(schemas)
        # Build the concatenated table.
        table = pa.Table.from_arrays(cols, schema=schema)
        # Validate table schema (this is a cheap check by default).
        table.validate()
    else:
        # No extension array columns, so use built-in pa.concat_tables.
        # TODO(Clark): Remove this upcasting once Arrow's concatenate supports implicit
        # upcasting.
        # NOTE(Clark): We only use Polars for workloads that don't have extension types,
        # so we only need to upcast if there are no extension types.
        blocks = _upcast_tables(blocks)
        table = pa.concat_tables(blocks, promote=True)
    return table


def concat_and_sort(
    blocks: List["pa.Table"], key: "SortKeyT", descending: bool
) -> "pa.Table":
    ret = concat(blocks)
    indices = pa.compute.sort_indices(ret, sort_keys=key)
    return take_table(ret, indices)


def _upcast_tables(tables: List["pa.Table"]) -> List["pa.Table"]:
    """Upcast columns across tables to their minimally sufficient dtype to ensure that
    the tables can be properly concatenated.

    This is required for Polars workloads because Polars will aggressively downcast
    columns.
    """
    field_dtypes = collections.defaultdict(list)
    field_meta = collections.defaultdict(dict)
    field_nullable = collections.defaultdict(bool)
    schema_meta = {}
    for table in tables:
        if table.schema.metadata is not None:
            schema_meta.update(table.schema.metadata)
        for field in table.schema:
            if not pa.types.is_null(field.type):
                # All Arrow types are nullable, so drop any null-type columns from
                # the common dtype search.
                field_dtypes[field.name].append(field.type)
            if field.metadata is not None:
                field_meta[field.name].update(field.metadata)
            field_nullable[field.name] = field.nullable
    common_dtypes = {}
    for name in field_nullable.keys():
        dtypes = field_dtypes[name]
        if dtypes:
            # Find the common Arrow type among all dtypes.
            common_dtypes[name] = _common_arrow_type(dtypes)
        else:
            # All table's dtypes for this column are null, just use the first
            # table's dtype for this column.
            common_dtypes[name] = tables[0].schema.field(name).type
    fields = [
        pa.field(name, common_dtypes[name], field_nullable[name], field_meta[name])
        for name in field_nullable.keys()
    ]
    schema = pa.schema(fields, schema_meta)
    return [table.cast(schema) for table in tables]


def _common_arrow_type(dtypes: List["pa.DataType"]) -> "pa.DataType":
    """Get common Arrow type for the provided Arrow types, such that casting to the
    returned type will not result in any loss of precision or other errors.
    """
    import pandas as pd

    # Delegate to NumPy to find the minimally sufficient type for this column
    # across all tables.
    np_dtypes = [dtype.to_pandas_dtype() for dtype in dtypes]
    if all(
        np_dtype is not np.object_
        and not pd.api.types.is_extension_array_dtype(np_dtype)
        for np_dtype in np_dtypes
    ):
        # Use NumPy to find common dtype if all dtypes are known to NumPy.
        try:
            common_np_dtype = np.find_common_type(np_dtypes, [])
            return pa.from_numpy_dtype(common_np_dtype)
        except (TypeError, ValueError, pa.ArrowNotImplementedError):
            # Fall back to manual Arrow type analysis.
            pass
    dtype = _common_arrow_binary_type(dtypes)
    if dtype is not None:
        return dtype
    dtype = _common_arrow_tensor_extension_type(dtypes)
    if dtype is not None:
        return dtype
    # Fall back to dtype of first table.
    return dtypes[0]


def _common_arrow_binary_type(
    dtypes: List["pa.DataType"],
) -> Optional["pa.DataType"]:
    """Get common binary Arrow type for the provided Arrow types. Returns None if any
    are non-binary.
    """
    all_utf8, all_offset32 = True, True
    for dtype in dtypes:
        if pa.types.is_string(dtype):
            pass
        elif pa.types.is_binary(dtype):
            all_utf8 = False
        elif pa.types.is_fixed_size_binary(dtype):
            all_utf8 = False
        elif pa.types.is_large_string(dtype):
            all_offset32 = False
        elif pa.types.is_large_binary(dtype):
            all_offset32 = False
            all_utf8 = False
        else:
            return None
    if all_utf8:
        # Strings.
        return pa.string() if all_offset32 else pa.large_string()
    # Binary.
    return pa.binary() if all_offset32 else pa.large_binary()


def _common_arrow_tensor_extension_type(
    dtypes: List["pa.DataType"],
) -> Optional["pa.DataType"]:
    """Get common tensor extension Arrow type for the provided Arrow types. Returns
    None if any are non-tensor-extension.
    """
    shape = None
    value_dtypes = []
    for dtype in dtypes:
        if isinstance(dtype, ArrowTensorType):
            if shape is None:
                shape = dtype.shape
            else:
                if dtype.shape != shape:
                    return None
                value_dtypes.append(dtype.storage_type.value_type)
        else:
            return None
    value_dtype = _common_arrow_type(value_dtypes)
    if value_dtype is not None:
        return ArrowTensorType(shape, value_dtype)
    return None
