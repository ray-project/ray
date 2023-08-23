from typing import TYPE_CHECKING, List, Union

try:
    import pyarrow
except ImportError:
    pyarrow = None

if TYPE_CHECKING:
    from ray.data._internal.sort import SortKey


def sort(table: "pyarrow.Table", sort_key: "SortKey") -> "pyarrow.Table":
    import pyarrow.compute as pac

    indices = pac.sort_indices(table, sort_keys=sort_key.to_arrow_sort_args())
    return take_table(table, indices)


def take_table(
    table: "pyarrow.Table",
    indices: Union[List[int], "pyarrow.Array", "pyarrow.ChunkedArray"],
) -> "pyarrow.Table":
    """Select rows from the table.

    This method is an alternative to pyarrow.Table.take(), which breaks for
    extension arrays. This is exposed as a static method for easier use on
    intermediate tables, not underlying an ArrowBlockAccessor.
    """
    from ray.air.util.transform_pyarrow import (
        _concatenate_extension_column,
        _is_column_extension_type,
    )

    if any(_is_column_extension_type(col) for col in table.columns):
        new_cols = []
        for col in table.columns:
            if _is_column_extension_type(col) and col.num_chunks > 1:
                # .take() will concatenate internally, which currently breaks for
                # extension arrays.
                col = _concatenate_extension_column(col)
            new_cols.append(col.take(indices))
        table = pyarrow.Table.from_arrays(new_cols, schema=table.schema)
    else:
        table = table.take(indices)
    return table


def unify_schemas(
    schemas: List["pyarrow.Schema"],
) -> "pyarrow.Schema":
    """Version of `pyarrow.unify_schemas()` which also handles checks for
    variable-shaped tensors in the given schemas."""
    import pyarrow as pa

    from ray.air.util.tensor_extensions.arrow import (
        ArrowTensorType,
        ArrowVariableShapedTensorType,
    )

    schemas_to_unify = []
    schema_field_overrides = {}

    # Rollup columns with opaque (null-typed) lists, to override types in
    # the following for-loop.
    cols_with_null_list = set()
    for schema in schemas:
        for col_name in schema.names:
            col_type = schema.field(col_name).type
            if pa.types.is_list(col_type) and pa.types.is_null(col_type.value_type):
                cols_with_null_list.add(col_name)

    if any(isinstance(type_, pyarrow.ExtensionType) for type_ in schemas[0].types):
        # If we have pyarrow extension types that may potentially be variable shaped,
        # examine the first schema to gather the columns that need type conversions.
        for col_field in schemas[0]:
            col_name, col_type = col_field.name, col_field.type
            tensor_array_types = [
                s.field(col_name).type
                for s in schemas
                if isinstance(s.field(col_name).type, pyarrow.ExtensionType)
            ]
            if ArrowTensorType._need_variable_shaped_tensor_array(tensor_array_types):
                if isinstance(tensor_array_types[0], ArrowVariableShapedTensorType):
                    new_type = tensor_array_types[0]
                elif isinstance(tensor_array_types[0], ArrowTensorType):
                    new_type = ArrowVariableShapedTensorType(
                        dtype=tensor_array_types[0].scalar_type,
                        ndim=len(tensor_array_types[0].shape),
                    )
                else:
                    raise ValueError(
                        "Detected need for variable shaped tensor representation, "
                        f"but schema is not ArrayTensorType: {tensor_array_types[0]}"
                    )
                schema_field_overrides[col_name] = new_type

    if cols_with_null_list:
        # For each opaque list column, iterate through all schemas until we find
        # a valid value_type that can be used to override the column types in
        # the following for-loop.
        for col_name in cols_with_null_list:
            for schema in schemas:
                col_type = schema.field(col_name).type
                if not pa.types.is_list(col_type) or not pa.types.is_null(
                    col_type.value_type
                ):
                    schema_field_overrides[col_name] = col_type
                    break

    if schema_field_overrides:
        # Go through all schemas and update the types of columns from the above loop.
        for schema in schemas:
            for col_name, col_new_type in schema_field_overrides.items():
                var_shaped_col = schema.field(col_name).with_type(col_new_type)
                col_idx = schema.get_field_index(col_name)
                schema = schema.set(col_idx, var_shaped_col)
            schemas_to_unify.append(schema)
    else:
        schemas_to_unify = schemas
    # Let Arrow unify the schema of non-tensor extension type columns.
    return pyarrow.unify_schemas(schemas_to_unify)


def _concatenate_chunked_arrays(arrs: "pyarrow.ChunkedArray") -> "pyarrow.ChunkedArray":
    """
    Concatenate provided chunked arrays into a single chunked array.
    """
    from ray.data.extensions import ArrowTensorType, ArrowVariableShapedTensorType

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
            assert type_ == arr.type, f"Types mismatch: {type_} != {arr.type}"
        # Add chunks for this chunked array to flat chunk list.
        chunks.extend(arr.chunks)
    # Construct chunked array on flat list of chunks.
    return pyarrow.chunked_array(chunks, type=type_)


def concat(blocks: List["pyarrow.Table"]) -> "pyarrow.Table":
    """Concatenate provided Arrow Tables into a single Arrow Table. This has special
    handling for extension types that pyarrow.concat_tables does not yet support.
    """
    import pyarrow as pa

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

    # Rollup columns with opaque (null-typed) lists, to process in following for-loop.
    cols_with_null_list = set()
    for b in blocks:
        for col_name in b.schema.names:
            col_type = b.schema.field(col_name).type
            if pa.types.is_list(col_type) and pa.types.is_null(col_type.value_type):
                cols_with_null_list.add(col_name)

    schema = blocks[0].schema
    if (
        any(isinstance(type_, pa.ExtensionType) for type_ in schema.types)
        or cols_with_null_list
    ):
        # Custom handling for extension array columns.
        cols = []
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
            else:
                if col_name in cols_with_null_list:
                    # For each opaque list column, iterate through all schemas until
                    # we find a valid value_type that can be used to override the
                    # column types in the following for-loop.
                    scalar_type = None
                    for arr in col_chunked_arrays:
                        if not pa.types.is_list(arr.type) or not pa.types.is_null(
                            arr.type.value_type
                        ):
                            scalar_type = arr.type
                            break

                    if scalar_type is not None:
                        for c_idx in range(len(col_chunked_arrays)):
                            c = col_chunked_arrays[c_idx]
                            if pa.types.is_list(c.type) and pa.types.is_null(
                                c.type.value_type
                            ):
                                if pa.types.is_list(scalar_type):
                                    # If we are dealing with a list input,
                                    # cast the array to the scalar_type found above.
                                    col_chunked_arrays[c_idx] = c.cast(scalar_type)
                                else:
                                    # If we are dealing with a single value, construct
                                    # a new array with null values filled.
                                    col_chunked_arrays[c_idx] = pa.chunked_array(
                                        [pa.nulls(c.length(), type=scalar_type)]
                                    )
                col = _concatenate_chunked_arrays(col_chunked_arrays)
            cols.append(col)

        # If the result contains pyarrow schemas, unify them
        schemas_to_unify = [b.schema for b in blocks]
        if pyarrow is not None and any(
            isinstance(s, pyarrow.Schema) for s in schemas_to_unify
        ):
            schema = unify_schemas(schemas_to_unify)
        else:
            # Otherwise, if the resulting schemas are simple types (e.g. int),
            # check that all blocks with valid schemas have the same type.
            schema = schemas_to_unify[0]
            if schema is not None:
                for s in schemas_to_unify:
                    if s is not None and s != schema:
                        raise ValueError(
                            "Found blocks with different types "
                            f"in schemas: {schemas_to_unify}"
                        )
        # Build the concatenated table.
        table = pyarrow.Table.from_arrays(cols, schema=schema)
        # Validate table schema (this is a cheap check by default).
        table.validate()
    else:
        # No extension array columns, so use built-in pyarrow.concat_tables.
        table = pyarrow.concat_tables(blocks, promote=True)
    return table


def concat_and_sort(
    blocks: List["pyarrow.Table"], sort_key: "SortKey"
) -> "pyarrow.Table":
    import pyarrow.compute as pac

    ret = concat(blocks)
    indices = pac.sort_indices(ret, sort_keys=sort_key.to_arrow_sort_args())
    return take_table(ret, indices)


def combine_chunks(table: "pyarrow.Table") -> "pyarrow.Table":
    """This is pyarrow.Table.combine_chunks()
    with support for extension types.

    This will create a new table by combining the chunks the input table has.
    """
    from ray.air.util.transform_pyarrow import (
        _concatenate_extension_column,
        _is_column_extension_type,
    )

    cols = table.columns
    new_cols = []
    for col in cols:
        if _is_column_extension_type(col):
            # Extension arrays don't support concatenation.
            arr = _concatenate_extension_column(col)
        else:
            arr = col.combine_chunks()
        new_cols.append(arr)
    return pyarrow.Table.from_arrays(new_cols, schema=table.schema)
