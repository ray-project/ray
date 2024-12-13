from typing import TYPE_CHECKING, List, Union

import numpy as np
from packaging.version import parse as parse_version

from ray._private.utils import _get_pyarrow_version
from ray.air.util.tensor_extensions.arrow import (
    INT32_OVERFLOW_THRESHOLD,
    MIN_PYARROW_VERSION_CHUNKED_ARRAY_TO_NUMPY_ZERO_COPY_ONLY,
    PYARROW_VERSION,
)

try:
    import pyarrow
except ImportError:
    pyarrow = None


if TYPE_CHECKING:
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey


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
    variable-shaped tensors in the given schemas.

    This function scans all input schemas to identify columns that contain
    variable-shaped tensors or objects. For tensor columns, it ensures the
    use of appropriate tensor types (including variable-shaped tensor types).
    For object columns, it uses a specific object type to accommodate any
    objects present. Additionally, it handles columns with null-typed lists
    by determining their actual types from the given schemas.

    Currently, it disallows the concatenation of tensor columns and
    pickled object columsn for performance reasons.
    """
    import pyarrow as pa

    from ray.air.util.object_extensions.arrow import ArrowPythonObjectType
    from ray.air.util.tensor_extensions.arrow import (
        ArrowTensorType,
        ArrowVariableShapedTensorType,
    )

    schemas_to_unify = []
    schema_field_overrides = {}

    # Rollup columns with opaque (null-typed) lists, to override types in
    # the following for-loop.
    cols_with_null_list = set()

    all_columns = set()
    for schema in schemas:
        for col_name in schema.names:
            col_type = schema.field(col_name).type
            if pa.types.is_list(col_type) and pa.types.is_null(col_type.value_type):
                cols_with_null_list.add(col_name)
            all_columns.add(col_name)

    from ray.air.util.tensor_extensions.arrow import (
        get_arrow_extension_fixed_shape_tensor_types,
        get_arrow_extension_tensor_types,
    )

    arrow_tensor_types = get_arrow_extension_tensor_types()
    arrow_fixed_shape_tensor_types = get_arrow_extension_fixed_shape_tensor_types()

    columns_with_objects = set()
    columns_with_tensor_array = set()
    for col_name in all_columns:
        for s in schemas:
            indices = s.get_all_field_indices(col_name)
            if len(indices) > 1:
                # This is broken for Pandas blocks and broken with the logic here
                raise ValueError(
                    f"Schema {s} has multiple fields with the same name: {col_name}"
                )
            elif len(indices) == 0:
                continue
            if isinstance(s.field(col_name).type, ArrowPythonObjectType):
                columns_with_objects.add(col_name)
            if isinstance(s.field(col_name).type, arrow_tensor_types):
                columns_with_tensor_array.add(col_name)

    if len(columns_with_objects.intersection(columns_with_tensor_array)) > 0:
        # This is supportable if we use object type, but it will be expensive
        raise ValueError(
            "Found columns with both objects and tensors: "
            f"{columns_with_tensor_array.intersection(columns_with_objects)}"
        )
    for col_name in columns_with_tensor_array:
        tensor_array_types = [
            s.field(col_name).type
            for s in schemas
            if isinstance(s.field(col_name).type, arrow_tensor_types)
        ]

        if ArrowTensorType._need_variable_shaped_tensor_array(tensor_array_types):
            if isinstance(tensor_array_types[0], ArrowVariableShapedTensorType):
                new_type = tensor_array_types[0]
            elif isinstance(tensor_array_types[0], arrow_fixed_shape_tensor_types):
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

    for col_name in columns_with_objects:
        schema_field_overrides[col_name] = ArrowPythonObjectType()

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
    from ray.data.extensions import get_arrow_extension_tensor_types

    tensor_types = get_arrow_extension_tensor_types()

    # Single flat list of chunks across all chunked arrays.
    chunks = []
    type_ = None
    for arr in arrs:
        if type_ is None:
            type_ = arr.type
        else:
            if isinstance(type_, tensor_types):
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

    from ray.air.util.tensor_extensions.arrow import ArrowConversionError
    from ray.data.extensions import (
        ArrowPythonObjectArray,
        ArrowPythonObjectType,
        ArrowTensorArray,
        get_arrow_extension_tensor_types,
    )

    tensor_types = get_arrow_extension_tensor_types()

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

    # If the result contains pyarrow schemas, unify them
    schemas_to_unify = [b.schema for b in blocks]
    try:
        schema = unify_schemas(schemas_to_unify)
    except Exception as e:
        raise ArrowConversionError(str(blocks)) from e

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

            if isinstance(schema.field(col_name).type, tensor_types):
                # For our tensor extension types, manually construct a chunked array
                # containing chunks from all blocks. This is to handle
                # homogeneous-shaped block columns having different shapes across
                # blocks: if tensor element shapes differ across blocks, a
                # variable-shaped tensor array will be returned.
                col = ArrowTensorArray._chunk_tensor_arrays(
                    [chunk for ca in col_chunked_arrays for chunk in ca.chunks]
                )
            elif isinstance(schema.field(col_name).type, ArrowPythonObjectType):
                chunks_to_concat = []
                # Cast everything to objects if concatenated with an object column
                for ca in col_chunked_arrays:
                    for chunk in ca.chunks:
                        if isinstance(ca.type, ArrowPythonObjectType):
                            chunks_to_concat.append(chunk)
                        else:
                            chunks_to_concat.append(
                                ArrowPythonObjectArray.from_objects(chunk.to_pylist())
                            )
                col = pa.chunked_array(chunks_to_concat)
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

        # Build the concatenated table.
        table = pyarrow.Table.from_arrays(cols, schema=schema)
        # Validate table schema (this is a cheap check by default).
        table.validate()
    else:
        # No extension array columns, so use built-in pyarrow.concat_tables.
        if parse_version(_get_pyarrow_version()) >= parse_version("14.0.0"):
            # `promote` was superseded by `promote_options='default'` in Arrow 14. To
            # prevent `FutureWarning`s, we manually check the Arrow version and use the
            # appropriate parameter.
            table = pyarrow.concat_tables(blocks, promote_options="default")
        else:
            table = pyarrow.concat_tables(blocks, promote=True)
    return table


def concat_and_sort(
    blocks: List["pyarrow.Table"], sort_key: "SortKey"
) -> "pyarrow.Table":
    import pyarrow.compute as pac

    ret = concat(blocks)
    indices = pac.sort_indices(ret, sort_keys=sort_key.to_arrow_sort_args())
    return take_table(ret, indices)


def to_numpy(
    array: Union["pyarrow.Array", "pyarrow.ChunkedArray"],
    *,
    zero_copy_only: bool = True,
) -> np.ndarray:
    """Wrapper for `Array`s and `ChunkedArray`s `to_numpy` API,
    handling API divergence b/w Arrow versions"""

    import pyarrow as pa

    if isinstance(array, pa.Array):
        return array.to_numpy(zero_copy_only=zero_copy_only)
    elif isinstance(array, pa.ChunkedArray):
        if PYARROW_VERSION >= MIN_PYARROW_VERSION_CHUNKED_ARRAY_TO_NUMPY_ZERO_COPY_ONLY:
            return array.to_numpy(zero_copy_only=zero_copy_only)
        else:
            return array.to_numpy()
    else:
        raise ValueError(
            f"Either of `Array` or `ChunkedArray` was expected, got {type(array)}"
        )


def combine_chunks(table: "pyarrow.Table") -> "pyarrow.Table":
    """This is counterpart for Pyarrow's `Table.combine_chunks` that's using
    extended `ChunkedArray` combination protocol.

    For more details check out `combine_chunked_array` py-doc
    """

    new_column_values_arrays = []

    for col in table.columns:
        new_column_values_arrays.append(combine_chunked_array(col))

    return pyarrow.Table.from_arrays(new_column_values_arrays, schema=table.schema)


def combine_chunked_array(
    array: "pyarrow.ChunkedArray",
) -> Union["pyarrow.Array", "pyarrow.ChunkedArray"]:
    """This is counterpart for Pyarrow's `ChunkedArray.combine_chunks` that additionally

        1. Handles `ExtensionType`s (like ArrowTensorType, ArrowTensorTypeV2,
           ArrowPythonObjectType, etc)

        2. Making sure `ChunkedArray`s comprising provided `Table` are combined
           safely, ie avoiding overflows of Arrow's internal offsets (using int32 for
           most of its native types, other than "large" kind).

    For more details check py-doc of `_try_combine_chunks_safe` method.
    """

    import pyarrow as pa

    from ray.air.util.transform_pyarrow import (
        _concatenate_extension_column,
        _is_column_extension_type,
    )

    assert isinstance(
        array, pa.ChunkedArray
    ), f"Expected `ChunkedArray`, got {type(array)}"

    if _is_column_extension_type(array):
        # Arrow `ExtensionArray`s can't be concatenated via `combine_chunks`,
        # hence require manual concatenation
        return _concatenate_extension_column(array)
    elif len(array.chunks) == 0:
        # NOTE: In case there's no chunks, we need to explicitly create
        #       an empty array since calling into `combine_chunks` would fail
        #       due to it expecting at least 1 chunk to be present
        return pa.array([], type=array.type)
    else:
        return _try_combine_chunks_safe(array)


def _try_combine_chunks_safe(
    array: "pyarrow.ChunkedArray", max_chunk_size=INT32_OVERFLOW_THRESHOLD
) -> Union["pyarrow.Array", "pyarrow.ChunkedArray"]:
    """This method provides a safe way of combining `ChunkedArray`s exceeding 2 GiB
    in size, which aren't using "large_*" types (and therefore relying on int32
    offsets).

    When handling provided `ChunkedArray` this method will be either

        - Relying on PyArrow's default `combine_chunks` (therefore returning single
        contiguous `Array`) in cases when
            - Array's total size is < 2 GiB
            - Array's underlying type is of "large" kind (ie using one of the
            `large_*` type family)
        - Safely combining subsets of tasks such that resulting `Array`s to not
        exceed 2 GiB in size (therefore returning another `ChunkedArray` albeit
        with potentially smaller number of chunks that have resulted from clumping
        the original ones)

    Returns:
        - pa.Array if it's possible to combine provided pa.ChunkedArray into single
        contiguous array
        - pa.ChunkedArray (albeit with chunks re-combined) if it's not possible to
        produce single pa.Array
    """

    import pyarrow as pa

    from ray.air.util.transform_pyarrow import _is_column_extension_type

    assert not _is_column_extension_type(
        array
    ), f"Arrow `ExtensionType`s are not accepted (got {array.type})"

    int64_type_predicates = [
        pa.types.is_large_list,
        pa.types.is_large_string,
        pa.types.is_large_binary,
        pa.types.is_large_unicode,
    ]

    if array.nbytes < max_chunk_size or any(
        p(array.type) for p in int64_type_predicates
    ):
        # It's safe to combine provided `ChunkedArray` in either of 2 cases:
        #   - It's cumulative size is < 2 GiB
        #   - It's of 'large' kind (ie one using int64 offsets internally)
        return array.combine_chunks()

    # In this case it's actually *NOT* safe to try to directly combine
    # Arrow's `ChunkedArray` and is impossible to produce single, contiguous
    # `Array` since
    #     - It's estimated to hold > 2 GiB
    #     - Its type is not of the "large" kind (and hence is using int32
    #       offsets internally, which would overflow)
    #
    # In this case instead of combining into single contiguous array, we
    # instead just "clump" existing chunks into bigger ones, but no bigger
    # than 2 GiB each.
    #
    # NOTE: This branch actually returns `ChunkedArray` and not an `Array`

    # To stay under 2 GiB limit we are slicing provided list of chunks into
    # slices no larger than 2 GiB (as compared to just directly using `concat_arrays`)
    slices = []

    cur_slice_start = 0
    cur_slice_size_bytes = 0

    for i, chunk in enumerate(array.chunks):
        chunk_size = chunk.nbytes

        if cur_slice_size_bytes + chunk_size > max_chunk_size:
            slices.append(array.chunks[cur_slice_start:i])

            cur_slice_start = i
            cur_slice_size_bytes = 0

        cur_slice_size_bytes += chunk_size

    # Add remaining chunks as last slice
    slices.append(array.chunks[cur_slice_start:])

    return pa.chunked_array([pa.concat_arrays(s) for s in slices])
