import collections
from types import GeneratorType
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa

import ray
from ray.data._internal.compute import get_compute
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.map_transformer import (
    create_map_transformer_for_map_batches_op,
    create_map_transformer_for_row_based_map_op,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.util import make_callable_class_concurrent
from ray.data._internal.logical.operators.map_operator import (
    AbstractUDFMap,
    Filter,
    FlatMap,
    MapBatches,
    MapRows,
)
from ray.data._internal.numpy_support import is_valid_udf_return
from ray.data._internal.util import _truncated_repr, validate_compute
from ray.data.block import Block, BlockAccessor, CallableClass


def _parse_op_fn(op: AbstractUDFMap):
    fn_args = op._fn_args or ()
    fn_kwargs = op._fn_kwargs or {}

    if isinstance(op._fn, CallableClass):
        fn_constructor_args = op._fn_constructor_args or ()
        fn_constructor_kwargs = op._fn_constructor_kwargs or {}

        fn_ = make_callable_class_concurrent(op._fn)

        def fn(item: Any) -> Any:
            assert ray.data._cached_fn is not None
            assert ray.data._cached_cls == fn_
            return ray.data._cached_fn(item, *fn_args, **fn_kwargs)

        def init_fn():
            if ray.data._cached_fn is None:
                ray.data._cached_cls = fn_
                ray.data._cached_fn = fn_(*fn_constructor_args, **fn_constructor_kwargs)

    else:

        def fn(item: Any) -> Any:
            return op._fn(item, *fn_args, **fn_kwargs)

        def init_fn():
            pass

    return fn, init_fn


def validate_batch(batch: Block) -> None:
    if not isinstance(
        batch,
        (
            list,
            pa.Table,
            np.ndarray,
            collections.abc.Mapping,
            pd.core.frame.DataFrame,
        ),
    ):
        raise ValueError(
            "The `fn` you passed to `map_batches` returned a value of type "
            f"{type(batch)}. This isn't allowed -- `map_batches` expects "
            "`fn` to return a `pandas.DataFrame`, `pyarrow.Table`, "
            "`numpy.ndarray`, `list`, or `dict[str, numpy.ndarray]`."
        )

    if isinstance(batch, list):
        raise ValueError(
            f"Error validating {_truncated_repr(batch)}: "
            "Returning a list of objects from `map_batches` is not "
            "allowed in Ray 2.5. To return Python objects, "
            "wrap them in a named dict field, e.g., "
            "return `{'results': objects}` instead of just `objects`."
        )

    if isinstance(batch, collections.abc.Mapping):
        for key, value in list(batch.items()):
            if not is_valid_udf_return(value):
                raise ValueError(
                    f"Error validating {_truncated_repr(batch)}: "
                    "The `fn` you passed to `map_batches` returned a "
                    f"`dict`. `map_batches` expects all `dict` values "
                    f"to be `list` or `np.ndarray` type, but the value "
                    f"corresponding to key {key!r} is of type "
                    f"{type(value)}. To fix this issue, convert "
                    f"the {type(value)} to a `np.ndarray`."
                )


def _create_map_transformer_for_map_batches_op(op: MapBatches):
    fn, init_fn = _parse_op_fn(op)

    def op_transform_fn(batches, _):
        for batch in batches:
            try:
                if (
                    not isinstance(batch, collections.abc.Mapping)
                    and BlockAccessor.for_block(batch).num_rows() == 0
                ):
                    # For empty input blocks, we directly ouptut them without
                    # calling the UDF.
                    # TODO(hchen): This workaround is because some all-to-all
                    # operators output empty blocks with no schema.
                    res = [batch]
                else:
                    res = fn(batch)
                    if not isinstance(res, GeneratorType):
                        res = [res]
            except ValueError as e:
                read_only_msgs = [
                    "assignment destination is read-only",
                    "buffer source array is read-only",
                ]
                err_msg = str(e)
                if any(msg in err_msg for msg in read_only_msgs):
                    raise ValueError(
                        f"Batch mapper function {fn.__name__} tried to mutate a "
                        "zero-copy read-only batch. To be able to mutate the "
                        "batch, pass zero_copy_batch=False to map_batches(); "
                        "this will create a writable copy of the batch before "
                        "giving it to fn. To elide this copy, modify your mapper "
                        "function so it doesn't try to mutate its input."
                    ) from e
                else:
                    raise e from None
            else:
                for out_batch in res:
                    validate_batch(out_batch)
                    yield out_batch

    return create_map_transformer_for_map_batches_op(
        op_transform_fn,
        op._batch_size,
        op._batch_format,
        op._zero_copy_batch,
        init_fn,
    )


def validate_row_output(item):
    if not isinstance(item, collections.abc.Mapping):
        raise ValueError(
            f"Error validating {_truncated_repr(item)}: "
            "Standalone Python objects are not "
            "allowed in Ray 2.5. To return Python objects from map(), "
            "wrap them in a dict, e.g., "
            "return `{'item': item}` instead of just `item`."
        )


def _create_map_transformer_for_row_based_op(op: AbstractUDFMap):
    fn, init_fn = _parse_op_fn(op)

    if isinstance(op, MapRows):

        def op_transform_fn(rows, _):
            for row in rows:
                item = fn(row)
                validate_row_output(item)
                yield item

    elif isinstance(op, FlatMap):

        def op_transform_fn(rows, _):
            for row in rows:
                for out_row in fn(row):
                    validate_row_output(out_row)
                    yield out_row

    elif isinstance(op, Filter):

        def op_transform_fn(rows, _):
            for row in rows:
                if fn(row):
                    yield row

    else:
        raise ValueError(f"Found unknown logical operator during planning: {op}")

    return create_map_transformer_for_row_based_map_op(op_transform_fn, init_fn)


def _plan_udf_map_op(
    op: AbstractUDFMap, input_physical_dag: PhysicalOperator
) -> MapOperator:
    """Get the corresponding physical operators DAG for AbstractUDFMap operators.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """

    compute = get_compute(op._compute)
    validate_compute(op._fn, compute)

    if isinstance(op, MapBatches):
        map_transformer = _create_map_transformer_for_map_batches_op(op)
    else:
        map_transformer = _create_map_transformer_for_row_based_op(op)

    return MapOperator.create(
        map_transformer,
        input_physical_dag,
        name=op.name,
        compute_strategy=compute,
        min_rows_per_bundle=op._target_block_size,
        ray_remote_args=op._ray_remote_args,
    )
