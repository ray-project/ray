from typing import Iterator

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.air.util.tensor_extensions.arrow import (
    get_arrow_extension_fixed_shape_tensor_types,
)
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data.block import Block
from ray.data.extensions import ArrowTensorArray, TensorDtype
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa
from ray.types import ObjectRef


def _get_first_block(bundles: Iterator[RefBundle]) -> ObjectRef[Block]:
    return next(bundles).block_refs[0]


@pytest.mark.parametrize("enable_pandas_block", [False, True])
def test_from_pandas(ray_start_regular_shared, enable_pandas_block):
    ctx = ray.data.context.DataContext.get_current()
    old_enable_pandas_block = ctx.enable_pandas_block
    ctx.enable_pandas_block = enable_pandas_block
    try:
        df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
        df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
        ds = ray.data.from_pandas([df1, df2])
        block = ray.get(_get_first_block(ds.iter_internal_ref_bundles()))
        assert (
            isinstance(block, pd.DataFrame)
            if enable_pandas_block
            else isinstance(block, pa.Table)
        )
        values = [(r["one"], r["two"]) for r in ds.take(6)]
        rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
        assert values == rows
        # Check that metadata fetch is included in stats.
        assert "FromPandas" in ds.stats()

        # test from single pandas dataframe
        ds = ray.data.from_pandas(df1)
        block = ray.get(_get_first_block(ds.iter_internal_ref_bundles()))
        assert (
            isinstance(block, pd.DataFrame)
            if enable_pandas_block
            else isinstance(block, pa.Table)
        )
        values = [(r["one"], r["two"]) for r in ds.take(3)]
        rows = [(r.one, r.two) for _, r in df1.iterrows()]
        assert values == rows
        # Check that metadata fetch is included in stats.
        assert "FromPandas" in ds.stats()
    finally:
        ctx.enable_pandas_block = old_enable_pandas_block


@pytest.mark.parametrize("num_inputs", [1, 2])
def test_from_pandas_override_num_blocks(num_inputs, ray_start_regular_shared):
    df = pd.DataFrame({"number": [0]})

    ds = ray.data.from_pandas([df] * num_inputs, override_num_blocks=2)

    assert ds.materialize().num_blocks() == 2


@pytest.mark.parametrize("enable_pandas_block", [False, True])
def test_from_pandas_refs(ray_start_regular_shared, enable_pandas_block):
    ctx = ray.data.context.DataContext.get_current()
    old_enable_pandas_block = ctx.enable_pandas_block
    ctx.enable_pandas_block = enable_pandas_block
    try:
        df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
        df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
        ds = ray.data.from_pandas_refs([ray.put(df1), ray.put(df2)])
        block = ray.get(_get_first_block(ds.iter_internal_ref_bundles()))
        assert (
            isinstance(block, pd.DataFrame)
            if enable_pandas_block
            else isinstance(block, pa.Table)
        )
        values = [(r["one"], r["two"]) for r in ds.take(6)]
        rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
        assert values == rows
        # Check that metadata fetch is included in stats.
        assert "FromPandas" in ds.stats()

        # test from single pandas dataframe ref
        ds = ray.data.from_pandas_refs(ray.put(df1))
        block = ray.get(_get_first_block(ds.iter_internal_ref_bundles()))
        assert (
            isinstance(block, pd.DataFrame)
            if enable_pandas_block
            else isinstance(block, pa.Table)
        )
        values = [(r["one"], r["two"]) for r in ds.take(3)]
        rows = [(r.one, r.two) for _, r in df1.iterrows()]
        assert values == rows
        # Check that metadata fetch is included in stats.
        assert "FromPandas" in ds.stats()
    finally:
        ctx.enable_pandas_block = old_enable_pandas_block


def test_to_pandas(ray_start_regular_shared):
    n = 5
    df = pd.DataFrame({"id": list(range(n))})
    ds = ray.data.range(n)
    dfds = ds.to_pandas()
    assert df.equals(dfds)

    # Test limit.
    with pytest.raises(ValueError):
        dfds = ds.to_pandas(limit=3)

    # Test limit greater than number of rows.
    dfds = ds.to_pandas(limit=6)
    assert df.equals(dfds)


def test_to_pandas_different_block_types(ray_start_regular_shared):
    # Test for https://github.com/ray-project/ray/issues/48575.
    df = pd.DataFrame({"a": [0]})
    ds1 = ray.data.from_pandas(df)

    table = pa.Table.from_pandas(df)
    ds2 = ray.data.from_arrow(table)

    actual_df = ds1.union(ds2).to_pandas()

    expected_df = pd.DataFrame({"a": [0, 0]})
    pd.testing.assert_frame_equal(actual_df, expected_df)


def test_to_pandas_refs(ray_start_regular_shared):
    n = 5
    df = pd.DataFrame({"id": list(range(n))})
    ds = ray.data.range(n)
    dfds = pd.concat(ray.get(ds.to_pandas_refs()), ignore_index=True)
    assert df.equals(dfds)


def test_pandas_roundtrip(ray_start_regular_shared, tmp_path):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_pandas([df1, df2], override_num_blocks=2)
    dfds = ds.to_pandas()
    assert pd.concat([df1, df2], ignore_index=True).equals(dfds)


def test_to_pandas_tensor_column_cast_pandas(ray_start_regular_shared):
    # Check that tensor column casting occurs when converting a Dataset to a Pandas
    # DataFrame.
    data = np.arange(12).reshape((3, 2, 2))
    ctx = ray.data.context.DataContext.get_current()
    original = ctx.enable_tensor_extension_casting
    try:
        ctx.enable_tensor_extension_casting = True
        in_df = pd.DataFrame({"a": [data]})
        ds = ray.data.from_pandas(in_df)
        dtypes = ds.schema().base_schema.types
        assert len(dtypes) == 1
        # Tensor column should be automatically cast to Tensor extension.
        assert isinstance(dtypes[0], TensorDtype)
        # Original df should not be changed.
        assert not isinstance(in_df.dtypes[0], TensorDtype)
        out_df = ds.to_pandas()
        # Column should be cast back to object dtype when returning back to user.
        assert out_df["a"].dtype.type is np.object_
        expected_df = pd.DataFrame({"a": [data]})
        pd.testing.assert_frame_equal(out_df, expected_df)
    finally:
        ctx.enable_tensor_extension_casting = original


def test_to_pandas_tensor_column_cast_arrow(ray_start_regular_shared):
    # Check that tensor column casting occurs when converting a Dataset to a Pandas
    # DataFrame.
    data = np.arange(12).reshape((3, 2, 2))
    ctx = ray.data.context.DataContext.get_current()
    original = ctx.enable_tensor_extension_casting
    try:
        ctx.enable_tensor_extension_casting = True
        in_table = pa.table({"a": ArrowTensorArray.from_numpy(data)})
        ds = ray.data.from_arrow(in_table)
        dtype = ds.schema().base_schema.field(0).type
        assert isinstance(dtype, get_arrow_extension_fixed_shape_tensor_types())
        out_df = ds.to_pandas()
        assert out_df["a"].dtype.type is np.object_
        expected_df = pd.DataFrame({"a": list(data)})
        pd.testing.assert_frame_equal(out_df, expected_df)
    finally:
        ctx.enable_tensor_extension_casting = original


def test_read_pandas_data_array_column(ray_start_regular_shared):
    df = pd.DataFrame(
        {
            "one": [1, 2, 3],
            "array": [
                np.array([1, 1, 1]),
                np.array([2, 2, 2]),
                np.array([3, 3, 3]),
            ],
        }
    )
    ds = ray.data.from_pandas(df)
    row = ds.take(1)[0]
    assert row["one"] == 1
    assert all(row["array"] == [1, 1, 1])


def test_add_column_to_pandas(ray_start_regular_shared):
    # Refer to issue https://github.com/ray-project/ray/issues/51758
    ds = ray.data.from_pandas(
        pd.DataFrame({"a": list(range(20))}), override_num_blocks=2
    )

    ds = ds.add_column(
        "foo1", lambda df: pd.Series([1] * len(df)), batch_format="pandas"
    )
    ds = ds.add_column(
        "foo2", lambda df: pd.DatetimeIndex([1] * len(df)), batch_format="pandas"
    )
    ds = ds.add_column(
        "foo3", lambda df: pd.DataFrame({"foo": [1] * len(df)}), batch_format="pandas"
    )
    for row in ds.iter_rows():
        assert row["foo1"] == 1 and row["foo2"] == pd.Timestamp(1) and row["foo3"] == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
