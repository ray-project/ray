import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.air.util.tensor_extensions.arrow import (
    get_arrow_extension_fixed_shape_tensor_types,
)
from ray.data.extensions.tensor_extension import (
    ArrowTensorArray,
    TensorArray,
    TensorDtype,
)
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_util import _check_usage_record
from ray.tests.conftest import *  # noqa


def test_from_dask(ray_start_regular_shared):
    import dask.dataframe as dd

    df = pd.DataFrame({"one": list(range(100)), "two": list(range(100))})
    ddf = dd.from_pandas(df, npartitions=10)
    ds = ray.data.from_dask(ddf)
    dfds = ds.to_pandas()
    assert df.equals(dfds)


def test_from_dask_e2e(ray_start_regular_shared):
    import dask.dataframe as dd

    df = pd.DataFrame({"one": list(range(100)), "two": list(range(100))})
    ddf = dd.from_pandas(df, npartitions=10)
    ds = ray.data.from_dask(ddf)
    # `ds.take_all()` triggers execution with new backend, which is
    # needed for checking operator usage below.
    assert len(ds.take_all()) == len(df)
    dfds = ds.to_pandas()
    assert df.equals(dfds)

    # Underlying implementation uses `FromPandas` operator
    assert "FromPandas" in ds.stats()
    assert ds._plan._logical_plan.dag.name == "FromPandas"
    _check_usage_record(["FromPandas"])


def test_to_dask_simple(ray_start_regular_shared):
    ds = ray.data.range(100)
    assert ds.to_dask().sum().compute()[0] == 4950


@pytest.mark.parametrize("ds_format", ["pandas", "arrow"])
def test_to_dask(ray_start_regular_shared, ds_format):
    # Since 2023.7.1, Dask DataFrame automatically converts text data using object data types to string[pyarrow]
    # For the purpose of this test, we need to disable this behavior.
    import dask

    dask.config.set({"dataframe.convert-string": False})

    from ray.util.dask import ray_dask_get

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    df = pd.concat([df1, df2])
    ds = ray.data.from_blocks([df1, df2])
    if ds_format == "arrow":
        ds = ds.map_batches(lambda df: df, batch_format="pyarrow", batch_size=None)
    ddf = ds.to_dask()
    meta = ddf._meta
    # Check metadata.
    assert isinstance(meta, pd.DataFrame)
    assert meta.empty
    assert list(meta.columns) == ["one", "two"]
    assert list(meta.dtypes) == [np.int64, object]
    # Explicit Dask-on-Ray
    assert df.equals(ddf.compute(scheduler=ray_dask_get))
    # Implicit Dask-on-Ray.
    assert df.equals(ddf.compute())

    # Explicit metadata.
    df1["two"] = df1["two"].astype(pd.StringDtype())
    df2["two"] = df2["two"].astype(pd.StringDtype())
    df = pd.concat([df1, df2])
    ds = ray.data.from_blocks([df1, df2])
    if ds_format == "arrow":
        ds = ds.map_batches(lambda df: df, batch_format="pyarrow", batch_size=None)
    ddf = ds.to_dask(
        meta=pd.DataFrame(
            {"one": pd.Series(dtype=np.int16), "two": pd.Series(dtype=pd.StringDtype())}
        ),
    )

    meta = ddf._meta
    # Check metadata.
    assert isinstance(meta, pd.DataFrame)
    assert meta.empty
    assert list(meta.columns) == ["one", "two"]
    assert list(meta.dtypes) == [np.int16, pd.StringDtype()]

    # Explicit Dask-on-Ray
    result = ddf.compute(scheduler=ray_dask_get)

    print("Expected: ", df)
    print("Result: ", result)

    pd.testing.assert_frame_equal(df, result)

    # Implicit Dask-on-Ray.
    pd.testing.assert_frame_equal(df, ddf.compute())

    # Test case with blocks which have different schema, where we must
    # skip the metadata check in order to avoid a Dask metadata mismatch error.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"three": [4, 5, 6], "four": ["e", "f", "g"]})
    df = pd.concat([df1, df2])
    ds = ray.data.from_blocks([df1, df2])
    if ds_format == "arrow":
        ds = ds.map_batches(lambda df: df, batch_format="pyarrow", batch_size=None)

    ddf = ds.to_dask(verify_meta=False)

    # Explicit Dask-on-Ray
    result = ddf.compute(scheduler=ray_dask_get)

    print("Expected: ", df)
    print("Result (1): ", result)

    pd.testing.assert_frame_equal(df, result)

    # Implicit Dask-on-Ray.
    result = ddf.compute()

    print("Result (2): ", result)

    pd.testing.assert_frame_equal(df, result)


def test_to_dask_tensor_column_cast_pandas(ray_start_regular_shared):
    # Check that tensor column casting occurs when converting a Dataset to a Dask
    # DataFrame.
    data = np.arange(12).reshape((3, 2, 2))
    ctx = ray.data.context.DataContext.get_current()
    original = ctx.enable_tensor_extension_casting
    try:
        ctx.enable_tensor_extension_casting = True
        in_df = pd.DataFrame({"a": TensorArray(data)})
        ds = ray.data.from_pandas(in_df)
        dtypes = ds.schema().base_schema.types
        assert len(dtypes) == 1
        assert isinstance(dtypes[0], TensorDtype)
        out_df = ds.to_dask().compute()
        assert out_df["a"].dtype.type is np.object_
        expected_df = pd.DataFrame({"a": list(data)})
        pd.testing.assert_frame_equal(out_df, expected_df)
    finally:
        ctx.enable_tensor_extension_casting = original


def test_to_dask_tensor_column_cast_arrow(ray_start_regular_shared):
    # Check that tensor column casting occurs when converting a Dataset to a Dask
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
        out_df = ds.to_dask().compute()
        assert out_df["a"].dtype.type is np.object_
        expected_df = pd.DataFrame({"a": list(data)})
        pd.testing.assert_frame_equal(out_df, expected_df)
    finally:
        ctx.enable_tensor_extension_casting = original


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
