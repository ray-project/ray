import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from packaging.version import parse as parse_version

import ray
from ray._private.arrow_utils import get_pyarrow_version
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_util import _check_usage_record
from ray.data.tests.util import extract_values
from ray.tests.conftest import *  # noqa


def _should_skip_huggingface_test():
    """Check if we should skip the HuggingFace test due to version incompatibility."""
    pyarrow_version = get_pyarrow_version()
    if pyarrow_version is None:
        return False

    try:
        datasets_version = __import__("datasets").__version__
        if datasets_version is None:
            return False

        return pyarrow_version < parse_version("12.0.0") and parse_version(
            datasets_version
        ) >= parse_version("3.0.0")
    except (ImportError, AttributeError):
        return False


def test_from_modin_e2e(ray_start_regular_shared_2_cpus):
    import modin.pandas as mopd

    df = pd.DataFrame(
        {"one": list(range(100)), "two": list(range(100))},
    )
    modf = mopd.DataFrame(df)
    ds = ray.data.from_modin(modf)
    # `ds.take_all()` triggers execution with new backend, which is
    # needed for checking operator usage below.
    assert len(ds.take_all()) == len(df)
    # `ds.to_pandas()` does not use the new backend.
    dfds = ds.to_pandas()

    assert df.equals(dfds)
    # Check that metadata fetch is included in stats. This is `FromPandas`
    # instead of `FromModin` because `from_modin` reduces to `from_pandas_refs`.
    assert "FromPandas" in ds.stats()
    assert ds._plan._logical_plan.dag.name == "FromPandas"
    _check_usage_record(["FromPandas"])


@pytest.mark.parametrize("enable_pandas_block", [False, True])
def test_from_pandas_refs_e2e(ray_start_regular_shared_2_cpus, enable_pandas_block):
    ctx = ray.data.context.DataContext.get_current()
    old_enable_pandas_block = ctx.enable_pandas_block
    ctx.enable_pandas_block = enable_pandas_block

    try:
        df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
        df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})

        ds = ray.data.from_pandas_refs([ray.put(df1), ray.put(df2)])
        values = [(r["one"], r["two"]) for r in ds.take(6)]
        rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
        assert values == rows
        # Check that metadata fetch is included in stats.
        assert "FromPandas" in ds.stats()
        assert ds._plan._logical_plan.dag.name == "FromPandas"

        # Test chaining multiple operations
        ds2 = ds.map_batches(lambda x: x)
        values = [(r["one"], r["two"]) for r in ds2.take(6)]
        assert values == rows
        assert "MapBatches" in ds2.stats()
        assert "FromPandas" in ds2.stats()
        assert ds2._plan._logical_plan.dag.name == "MapBatches(<lambda>)"

        # test from single pandas dataframe
        ds = ray.data.from_pandas_refs(ray.put(df1))
        values = [(r["one"], r["two"]) for r in ds.take(3)]
        rows = [(r.one, r.two) for _, r in df1.iterrows()]
        assert values == rows
        # Check that metadata fetch is included in stats.
        assert "FromPandas" in ds.stats()
        assert ds._plan._logical_plan.dag.name == "FromPandas"
        _check_usage_record(["FromPandas"])
    finally:
        ctx.enable_pandas_block = old_enable_pandas_block


def test_from_numpy_refs_e2e(ray_start_regular_shared_2_cpus):

    arr1 = np.expand_dims(np.arange(0, 4), axis=1)
    arr2 = np.expand_dims(np.arange(4, 8), axis=1)

    ds = ray.data.from_numpy_refs([ray.put(arr1), ray.put(arr2)])
    values = np.stack(extract_values("data", ds.take(8)))
    np.testing.assert_array_equal(values, np.concatenate((arr1, arr2)))
    # Check that conversion task is included in stats.
    assert "FromNumpy" in ds.stats()
    assert ds._plan._logical_plan.dag.name == "FromNumpy"
    _check_usage_record(["FromNumpy"])

    # Test chaining multiple operations
    ds2 = ds.map_batches(lambda x: x)
    values = np.stack(extract_values("data", ds2.take(8)))
    np.testing.assert_array_equal(values, np.concatenate((arr1, arr2)))
    assert "MapBatches" in ds2.stats()
    assert "FromNumpy" in ds2.stats()
    assert ds2._plan._logical_plan.dag.name == "MapBatches(<lambda>)"
    _check_usage_record(["FromNumpy", "MapBatches"])

    # Test from single NumPy ndarray.
    ds = ray.data.from_numpy_refs(ray.put(arr1))
    values = np.stack(extract_values("data", ds.take(4)))
    np.testing.assert_array_equal(values, arr1)
    # Check that conversion task is included in stats.
    assert "FromNumpy" in ds.stats()
    assert ds._plan._logical_plan.dag.name == "FromNumpy"
    _check_usage_record(["FromNumpy"])


def test_from_arrow_refs_e2e(ray_start_regular_shared_2_cpus):

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_arrow_refs(
        [ray.put(pa.Table.from_pandas(df1)), ray.put(pa.Table.from_pandas(df2))]
    )

    values = [(r["one"], r["two"]) for r in ds.take(6)]
    rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
    assert values == rows
    # Check that metadata fetch is included in stats.
    assert "FromArrow" in ds.stats()
    assert ds._plan._logical_plan.dag.name == "FromArrow"
    _check_usage_record(["FromArrow"])

    # test from single pyarrow table ref
    ds = ray.data.from_arrow_refs(ray.put(pa.Table.from_pandas(df1)))
    values = [(r["one"], r["two"]) for r in ds.take(3)]
    rows = [(r.one, r.two) for _, r in df1.iterrows()]
    assert values == rows
    # Check that conversion task is included in stats.
    assert "FromArrow" in ds.stats()
    assert ds._plan._logical_plan.dag.name == "FromArrow"
    _check_usage_record(["FromArrow"])


@pytest.mark.skipif(
    _should_skip_huggingface_test,
    reason="Skip due to HuggingFace datasets >= 3.0.0 requiring pyarrow >= 12.0.0",
)
def test_from_huggingface_e2e(ray_start_regular_shared_2_cpus):
    import datasets

    from ray.data.tests.test_huggingface import hfds_assert_equals

    data = datasets.load_dataset("tweet_eval", "emotion")
    assert isinstance(data, datasets.DatasetDict)
    ray_datasets = {
        "train": ray.data.from_huggingface(data["train"]),
        "validation": ray.data.from_huggingface(data["validation"]),
        "test": ray.data.from_huggingface(data["test"]),
    }

    for ds_key, ds in ray_datasets.items():
        assert isinstance(ds, ray.data.Dataset)
        # `ds.take_all()` triggers execution with new backend, which is
        # needed for checking operator usage below.
        assert len(ds.take_all()) > 0
        # Check that metadata fetch is included in stats;
        # the underlying implementation uses the `ReadParquet` operator
        # as this is an un-transformed public dataset.
        assert "ReadParquet" in ds.stats() or "FromArrow" in ds.stats()
        assert (
            ds._plan._logical_plan.dag.name == "ReadParquet"
            or ds._plan._logical_plan.dag.name == "FromArrow"
        )
        # use sort by 'text' to match order of rows
        hfds_assert_equals(data[ds_key], ds)
        try:
            _check_usage_record(["ReadParquet"])
        except AssertionError:
            _check_usage_record(["FromArrow"])

    # test transformed public dataset for fallback behavior
    base_hf_dataset = data["train"]
    hf_dataset_split = base_hf_dataset.train_test_split(test_size=0.2)
    ray_dataset_split_train = ray.data.from_huggingface(hf_dataset_split["train"])
    assert isinstance(ray_dataset_split_train, ray.data.Dataset)
    # `ds.take_all()` triggers execution with new backend, which is
    # needed for checking operator usage below.
    assert len(ray_dataset_split_train.take_all()) > 0
    # Check that metadata fetch is included in stats;
    # the underlying implementation uses the `FromArrow` operator.
    assert "FromArrow" in ray_dataset_split_train.stats()
    assert ray_dataset_split_train._plan._logical_plan.dag.name == "FromArrow"
    assert ray_dataset_split_train.count() == hf_dataset_split["train"].num_rows
    _check_usage_record(["FromArrow"])


@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Skip due to incompatibility tensorflow with Python 3.12+",
)
def test_from_tf_e2e(ray_start_regular_shared_2_cpus):
    import tensorflow as tf
    import tensorflow_datasets as tfds

    tf_dataset = tfds.load("mnist", split=["train"], as_supervised=True)[0]
    tf_dataset = tf_dataset.take(8)  # Use subset to make test run faster.

    ray_dataset = ray.data.from_tf(tf_dataset)

    actual_data = extract_values("item", ray_dataset.take_all())
    expected_data = list(tf_dataset)
    assert len(actual_data) == len(expected_data)
    for (expected_features, expected_label), (actual_features, actual_label) in zip(
        expected_data, actual_data
    ):
        tf.debugging.assert_equal(expected_features, actual_features)
        tf.debugging.assert_equal(expected_label, actual_label)

    # Check that metadata fetch is included in stats.
    assert "FromItems" in ray_dataset.stats()
    # Underlying implementation uses `FromItems` operator
    assert ray_dataset._plan._logical_plan.dag.name == "FromItems"
    _check_usage_record(["FromItems"])


def test_from_torch_e2e(ray_start_regular_shared_2_cpus, tmp_path):
    import torchvision

    torch_dataset = torchvision.datasets.FashionMNIST(tmp_path, download=True)

    ray_dataset = ray.data.from_torch(torch_dataset)

    expected_data = list(torch_dataset)
    actual_data = list(ray_dataset.take_all())
    assert extract_values("item", actual_data) == expected_data

    # Check that metadata fetch is included in stats.
    assert "ReadTorch" in ray_dataset.stats()

    # Underlying implementation uses `FromItems` operator
    assert ray_dataset._plan._logical_plan.dag.name == "ReadTorch"
    _check_usage_record(["ReadTorch"])


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
