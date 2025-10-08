import math
import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def test_iter_rows(ray_start_regular_shared):
    # Test simple rows.
    n = 10
    ds = ray.data.range(n)
    for row, k in zip(ds.iter_rows(), range(n)):
        assert row == {"id": k}

    # Test tabular rows.
    t1 = pa.Table.from_pydict({"one": [1, 2, 3], "two": [2, 3, 4]})
    t2 = pa.Table.from_pydict({"one": [4, 5, 6], "two": [5, 6, 7]})
    t3 = pa.Table.from_pydict({"one": [7, 8, 9], "two": [8, 9, 10]})
    t4 = pa.Table.from_pydict({"one": [10, 11, 12], "two": [11, 12, 13]})
    ts = [t1, t2, t3, t4]
    t = pa.concat_tables(ts)
    ds = ray.data.from_arrow(ts)

    def to_pylist(table):
        pydict = table.to_pydict()
        names = table.schema.names
        pylist = [
            {column: pydict[column][row] for column in names}
            for row in range(table.num_rows)
        ]
        return pylist

    # Default ArrowRows.
    for row, t_row in zip(ds.iter_rows(), to_pylist(t)):
        assert isinstance(row, dict)
        assert row == t_row

    # PandasRows after conversion.
    pandas_ds = ds.map_batches(lambda x: x, batch_format="pandas")
    df = t.to_pandas()
    for row, (index, df_row) in zip(pandas_ds.iter_rows(), df.iterrows()):
        assert isinstance(row, dict)
        assert row == df_row.to_dict()


def test_iter_batches_basic(ray_start_regular_shared):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": [5, 6, 7]})
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": [8, 9, 10]})
    df4 = pd.DataFrame({"one": [10, 11, 12], "two": [11, 12, 13]})
    dfs = [df1, df2, df3, df4]
    ds = ray.data.from_blocks(dfs)

    # Default.
    for batch, df in zip(ds.iter_batches(batch_size=None, batch_format="pandas"), dfs):
        assert isinstance(batch, pd.DataFrame)
        assert batch.equals(df)

    # pyarrow.Table format.
    for batch, df in zip(ds.iter_batches(batch_size=None, batch_format="pyarrow"), dfs):
        assert isinstance(batch, pa.Table)
        assert batch.equals(pa.Table.from_pandas(df))

    # NumPy format.
    for batch, df in zip(ds.iter_batches(batch_size=None, batch_format="numpy"), dfs):
        assert isinstance(batch, dict)
        assert list(batch.keys()) == ["one", "two"]
        assert all(isinstance(col, np.ndarray) for col in batch.values())
        pd.testing.assert_frame_equal(pd.DataFrame(batch), df)

    # Test NumPy format on Arrow blocks.
    ds2 = ds.map_batches(lambda b: b, batch_size=None, batch_format="pyarrow")
    for batch, df in zip(ds2.iter_batches(batch_size=None, batch_format="numpy"), dfs):
        assert isinstance(batch, dict)
        assert list(batch.keys()) == ["one", "two"]
        assert all(isinstance(col, np.ndarray) for col in batch.values())
        pd.testing.assert_frame_equal(pd.DataFrame(batch), df)

    # Default format -> numpy.
    for batch, df in zip(ds.iter_batches(batch_size=None, batch_format="default"), dfs):
        assert isinstance(batch, dict)
        assert list(batch.keys()) == ["one", "two"]
        assert all(isinstance(col, np.ndarray) for col in batch.values())
        pd.testing.assert_frame_equal(pd.DataFrame(batch), df)

    # Batch size.
    batch_size = 2
    batches = list(ds.iter_batches(batch_size=batch_size, batch_format="pandas"))
    assert all(len(batch) == batch_size for batch in batches)
    assert len(batches) == math.ceil(
        (len(df1) + len(df2) + len(df3) + len(df4)) / batch_size
    )
    assert pd.concat(batches, ignore_index=True).equals(
        pd.concat(dfs, ignore_index=True)
    )

    # Batch size larger than block.
    batch_size = 4
    batches = list(ds.iter_batches(batch_size=batch_size, batch_format="pandas"))
    assert all(len(batch) == batch_size for batch in batches)
    assert len(batches) == math.ceil(
        (len(df1) + len(df2) + len(df3) + len(df4)) / batch_size
    )
    assert pd.concat(batches, ignore_index=True).equals(
        pd.concat(dfs, ignore_index=True)
    )

    # Batch size larger than dataset.
    batch_size = 15
    batches = list(ds.iter_batches(batch_size=batch_size, batch_format="pandas"))
    assert all(len(batch) == ds.count() for batch in batches)
    assert len(batches) == 1
    assert pd.concat(batches, ignore_index=True).equals(
        pd.concat(dfs, ignore_index=True)
    )

    # Batch size drop partial.
    batch_size = 5
    batches = list(
        ds.iter_batches(batch_size=batch_size, drop_last=True, batch_format="pandas")
    )
    assert all(len(batch) == batch_size for batch in batches)
    assert len(batches) == (len(df1) + len(df2) + len(df3) + len(df4)) // batch_size
    assert pd.concat(batches, ignore_index=True).equals(
        pd.concat(dfs, ignore_index=True)[:10]
    )

    # Batch size don't drop partial.
    batch_size = 5
    batches = list(
        ds.iter_batches(batch_size=batch_size, drop_last=False, batch_format="pandas")
    )
    assert all(len(batch) == batch_size for batch in batches[:-1])
    assert len(batches[-1]) == (len(df1) + len(df2) + len(df3) + len(df4)) % batch_size
    assert len(batches) == math.ceil(
        (len(df1) + len(df2) + len(df3) + len(df4)) / batch_size
    )
    assert pd.concat(batches, ignore_index=True).equals(
        pd.concat(dfs, ignore_index=True)
    )

    # Prefetch.
    batches = list(
        ds.iter_batches(prefetch_batches=1, batch_size=None, batch_format="pandas")
    )
    assert len(batches) == len(dfs)
    for batch, df in zip(batches, dfs):
        assert isinstance(batch, pd.DataFrame)
        assert batch.equals(df)

    batch_size = 2
    batches = list(
        ds.iter_batches(
            prefetch_batches=2, batch_size=batch_size, batch_format="pandas"
        )
    )
    assert all(len(batch) == batch_size for batch in batches)
    assert len(batches) == math.ceil(
        (len(df1) + len(df2) + len(df3) + len(df4)) / batch_size
    )
    assert pd.concat(batches, ignore_index=True).equals(
        pd.concat(dfs, ignore_index=True)
    )

    # Prefetch more than number of blocks.
    batches = list(
        ds.iter_batches(
            prefetch_batches=len(dfs), batch_size=None, batch_format="pandas"
        )
    )
    assert len(batches) == len(dfs)
    for batch, df in zip(batches, dfs):
        assert isinstance(batch, pd.DataFrame)
        assert batch.equals(df)

    # Prefetch with ray.wait.
    context = DataContext.get_current()
    old_config = context.actor_prefetcher_enabled
    try:
        context.actor_prefetcher_enabled = False
        batches = list(
            ds.iter_batches(prefetch_batches=1, batch_size=None, batch_format="pandas")
        )
        assert len(batches) == len(dfs)
        for batch, df in zip(batches, dfs):
            assert isinstance(batch, pd.DataFrame)
            assert batch.equals(df)
    finally:
        context.actor_prefetcher_enabled = old_config


def test_iter_batches_empty_block(ray_start_regular_shared):
    ds = ray.data.range(1).repartition(10)
    assert str(list(ds.iter_batches(batch_size=None))) == "[{'id': array([0])}]"
    assert (
        str(list(ds.iter_batches(batch_size=1, local_shuffle_buffer_size=1)))
        == "[{'id': array([0])}]"
    )


@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_iter_batches_local_shuffle(shutdown_only, ds_format):
    # Input validation.
    # Batch size must be given for local shuffle.
    with pytest.raises(ValueError):
        list(
            ray.data.range(100).iter_batches(
                batch_size=None, local_shuffle_buffer_size=10
            )
        )

    def range(n, parallelism=200):
        if ds_format == "arrow":
            ds = ray.data.range(n, override_num_blocks=parallelism)
        elif ds_format == "pandas":
            ds = ray.data.range(n, override_num_blocks=parallelism).map_batches(
                lambda df: df, batch_size=None, batch_format="pandas"
            )
        return ds

    def to_row_dicts(batch):
        if isinstance(batch, pd.DataFrame):
            return batch.to_dict(orient="records")
        return [{"id": v} for v in batch["id"]]

    def unbatch(batches):
        return [r for batch in batches for r in to_row_dicts(batch)]

    def sort(r):
        return sorted(r, key=lambda v: v["id"])

    base = range(100).take_all()

    # Local shuffle.
    r1 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
        )
    )
    r2 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    assert sort(r1) == sort(base)
    assert sort(r2) == sort(base)

    # Set seed.
    r1 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
            local_shuffle_seed=0,
        )
    )
    r2 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
            local_shuffle_seed=0,
        )
    )
    # Check randomness of shuffle.
    assert r1 == r2, (r1, r2)
    assert r1 != base
    # Check content.
    assert sort(r1) == sort(base)

    # Single block.
    r1 = unbatch(
        range(100, parallelism=1).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
        )
    )
    r2 = unbatch(
        range(100, parallelism=1).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    assert sort(r1) == sort(base)
    assert sort(r2) == sort(base)

    # Single-row blocks.
    r1 = unbatch(
        range(100, parallelism=100).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
        )
    )
    r2 = unbatch(
        range(100, parallelism=100).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=25,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    assert sort(r1) == sort(base)
    assert sort(r2) == sort(base)

    # Buffer larger than dataset.
    r1 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=200,
        )
    )
    r2 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=3,
            local_shuffle_buffer_size=200,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    assert sort(r1) == sort(base)
    assert sort(r2) == sort(base)

    # Batch size larger than block.
    r1 = unbatch(
        range(100, parallelism=20).iter_batches(
            batch_size=12,
            local_shuffle_buffer_size=25,
        )
    )
    r2 = unbatch(
        range(100, parallelism=20).iter_batches(
            batch_size=12,
            local_shuffle_buffer_size=25,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    assert sort(r1) == sort(base)
    assert sort(r2) == sort(base)

    # Batch size larger than dataset.
    r1 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=200,
            local_shuffle_buffer_size=400,
        )
    )
    r2 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=200,
            local_shuffle_buffer_size=400,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    assert sort(r1) == sort(base)
    assert sort(r2) == sort(base)

    # Drop partial batches.
    r1 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=7,
            local_shuffle_buffer_size=21,
            drop_last=True,
        )
    )
    r2 = unbatch(
        range(100, parallelism=10).iter_batches(
            batch_size=7,
            local_shuffle_buffer_size=21,
            drop_last=True,
        )
    )
    # Check randomness of shuffle.
    assert r1 != r2, (r1, r2)
    assert r1 != base
    assert r2 != base
    # Check content.
    # Check that partial batches were dropped.
    assert len(r1) % 7 == 0
    assert len(r2) % 7 == 0
    tmp_base = base
    if ds_format in ("arrow", "pandas"):
        r1 = [tuple(r.items()) for r in r1]
        r2 = [tuple(r.items()) for r in r2]
        tmp_base = [tuple(r.items()) for r in base]
    assert set(r1) <= set(tmp_base)
    assert set(r2) <= set(tmp_base)

    # Test empty dataset.
    ds = ray.data.from_items([])
    r1 = unbatch(ds.iter_batches(batch_size=2, local_shuffle_buffer_size=10))
    assert len(r1) == 0
    assert r1 == ds.take()


@pytest.mark.parametrize(
    "block_sizes,batch_size,drop_last",
    [
        # Single block, batch smaller than block, keep partial
        ([10], 3, False),
        # Single block, batch smaller than block, drop partial
        ([10], 3, True),
        # Single block, exact division
        ([10], 5, False),
        # Multiple equal-sized blocks, batch doesn't divide evenly, keep partial
        ([5, 5, 5], 7, False),
        # Multiple equal-sized blocks, batch doesn't divide evenly, drop partial
        ([5, 5, 5], 7, True),
        # Multiple unequal-sized blocks, keep partial
        ([1, 5, 10], 4, False),
        # Multiple unequal-sized blocks, drop partial
        ([1, 5, 10], 4, True),
        # Edge case: batch_size = 1
        ([5, 3, 7], 1, False),
        # Edge case: batch larger than total rows
        ([2, 3, 4], 100, False),
        # Exact division across multiple blocks
        ([6, 12, 18], 6, False),
    ],
)
def test_iter_batches_grid(
    ray_start_regular_shared,
    block_sizes,
    batch_size,
    drop_last,
):
    # Tests slicing, batch combining, and partial batch dropping logic over
    # specific dataset, batching, and dropping configurations.
    # Create the dataset with the given block sizes.
    dfs = []
    running_size = 0
    for block_size in block_sizes:
        dfs.append(
            pd.DataFrame(
                {"value": list(range(running_size, running_size + block_size))}
            )
        )
        running_size += block_size
    num_rows = running_size
    ds = ray.data.from_blocks(dfs)

    batches = list(
        ds.iter_batches(
            batch_size=batch_size,
            drop_last=drop_last,
            batch_format="pandas",
        )
    )
    if num_rows % batch_size == 0 or not drop_last:
        # Number of batches should be equal to
        # num_rows / batch_size,  rounded up.
        assert len(batches) == math.ceil(num_rows / batch_size)
        # Concatenated batches should equal the DataFrame
        # representation of the entire dataset.
        assert pd.concat(batches, ignore_index=True).equals(ds.to_pandas())
    else:
        # Number of batches should be equal to
        # num_rows / batch_size, rounded down.
        assert len(batches) == num_rows // batch_size
        # Concatenated batches should equal the DataFrame
        # representation of the dataset with the partial batch
        # remainder sliced off.
        assert pd.concat(batches, ignore_index=True).equals(
            ds.to_pandas()[: batch_size * (num_rows // batch_size)]
        )
    if num_rows % batch_size == 0 or drop_last:
        assert all(len(batch) == batch_size for batch in batches)
    else:
        assert all(len(batch) == batch_size for batch in batches[:-1])
        assert len(batches[-1]) == num_rows % batch_size


@pytest.mark.skipif(
    sys.version_info >= (3, 12), reason="No tensorflow for Python 3.12+"
)
def test_iter_tf_batches_emits_deprecation_warning(ray_start_regular_shared):
    with pytest.warns(DeprecationWarning):
        ray.data.range(1).iter_tf_batches()


@pytest.mark.skipif(
    sys.version_info >= (3, 12), reason="No tensorflow for Python 3.12+"
)
def test_iter_tf_batches(ray_start_regular_shared):
    df1 = pd.DataFrame(
        {"one": [1, 2, 3], "two": [1.0, 2.0, 3.0], "label": [1.0, 2.0, 3.0]}
    )
    df2 = pd.DataFrame(
        {"one": [4, 5, 6], "two": [4.0, 5.0, 6.0], "label": [4.0, 5.0, 6.0]}
    )
    df3 = pd.DataFrame({"one": [7, 8], "two": [7.0, 8.0], "label": [7.0, 8.0]})
    df = pd.concat([df1, df2, df3])
    ds = ray.data.from_pandas([df1, df2, df3])

    num_epochs = 2
    for _ in range(num_epochs):
        iterations = []
        for batch in ds.iter_tf_batches(batch_size=3):
            iterations.append(
                np.stack((batch["one"], batch["two"], batch["label"]), axis=1)
            )
        combined_iterations = np.concatenate(iterations)
        np.testing.assert_array_equal(np.sort(df.values), np.sort(combined_iterations))


@pytest.mark.skipif(
    sys.version_info >= (3, 12), reason="No tensorflow for Python 3.12+"
)
def test_iter_tf_batches_tensor_ds(ray_start_regular_shared):
    arr1 = np.arange(12).reshape((3, 2, 2))
    arr2 = np.arange(12, 24).reshape((3, 2, 2))
    arr = np.concatenate((arr1, arr2))
    ds = ray.data.from_numpy([arr1, arr2])

    num_epochs = 2
    for _ in range(num_epochs):
        iterations = []
        for batch in ds.iter_tf_batches(batch_size=2):
            iterations.append(batch["data"])
        combined_iterations = np.concatenate(iterations)
        np.testing.assert_array_equal(arr, combined_iterations)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
