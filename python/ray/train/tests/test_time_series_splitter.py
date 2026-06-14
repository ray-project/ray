import pandas as pd
import pytest

import ray
from ray.train.cross_validation import TimeSeriesSplitter


def _make_df(n=12):
    ids = list(range(n))
    ts = pd.date_range("2020-01-01", periods=n, freq="D")
    return pd.DataFrame({"id": ids, "ts": ts})


def test_time_series_splitter_integration_order_and_gap():
    ray.init(num_cpus=2)
    try:
        df = _make_df(12)
        # shuffle rows to ensure sorter is used
        df_shuf = df.sample(frac=1, random_state=1).reset_index(drop=True)
        ds = ray.data.from_pandas(df_shuf)

        splitter = TimeSeriesSplitter(n_splits=3, time_column="ts", gap=1)
        folds = splitter.split(ds)
        assert len(folds) == 3

        # Verify chronological order: max(train.ts) < min(val.ts) for each fold
        for train_ds, val_ds in folds:
            tdf = train_ds.to_pandas()
            vdf = val_ds.to_pandas()
            assert tdf["ts"].max() < vdf["ts"].min()
    finally:
        ray.shutdown()


def test_time_series_raises_when_time_column_missing():
    ray.init(num_cpus=2)
    try:
        df = _make_df(6)
        ds = ray.data.from_pandas(df.drop(columns=["ts"]))
        splitter = TimeSeriesSplitter(n_splits=2, time_column="ts")
        with pytest.raises(ValueError):
            splitter.split(ds)
    finally:
        ray.shutdown()


def test_time_series_non_uniform_partitions():
    ray.init(num_cpus=2)
    try:
        # 5 blocks of unequal sizes [1, 2, 3, 4, 5]; no time_column so the
        # layout is preserved. Verifies splitting is layout-agnostic.
        df = _make_df(15)
        ds = ray.data.from_pandas(
            [df.iloc[:1], df.iloc[1:3], df.iloc[3:6], df.iloc[6:10], df.iloc[10:]]
        )

        splitter = TimeSeriesSplitter(n_splits=3)
        folds = splitter.split(ds)
        assert len(folds) == 3

        for train_ds, val_ds in folds:
            tdf = train_ds.to_pandas()
            vdf = val_ds.to_pandas()
            assert tdf["ts"].max() < vdf["ts"].min()
    finally:
        ray.shutdown()


def test_time_series_respects_max_train_size():
    ray.init(num_cpus=2)
    try:
        df = _make_df(10)
        ds = ray.data.from_pandas(df)
        splitter = TimeSeriesSplitter(n_splits=2, max_train_size=2)
        folds = splitter.split(ds)
        assert len(folds) == 2

        for train_ds, val_ds in folds:
            tdf = train_ds.to_pandas()
            # Each train set should have at most 2 rows
            assert len(tdf) <= 2
    finally:
        ray.shutdown()
