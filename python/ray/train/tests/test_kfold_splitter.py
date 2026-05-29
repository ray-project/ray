import pandas as pd

import ray
from ray.train.cross_validation import KFoldSplitter


def _make_dataframe(n=12):
    ids = list(range(n))
    labels = [i % 3 for i in ids]
    groups = [i // 4 for i in ids]
    ts = pd.date_range("2020-01-01", periods=n, freq="D")
    return pd.DataFrame({"id": ids, "label": labels, "group": groups, "ts": ts})


def test_kfold_splitter_basic():
    ray.init(num_cpus=2)
    try:
        df = _make_dataframe(12)
        ds = ray.data.from_pandas(df)

        splitter = KFoldSplitter(n_splits=3, seed=123)
        folds = splitter.split(ds)
        assert len(folds) == 3

        # Collect validation ids and ensure they partition the dataset
        all_val_ids = set()
        seen = set()
        for train_ds, val_ds in folds:
            val_df = val_ds.to_pandas()
            ids = set(val_df["id"].tolist())
            # no overlap
            assert seen.isdisjoint(ids)
            seen.update(ids)
            all_val_ids.update(ids)
        assert all_val_ids == set(df["id"].tolist())
    finally:
        ray.shutdown()
