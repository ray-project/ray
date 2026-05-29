import pandas as pd
import pytest

import ray
from ray.train.cross_validation import GroupedKFoldSplitter


def _make_dataframe(n=30):
    ids = list(range(n))
    labels = [i % 3 for i in ids]
    groups = [i // 2 for i in ids]
    ts = pd.date_range("2020-01-01", periods=n, freq="D")
    return pd.DataFrame({"id": ids, "label": labels, "group": groups, "ts": ts})


def test_grouped_kfold_respects_groups():
    ray.init(num_cpus=2)
    try:
        df = _make_dataframe()
        ds = ray.data.from_pandas(df)

        splitter = GroupedKFoldSplitter(n_splits=3, group_columns=["group"], seed=7)
        folds = splitter.split(ds)
        assert len(folds) == 3

        # For each group, ensure it appears in validation of at most one fold
        group_to_fold = {}
        for i, (train_ds, val_ds) in enumerate(folds):
            val_groups = set(val_ds.to_pandas()["group"].tolist())
            for g in val_groups:
                assert g not in group_to_fold
                group_to_fold[g] = i
        assert set(group_to_fold.keys()) == set(df["group"].tolist())
    finally:
        ray.shutdown()


def test_grouped_kfold_constructor_rejects_empty_group_columns():
    with pytest.raises(ValueError):
        GroupedKFoldSplitter(n_splits=3, group_columns=[])


def test_grouped_kfold_raises_if_group_column_missing():
    df = pd.DataFrame({"id": [0, 1, 2], "user": ["a", "b", "c"]})
    ds = ray.data.from_pandas(df)
    splitter = GroupedKFoldSplitter(n_splits=2, group_columns=["group"])
    with pytest.raises(ValueError):
        splitter.split(ds)
