import pandas as pd
import pytest

import ray
from ray.train.cross_validation import StratifiedKFoldSplitter


def _make_dataframe(n=12):
    ids = list(range(n))
    labels = [i % 3 for i in ids]
    groups = [i // 4 for i in ids]
    ts = pd.date_range("2020-01-01", periods=n, freq="D")
    return pd.DataFrame({"id": ids, "label": labels, "group": groups, "ts": ts})


def test_stratified_kfold_preserves_label_proportions():
    ray.init(num_cpus=2)
    try:
        df = _make_dataframe(9)
        ds = ray.data.from_pandas(df)

        splitter = StratifiedKFoldSplitter(n_splits=3, stratify_column="label", seed=9)
        folds = splitter.split(ds)
        assert len(folds) == 3

        # Check that each validation set has approximately the same class counts
        val_label_counts = []
        for _, val_ds in folds:
            vc = val_ds.to_pandas()["label"].value_counts().sort_index().tolist()
            val_label_counts.append(vc)

        # All folds should have same label distribution for this simple dataset
        assert all(c == val_label_counts[0] for c in val_label_counts)
    finally:
        ray.shutdown()


def test_stratified_kfold_raises_on_small_class_counts():
    # Create a dataset where one class has fewer examples than n_splits
    df = pd.DataFrame({"id": [0, 1, 2, 3], "label": [0, 0, 1, 1]})
    # class 0 and 1 both have 2 examples; request n_splits=3 should error
    ds = ray.data.from_pandas(df)

    with pytest.raises(ValueError):
        StratifiedKFoldSplitter(n_splits=3, stratify_column="label").split(ds)
