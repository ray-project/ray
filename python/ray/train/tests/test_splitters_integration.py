import pytest

import ray
from ray.data import from_items
from ray.train.cross_validation.kfold.grouped_kfold_splitter import (
    GroupedKFoldSplitter,
)
from ray.train.cross_validation.kfold.kfold_splitter import KFoldSplitter
from ray.train.cross_validation.kfold.stratified_kfold_splitter import (
    StratifiedKFoldSplitter,
)
from ray.train.cross_validation.time_series_splitter import TimeSeriesSplitter


@pytest.fixture(scope="module", autouse=True)
def init_ray():
    ray.init(ignore_reinit_error=True)
    yield
    ray.shutdown()


def test_time_series_splitter_sort_and_sizes():
    # Create 6 items with timestamps out of order
    items = [
        {"t": 5, "x": "e"},
        {"t": 1, "x": "a"},
        {"t": 3, "x": "c"},
        {"t": 2, "x": "b"},
        {"t": 6, "x": "f"},
        {"t": 4, "x": "d"},
    ]

    ds = from_items(items)

    splitter = TimeSeriesSplitter(n_splits=2)
    folds = splitter.split(ds)

    # Expect 2 folds
    assert len(folds) == 2

    # Using default val_size (n_samples // (n_splits+1)) => 6 // 3 == 2
    train0, val0 = folds[0]
    train1, val1 = folds[1]

    assert train0.count() == 2
    assert val0.count() == 2
    assert train1.count() == 4
    assert val1.count() == 2

    # If we supply time_column, ensure sorting happens and results identical
    ds_unsorted = from_items(items)
    splitter_sorted = TimeSeriesSplitter(n_splits=2, time_column="t")
    folds_sorted = splitter_sorted.split(ds_unsorted)
    # Compare validation contents by timestamps
    v0 = sorted([r["t"] for r in folds_sorted[0][1].take_all()])
    assert v0 == [3, 4] or v0 == [2, 3] or v0 == [2, 3]


def test_stratified_kfold_balances_and_raises():
    # Two classes with 4 samples each
    items = []
    for label in [0, 1]:
        for i in range(4):
            items.append({"id": f"{label}_{i}", "label": label})

    ds = from_items(items)
    splitter = StratifiedKFoldSplitter(n_splits=2, stratify_column="label")
    folds = splitter.split(ds)
    assert len(folds) == 2

    # Each validation set should contain 2 examples of each class
    for _, val in folds:
        df = val.to_pandas()
        counts = df["label"].value_counts().to_dict()
        assert counts.get(0, 0) == 2
        assert counts.get(1, 0) == 2

    # Now test error when a class has fewer than n_splits examples
    items_bad = [{"id": "a", "label": 0}] + [
        {"id": f"1_{i}", "label": 1} for i in range(3)
    ]
    ds_bad = from_items(items_bad)
    splitter_bad = StratifiedKFoldSplitter(n_splits=2, stratify_column="label")
    with pytest.raises(ValueError):
        splitter_bad.split(ds_bad)


def test_kfold_and_grouped_properties():
    # Simple dataset for KFold: unique content per row
    items = [{"i": i, "v": f"x{i}"} for i in range(12)]
    ds = from_items(items)
    ksplit = KFoldSplitter(n_splits=3, seed=42)
    folds = ksplit.split(ds)
    assert len(folds) == 3

    # Union of validation sets should equal the full dataset
    vals = []
    for _, val in folds:
        vals.extend(val.to_pandas()["i"].tolist())

    assert set(vals) == set(range(12))

    # GroupedKFold: ensure groups stay together in same fold
    group_items = [
        {"id": 0, "group": "A"},
        {"id": 1, "group": "A"},
        {"id": 2, "group": "B"},
        {"id": 3, "group": "B"},
        {"id": 4, "group": "C"},
        {"id": 5, "group": "C"},
    ]
    gds = from_items(group_items)
    gsplit = GroupedKFoldSplitter(n_splits=2, group_columns=["group"], seed=1)
    gfolds = gsplit.split(gds)
    # For each group, pick its ids and ensure they appear together in the same validation set
    group_to_ids = {"A": {0, 1}, "B": {2, 3}, "C": {4, 5}}
    for _, val in gfolds:
        ids = set(val.to_pandas()["id"].tolist())
        for g, ids_set in group_to_ids.items():
            # Either the group is fully in this val or not present
            inter = ids & ids_set
            assert (len(inter) == 0) or (inter == ids_set)
