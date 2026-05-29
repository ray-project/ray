import pandas as pd
import pytest

from ray.train.cross_validation.kfold._hashbased import _compute_fold_ids
from ray.train.cross_validation.splitter import _get_noncolliding_column
from ray.train.cross_validation.time_series_splitter import (
    _compute_fold_boundaries,
)


def test_get_noncolliding_column_basic():
    existing = {"fold_id", "other"}
    name = _get_noncolliding_column(existing, "fold_id")
    assert name != "fold_id"
    assert name.startswith("fold_id_")


def test_compute_fold_boundaries_basic():
    folds = _compute_fold_boundaries(
        n_samples=20, n_splits=3, val_size=None, gap=0, max_train_size=None
    )
    # default val_size = n_samples // (n_splits + 1) -> 20 // 4 = 5
    assert len(folds) == 3
    # first fold boundaries
    t0, t1, v0, v1 = folds[0]
    assert v1 - v0 == 5
    assert t1 <= v0


def test_compute_fold_boundaries_errors():
    with pytest.raises(ValueError):
        _compute_fold_boundaries(
            n_samples=3, n_splits=3, val_size=1, gap=0, max_train_size=None
        )

    with pytest.raises(ValueError):
        # not enough rows given gap and val_size
        _compute_fold_boundaries(
            n_samples=5, n_splits=2, val_size=2, gap=2, max_train_size=None
        )


def test_compute_fold_ids_deterministic():
    df = pd.DataFrame({"a": [1, 2, 3, 4]})
    key_frame = df[["a"]]
    ids1 = _compute_fold_ids(key_frame, seed=42, n_splits=3)
    ids2 = _compute_fold_ids(key_frame, seed=42, n_splits=3)
    assert ids1.tolist() == ids2.tolist()
    assert ids1.dtypes == "int32"
    assert all(0 <= v < 3 for v in ids1.tolist())
