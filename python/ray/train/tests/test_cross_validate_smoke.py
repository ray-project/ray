import pandas as pd

import ray
from ray.train.cross_validation import cross_validate
from ray.train.cross_validation.cross_validate import CVResult
from ray.train.cross_validation.kfold.grouped_kfold_splitter import (
    GroupedKFoldSplitter,
)
from ray.train.cross_validation.kfold.stratified_kfold_splitter import (
    StratifiedKFoldSplitter,
)
from ray.train.cross_validation.splitter import Splitter
from ray.train.cross_validation.time_series_splitter import TimeSeriesSplitter


class DummyTrainer:
    def __init__(self, datasets, **kwargs):
        self._train_ds = datasets["train"]
        self._val_ds = datasets["val"]

    def fit(self):
        return type(
            "Result",
            (),
            {
                "metrics": {
                    "train_count": self._train_ds.count(),
                    "val_count": self._val_ds.count(),
                }
            },
        )()


def _run_and_assert_cv(splitter):
    ds = ray.data.from_pandas(pd.DataFrame({"id": list(range(12))}))
    result = cross_validate(DummyTrainer, dataset=ds, splitter=splitter)
    assert isinstance(result, CVResult)
    assert len(result.fold_results) == splitter._n_splits


def test_grouped_kfold_smoke():
    ray.init(num_cpus=2)
    try:
        df = pd.DataFrame(
            {
                "id": list(range(12)),
                "group": [0, 0, 1, 1, 2, 2, 0, 0, 1, 1, 2, 2],
            }
        )
        ds = ray.data.from_pandas(df)
        splitter = GroupedKFoldSplitter(n_splits=3, group_columns=["group"], seed=1)
        result = cross_validate(DummyTrainer, dataset=ds, splitter=splitter)
        assert isinstance(result, CVResult)
        assert len(result.fold_results) == 3
    finally:
        ray.shutdown()


def test_stratified_kfold_smoke():
    ray.init(num_cpus=2)
    try:
        # 3 classes, each with 3 examples -> valid for n_splits=3
        df = pd.DataFrame(
            {
                "id": list(range(9)),
                "label": [0] * 3 + [1] * 3 + [2] * 3,
            }
        )
        ds = ray.data.from_pandas(df)
        splitter = StratifiedKFoldSplitter(n_splits=3, stratify_column="label", seed=1)
        result = cross_validate(DummyTrainer, dataset=ds, splitter=splitter)
        assert isinstance(result, CVResult)
        assert len(result.fold_results) == 3
    finally:
        ray.shutdown()


def test_time_series_smoke():
    ray.init(num_cpus=2)
    try:
        df = pd.DataFrame({"id": list(range(12)), "ts": list(range(12))})
        ds = ray.data.from_pandas(df)
        splitter = TimeSeriesSplitter(n_splits=3, time_column="ts")
        result = cross_validate(DummyTrainer, dataset=ds, splitter=splitter)
        assert isinstance(result, CVResult)
        assert len(result.fold_results) == 3
    finally:
        ray.shutdown()


def test_cross_validate_raises_on_datasets_kwarg():
    ray.init(num_cpus=1)
    try:
        df = pd.DataFrame({"id": list(range(4))})
        ds = ray.data.from_pandas(df)
        splitter = GroupedKFoldSplitter(n_splits=2, group_columns=["id"], seed=1)
        try:
            cross_validate(
                DummyTrainer, dataset=ds, splitter=splitter, datasets={"a": ds}
            )
            raise AssertionError("Expected ValueError when passing `datasets` kwarg")
        except ValueError:
            pass
    finally:
        ray.shutdown()


def test_cross_validate_raises_on_empty_splitter():
    ray.init(num_cpus=1)
    try:

        class EmptySplitter(Splitter):
            def __init__(self):
                super().__init__(n_splits=3)

            def split(self, dataset):
                return []

        df = pd.DataFrame({"id": list(range(4))})
        ds = ray.data.from_pandas(df)
        splitter = EmptySplitter()
        try:
            cross_validate(DummyTrainer, dataset=ds, splitter=splitter)
            raise AssertionError("Expected ValueError when splitter produces no folds")
        except ValueError:
            pass
    finally:
        ray.shutdown()
