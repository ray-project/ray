import statistics

import pandas as pd

import ray
from ray.train.cross_validation import cross_validate
from ray.train.cross_validation.cross_validate import CVResult
from ray.train.cross_validation.kfold.kfold_splitter import KFoldSplitter


class DummyTrainer:
    def __init__(self, datasets, **kwargs):
        self._train_ds = datasets["train"]
        self._val_ds = datasets["val"]

    def fit(self):
        train_count = self._train_ds.count()
        val_count = self._val_ds.count()
        return type(
            "Result",
            (),
            {"metrics": {"train_count": train_count, "val_count": val_count}},
        )()


def test_cross_validate_runs_and_aggregates_metrics():
    ray.init(num_cpus=2)
    try:
        df = pd.DataFrame({"id": list(range(12))})
        ds = ray.data.from_pandas(df)

        splitter = KFoldSplitter(n_splits=3, seed=1)
        result = cross_validate(DummyTrainer, dataset=ds, splitter=splitter)

        assert isinstance(result, CVResult)
        assert len(result.fold_results) == 3

        # Aggregated metrics should include train_count and val_count
        assert "train_count" in result.mean_metrics
        assert "val_count" in result.mean_metrics

        # Verify aggregated mean/std equal the per-fold metrics
        train_vals = [res.metrics["train_count"] for res in result.fold_results]
        val_vals = [res.metrics["val_count"] for res in result.fold_results]

        expected_mean_train = statistics.mean(train_vals)
        expected_mean_val = statistics.mean(val_vals)
        expected_std_train = (
            statistics.stdev(train_vals) if len(train_vals) > 1 else 0.0
        )
        expected_std_val = statistics.stdev(val_vals) if len(val_vals) > 1 else 0.0

        assert result.mean_metrics["train_count"] == expected_mean_train
        assert result.mean_metrics["val_count"] == expected_mean_val
        assert result.std_metrics["train_count"] == expected_std_train
        assert result.std_metrics["val_count"] == expected_std_val
    finally:
        ray.shutdown()
