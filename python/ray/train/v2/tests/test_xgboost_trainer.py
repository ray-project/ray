import pandas as pd
import pytest
import xgboost
from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split

import ray
from ray.train import ScalingConfig
from ray.train.constants import TRAIN_DATASET_KEY
from ray.train.v2._internal.constants import is_v2_enabled
from ray.train.xgboost import RayTrainReportCallback, XGBoostTrainer

assert is_v2_enabled()


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


scale_config = ScalingConfig(num_workers=2)

data_raw = load_breast_cancer()
dataset_df = pd.DataFrame(data_raw["data"], columns=data_raw["feature_names"])
dataset_df["target"] = data_raw["target"]
train_df, test_df = train_test_split(dataset_df, test_size=0.3)

params = {
    "tree_method": "approx",
    "objective": "binary:logistic",
    "eval_metric": ["logloss", "error"],
}


def test_fit(ray_start_4_cpus):
    @ray.remote
    class ValidationCollector:
        def __init__(self):
            self.validation_scores = {}

        def report(self, rank, logloss, error):
            self.validation_scores[rank] = {
                "logloss": logloss,
                "error": error,
            }

        def get_validation_scores(self):
            return self.validation_scores

    # Ensure all workers have the same model in data parallel training
    # by comparing their validation scores.
    # Comparing xgboost models directly seems less reliable.
    collector = ValidationCollector.remote()

    def xgboost_train_fn_per_worker(
        label_column: str,
        dataset_keys: set,
    ):
        checkpoint = ray.train.get_checkpoint()
        starting_model = None
        remaining_iters = 10
        if checkpoint:
            starting_model = RayTrainReportCallback.get_model(checkpoint)
            starting_iter = starting_model.num_boosted_rounds()
            remaining_iters = remaining_iters - starting_iter

        train_ds_iter = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
        train_df = train_ds_iter.materialize().to_pandas()

        eval_ds_iters = {
            k: ray.train.get_dataset_shard(k)
            for k in dataset_keys
            if k != TRAIN_DATASET_KEY
        }
        eval_dfs = {k: d.materialize().to_pandas() for k, d in eval_ds_iters.items()}

        train_X, train_y = train_df.drop(label_column, axis=1), train_df[label_column]
        dtrain = xgboost.DMatrix(train_X, label=train_y)

        # NOTE: Include the training dataset in the evaluation datasets.
        # This allows `train-*` metrics to be calculated and reported.
        evals = [(dtrain, TRAIN_DATASET_KEY)]

        for eval_name, eval_df in eval_dfs.items():
            eval_X, eval_y = eval_df.drop(label_column, axis=1), eval_df[label_column]
            evals.append((xgboost.DMatrix(eval_X, label=eval_y), eval_name))

        evals_result = {}
        xgboost.train(
            params,
            dtrain=dtrain,
            evals=evals,
            evals_result=evals_result,
            num_boost_round=remaining_iters,
            xgb_model=starting_model,
        )
        collector.report.remote(
            ray.train.get_context().get_world_rank(),
            evals_result["valid"]["logloss"],
            evals_result["valid"]["error"],
        )

    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    trainer = XGBoostTrainer(
        train_loop_per_worker=lambda: xgboost_train_fn_per_worker(
            label_column="target",
            dataset_keys={TRAIN_DATASET_KEY, "valid"},
        ),
        scaling_config=scale_config,
        # Sharding the validation dataset across workers is ok because xgboost allreduces metrics.
        # See https://github.com/dmlc/xgboost/blob/d0135d0f43ff91e738edcbcea54e44b50d336adf/python-package/xgboost/callback.py#L131.
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )
    result = trainer.fit()
    validation_scores = ray.get(collector.get_validation_scores.remote())
    assert validation_scores[0]["logloss"] == pytest.approx(
        validation_scores[1]["logloss"], abs=1e-6
    )
    assert validation_scores[0]["error"] == pytest.approx(
        validation_scores[1]["error"], abs=1e-6
    )
    with pytest.raises(DeprecationWarning):
        XGBoostTrainer.get_model(result.checkpoint)


# TODO: Unit test RayTrainReportCallback


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
