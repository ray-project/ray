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
            {},
            dtrain=dtrain,
            evals=evals,
            evals_result=evals_result,
            num_boost_round=remaining_iters,
            xgb_model=starting_model,
        )

    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    trainer = XGBoostTrainer(
        train_loop_per_worker=lambda: xgboost_train_fn_per_worker(
            label_column="target",
            dataset_keys={TRAIN_DATASET_KEY, "valid"},
        ),
        train_loop_config=params,
        scaling_config=scale_config,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )
    result = trainer.fit()
    with pytest.raises(DeprecationWarning):
        XGBoostTrainer.get_model(result.checkpoint)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
