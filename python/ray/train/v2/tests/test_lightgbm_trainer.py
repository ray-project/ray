import math

import lightgbm
import pandas as pd
import pytest
from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split

import ray
import ray.data
from ray.train import ScalingConfig
from ray.train.constants import TRAIN_DATASET_KEY
from ray.train.lightgbm import LightGBMTrainer, RayTrainReportCallback
from ray.train.v2._internal.constants import is_v2_enabled

assert is_v2_enabled()


@pytest.fixture
def ray_start_6_cpus():
    address_info = ray.init(num_cpus=6)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


scale_config = ScalingConfig(num_workers=2)

data_raw = load_breast_cancer()
dataset_df = pd.DataFrame(data_raw["data"], columns=data_raw["feature_names"])
dataset_df["target"] = data_raw["target"]
train_df, test_df = train_test_split(dataset_df, test_size=0.3)

params = {
    "objective": "binary",
    "metric": ["binary_logloss", "binary_error"],
}


def test_fit_with_categoricals(ray_start_6_cpus):
    @ray.remote
    class ValidationCollector:
        def __init__(self):
            self.validation_scores = {}

        def report(self, rank, binary_logloss, binary_error):
            self.validation_scores[rank] = {
                "binary_logloss": binary_logloss,
                "binary_error": binary_error,
            }

        def get_validation_scores(self):
            return self.validation_scores

    # Ensure all workers have the same model in data parallel training
    # by comparing their validation scores.
    # Comparing lightgbm models directly seems less reliable.
    collector = ValidationCollector.remote()

    def lightgbm_train_fn_per_worker(
        config: dict,
        label_column: str,
        valid_dataset: ray.data.Dataset,
        num_boost_round: int = 10,
    ):
        remaining_iters = num_boost_round
        train_ds_iter = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
        train_df = train_ds_iter.materialize().to_pandas()

        eval_df = valid_dataset.materialize().to_pandas()
        eval_X, eval_y = eval_df.drop(label_column, axis=1), eval_df[label_column]
        valid_set = lightgbm.Dataset(eval_X, label=eval_y)

        train_X, train_y = train_df.drop(label_column, axis=1), train_df[label_column]
        train_set = lightgbm.Dataset(train_X, label=train_y)

        # Add network params of the worker group to enable distributed training.
        config.update(ray.train.lightgbm.get_network_params())

        # Add lightgbm-specific distributed training params.
        config.update(
            {
                "tree_learner": "data_parallel",
                "pre_partition": True,
            }
        )

        booster = lightgbm.train(
            params=config,
            train_set=train_set,
            num_boost_round=remaining_iters,
            # NOTE: Include the training dataset in the evaluation datasets.
            # This allows `train-*` metrics to be calculated and reported.
            valid_sets=[valid_set, train_set],
            valid_names=["valid", TRAIN_DATASET_KEY],
            init_model=None,
            callbacks=[RayTrainReportCallback()],
        )
        collector.report.remote(
            ray.train.get_context().get_world_rank(),
            booster.best_score["valid"]["binary_logloss"],
            booster.best_score["valid"]["binary_error"],
        )

    train_df_with_cat = train_df.copy()
    test_df_with_cat = test_df.copy()
    train_df_with_cat["categorical_column"] = pd.Series(
        (["A", "B"] * math.ceil(len(train_df_with_cat) / 2))[: len(train_df_with_cat)]
    ).astype("category")
    test_df_with_cat["categorical_column"] = pd.Series(
        (["A", "B"] * math.ceil(len(test_df_with_cat) / 2))[: len(test_df_with_cat)]
    ).astype("category")

    train_dataset = ray.data.from_pandas(train_df_with_cat)
    valid_dataset = ray.data.from_pandas(test_df_with_cat)
    trainer = LightGBMTrainer(
        train_loop_per_worker=lambda: lightgbm_train_fn_per_worker(
            config=params,
            label_column="target",
            # Do not shard the validation dataset across workers to ensure all workers compute
            # the same validation score. See https://github.com/microsoft/LightGBM/issues/4392.
            valid_dataset=valid_dataset,
        ),
        scaling_config=scale_config,
        datasets={TRAIN_DATASET_KEY: train_dataset},
    )
    result = trainer.fit()
    checkpoint = result.checkpoint
    model = RayTrainReportCallback.get_model(checkpoint)
    assert model.pandas_categorical == [["A", "B"]]
    validation_scores = ray.get(collector.get_validation_scores.remote())
    assert validation_scores[0]["binary_logloss"] == pytest.approx(
        validation_scores[1]["binary_logloss"], abs=1e-6
    )
    assert validation_scores[0]["binary_error"] == pytest.approx(
        validation_scores[1]["binary_error"], abs=1e-6
    )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
