import math

import lightgbm
import pandas as pd
import pytest
from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split

import ray
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
    def lightgbm_train_fn_per_worker(
        config: dict,
        label_column: str,
        dataset_keys: set,
        num_boost_round: int = 10,
    ):
        remaining_iters = num_boost_round
        train_ds_iter = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
        train_df = train_ds_iter.materialize().to_pandas()

        eval_ds_iters = {
            k: ray.train.get_dataset_shard(k)
            for k in dataset_keys
            if k != TRAIN_DATASET_KEY
        }
        eval_dfs = {k: d.materialize().to_pandas() for k, d in eval_ds_iters.items()}

        train_X, train_y = train_df.drop(label_column, axis=1), train_df[label_column]
        train_set = lightgbm.Dataset(train_X, label=train_y)

        # NOTE: Include the training dataset in the evaluation datasets.
        # This allows `train-*` metrics to be calculated and reported.
        valid_sets = [train_set]
        valid_names = [TRAIN_DATASET_KEY]

        for eval_name, eval_df in eval_dfs.items():
            eval_X, eval_y = eval_df.drop(label_column, axis=1), eval_df[label_column]
            valid_sets.append(lightgbm.Dataset(eval_X, label=eval_y))
            valid_names.append(eval_name)

        # Add network params of the worker group to enable distributed training.
        config.update(ray.train.lightgbm.get_network_params())

        lightgbm.train(
            params=config,
            train_set=train_set,
            num_boost_round=remaining_iters,
            valid_sets=valid_sets,
            valid_names=valid_names,
            init_model=None,
            callbacks=[RayTrainReportCallback()],
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
            config={},
            label_column="target",
            dataset_keys={TRAIN_DATASET_KEY, "valid"},
        ),
        train_loop_config=params,
        scaling_config=scale_config,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )
    result = trainer.fit()
    checkpoint = result.checkpoint
    model = RayTrainReportCallback.get_model(checkpoint)
    assert model.pandas_categorical == [["A", "B"]]


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
