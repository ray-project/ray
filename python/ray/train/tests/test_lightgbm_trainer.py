import math

import lightgbm as lgbm
import pandas as pd
import pytest
from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split

import ray
from ray import tune
from ray.train import ScalingConfig
from ray.train.constants import TRAIN_DATASET_KEY
from ray.train.lightgbm import LightGBMTrainer


@pytest.fixture
def ray_start_6_cpus():
    address_info = ray.init(num_cpus=6)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_8_cpus():
    address_info = ray.init(num_cpus=8)
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


def get_num_trees(booster: lgbm.Booster) -> int:
    return booster.current_iteration()


def test_fit_with_categoricals(ray_start_6_cpus):
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
        scaling_config=scale_config,
        label_column="target",
        params=params,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )
    result = trainer.fit()
    checkpoint = result.checkpoint
    model = LightGBMTrainer.get_model(checkpoint)
    assert model.pandas_categorical == [["A", "B"]]


def test_resume_from_checkpoint(ray_start_6_cpus, tmpdir):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    trainer = LightGBMTrainer(
        scaling_config=scale_config,
        label_column="target",
        params=params,
        num_boost_round=5,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )
    result = trainer.fit()
    model = LightGBMTrainer.get_model(result.checkpoint)
    assert get_num_trees(model) == 5

    trainer = LightGBMTrainer(
        scaling_config=scale_config,
        label_column="target",
        params=params,
        num_boost_round=10,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        resume_from_checkpoint=result.checkpoint,
    )
    result = trainer.fit()
    checkpoint = result.checkpoint
    model = LightGBMTrainer.get_model(checkpoint)
    assert get_num_trees(model) == 10


@pytest.mark.parametrize(
    "freq_end_expected",
    [
        (4, True, 7),  # 4, 8, 12, 16, 20, 24, 25
        (4, False, 6),  # 4, 8, 12, 16, 20, 24
        (5, True, 5),  # 5, 10, 15, 20, 25
        (0, True, 1),
        (0, False, 0),
    ],
)
def test_checkpoint_freq(ray_start_6_cpus, freq_end_expected):
    freq, end, expected = freq_end_expected

    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    trainer = LightGBMTrainer(
        run_config=ray.train.RunConfig(
            checkpoint_config=ray.train.CheckpointConfig(
                checkpoint_frequency=freq, checkpoint_at_end=end
            )
        ),
        scaling_config=scale_config,
        label_column="target",
        params=params,
        num_boost_round=25,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )
    result = trainer.fit()

    # Assert number of checkpoints
    assert len(result.best_checkpoints) == expected, str(
        [(metrics["training_iteration"], cp) for cp, metrics in result.best_checkpoints]
    )

    # Assert checkpoint numbers are increasing
    cp_paths = [cp.path for cp, _ in result.best_checkpoints]
    assert cp_paths == sorted(cp_paths), str(cp_paths)


def test_tune(ray_start_8_cpus):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    trainer = LightGBMTrainer(
        scaling_config=ScalingConfig(num_workers=2, resources_per_worker={"CPU": 1}),
        label_column="target",
        params={**params, **{"max_depth": 1}},
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )

    tune.run(
        trainer.as_trainable(),
        config={"params": {"max_depth": tune.randint(2, 4)}},
        num_samples=2,
    )

    # Make sure original Trainer is not affected.
    assert trainer.params["max_depth"] == 1


def test_validation(ray_start_6_cpus):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    with pytest.raises(KeyError, match=TRAIN_DATASET_KEY):
        LightGBMTrainer(
            scaling_config=ScalingConfig(num_workers=2),
            label_column="target",
            params=params,
            datasets={"valid": valid_dataset},
        )
    with pytest.raises(KeyError, match="dmatrix_params"):
        LightGBMTrainer(
            scaling_config=ScalingConfig(num_workers=2),
            label_column="target",
            params=params,
            dmatrix_params={"data": {}},
            datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        )


def test_default_parameters_default():
    trainer = LightGBMTrainer(
        datasets={TRAIN_DATASET_KEY: ray.data.from_pandas(train_df)},
        label_column="target",
        params=params,
    )
    assert trainer._ray_params.cpus_per_actor == 2


def test_default_parameters_scaling_config():
    trainer = LightGBMTrainer(
        datasets={TRAIN_DATASET_KEY: ray.data.from_pandas(train_df)},
        label_column="target",
        params=params,
        scaling_config=ScalingConfig(resources_per_worker={"CPU": 4}),
    )
    assert trainer._ray_params.cpus_per_actor == 4


def test_lightgbm_trainer_resources():
    """`trainer_resources` is not allowed in the scaling config"""
    with pytest.raises(ValueError):
        LightGBMTrainer._validate_scaling_config(
            ScalingConfig(trainer_resources={"something": 1})
        )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
