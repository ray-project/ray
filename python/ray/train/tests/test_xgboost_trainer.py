from unittest import mock

import pandas as pd
import pytest
import xgboost as xgb
from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split

import ray
from ray import train, tune
from ray.train import ScalingConfig
from ray.train.constants import TRAIN_DATASET_KEY
from ray.train.xgboost import RayTrainReportCallback, XGBoostTrainer


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
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
    "tree_method": "approx",
    "objective": "binary:logistic",
    "eval_metric": ["logloss", "error"],
}


def test_fit(ray_start_4_cpus):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    trainer = XGBoostTrainer(
        scaling_config=scale_config,
        label_column="target",
        params=params,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )
    trainer.fit()


class ScalingConfigAssertingXGBoostTrainer(XGBoostTrainer):
    def training_loop(self) -> None:
        pgf = train.get_context().get_trial_resources()
        assert pgf.strategy == "SPREAD"
        return super().training_loop()


def test_fit_with_advanced_scaling_config(ray_start_4_cpus):
    """Ensure that extra ScalingConfig arguments are respected."""
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    trainer = ScalingConfigAssertingXGBoostTrainer(
        scaling_config=ScalingConfig(
            num_workers=2,
            placement_strategy="SPREAD",
        ),
        label_column="target",
        params=params,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )
    trainer.fit()


def test_resume_from_checkpoint(ray_start_4_cpus, tmpdir):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    trainer = XGBoostTrainer(
        scaling_config=scale_config,
        label_column="target",
        params=params,
        num_boost_round=5,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )
    result = trainer.fit()
    checkpoint = result.checkpoint
    xgb_model = XGBoostTrainer.get_model(checkpoint)
    assert xgb_model.num_boosted_rounds() == 5

    trainer = XGBoostTrainer(
        scaling_config=scale_config,
        label_column="target",
        params=params,
        num_boost_round=10,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        resume_from_checkpoint=checkpoint,
    )
    result = trainer.fit()
    xgb_model = XGBoostTrainer.get_model(result.checkpoint)
    assert xgb_model.num_boosted_rounds() == 10


def test_external_memory_basic(ray_start_4_cpus):
    """Test V1 XGBoost Trainer with external memory enabled."""
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    
    # Use hist tree method (required for external memory)
    external_memory_params = {
        "tree_method": "hist",  # Required for external memory
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
    }
    
    trainer = XGBoostTrainer(
        scaling_config=scale_config,
        label_column="target",
        params=external_memory_params,
        num_boost_round=10,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        use_external_memory=True,
        external_memory_cache_dir="/tmp/xgboost_v1_test_cache",
        external_memory_device="cpu",
        external_memory_batch_size=1000,
    )
    
    result = trainer.fit()
    
    # Verify results
    assert result.checkpoint is not None
    xgb_model = XGBoostTrainer.get_model(result.checkpoint)
    assert xgb_model.num_boosted_rounds() == 10
    
    # Verify external memory configuration
    assert trainer.is_external_memory_enabled()
    config = trainer.get_external_memory_config()
    assert config["use_external_memory"] is True
    assert config["cache_dir"] == "/tmp/xgboost_v1_test_cache"
    assert config["device"] == "cpu"
    assert config["batch_size"] == 1000


def test_external_memory_auto_configuration(ray_start_4_cpus):
    """Test V1 XGBoost Trainer with automatic external memory configuration."""
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    
    # Use hist tree method (required for external memory)
    external_memory_params = {
        "tree_method": "hist",  # Required for external memory
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
    }
    
    trainer = XGBoostTrainer(
        scaling_config=scale_config,
        label_column="target",
        params=external_memory_params,
        num_boost_round=10,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        use_external_memory=True,
        # Let the trainer auto-select cache directory and batch size
    )
    
    result = trainer.fit()
    
    # Verify results
    assert result.checkpoint is not None
    xgb_model = XGBoostTrainer.get_model(result.checkpoint)
    assert xgb_model.num_boosted_rounds() == 10
    
    # Verify external memory is enabled
    assert trainer.is_external_memory_enabled()


def test_external_memory_gpu(ray_start_8_cpus):
    """Test V1 XGBoost Trainer with GPU external memory."""
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    
    # Use hist tree method (required for external memory)
    external_memory_params = {
        "tree_method": "hist",  # Required for external memory
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
    }
    
    trainer = XGBoostTrainer(
        scaling_config=ScalingConfig(num_workers=2, use_gpu=True),
        label_column="target",
        params=external_memory_params,
        num_boost_round=10,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        use_external_memory=True,
        external_memory_device="cuda",
        external_memory_batch_size=5000,  # Smaller batch size for GPU
    )
    
    result = trainer.fit()
    
    # Verify results
    assert result.checkpoint is not None
    xgb_model = XGBoostTrainer.get_model(result.checkpoint)
    assert xgb_model.num_boosted_rounds() == 10
    
    # Verify GPU external memory configuration
    config = trainer.get_external_memory_config()
    assert config["device"] == "cuda"


def test_external_memory_utilities(ray_start_4_cpus):
    """Test V1 XGBoost Trainer external memory utility methods."""
    # Test GPU setup method
    gpu_setup_result = XGBoostTrainer.setup_gpu_external_memory()
    # This should return False on CPU-only systems, True on GPU systems
    assert isinstance(gpu_setup_result, bool)


def test_external_memory_with_large_dataset(ray_start_8_cpus):
    """Test V1 XGBoost Trainer with a larger dataset to verify external memory benefits."""
    # Create a larger dataset
    large_train_df = pd.concat([train_df] * 10, ignore_index=True)
    large_test_df = pd.concat([test_df] * 5, ignore_index=True)
    
    large_train_dataset = ray.data.from_pandas(large_train_df)
    large_valid_dataset = ray.data.from_pandas(large_test_df)
    
    # Use hist tree method (required for external memory)
    external_memory_params = {
        "tree_method": "hist",  # Required for external memory
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
        "max_depth": 3,  # Limit depth for faster training
        "eta": 0.1,
    }
    
    trainer = XGBoostTrainer(
        scaling_config=ScalingConfig(num_workers=4),
        label_column="target",
        params=external_memory_params,
        num_boost_round=5,  # Fewer rounds for faster testing
        datasets={TRAIN_DATASET_KEY: large_train_dataset, "valid": large_valid_dataset},
        use_external_memory=True,
        external_memory_cache_dir="/tmp/xgboost_large_test_cache",
        external_memory_batch_size=2000,
    )
    
    result = trainer.fit()
    
    # Verify results
    assert result.checkpoint is not None
    xgb_model = XGBoostTrainer.get_model(result.checkpoint)
    assert xgb_model.num_boosted_rounds() == 5
    
    # Verify external memory configuration
    assert trainer.is_external_memory_enabled()
    config = trainer.get_external_memory_config()
    assert config["use_external_memory"] is True
    assert config["batch_size"] == 2000


def test_external_memory_backward_compatibility(ray_start_4_cpus):
    """Test that V1 XGBoost Trainer maintains backward compatibility when external memory is disabled."""
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    
    # Use standard parameters (no external memory)
    standard_params = {
        "tree_method": "approx",  # Can use approx for standard DMatrix
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
    }
    
    trainer = XGBoostTrainer(
        scaling_config=scale_config,
        label_column="target",
        params=standard_params,
        num_boost_round=10,
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        # External memory disabled by default
    )
    
    result = trainer.fit()
    
    # Verify results
    assert result.checkpoint is not None
    xgb_model = XGBoostTrainer.get_model(result.checkpoint)
    assert xgb_model.num_boosted_rounds() == 10
    
    # Verify external memory is disabled
    assert not trainer.is_external_memory_enabled()
    config = trainer.get_external_memory_config()
    assert config["use_external_memory"] is False


@pytest.mark.parametrize(
    "freq_end_expected",
    [
        # With num_boost_round=25 with 0 indexing, the checkpoints will be at:
        (4, True, 7),  # 3, 7, 11, 15, 19, 23, 24 (end)
        (4, False, 6),  # 3, 7, 11, 15, 19, 23
        (5, True, 5),  # 4, 9, 14, 19, 24
        (0, True, 1),  # 24 (end)
        (0, False, 0),
    ],
)
def test_checkpoint_freq(ray_start_4_cpus, freq_end_expected):
    freq, end, expected = freq_end_expected

    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    trainer = XGBoostTrainer(
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


@pytest.mark.parametrize("rank", [None, 0, 1])
def test_checkpoint_only_on_rank0(rank):
    """Tests that the callback only reports checkpoints on rank 0,
    or if the rank is not available (Tune usage)."""
    callback = RayTrainReportCallback(frequency=2, checkpoint_at_end=True)

    booster = mock.MagicMock()

    with mock.patch("ray.train.get_context") as mock_get_context:
        mock_context = mock.MagicMock()
        mock_context.get_world_rank.return_value = rank
        mock_get_context.return_value = mock_context

        with callback._get_checkpoint(booster) as checkpoint:
            if rank in (0, None):
                assert checkpoint
            else:
                assert not checkpoint


def test_tune(ray_start_8_cpus):
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)
    trainer = XGBoostTrainer(
        scaling_config=scale_config,
        label_column="target",
        params={**params, "max_depth": 1},
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )

    tuner = tune.Tuner(
        trainer,
        param_space={"params": {"max_depth": tune.grid_search([2, 4])}},
    )
    results = tuner.fit()
    assert sorted([r.config["params"]["max_depth"] for r in results]) == [2, 4]


def test_validation(ray_start_4_cpus):
    valid_dataset = ray.data.from_pandas(test_df)
    with pytest.raises(ValueError, match=TRAIN_DATASET_KEY):
        XGBoostTrainer(
            scaling_config=ScalingConfig(num_workers=2),
            label_column="target",
            params=params,
            datasets={"valid": valid_dataset},
        )

    with pytest.raises(ValueError, match="label_column"):
        XGBoostTrainer(
            scaling_config=ScalingConfig(num_workers=2),
            datasets={"train": valid_dataset},
        )


def test_callback_get_model(tmp_path):
    custom_filename = "custom.json"

    bst = xgb.train(
        params,
        dtrain=xgb.DMatrix(train_df, label=train_df["target"]),
        num_boost_round=1,
    )
    bst.save_model(tmp_path.joinpath(custom_filename).as_posix())
    checkpoint = train.Checkpoint.from_directory(tmp_path.as_posix())

    RayTrainReportCallback.get_model(checkpoint, filename=custom_filename)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
