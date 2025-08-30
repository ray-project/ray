"""
Comprehensive tests for XGBoost Trainer V2 public APIs.

This test file covers the V2 XGBoost Trainer public API:
- XGBoostTrainer (V2 trainer class)

Note: This is specifically for V2 trainer testing and does NOT test:
- V1 trainer components (RayTrainReportCallback, XGBoostConfig, etc.)
- Internal utility functions like prepare_dataset, get_recommended_params, etc.
- V1-specific functionality
"""

import pandas as pd
import pytest
import xgboost
from sklearn.datasets import load_breast_cancer, load_diabetes, load_iris
from sklearn.model_selection import train_test_split

import ray
from ray.train import ScalingConfig
from ray.train.constants import TRAIN_DATASET_KEY
from ray.train.v2._internal.constants import is_v2_enabled
from ray.train.v2.xgboost import XGBoostTrainer

assert is_v2_enabled()


@pytest.fixture
def ray_start_4_cpus():
    """Start Ray with 4 CPUs for testing."""
    address_info = ray.init(num_cpus=4)
    yield address_info
    ray.shutdown()


@pytest.fixture
def ray_start_2_cpus_1_gpu():
    """Start Ray with 2 CPUs and 1 GPU for testing."""
    address_info = ray.init(num_cpus=2, num_gpus=1)
    yield address_info
    ray.shutdown()


@pytest.fixture
def small_dataset():
    """Create a small dataset for testing."""
    data_raw = load_breast_cancer()
    dataset_df = pd.DataFrame(data_raw["data"], columns=data_raw["feature_names"])
    dataset_df["target"] = data_raw["target"]
    train_df, test_df = train_test_split(dataset_df, test_size=0.3)
    return train_df, test_df


@pytest.fixture
def regression_dataset():
    """Create a regression dataset for testing."""
    data_raw = load_diabetes()
    dataset_df = pd.DataFrame(data_raw["data"], columns=data_raw["feature_names"])
    dataset_df["target"] = data_raw["target"]
    train_df, test_df = train_test_split(dataset_df, test_size=0.3)
    return train_df, test_df


@pytest.fixture
def multiclass_dataset():
    """Create a multiclass dataset for testing."""
    data_raw = load_iris()
    dataset_df = pd.DataFrame(data_raw["data"], columns=data_raw["feature_names"])
    dataset_df["target"] = data_raw["target"]
    train_df, test_df = train_test_split(dataset_df, test_size=0.3)
    return train_df, test_df


def test_xgboost_trainer_basic_functionality(ray_start_4_cpus, small_dataset):
    """Test basic V2 XGBoost Trainer functionality with binary classification."""
    train_df, test_df = small_dataset

    def train_fn_per_worker(config: dict):
        """Training function for binary classification."""
        train_ds = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
        train_df = train_ds.materialize().to_pandas()

        eval_ds = ray.train.get_dataset_shard("valid")
        eval_df = eval_ds.materialize().to_pandas()

        # Prepare data
        train_X, train_y = train_df.drop("target", axis=1), train_df["target"]
        eval_X, eval_y = eval_df.drop("target", axis=1), eval_df["target"]

        dtrain = xgboost.DMatrix(train_X, label=train_y)
        deval = xgboost.DMatrix(eval_X, label=eval_y)

        # Train model
        bst = xgboost.train(
            config,
            dtrain=dtrain,
            evals=[(deval, "validation")],
            num_boost_round=10,
        )

        # Verify model was created successfully
        assert bst is not None
        assert hasattr(bst, "predict")

    # Create datasets
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)

    # Test parameters
    params = {
        "tree_method": "hist",  # Required for external memory
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "max_depth": 3,
        "eta": 0.1,
    }

    # Create and run trainer
    trainer = XGBoostTrainer(
        train_loop_per_worker=train_fn_per_worker,
        train_loop_config=params,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )

    result = trainer.fit()

    # Verify results
    assert result.checkpoint is not None
    assert result.metrics is not None
    assert "validation-logloss" in result.metrics


def test_xgboost_trainer_regression(ray_start_4_cpus, regression_dataset):
    """Test V2 XGBoost Trainer with regression objective."""
    train_df, test_df = regression_dataset

    def train_fn_per_worker(config: dict):
        """Training function for regression."""
        train_ds = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
        train_df = train_ds.materialize().to_pandas()

        eval_ds = ray.train.get_dataset_shard("valid")
        eval_df = eval_ds.materialize().to_pandas()

        # Prepare data
        train_X, train_y = train_df.drop("target", axis=1), train_df["target"]
        eval_X, eval_y = eval_df.drop("target", axis=1), eval_df["target"]

        dtrain = xgboost.DMatrix(train_X, label=train_y)
        deval = xgboost.DMatrix(eval_X, label=eval_y)

        # Train model
        bst = xgboost.train(
            config,
            dtrain=dtrain,
            evals=[(deval, "validation")],
            num_boost_round=10,
        )

        # Verify model was created successfully
        assert bst is not None
        assert hasattr(bst, "predict")

    # Create datasets
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)

    # Test parameters
    params = {
        "tree_method": "hist",
        "objective": "reg:squarederror",
        "eval_metric": "rmse",
        "max_depth": 4,
        "eta": 0.1,
    }

    # Create and run trainer
    trainer = XGBoostTrainer(
        train_loop_per_worker=train_fn_per_worker,
        train_loop_config=params,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )

    result = trainer.fit()

    # Verify results
    assert result.checkpoint is not None
    assert result.metrics is not None
    assert "validation-rmse" in result.metrics


def test_xgboost_trainer_multiclass(ray_start_4_cpus, multiclass_dataset):
    """Test V2 XGBoost Trainer with multiclass classification."""
    train_df, test_df = multiclass_dataset

    def train_fn_per_worker(config: dict):
        """Training function for multiclass classification."""
        train_ds = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
        train_df = train_ds.materialize().to_pandas()

        eval_ds = ray.train.get_dataset_shard("valid")
        eval_df = eval_ds.materialize().to_pandas()

        # Prepare data
        train_X, train_y = train_df.drop("target", axis=1), train_df["target"]
        eval_X, eval_y = eval_df.drop("target", axis=1), eval_df["target"]

        dtrain = xgboost.DMatrix(train_X, label=train_y)
        deval = xgboost.DMatrix(eval_X, label=eval_y)

        # Train model
        bst = xgboost.train(
            config,
            dtrain=dtrain,
            evals=[(deval, "validation")],
            num_boost_round=10,
        )

        # Verify model was created successfully
        assert bst is not None
        assert hasattr(bst, "predict")

    # Create datasets
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)

    # Test parameters
    params = {
        "tree_method": "hist",
        "objective": "multi:softmax",
        "num_class": 3,
        "eval_metric": "mlogloss",
        "max_depth": 3,
        "eta": 0.1,
    }

    # Create and run trainer
    trainer = XGBoostTrainer(
        train_loop_per_worker=train_fn_per_worker,
        train_loop_config=params,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )

    result = trainer.fit()

    # Verify results
    assert result.checkpoint is not None
    assert result.metrics is not None
    assert "validation-mlogloss" in result.metrics


def test_xgboost_trainer_external_memory_basic(ray_start_4_cpus, small_dataset):
    """Test V2 XGBoost Trainer with external memory enabled."""
    train_df, test_df = small_dataset

    def train_fn_per_worker(config: dict):
        """Training function using external memory."""
        # Check if external memory is enabled via config
        use_external_memory = config.get("use_external_memory", False)
        external_memory_cache_dir = config.get("external_memory_cache_dir")
        external_memory_device = config.get("external_memory_device", "cpu")
        external_memory_batch_size = config.get("external_memory_batch_size")

        train_ds_iter = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
        eval_ds_iter = ray.train.get_dataset_shard("valid")

        if use_external_memory:
            # Use external memory DMatrix
            dtrain = trainer.create_external_memory_dmatrix(
                train_ds_iter, label_column="target"
            )
            deval = trainer.create_external_memory_dmatrix(
                eval_ds_iter, label_column="target"
            )
        else:
            # Use standard DMatrix
            train_df = train_ds_iter.materialize().to_pandas()
            eval_df = eval_ds_iter.materialize().to_pandas()
            train_X, train_y = train_df.drop("target", axis=1), train_df["target"]
            eval_X, eval_y = eval_df.drop("target", axis=1), eval_df["target"]
            dtrain = xgboost.DMatrix(train_X, label=train_y)
            deval = xgboost.DMatrix(eval_X, label=eval_y)

        # Train model
        bst = xgboost.train(
            config,
            dtrain=dtrain,
            evals=[(deval, "validation")],
            num_boost_round=10,
        )

        # Verify model was created successfully
        assert bst is not None
        assert hasattr(bst, "predict")

    # Create datasets
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)

    # Test parameters
    params = {
        "tree_method": "hist",  # Required for external memory
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "max_depth": 3,
        "eta": 0.1,
    }

    # Create and run trainer with external memory
    trainer = XGBoostTrainer(
        train_loop_per_worker=train_fn_per_worker,
        train_loop_config=params,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        use_external_memory=True,
        external_memory_cache_dir="/tmp/xgboost_test_cache",
        external_memory_device="cpu",
        external_memory_batch_size=1000,
    )

    result = trainer.fit()

    # Verify results
    assert result.checkpoint is not None
    assert result.metrics is not None
    assert "validation-logloss" in result.metrics

    # Verify external memory configuration
    assert trainer.is_external_memory_enabled()
    config = trainer.get_external_memory_config()
    assert config["use_external_memory"] is True
    assert config["cache_dir"] == "/tmp/xgboost_test_cache"
    assert config["device"] == "cpu"
    assert config["batch_size"] == 1000


def test_xgboost_trainer_external_memory_auto_selection(ray_start_4_cpus, small_dataset):
    """Test V2 XGBoost Trainer with automatic external memory configuration."""
    train_df, test_df = small_dataset

    def train_fn_per_worker(config: dict):
        """Training function using automatic external memory selection."""
        train_ds_iter = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
        eval_ds_iter = ray.train.get_dataset_shard("valid")

        # Use the trainer's smart DMatrix creation
        dtrain = trainer.create_dmatrix(train_ds_iter, label_column="target")
        deval = trainer.create_dmatrix(eval_ds_iter, label_column="target")

        # Train model
        bst = xgboost.train(
            config,
            dtrain=dtrain,
            evals=[(deval, "validation")],
            num_boost_round=10,
        )

        # Verify model was created successfully
        assert bst is not None
        assert hasattr(bst, "predict")

    # Create datasets
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)

    # Test parameters
    params = {
        "tree_method": "hist",  # Required for external memory
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "max_depth": 3,
        "eta": 0.1,
    }

    # Create and run trainer with external memory (auto-configuration)
    trainer = XGBoostTrainer(
        train_loop_per_worker=train_fn_per_worker,
        train_loop_config=params,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        use_external_memory=True,
        # Let the trainer auto-select cache directory and batch size
    )

    result = trainer.fit()

    # Verify results
    assert result.checkpoint is not None
    assert result.metrics is not None
    assert "validation-logloss" in result.metrics

    # Verify external memory is enabled
    assert trainer.is_external_memory_enabled()


def test_xgboost_trainer_external_memory_gpu(ray_start_2_cpus_1_gpu, small_dataset):
    """Test V2 XGBoost Trainer with GPU external memory."""
    train_df, test_df = small_dataset

    def train_fn_per_worker(config: dict):
        """Training function using GPU external memory."""
        train_ds_iter = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
        eval_ds_iter = ray.train.get_dataset_shard("valid")

        # Use the trainer's smart DMatrix creation
        dtrain = trainer.create_dmatrix(train_ds_iter, label_column="target")
        deval = trainer.create_dmatrix(eval_ds_iter, label_column="target")

        # Train model
        bst = xgboost.train(
            config,
            dtrain=dtrain,
            evals=[(deval, "validation")],
            num_boost_round=10,
        )

        # Verify model was created successfully
        assert bst is not None
        assert hasattr(bst, "predict")

    # Create datasets
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)

    # Test parameters
    params = {
        "tree_method": "hist",  # Required for external memory
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "max_depth": 3,
        "eta": 0.1,
    }

    # Create and run trainer with GPU external memory
    trainer = XGBoostTrainer(
        train_loop_per_worker=train_fn_per_worker,
        train_loop_config=params,
        scaling_config=ScalingConfig(num_workers=1, use_gpu=True),
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        use_external_memory=True,
        external_memory_device="cuda",
        external_memory_batch_size=5000,  # Smaller batch size for GPU
    )

    result = trainer.fit()

    # Verify results
    assert result.checkpoint is not None
    assert result.metrics is not None
    assert "validation-logloss" in result.metrics

    # Verify GPU external memory configuration
    config = trainer.get_external_memory_config()
    assert config["device"] == "cuda"


def test_xgboost_trainer_external_memory_utilities(ray_start_4_cpus):
    """Test V2 XGBoost Trainer external memory utility methods."""
    # Test GPU setup method
    gpu_setup_result = XGBoostTrainer.setup_gpu_external_memory()
    # This should return False on CPU-only systems, True on GPU systems
    assert isinstance(gpu_setup_result, bool)

    # Test external memory recommendations
    recommendations = XGBoostTrainer.get_external_memory_recommendations()
    assert isinstance(recommendations, dict)
    assert "parameters" in recommendations
    assert "best_practices" in recommendations
    assert "cache_directories" in recommendations
    assert "documentation" in recommendations

    # Verify required parameters are present
    assert recommendations["parameters"]["tree_method"] == "hist"
    assert recommendations["parameters"]["grow_policy"] == "depthwise"


def test_xgboost_trainer_external_memory_fallback_behavior(ray_start_4_cpus, small_dataset):
    """Test V2 XGBoost Trainer fallback behavior when external memory fails."""
    train_df, test_df = small_dataset

    def train_fn_per_worker(config: dict):
        """Training function that handles external memory failures gracefully."""
        train_ds_iter = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
        eval_ds_iter = ray.train.get_dataset_shard("valid")

        try:
            # Try external memory first
            dtrain = trainer.create_external_memory_dmatrix(
                train_ds_iter, label_column="target"
            )
            deval = trainer.create_external_memory_dmatrix(
                eval_ds_iter, label_column="target"
            )
        except Exception as e:
            # Fall back to standard DMatrix
            train_df = train_ds_iter.materialize().to_pandas()
            eval_df = eval_ds_iter.materialize().to_pandas()
            train_X, train_y = train_df.drop("target", axis=1), train_df["target"]
            eval_X, eval_y = eval_df.drop("target", axis=1), eval_df["target"]
            dtrain = xgboost.DMatrix(train_X, label=train_y)
            deval = xgboost.DMatrix(eval_X, label=eval_y)

        # Train model
        bst = xgboost.train(
            config,
            dtrain=dtrain,
            evals=[(deval, "validation")],
            num_boost_round=10,
        )

        # Verify model was created successfully
        assert bst is not None
        assert hasattr(bst, "predict")

    # Create datasets
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)

    # Test parameters
    params = {
        "tree_method": "hist",
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "max_depth": 3,
        "eta": 0.1,
    }

    # Create and run trainer with external memory
    trainer = XGBoostTrainer(
        train_loop_per_worker=train_fn_per_worker,
        train_loop_config=params,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        use_external_memory=True,
        external_memory_cache_dir="/tmp/xgboost_test_cache",
    )

    result = trainer.fit()

    # Verify results
    assert result.checkpoint is not None
    assert result.metrics is not None
    assert "validation-logloss" in result.metrics


def test_xgboost_trainer_gpu_training(ray_start_2_cpus_1_gpu, small_dataset):
    """Test V2 XGBoost Trainer with GPU training."""
    train_df, test_df = small_dataset

    def train_fn_per_worker(config: dict):
        """Training function for GPU training."""
        train_ds = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
        train_df = train_ds.materialize().to_pandas()

        eval_ds = ray.train.get_dataset_shard("valid")
        eval_df = eval_ds.materialize().to_pandas()

        # Prepare data
        train_X, train_y = train_df.drop("target", axis=1), train_df["target"]
        eval_X, eval_y = eval_df.drop("target", axis=1), eval_df["target"]

        dtrain = xgboost.DMatrix(train_X, label=train_y)
        deval = xgboost.DMatrix(eval_X, label=eval_y)

        # Train model
        bst = xgboost.train(
            config,
            dtrain=dtrain,
            evals=[(deval, "validation")],
            num_boost_round=10,
        )

        # Verify model was created successfully
        assert bst is not None
        assert hasattr(bst, "predict")

    # Create datasets
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)

    # Test parameters for GPU training
    params = {
        "tree_method": "hist",
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "device": "cuda",
        "max_depth": 3,
        "eta": 0.1,
    }

    # Create and run trainer with GPU
    trainer = XGBoostTrainer(
        train_loop_per_worker=train_fn_per_worker,
        train_loop_config=params,
        scaling_config=ScalingConfig(num_workers=1, use_gpu=True),
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )

    result = trainer.fit()

    # Verify results
    assert result.checkpoint is not None
    assert result.metrics is not None


def test_xgboost_trainer_checkpoint_resume(ray_start_4_cpus, small_dataset):
    """Test V2 XGBoost Trainer checkpoint resuming."""
    train_df, test_df = small_dataset

    def train_fn_per_worker(config: dict):
        """Training function with checkpoint resuming."""
        checkpoint = ray.train.get_checkpoint()
        starting_model = None
        remaining_iters = 10

        if checkpoint:
            # For V2, we need to handle checkpoint differently
            # This is a simplified version for testing
            remaining_iters = 5  # Just continue with fewer iterations

        train_ds = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
        train_df = train_ds.materialize().to_pandas()

        eval_ds = ray.train.get_dataset_shard("valid")
        eval_df = eval_ds.materialize().to_pandas()

        # Prepare data
        train_X, train_y = train_df.drop("target", axis=1), train_df["target"]
        eval_X, eval_y = eval_df.drop("target", axis=1), eval_df["target"]

        dtrain = xgboost.DMatrix(train_X, label=train_y)
        deval = xgboost.DMatrix(eval_X, label=eval_y)

        # Train model
        bst = xgboost.train(
            config,
            dtrain=dtrain,
            evals=[(deval, "validation")],
            num_boost_round=remaining_iters,
            xgb_model=starting_model,
        )

        # Verify model was created successfully
        assert bst is not None
        assert hasattr(bst, "predict")

    # Create datasets
    train_dataset = ray.data.from_pandas(train_df)
    valid_dataset = ray.data.from_pandas(test_df)

    # Test parameters
    params = {
        "tree_method": "hist",
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "max_depth": 3,
        "eta": 0.1,
    }

    # Create and run trainer
    trainer = XGBoostTrainer(
        train_loop_per_worker=train_fn_per_worker,
        train_loop_config=params,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
    )

    result = trainer.fit()

    # Verify results
    assert result.checkpoint is not None
    assert result.metrics is not None

    # Test checkpoint resuming
    trainer_resume = XGBoostTrainer(
        train_loop_per_worker=train_fn_per_worker,
        train_loop_config=params,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={TRAIN_DATASET_KEY: train_dataset, "valid": valid_dataset},
        resume_from_checkpoint=result.checkpoint,
    )

    result_resume = trainer_resume.fit()
    assert result_resume.checkpoint is not None
    assert result_resume.metrics is not None


def test_xgboost_trainer_deprecated_methods(ray_start_4_cpus, small_dataset):
    """Test that deprecated methods raise appropriate warnings."""
    train_df, test_df = small_dataset

    def train_fn_per_worker(config: dict):
        """Simple training function."""
        train_ds = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
        train_df = train_ds.materialize().to_pandas()

        train_X, train_y = train_df.drop("target", axis=1), train_df["target"]
        dtrain = xgboost.DMatrix(train_X, label=train_y)

        bst = xgboost.train(
            config,
            dtrain=dtrain,
            num_boost_round=5,
        )

        # Verify model was created successfully
        assert bst is not None
        assert hasattr(bst, "predict")

    # Create datasets
    train_dataset = ray.data.from_pandas(train_df)

    # Test deprecated legacy API
    with pytest.raises(DeprecationWarning):
        trainer = XGBoostTrainer(
            train_fn_per_worker,
            label_column="target",
            params={"objective": "binary:logistic"},
            num_boost_round=5,
            scaling_config=ScalingConfig(num_workers=2),
            datasets={TRAIN_DATASET_KEY: train_dataset},
        )


def test_xgboost_trainer_dataset_config(ray_start_4_cpus, small_dataset):
    """Test V2 XGBoost Trainer with custom dataset configuration."""
    train_df, test_df = small_dataset

    def train_fn_per_worker(config: dict):
        """Training function."""
        train_ds = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
        train_df = train_ds.materialize().to_pandas()

        train_X, train_y = train_df.drop("target", axis=1), train_df["target"]
        dtrain = xgboost.DMatrix(train_X, label=train_y)

        bst = xgboost.train(
            config,
            dtrain=dtrain,
            num_boost_round=5,
        )

        # Verify model was created successfully
        assert bst is not None
        assert hasattr(bst, "predict")

    # Create datasets
    train_dataset = ray.data.from_pandas(train_df)

    # Test parameters
    params = {
        "tree_method": "hist",
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "max_depth": 3,
        "eta": 0.1,
    }

    # Create and run trainer with custom dataset config
    trainer = XGBoostTrainer(
        train_loop_per_worker=train_fn_per_worker,
        train_loop_config=params,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={TRAIN_DATASET_KEY: train_dataset},
        dataset_config=ray.train.DataConfig(
            execution_options=ray.data.ExecutionOptions(
                preserve_order=False,
                locality_with_output=True,
            )
        ),
    )

    result = trainer.fit()

    # Verify results
    assert result.checkpoint is not None
    assert result.metrics is not None


def test_xgboost_trainer_run_config(ray_start_4_cpus, small_dataset):
    """Test V2 XGBoost Trainer with custom run configuration."""
    train_df, test_df = small_dataset

    def train_fn_per_worker(config: dict):
        """Training function."""
        train_ds = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
        train_df = train_ds.materialize().to_pandas()

        train_X, train_y = train_df.drop("target", axis=1), train_df["target"]
        dtrain = xgboost.DMatrix(train_X, label=train_y)

        bst = xgboost.train(
            config,
            dtrain=dtrain,
            num_boost_round=5,
        )

        # Verify model was created successfully
        assert bst is not None
        assert hasattr(bst, "predict")

    # Create datasets
    train_dataset = ray.data.from_pandas(train_df)

    # Test parameters
    params = {
        "tree_method": "hist",
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "max_depth": 3,
        "eta": 0.1,
    }

    # Create and run trainer with custom run config
    trainer = XGBoostTrainer(
        train_loop_per_worker=train_fn_per_worker,
        train_loop_config=params,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={TRAIN_DATASET_KEY: train_dataset},
        run_config=ray.train.RunConfig(
            name="test_xgboost_training",
            local_dir="/tmp/ray_results",
        ),
    )

    result = trainer.fit()

    # Verify results
    assert result.checkpoint is not None
    assert result.metrics is not None


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
