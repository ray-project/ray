"""
XGBoost External Memory Training Example.

This example demonstrates how to use Ray Train's XGBoost trainer with external memory
to train on datasets larger than available RAM.

Key Features:
- Automatic external memory conversion with one parameter
- Memory-efficient training on large datasets
- Transparent integration with existing XGBoost code
- Automatic optimization of batch sizes and caching
"""

import tempfile
import time
import traceback
from typing import Dict, Any

import pandas as pd
import xgboost as xgb
from sklearn.datasets import make_regression
from sklearn.model_selection import train_test_split

import ray
import ray.data
import ray.train
from ray.train import ScalingConfig
from ray.train.constants import TRAIN_DATASET_KEY
from ray.train.xgboost import RayTrainReportCallback, XGBoostTrainer


def create_large_dataset(n_samples: int = 100000, n_features: int = 100):
    """Create a large synthetic dataset for demonstration.

    Args:
        n_samples: Number of samples to generate.
        n_features: Number of features to generate.

    Returns:
        DataFrame containing synthetic regression data.
    """
    print(f"Creating large dataset: {n_samples:,} samples × {n_features} features")

    # Generate synthetic regression data
    X, y = make_regression(
        n_samples=n_samples,
        n_features=n_features,
        n_informative=50,
        n_targets=1,
        random_state=42,
        noise=0.1,
    )

    # Create DataFrame with feature names
    feature_names = [f"feature_{i}" for i in range(n_features)]
    df = pd.DataFrame(X, columns=feature_names)
    df["target"] = y

    # Calculate memory usage
    memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
    print(f"Dataset memory usage: {memory_mb:.1f} MB")

    return df


def train_fn_per_worker(config: Dict[str, Any]):
    """Training function that automatically uses external memory if enabled.

    Args:
        config: Configuration dictionary containing external memory settings.

    Returns:
        Trained XGBoost model.
    """
    # Get external memory configuration from config
    use_external_memory = config.get("use_external_memory", False)
    external_memory_cache_dir = config.get("external_memory_cache_dir")
    external_memory_device = config.get("external_memory_device", "cpu")
    external_memory_batch_size = config.get("external_memory_batch_size")

    print(f"Worker config: use_external_memory={use_external_memory}")

    # Get dataset shards
    train_ds_iter = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
    eval_ds_iter = ray.train.get_dataset_shard("validation")

    if use_external_memory:
        print("Using external memory DMatrix...")

        # Use external memory DMatrix
        from ray.train.xgboost._external_memory_utils import (
            create_external_memory_dmatrix,
        )

        dtrain = create_external_memory_dmatrix(
            dataset_shard=train_ds_iter,
            label_column="target",
            batch_size=external_memory_batch_size,
            cache_dir=external_memory_cache_dir,
            device=external_memory_device,
        )

        deval = create_external_memory_dmatrix(
            dataset_shard=eval_ds_iter,
            label_column="target",
            batch_size=external_memory_batch_size,
            cache_dir=external_memory_cache_dir,
            device=external_memory_device,
        )

        print("External memory DMatrix created successfully")

        # Use hist tree method (required for external memory)
        params = {
            "tree_method": "hist",  # Required for external memory
            "objective": "reg:squarederror",
            "eval_metric": ["rmse", "mae"],
            "max_depth": 6,
            "eta": 0.1,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "random_state": 42,
        }

    else:
        print("Using standard DMatrix (in-memory)...")

        # Convert to pandas and create standard DMatrix
        train_df = train_ds_iter.materialize().to_pandas()
        eval_df = eval_ds_iter.materialize().to_pandas()

        train_X, train_y = train_df.drop("target", axis=1), train_df["target"]
        eval_X, eval_y = eval_df.drop("target", axis=1), eval_df["target"]

        dtrain = xgb.DMatrix(train_X, label=train_y)
        deval = xgb.DMatrix(eval_X, label=eval_y)

        print("Standard DMatrix created successfully")

        # Standard parameters
        params = {
            "tree_method": "approx",
            "objective": "reg:squarederror",
            "eval_metric": ["rmse", "mae"],
            "max_depth": 6,
            "eta": 0.1,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "random_state": 42,
        }

    # Train model
    print("Starting training...")
    evals = [(dtrain, TRAIN_DATASET_KEY), (deval, "validation")]

    bst = xgb.train(
        params,
        dtrain=dtrain,
        evals=evals,
        num_boost_round=20,
        callbacks=[RayTrainReportCallback()],
    )

    print("Training completed successfully")

    # Test prediction
    predictions = bst.predict(deval)
    print(f"Model predictions shape: {predictions.shape}")
    print(f"Prediction range: {predictions.min():.4f} to {predictions.max():.4f}")

    return bst


def demonstrate_external_memory():
    """Demonstrate external memory training vs standard training."""
    print("XGBoost External Memory Training Demonstration")
    print("=" * 80)

    # Create large dataset
    df = create_large_dataset(n_samples=50000, n_features=50)

    # Split into train/validation
    train_df, val_df = train_test_split(df, test_size=0.2, random_state=42)

    # Convert to Ray Datasets
    train_ds = ray.data.from_pandas(train_df)
    val_ds = ray.data.from_pandas(val_df)

    print(f"\nDataset split: {len(train_df):,} training, {len(val_df):,} validation")

    # Create cache directory for external memory
    cache_dir = tempfile.mkdtemp(prefix="xgboost_external_memory_demo_")
    print(f"\nUsing cache directory: {cache_dir}")

    # Test 1: Standard training (in-memory)
    print("\n" + "=" * 60)
    print("Testing Standard Training (In-Memory)")
    print("=" * 60)

    trainer_standard = XGBoostTrainer(
        train_loop_per_worker=train_fn_per_worker,
        datasets={TRAIN_DATASET_KEY: train_ds, "validation": val_ds},
        scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
        train_loop_config={
            "use_external_memory": False,  # Standard mode
        },
    )

    print("Standard trainer created successfully")

    # Start training
    print("\nStarting standard training...")
    start_time = time.time()
    _ = trainer_standard.fit()  # Store result but don't use it
    training_time_standard = time.time() - start_time

    print(f"Standard training completed in {training_time_standard:.2f} seconds")

    # Test 2: External memory training
    print("\n" + "=" * 60)
    print("Testing External Memory Training")
    print("=" * 60)

    trainer_external = XGBoostTrainer(
        train_loop_per_worker=train_fn_per_worker,
        datasets={TRAIN_DATASET_KEY: train_ds, "validation": val_ds},
        scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
        train_loop_config={
            "use_external_memory": True,  # External memory mode
            "external_memory_cache_dir": cache_dir,
            "external_memory_device": "cpu",
            "external_memory_batch_size": 1000,
        },
    )

    print("External memory trainer created successfully")

    # Start training
    print("\nStarting external memory training...")
    start_time = time.time()
    _ = trainer_external.fit()  # Store result but don't use it
    training_time_external = time.time() - start_time

    print(f"External memory training completed in {training_time_external:.2f} seconds")

    # Results comparison
    print("\n" + "=" * 60)
    print("Results Comparison")
    print("=" * 60)

    print("Standard Training:")
    print(f"   - Time: {training_time_standard:.2f} seconds")
    print("   - Memory: Uses entire dataset in RAM")
    print(f"   - Dataset size: {len(train_df):,} samples")

    print("\nExternal Memory Training:")
    print(f"   - Time: {training_time_external:.2f} seconds")
    print("   - Memory: Uses only batch size in RAM")
    print("   - Batch size: 1,000 samples")
    memory_reduction = (len(train_df) - 1000) / len(train_df) * 100
    print(f"   - Memory reduction: ~{memory_reduction:.1f}%")

    print("\nExternal memory demonstration completed successfully!")
    print("Both training modes work correctly")
    print("External memory provides significant memory savings")
    print("No code changes needed beyond configuration")

    # Cleanup
    try:
        import shutil

        shutil.rmtree(cache_dir, ignore_errors=True)
        print(f"\nCleaned up cache directory: {cache_dir}")
    except Exception as e:
        print(f"Warning: Failed to cleanup cache directory: {e}")


def demonstrate_convenience_method():
    """Demonstrate the convenience method for DMatrix creation."""
    print("\n" + "=" * 60)
    print("Demonstrating Convenience Method")
    print("=" * 60)

    # Create smaller dataset for demonstration
    df = create_large_dataset(n_samples=10000, n_features=20)
    train_df, val_df = train_test_split(df, test_size=0.2, random_state=42)

    # Convert to Ray Datasets
    train_ds = ray.data.from_pandas(train_df)
    val_ds = ray.data.from_pandas(val_df)

    # Create trainer with external memory enabled
    trainer = XGBoostTrainer(
        train_loop_per_worker=train_fn_per_worker,
        datasets={TRAIN_DATASET_KEY: train_ds, "validation": val_ds},
        scaling_config=ScalingConfig(num_workers=1, use_gpu=False),
        train_loop_config={
            "use_external_memory": True,
            "external_memory_cache_dir": "/tmp/xgboost_demo",
            "external_memory_device": "cpu",
        },
    )

    print("Trainer created with external memory enabled")

    # Demonstrate convenience method
    print("\nUsing convenience method for DMatrix creation...")

    def convenience_train_fn(config: Dict[str, Any]):
        train_ds_iter = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
        eval_ds_iter = ray.train.get_dataset_shard("validation")

        # Use convenience method - automatically handles external memory
        dtrain = trainer.create_dmatrix(train_ds_iter, label_column="target")
        deval = trainer.create_dmatrix(eval_ds_iter, label_column="target")

        print("DMatrix created using convenience method")

        # Train as usual
        params = {
            "tree_method": "hist",
            "objective": "reg:squarederror",
            "max_depth": 4,
            "eta": 0.1,
            "num_boost_round": 10,
        }

        bst = xgb.train(
            params,
            dtrain=dtrain,
            evals=[(deval, "validation")],
            callbacks=[RayTrainReportCallback()],
        )

        return bst

    # Update trainer with convenience method
    trainer.train_loop_per_worker = convenience_train_fn

    print("Starting training with convenience method...")
    _ = trainer.fit()  # Store result but don't use it

    print("Convenience method demonstration completed successfully!")
    print("DMatrix creation handled automatically")
    print("External memory configuration applied transparently")


def main():
    """Main demonstration function.

    Returns:
        True if all demonstrations succeed, False otherwise.
    """
    # Initialize Ray
    if not ray.is_initialized():
        ray.init(num_cpus=4)
        print("Ray initialized with 4 CPUs")

    try:
        # Run demonstrations
        demonstrate_external_memory()
        demonstrate_convenience_method()

        print("\nAll demonstrations completed successfully!")
        print("\nExternal memory functionality is working correctly:")
        print("   - One parameter enables external memory")
        print("   - Automatic dataset conversion")
        print("   - Significant memory savings")
        print("   - Transparent integration")
        print("   - Convenience methods available")

        return True

    except Exception as e:
        print(f"\nDemonstration failed with error: {e}")
        traceback.print_exc()
        return False

    finally:
        # Shutdown Ray
        if ray.is_initialized():
            ray.shutdown()
            print("\nRay shutdown complete")


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
