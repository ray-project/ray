import os
import pickle

# Enable Ray Train v2. This will be the default in an upcoming release.
os.environ["RAY_TRAIN_V2_ENABLED"] = "1"
# It is now safe to import Ray Train.

import ray
import xgboost
from ray.data.preprocessors import StandardScaler
from ray.train import CheckpointConfig, Result, RunConfig, ScalingConfig
from ray.train.xgboost import RayTrainReportCallback, XGBoostTrainer

from dist_xgboost.constants import storage_path, preprocessor_path
from dist_xgboost.data import log_run_to_mlflow, prepare_data
import dist_xgboost

NUM_WORKERS = 5
USE_GPU = True


def train_preprocessor(train_dataset: ray.data.Dataset) -> StandardScaler:
    # pick some dataset columns to scale
    columns_to_scale = [c for c in train_dataset.columns() if c != "target"]

    # Initialize the preprocessor
    preprocessor = StandardScaler(columns=columns_to_scale)
    # train the preprocessor on the training set
    preprocessor.fit(train_dataset)

    return preprocessor


def save_preprocessor(preprocessor: StandardScaler):
    with open(preprocessor_path, "wb") as f:
        pickle.dump(preprocessor, f)


def train_fn_per_worker(config: dict):
    # Get this worker's dataset shard convert
    train_ds, val_ds = (
        ray.train.get_dataset_shard("train"),
        ray.train.get_dataset_shard("validation"),
    )

    train_ds = train_ds.materialize().to_pandas()
    val_ds = val_ds.materialize().to_pandas()

    # Separate the labels from the features
    train_X, train_y = train_ds.drop("target", axis=1), train_ds["target"]
    eval_X, eval_y = val_ds.drop("target", axis=1), val_ds["target"]

    # Convert the data into a DMatrix
    dtrain = xgboost.DMatrix(train_X, label=train_y)
    deval = xgboost.DMatrix(eval_X, label=eval_y)

    # Do distributed data-parallel training.
    # Ray Train sets up the necessary coordinator processes and
    # environment variables for your workers to communicate with each other.
    # it also handles checkpointing via the `RayTrainReportCallback`
    _booster = xgboost.train(
        config["xgboost_params"],
        dtrain=dtrain,
        evals=[(dtrain, "train"), (deval, "validation")],
        num_boost_round=10,
        callbacks=[RayTrainReportCallback()],
    )


def main():
    ray.init(runtime_env={"py_modules": [dist_xgboost]})
    # Load and split the dataset
    train_dataset, valid_dataset, _test_dataset = prepare_data()

    # Train the preprocessor
    preprocessor = train_preprocessor(train_dataset)

    # Save the preprocessor
    save_preprocessor(preprocessor)

    train_dataset = preprocessor.transform(train_dataset)
    valid_dataset = preprocessor.transform(valid_dataset)

    # Configure checkpointing to save progress during training
    run_config = RunConfig(
        checkpoint_config=CheckpointConfig(
            # Checkpoint every 10 iterations.
            checkpoint_frequency=10,
            # Only keep the latest checkpoint.
            num_to_keep=1,
        ),
        ## If running in a multi-node cluster, this is where you
        ## should configure the run's persistent storage that is accessible
        ## across all worker nodes with `storage_path="s3://..."`
        storage_path=storage_path,
    )

    # Define the scaling config
    scaling_config = ScalingConfig(
        # Number of workers to use for data parallelism.
        num_workers=NUM_WORKERS,
        # Whether to use GPU acceleration. Set to True to schedule GPU workers.
        use_gpu=USE_GPU,
    )

    # Params that will be passed to the base XGBoost model.
    model_config = {
        "xgboost_params": {
            "objective": "binary:logistic",
            "eval_metric": ["logloss", "error"],
        }
    }

    trainer = XGBoostTrainer(
        train_fn_per_worker,
        train_loop_config=model_config,
        # Register the data subsets.
        datasets={"train": train_dataset, "validation": valid_dataset},
        scaling_config=scaling_config,
        run_config=run_config,
    )

    result: Result = trainer.fit()
    print(f"Training metrics: {result.metrics}")

    log_run_to_mlflow(model_config, result, preprocessor_path)
    print("Training complete")


if __name__ == "__main__":
    main()
