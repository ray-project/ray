# flake8: noqa
# isort: skip_file

# __xgboost_start__
import pandas as pd
import xgboost

# 1. Load your data as an `xgboost.DMatrix`.
train_df = pd.read_csv("s3://ray-example-data/iris/train/1.csv")
eval_df = pd.read_csv("s3://ray-example-data/iris/val/1.csv")

train_X = train_df.drop("target", axis=1)
train_y = train_df["target"]
eval_X = eval_df.drop("target", axis=1)
eval_y = eval_df["target"]

dtrain = xgboost.DMatrix(train_X, label=train_y)
deval = xgboost.DMatrix(eval_X, label=eval_y)

# 2. Define your xgboost model training parameters.
params = {
    "tree_method": "approx",
    "objective": "reg:squarederror",
    "eta": 1e-4,
    "subsample": 0.5,
    "max_depth": 2,
}

# 3. Do non-distributed training.
bst = xgboost.train(
    params,
    dtrain=dtrain,
    evals=[(deval, "validation")],
    num_boost_round=10,
)
# __xgboost_end__


# __xgboost_ray_start__
import xgboost

import ray.train
from ray.train.xgboost import XGBoostTrainer, RayTrainReportCallback

# 1. Load your data as a Ray Data Dataset.
train_dataset = ray.data.read_csv("s3://anonymous@ray-example-data/iris/train")
eval_dataset = ray.data.read_csv("s3://anonymous@ray-example-data/iris/val")


def train_func():
    # 2. Load your data shard as an `xgboost.DMatrix`.

    # Get dataset shards for this worker
    train_shard = ray.train.get_dataset_shard("train")
    eval_shard = ray.train.get_dataset_shard("eval")

    # Convert shards to pandas DataFrames
    train_df = train_shard.materialize().to_pandas()
    eval_df = eval_shard.materialize().to_pandas()

    train_X = train_df.drop("target", axis=1)
    train_y = train_df["target"]
    eval_X = eval_df.drop("target", axis=1)
    eval_y = eval_df["target"]

    dtrain = xgboost.DMatrix(train_X, label=train_y)
    deval = xgboost.DMatrix(eval_X, label=eval_y)

    # 3. Define your xgboost model training parameters.
    params = {
        "tree_method": "approx",
        "objective": "reg:squarederror",
        "eta": 1e-4,
        "subsample": 0.5,
        "max_depth": 2,
    }

    # 4. Do distributed data-parallel training.
    # Ray Train sets up the necessary coordinator processes and
    # environment variables for your workers to communicate with each other.
    bst = xgboost.train(
        params,
        dtrain=dtrain,
        evals=[(deval, "validation")],
        num_boost_round=10,
        # Optional: Use the `RayTrainReportCallback` to save and report checkpoints.
        callbacks=[RayTrainReportCallback()],
    )


# 5. Configure scaling and resource requirements.
scaling_config = ray.train.ScalingConfig(num_workers=2, resources_per_worker={"CPU": 2})

# 6. Launch distributed training job.
trainer = XGBoostTrainer(
    train_func,
    scaling_config=scaling_config,
    datasets={"train": train_dataset, "eval": eval_dataset},
    # If running in a multi-node cluster, this is where you
    # should configure the run's persistent storage that is accessible
    # across all worker nodes.
    # run_config=ray.train.RunConfig(storage_path="s3://..."),
)
result = trainer.fit()

# 7. Load the trained model
import os

with result.checkpoint.as_directory() as checkpoint_dir:
    model_path = os.path.join(checkpoint_dir, RayTrainReportCallback.CHECKPOINT_NAME)
    model = xgboost.Booster()
    model.load_model(model_path)
# __xgboost_ray_end__
