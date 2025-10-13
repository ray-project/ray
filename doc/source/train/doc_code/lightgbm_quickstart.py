# flake8: noqa
# isort: skip_file

# __lightgbm_start__
import pandas as pd
import lightgbm as lgb

# 1. Load your data as a `lightgbm.Dataset`.
train_df = pd.read_csv("s3://ray-example-data/iris/train/1.csv")
eval_df = pd.read_csv("s3://ray-example-data/iris/val/1.csv")

train_X = train_df.drop("target", axis=1)
train_y = train_df["target"]
eval_X = eval_df.drop("target", axis=1)
eval_y = eval_df["target"]

train_set = lgb.Dataset(train_X, label=train_y)
eval_set = lgb.Dataset(eval_X, label=eval_y)

# 2. Define your LightGBM model training parameters.
params = {
    "objective": "multiclass",
    "num_class": 3,
    "metric": ["multi_logloss", "multi_error"],
    "verbosity": -1,
    "boosting_type": "gbdt",
    "num_leaves": 31,
    "learning_rate": 0.05,
    "feature_fraction": 0.9,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
}

# 3. Do non-distributed training.
model = lgb.train(
    params,
    train_set,
    valid_sets=[eval_set],
    valid_names=["eval"],
    num_boost_round=100,
)
# __lightgbm_end__


# __lightgbm_ray_start__
import lightgbm as lgb

import ray.train
from ray.train.lightgbm import LightGBMTrainer, RayTrainReportCallback

# 1. Load your data as a Ray Data Dataset.
train_dataset = ray.data.read_csv("s3://anonymous@ray-example-data/iris/train")
eval_dataset = ray.data.read_csv("s3://anonymous@ray-example-data/iris/val")


def train_func():
    # 2. Load your data shard as a `lightgbm.Dataset`.

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

    train_set = lgb.Dataset(train_X, label=train_y)
    eval_set = lgb.Dataset(eval_X, label=eval_y)

    # 3. Define your LightGBM model training parameters.
    params = {
        "objective": "multiclass",
        "num_class": 3,
        "metric": ["multi_logloss", "multi_error"],
        "verbosity": -1,
        "boosting_type": "gbdt",
        "num_leaves": 31,
        "learning_rate": 0.05,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        # Adding the line below is the only change needed
        # for your `lgb.train` call!
        **ray.train.lightgbm.get_network_params(),
    }

    # 4. Do distributed data-parallel training.
    # Ray Train sets up the necessary coordinator processes and
    # environment variables for your workers to communicate with each other.
    model = lgb.train(
        params,
        train_set,
        valid_sets=[eval_set],
        valid_names=["eval"],
        num_boost_round=100,
        # Optional: Use the `RayTrainReportCallback` to save and report checkpoints.
        callbacks=[RayTrainReportCallback()],
    )


# 5. Configure scaling and resource requirements.
scaling_config = ray.train.ScalingConfig(num_workers=2, resources_per_worker={"CPU": 2})

# 6. Launch distributed training job.
trainer = LightGBMTrainer(
    train_func,
    scaling_config=scaling_config,
    datasets={"train": train_dataset, "eval": eval_dataset},
)
result = trainer.fit()

# 7. Load the trained model.
model = RayTrainReportCallback.get_model(result.checkpoint)
# __lightgbm_ray_end__
