# flake8: noqa
# isort: skip_file

# __xgboost_start__
import ray
from ray.train.xgboost import XGBoostTrainer
from ray.air.config import ScalingConfig

# Load data.
dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")

# Split data into train and validation.
train_dataset, valid_dataset = dataset.train_test_split(test_size=0.3)

trainer = XGBoostTrainer(
    scaling_config=ScalingConfig(
        # Number of workers to use for data parallelism.
        num_workers=2,
        # Whether to use GPU acceleration.
        use_gpu=False,
    ),
    label_column="target",
    num_boost_round=20,
    params={
        # XGBoost specific params
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
    },
    datasets={"train": train_dataset, "valid": valid_dataset},
)
result = trainer.fit()
print(result.metrics)

# __xgboost_end__

# __lightgbm_start__
import ray
from ray.train.lightgbm import LightGBMTrainer
from ray.air.config import ScalingConfig

# Load data.
dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")

# Split data into train and validation.
train_dataset, valid_dataset = dataset.train_test_split(test_size=0.3)

trainer = LightGBMTrainer(
    scaling_config=ScalingConfig(
        # Number of workers to use for data parallelism.
        num_workers=2,
        # Whether to use GPU acceleration.
        use_gpu=False,
    ),
    label_column="target",
    num_boost_round=20,
    params={
        # LightGBM specific params
        "objective": "binary",
        "metric": ["binary_logloss", "binary_error"],
    },
    datasets={"train": train_dataset, "valid": valid_dataset},
)
result = trainer.fit()
print(result.metrics)

# __lightgbm_end__

# __scaling_cpu_start__
scaling_config = ScalingConfig(
    num_workers=4,
    trainer_resources={"CPU": 0},
    resources_per_worker={"CPU": 8},
)
# __scaling_cpu_end__

# __scaling_gpu_start__
scaling_config = ScalingConfig(
    num_workers=4,
    use_gpu=True,
)
# __scaling_gpu_end__

# __scaling_gpumulti_start__
scaling_config = ScalingConfig(
    num_workers=16,
    use_gpu=True,
)
# __scaling_gpumulti_end__

# __gpu_xgboost_start__
trainer = XGBoostTrainer(
    scaling_config=ScalingConfig(
        # Number of workers to use for data parallelism.
        num_workers=2,
        # Whether to use GPU acceleration.
        use_gpu=True,
    ),
    params={
        # XGBoost specific params
        "tree_method": "gpu_hist",
        "eval_metric": ["logloss", "error"],
    },
    label_column="target",
    num_boost_round=20,
    datasets={"train": train_dataset, "valid": valid_dataset},
)
# __gpu_xgboost_end__
