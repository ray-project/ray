# flake8: noqa
# isort: skip_file

# __air_generic_preprocess_start__
import ray

# Load data.
dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")

# Split data into train and validation.
train_dataset, valid_dataset = dataset.train_test_split(test_size=0.3)

# Create a test dataset by dropping the target column.
test_dataset = valid_dataset.drop_columns(cols=["target"])
# __air_generic_preprocess_end__

# __air_xgb_preprocess_start__
# Create a preprocessor to scale some columns.
from ray.data.preprocessors import StandardScaler

preprocessor = StandardScaler(columns=["mean radius", "mean texture"])
# __air_xgb_preprocess_end__

# __air_xgb_train_start__
from ray.train import ScalingConfig
from ray.train.xgboost import XGBoostTrainer

trainer = XGBoostTrainer(
    scaling_config=ScalingConfig(
        # Number of workers to use for data parallelism.
        num_workers=2,
        # Whether to use GPU acceleration.
        use_gpu=False,
        # Make sure to leave some CPUs free for Ray Data operations.
        _max_cpu_fraction_per_node=0.9,
    ),
    label_column="target",
    num_boost_round=20,
    params={
        # XGBoost specific params
        "objective": "binary:logistic",
        # "tree_method": "gpu_hist",  # uncomment this to use GPUs.
        "eval_metric": ["logloss", "error"],
    },
    datasets={"train": train_dataset, "valid": valid_dataset},
    preprocessor=preprocessor,
)
best_result = trainer.fit()
print(best_result.metrics)
# __air_xgb_train_end__

# __air_xgb_tuner_start__
from ray import tune

param_space = {"params": {"max_depth": tune.randint(1, 9)}}
metric = "train-logloss"
# __air_xgb_tuner_end__

# __air_tune_generic_end__
from ray.tune.tuner import Tuner, TuneConfig

tuner = Tuner(
    trainer,
    param_space=param_space,
    tune_config=TuneConfig(num_samples=5, metric=metric, mode="min"),
)
result_grid = tuner.fit()
best_result = result_grid.get_best_result()
print("Best result:", best_result)
# __air_tune_generic_end__
