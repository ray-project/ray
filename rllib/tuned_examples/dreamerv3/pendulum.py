from ray.rllib.algorithms.dreamerv3.dreamerv3 import DreamerV3Config

# Run with:
# python run_regression_tests.py --dir [this file]

config = (
    DreamerV3Config()
    .env("Pendulum-v1")
    .training(model_dimension="XS", training_ratio=1024)
)
