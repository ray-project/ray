from ray.rllib.algorithms.dreamerv3.dreamerv3 import DreamerV3Config

# Run with:
# python run_regression_tests.py --dir [this file]

config = (
    DreamerV3Config()
    .env("FrozenLake-v1", env_config={
        "map_name": "4x4",
        "is_slippery": False,
    })
    .training(model_dimension="nano", training_ratio=1024)
)
