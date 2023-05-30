from ray.rllib.algorithms.dreamerv3.dreamerv3 import DreamerV3Config

# Run with:
# python run_regression_tests.py --dir [this file]

config = (
    DreamerV3Config()
    .env("FrozenLake-v1", env_config={
        "desc": [
            "SF",
            "HG",
        ],
        "is_slippery": False,
    })
    .training(model_dimension="XS", training_ratio=1024)
)
