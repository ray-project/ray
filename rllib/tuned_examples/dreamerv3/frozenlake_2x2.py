"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
from ray.rllib.algorithms.dreamerv3.dreamerv3 import DreamerV3Config

# Run with:
# python run_regression_tests.py --dir [this file]

config = (
    DreamerV3Config()
    .environment(
        "FrozenLake-v1",
        env_config={
            "desc": [
                "SF",
                "HG",
            ],
            "is_slippery": False,
        },
    )
    .training(
        model_size="XS",
        training_ratio=1024,
    )
)
