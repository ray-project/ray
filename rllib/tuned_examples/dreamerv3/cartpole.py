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
    .environment("CartPole-v1")
    .training(
        model_size="XS",
        training_ratio=32,
    )
)

# Keep it simple. DreamerV3 takes more time to learn stuff due to the large and complex
# world-model mechanism, even on seemingly easy environments.
stop = {
    "sampler_results/episode_reward_mean": 100.0,
}