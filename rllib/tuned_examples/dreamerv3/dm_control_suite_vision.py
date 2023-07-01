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
# python run_regression_tests.py --dir [this file] --env DMC/[task]/[domain]
# e.g. --env=DMC/cartpole/swingup

config = (
    DreamerV3Config()
    # Use image observations.
    .environment(env_config={"from_pixels": True})
    .resources(
        num_learner_workers=1,
        num_gpus_per_learner_worker=1,
        num_cpus_for_local_worker=1,
    )
    .rollouts(num_envs_per_worker=4, remote_worker_envs=True)
    # See Appendix A.
    .training(
        model_size="S",
        training_ratio=512,
        # TODO
        model={
            "batch_size_B": 16,
            "batch_length_T": 64,
            "horizon_H": 15,
            "gamma": 0.997,
            "model_size": "S",
        },
    )
)
