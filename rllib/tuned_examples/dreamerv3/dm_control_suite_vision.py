from ray.rllib.algorithms.dreamerv3.dreamerv3 import DreamerV3Config

# Run with:
# python run_regression_tests.py --dir [this file] --env DMC/[task]/[domain]
# e.g. --env=DMC/cartpole/swingup

config = (
    DreamerV3Config()
    # Use image observations.
    .environment(env_config={"from_pixels": True})
    .rollouts(num_envs_per_worker=4, remote_worker_envs=True)
    .env("Pendulum-v1")
    # See Appendix A.
    .training(model_dimension="S", training_ratio=512)
)
