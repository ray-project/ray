"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""

# Run with:
# python run_regression_tests.py --dir [this file] --env ALE/[gym ID e.g. Pong-v5]

from ray.rllib.algorithms.dreamerv3.dreamerv3 import DreamerV3Config


# Number of GPUs to run on.
num_gpus = 1

config = (
    DreamerV3Config()
    .environment(
        # [2]: "We follow the evaluation protocol of Machado et al. (2018) with 200M
        # environment steps, action repeat of 4, a time limit of 108,000 steps per
        # episode that correspond to 30 minutes of game play, no access to life
        # information, full action space, and sticky actions. Because the world model
        # integrates information over time, DreamerV2 does not use frame stacking.
        # The experiments use a single-task setup where a separate agent is trained
        # for each game. Moreover, each agent uses only a single environment instance.
        env_config={
            # "sticky actions" but not according to Danijar's 100k configs.
            "repeat_action_probability": 0.0,
            # "full action space" but not according to Danijar's 100k configs.
            "full_action_space": False,
            # Already done by MaxAndSkip wrapper: "action repeat" == 4.
            "frameskip": 1,
        }
    )
    .resources(
        num_learner_workers=0 if num_gpus == 1 else num_gpus,
        num_gpus_per_learner_worker=1 if num_gpus else 0,
        num_cpus_for_local_worker=1,
    )
    .rollouts(
        # If we use >1 GPU and increase the batch size accordingly, we should also
        # increase the number of envs per worker.
        num_envs_per_worker=(num_gpus or 1),
        remote_worker_envs=True,
    )
    .reporting(
        metrics_num_episodes_for_smoothing=(num_gpus or 1),
        report_images_and_videos=False,
        report_dream_data=False,
        report_individual_batch_item_stats=False,
    )
    # See Appendix A.
    .training(
        model_size="S",
        training_ratio=1024,
        batch_size_B=16 * (num_gpus or 1),
    )
)
