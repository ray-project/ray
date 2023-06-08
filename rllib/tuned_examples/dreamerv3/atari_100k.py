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
# python run_regression_tests.py --dir [this file] --env ALE/[gym ID e.g. Pong-v5]

config = (
    DreamerV3Config()
    .resources(
        num_learner_workers=1,
        num_gpus_per_learner_worker=1,
        num_cpus_for_local_worker=1,
    )
    # TODO (sven): concretize this: If you use >1 GPU and increase the batch size
    #  accordingly, you might also want to increase the number of envs per worker
    .rollouts(
        num_envs_per_worker=1,
        # Since we are using gymnasium.vector.Env, we can parallelize the individual
        # envs w/o performance hit.
        remote_worker_envs=True,
    )
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
    # See Appendix A.
    .training(
        model_size="S",
        training_ratio=1024,
        # TODO
        model={
            "batch_length_T": 64,
            "horizon_H": 15,
            "gamma": 0.997,
            "model_size": "S",
        },
    )
)
