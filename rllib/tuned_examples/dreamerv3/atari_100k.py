from ray.rllib.algorithms.dreamerv3.dreamerv3 import DreamerV3Config


# Run with:
# python run_regression_tests.py --dir [this file] --env ALE/[gym ID e.g. Pong-v5]

config = (
    DreamerV3Config()
    .env(
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
    .training(model_dimension="S", training_ratio=1024)
)
