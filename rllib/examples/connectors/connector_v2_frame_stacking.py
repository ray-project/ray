import argparse

import gymnasium as gym

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module.frame_stacking import FrameStackingEnvToModule
from ray.rllib.connectors.learner.frame_stacking import FrameStackingLearner
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.utils.test_utils import check_learning_achieved


parser = argparse.ArgumentParser()
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="torch",
    help="The DL framework specifier.",
)
parser.add_argument(
    "--num-gpus",
    type=int,
    default=0,
    help="The number of GPUs (Learner workers) to use.",
)
parser.add_argument(
    "--num-env-runners",
    type=int,
    default=2,
    help="The number of EnvRunners to use.",
)
parser.add_argument(
    "--num-frames",
    type=int,
    default=4,
    help="The number of observation frames to stack.",
)
parser.add_argument(
    "--use-connector-framestacking",
    action="store_true",
    help="Whether to use the ConnectorV2 APIs to perform framestacking (as opposed "
    "to doing it via an observation wrapper).",
)
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--stop-iters", type=int, default=2000, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=2000000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=20.0, help="Reward at which we stop training."
)


if __name__ == "__main__":
    import ray
    from ray import air, tune

    args = parser.parse_args()

    ray.init()

    # Define our custom connector pipelines.
    def _make_env_to_module_connector(env):
        # Create the env-to-module connector. We return an individual connector piece
        # here, which RLlib will then automatically integrate into a pipeline (and
        # add its default connector piece to the end of that pipeline).
        return FrameStackingEnvToModule(
            input_observation_space=env.single_observation_space,
            input_action_space=env.single_action_space,
            num_frames=args.num_frames,
        )

    def _make_learner_connector(input_observation_space, input_action_space):
        # Create the learner connector.
        return FrameStackingLearner(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            num_frames=args.num_frames,
        )

    # Create a custom Atari setup (w/o the usual RLlib-hard-coded framestacking in it).
    # We would like our frame stacking connector to do this job.
    tune.register_env(
        "env",
        lambda cfg: (
            wrap_atari_for_new_api_stack(
                gym.make("ALE/Pong-v5", **cfg, **{"render_mode": "rgb_array"}),
                # Perform framestacking either through ConnectorV2 or right here through
                # the observation wrapper.
                framestack=(
                    None if args.use_connector_framestacking else args.num_frames
                ),
            )
        ),
    )

    config = (
        PPOConfig()
        .framework(args.framework)
        .environment(
            "env",
            env_config={
                # Make analogous to old v4 + NoFrameskip.
                "frameskip": 1,
                "full_action_space": False,
                "repeat_action_probability": 0.0,
            },
            clip_rewards=True,
        )
        # Use new API stack ...
        .experimental(_enable_new_api_stack=True)
        .rollouts(
            # ... new EnvRunner and our frame stacking env-to-module connector.
            env_runner_cls=SingleAgentEnvRunner,
            env_to_module_connector=(
                _make_env_to_module_connector if args.use_connector_framestacking
                else None
            ),
            num_rollout_workers=args.num_env_runners,
        )
        .resources(
            num_learner_workers=args.num_gpus,
            num_gpus_per_learner_worker=1 if args.num_gpus else 0,
            num_cpus_for_local_worker=1,
        )
        .training(
            # Use our frame stacking learner connector.
            learner_connector=(
                _make_learner_connector if args.use_connector_framestacking else None
            ),
            lambda_=0.95,
            kl_coeff=0.5,
            clip_param=0.1,
            vf_clip_param=10.0,
            entropy_coeff=0.01,
            num_sgd_iter=10,
            # Linearly adjust learning rate based on number of GPUs.
            lr=0.00015 * (args.num_gpus or 1),
            grad_clip=100.0,
            grad_clip_by="global_norm",
            model={
                "vf_share_layers": True,
                "conv_filters": [[16, 4, 2], [32, 4, 2], [64, 4, 2], [128, 4, 2]],
                "conv_activation": "relu",
                "post_fcnet_hiddens": [256],
            },
        )
    )

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    tuner = tune.Tuner(
        config.algo_class,
        param_space=config,
        run_config=air.RunConfig(stop=stop),
        tune_config=tune.TuneConfig(num_samples=1),
    )
    results = tuner.fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
