"""Example using the PyFlyt Gymnasium environment to train a UAV to reach waypoints.

PyFlyt GitHub Repository: https://github.com/jjshoots/PyFlyt/tree/master/PyFlyt

How to run this script
----------------------
`python [script file name].py --enable-new-api-stack`


For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`
"""

import os

from ray.tune.registry import get_trainable_cls
import gymnasium as gym
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    TRAINING_ITERATION_TIMER,
)

parser = add_rllib_example_script_args(
    default_iters=200,
    default_timesteps=100000,
    default_reward=90.0,
)
parser.add_argument(
    "--run", type=str, default="PPO", help="The RLlib-registered algorithm to use."
)
parser.add_argument("--env-name", type=str, default="quadx_waypoints")
parser.add_argument("--num-envs-per-worker", type=int, default=4)


class RewardWrapper(gym.RewardWrapper):
    def __init__(self, env):
        super().__init__(env)

    def reward(self, reward):
        # Scale rewards:
        if reward >= 99.0 or reward <= -99.0:
            return reward / 10
        return reward


def create_quadx_waypoints_env(env_config):
    import PyFlyt.gym_envs  # noqa
    from PyFlyt.gym_envs import FlattenWaypointEnv

    env = gym.make("PyFlyt/QuadX-Waypoints-v1")
    # Wrap Environment to use max 10 and -10 for rewards
    env = RewardWrapper(env)

    return FlattenWaypointEnv(env, context_length=1)


if __name__ == "__main__":
    from ray.tune.registry import register_env

    args = parser.parse_args()
    num_gpus = int(os.environ.get("RLLIB_NUM_GPUS", "0"))

    register_env(args.env_name, env_creator=create_quadx_waypoints_env)

    algo_cls = get_trainable_cls(args.run)
    config = algo_cls.get_default_config()

    config.environment(env=args.env_name).resources(
        num_learner_workers=num_gpus,
        num_gpus_per_learner_worker=num_gpus,
    ).rollouts(
        num_rollout_workers=args.num_cpus,
        num_envs_per_worker=args.num_envs_per_worker,
    ).framework(
        args.framework
    ).api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    ).reporting(
        min_time_s_per_iteration=0.1
    )

    if args.run == "PPO":
        config.rl_module(
            model_config_dict={
                "fcnet_hiddens": [32],
                "fcnet_activation": "linear",
                "vf_share_layers": True,
            }
        )
        config.training(
            sgd_minibatch_size=128,
            train_batch_size=10000,
        )
    elif args.run == "IMPALA":
        config.rollouts(num_rollout_workers=2)
        config.resources(num_gpus=0)
        config.training(vf_loss_coeff=0.01)

    EPISODE_RETURN_MEAN_KEY = f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"

    stop = {
        TRAINING_ITERATION_TIMER: args.stop_iters,
        EPISODE_RETURN_MEAN_KEY: args.stop_reward,
    }

    run_rllib_example_script_experiment(
        config,
        args,
        stop=stop,
        success_metric={
            f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": args.stop_reward,
        },
    )
