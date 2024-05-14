import argparse
import os

from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.registry import get_trainable_cls
import gymnasium as gym

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run", type=str, default="PPO", help="The RLlib-registered algorithm to use."
)
parser.add_argument('--env-name', type=str, default="quadx_waypoints")
parser.add_argument("--num-cpus", type=int, default=4)
parser.add_argument("--num-envs-per-worker", type=int, default=4)
parser.add_argument(
    "--framework",
    choices=["tf2", "torch"],
    default="torch",
    help="The DL framework specifier.",
)

parser.add_argument(
    "--stop-iters", type=int, default=500, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=90.0, help="Reward at which we stop training."
)

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
    import ray
    from ray import air, tune
    from ray.tune.registry import register_env

    args = parser.parse_args()

    ray.init()

    register_env(args.env_name, env_creator=create_quadx_waypoints_env)

    algo_cls = get_trainable_cls(args.run)
    config = algo_cls.get_default_config()

    config.environment(
        env=args.env_name
    ).resources(
        num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0"))
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

    stop = {
        "training_iteration": args.stop_iters,
        "env_runner_results/episode_return_mean": args.stop_reward,
    }

    tuner = tune.Tuner(
        args.run,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop=stop,
        ),
    )
    results = tuner.fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()
