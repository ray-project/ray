import argparse
from gym.spaces import Box, Discrete
import os

from ray.rllib.examples.env.action_mask_env import ActionMaskEnv
from ray.rllib.examples.models.action_mask_model import \
    ActionMaskModel, TorchActionMaskModel

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run",
    type=str,
    default="APPO",
    help="The RLlib-registered algorithm to use.")
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "tfe", "torch"],
    default="tf",
    help="The DL framework specifier.")
parser.add_argument("--eager-tracing", action="store_true")
parser.add_argument(
    "--stop-iters",
    type=int,
    default=200,
    help="Number of iterations to train.")
parser.add_argument(
    "--stop-timesteps",
    type=int,
    default=100000,
    help="Number of timesteps to train.")
parser.add_argument(
    "--stop-reward",
    type=float,
    default=80.0,
    help="Reward at which we stop training.")
parser.add_argument(
    "--local-mode",
    action="store_true",
    help="Init Ray in local mode for easier debugging.")

if __name__ == "__main__":
    import ray
    from ray import tune

    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None, local_mode=args.local_mode)

    config = {
        "env": ActionMaskEnv,
        "env_config": {
            "action_space": Discrete(100),
            "observation_space": Box(-1.0, 1.0, (5, )),
        },
        "model": {
            "custom_model": ActionMaskModel
            if args.framework != "torch" else TorchActionMaskModel,
        },

        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "framework": args.framework,
        # Run with tracing enabled for tfe/tf2?
        "eager_tracing": args.eager_tracing,
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    results = tune.run(args.run, config=config, stop=stop, verbose=2)

    ray.shutdown()
