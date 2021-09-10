import argparse
from gym.spaces import Box, Discrete
import os

from ray.rllib.examples.env.random_env import RandomEnvWithActionMasking

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
        "env": RandomEnvWithActionMasking,
        "env_config": {
            "action_space": Discrete(100),
            "observation_space": Box(-1.0, 1.0, (5, )),
        },

        # Let the algorithm know that it has to apply the mask
        # found in the obs dict under this key to some loss terms
        # (e.g. entropy_loss).
        "action_masking_key": "action_mask",

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
