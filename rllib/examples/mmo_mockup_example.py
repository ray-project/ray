"""
Example for running a mock-up MMO with large observation spaces, long
trajectories (up to 10k) and varying agents within an episode.
"""
import argparse
import gym
import os

import ray
from ray import tune
from ray.rllib.examples.env.multi_agent import FlexAgentsMultiAgent
from ray.rllib.utils.framework import try_import_tf

tf1, tf, tfv = try_import_tf()


def get_cli_args():
    """Create CLI parser and return parsed arguments"""
    parser = argparse.ArgumentParser()

    # general args
    parser.add_argument(
        "--run",
        default="APPO",
        help="The RLlib-registered algorithm to use.")
    parser.add_argument("--num-cpus", type=int, default=3)
    parser.add_argument(
        "--framework",
        choices=["tf", "tf2", "tfe", "torch"],
        default="tf",
        help="The DL framework specifier.")
    parser.add_argument("--num-workers", type=int, default=2,
                        help="The number of workers to use.")
    parser.add_argument("--num-envs-per-worker", type=int, default=20,
                        help="The number of sub-env (vectorized) per worker.")
    parser.add_argument(
        "--max-episode-len",
        type=int,
        default=1000,
        help="The maximum number of env steps an episode will take.")
    parser.add_argument(
        "--p-done",
        type=float,
        default=0.0,
        help="The probability for terminating the ongoing episode "
        "for each agent at each timestep. ")
    parser.add_argument(
        "--stop-iters",
        type=int,
        default=200,
        help="Number of iterations to train.")
    parser.add_argument(
        "--stop-timesteps",
        type=int,
        default=500000,
        help="Number of timesteps to train.")
    parser.add_argument(
        "--stop-reward",
        type=float,
        default=80.0,
        help="Reward at which we stop training.")
    parser.add_argument(
        "--as-test",
        action="store_true",
        help="Whether this script should be run as a test: --stop-reward must "
        "be achieved within --stop-timesteps AND --stop-iters.")
    parser.add_argument(
        "--no-tune",
        action="store_true",
        help="Run without Tune using a manual train loop instead. Here,"
        "there is no TensorBoard support.")
    parser.add_argument(
        "--local-mode",
        action="store_true",
        help="Init Ray in local mode for easier debugging.")

    args = parser.parse_args()
    print(f"Running with following CLI args: {args}")
    return args


if __name__ == "__main__":
    args = get_cli_args()

    ray.init(num_cpus=args.num_cpus or None, local_mode=args.local_mode)

    config = {
        # Env will automatically spawn new agents (3 out of 4 steps)
        # and remove existing ones (1 out of 4 steps). On top of that,
        # each agent has a max episode len of 10k and terminates at
        # each step with p=0.0005.
        "env": FlexAgentsMultiAgent,
        # Make sure we have a massive obs-space to test memory robustness.
        "env_config": {
            "observation_space": gym.spaces.Tuple((
                gym.spaces.Box(-1.0, 1.0, (84, 84, 3)),
                gym.spaces.Box(-1.0, 1.0, (42, 42, 3)),
                gym.spaces.Box(0.0, 100.0, (100, )),
                # gym.spaces.Discrete(100),
            )),
            "action_space": gym.spaces.Discrete(10),
            "max_episode_len": args.max_episode_len,
            "p_done": args.p_done,
        },
        "gamma": 0.99,
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", 0)),
        "num_workers": args.num_workers,
        "num_envs_per_worker": args.num_envs_per_worker,

        "entropy_coeff": 0.001,
        "vf_loss_coeff": 1e-5,
        "framework": args.framework,
    }

    # APPO/IMPALA settings.
    if args.run in ["APPO", "IMPALA"]:
        config.update({
            "replay_proportion": 1.0,
            "replay_buffer_num_slots": 50,
        })
    # PPO settings.
    elif args.run == "PPO":
        config.update({
            "num_sgd_iter": 10,
        })

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    results = tune.run(args.run, config=config, stop=stop, verbose=2)

    ray.shutdown()
