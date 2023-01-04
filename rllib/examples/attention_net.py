"""
Example of using an RL agent (default: PPO) with an AttentionNet model,
which is useful for environments where state is important but not explicitly
part of the observations.

For example, in the "repeat after me" environment (default here), the agent
needs to repeat an observation from n timesteps before.
AttentionNet keeps state of previous observations and uses transformers to
learn a policy that successfully repeats previous observations.
Without attention, the RL agent only "sees" the last observation, not the one
n timesteps ago and cannot learn to repeat this previous observation.

AttentionNet paper: https://arxiv.org/abs/1506.07704

This example script also shows how to train and test a PPO agent with an
AttentionNet model manually, i.e., without using Tune.

---
Run this example with defaults (using Tune and AttentionNet on the "repeat
after me" environment):
$ python attention_net.py
Then run again without attention:
$ python attention_net.py --no-attention
Compare the learning curve on TensorBoard:
$ cd ~/ray-results/; tensorboard --logdir .
There will be a huge difference between the version with and without attention!

Other options for running this example:
$ python attention_net.py --help
"""
import argparse
import gymnasium as gym
import numpy as np
import os


import ray
from ray import air, tune
from ray.rllib.algorithms import ppo
from ray.rllib.examples.env.look_and_push import LookAndPush, OneHot
from ray.rllib.examples.env.repeat_after_me_env import RepeatAfterMeEnv
from ray.rllib.examples.env.repeat_initial_obs_env import RepeatInitialObsEnv
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.logger import pretty_print

tf1, tf, tfv = try_import_tf()
SUPPORTED_ENVS = [
    "RepeatAfterMeEnv",
    "RepeatInitialObsEnv",
    "LookAndPush",
    "StatelessCartPole",
]


def get_cli_args():
    """Create CLI parser and return parsed arguments"""
    parser = argparse.ArgumentParser()

    # example-specific args
    parser.add_argument(
        "--no-attention",
        action="store_true",
        help="Do NOT use attention. For comparison: The agent will not learn.",
    )
    parser.add_argument("--env", choices=SUPPORTED_ENVS, default="RepeatAfterMeEnv")

    # general args
    parser.add_argument(
        "--run", default="PPO", help="The RLlib-registered algorithm to use."
    )
    parser.add_argument("--num-cpus", type=int, default=3)
    parser.add_argument(
        "--framework",
        choices=["tf", "tf2", "torch"],
        default="tf",
        help="The DL framework specifier.",
    )
    parser.add_argument(
        "--stop-iters", type=int, default=200, help="Number of iterations to train."
    )
    parser.add_argument(
        "--stop-timesteps",
        type=int,
        default=500000,
        help="Number of timesteps to train.",
    )
    parser.add_argument(
        "--stop-reward",
        type=float,
        default=80.0,
        help="Reward at which we stop training.",
    )
    parser.add_argument(
        "--as-test",
        action="store_true",
        help="Whether this script should be run as a test: --stop-reward must "
        "be achieved within --stop-timesteps AND --stop-iters.",
    )
    parser.add_argument(
        "--no-tune",
        action="store_true",
        help="Run without Tune using a manual train loop instead. Here,"
        "there is no TensorBoard support.",
    )
    parser.add_argument(
        "--local-mode",
        action="store_true",
        help="Init Ray in local mode for easier debugging.",
    )

    args = parser.parse_args()
    print(f"Running with following CLI args: {args}")
    return args


if __name__ == "__main__":
    args = get_cli_args()

    ray.init(num_cpus=args.num_cpus or None, local_mode=args.local_mode)

    # register custom environments
    gym.register("RepeatAfterMeEnv", lambda c: RepeatAfterMeEnv(c))
    gym.register("RepeatInitialObsEnv", lambda: RepeatInitialObsEnv())
    gym.register("LookAndPush", lambda: OneHot(LookAndPush()))
    gym.register("StatelessCartPole", lambda: StatelessCartPole())

    # main part: RLlib config with AttentionNet model
    config = (
        ppo.PPOConfig()
        .environment(
            args.env,
            # This env_config is only used for the RepeatAfterMeEnv env.
            env_config={"repeat_delay": 2},
        )
        .training(
            gamma=0.99,
            entropy_coeff=0.001,
            num_sgd_iter=10,
            vf_loss_coeff=1e-5,
            model={
                "use_attention": not args.no_attention,
                "max_seq_len": 10,
                "attention_num_transformer_units": 1,
                "attention_dim": 32,
                "attention_memory_inference": 10,
                "attention_memory_training": 10,
                "attention_num_heads": 1,
                "attention_head_dim": 32,
                "attention_position_wise_mlp_dim": 32,
            },
        )
        .framework(args.framework)
        .rollouts(num_envs_per_worker=20)
        .resources(
            # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
            num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", 0))
        )
    )

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    # Manual training loop (no Ray tune).
    if args.no_tune:
        # manual training loop using PPO and manually keeping track of state
        if args.run != "PPO":
            raise ValueError("Only support --run PPO with --no-tune.")
        algo = config.build()
        # run manual training loop and print results after each iteration
        for _ in range(args.stop_iters):
            result = algo.train()
            print(pretty_print(result))
            # stop training if the target train steps or reward are reached
            if (
                result["timesteps_total"] >= args.stop_timesteps
                or result["episode_reward_mean"] >= args.stop_reward
            ):
                break

        # Run manual test loop (only for RepeatAfterMe env).
        if args.env == "RepeatAfterMeEnv":
            print("Finished training. Running manual test/inference loop.")
            # prepare env
            env = RepeatAfterMeEnv(config["env_config"])
            obs, info = env.reset()
            done = False
            total_reward = 0
            # start with all zeros as state
            num_transformers = config["model"]["attention_num_transformer_units"]
            state = algo.get_policy().get_initial_state()
            # run one iteration until done
            print(f"RepeatAfterMeEnv with {config['env_config']}")
            while not done:
                action, state_out, _ = algo.compute_single_action(obs, state)
                next_obs, reward, done, _, _ = env.step(action)
                print(f"Obs: {obs}, Action: {action}, Reward: {reward}")
                obs = next_obs
                total_reward += reward
                state = [
                    np.concatenate([state[i], [state_out[i]]], axis=0)[1:]
                    for i in range(num_transformers)
                ]
            print(f"Total reward in test episode: {total_reward}")

    # Run with Tune for auto env and algorithm creation and TensorBoard.
    else:
        tuner = tune.Tuner(
            args.run,
            param_space=config.to_dict(),
            run_config=air.RunConfig(stop=stop, verbose=2),
        )
        results = tuner.fit()

        if args.as_test:
            print("Checking if learning goals were achieved")
            check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
