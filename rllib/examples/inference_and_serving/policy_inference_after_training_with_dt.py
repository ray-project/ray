"""
Example showing how you can use your trained Decision Transformer (DT) policy for
inference (computing actions) in an environment.
"""
import argparse
from pathlib import Path

import gymnasium as gym
import os

import ray
from ray import air, tune
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.dt import DTConfig
from ray.tune.utils.log import Verbosity

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-cpus", type=int, default=0)
    parser.add_argument(
        "--input-files",
        nargs="+",
        default=[
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "../../tests/data/cartpole/large.json",
            )
        ],
        help="List of paths to offline json files/zips for training.",
    )
    parser.add_argument(
        "--num-episodes-during-inference",
        type=int,
        default=10,
        help="Number of episodes to do inference over after training.",
    )

    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None)

    # Bazel makes it hard to find files specified in `args` (and `data`).
    # Look for them here.
    input_files = []
    for input_file in args.input_files:
        if not os.path.exists(input_file):
            # This script runs in the ray/rllib/examples/inference_and_serving dir.
            rllib_dir = Path(__file__).parent.parent.parent
            input_dir = rllib_dir.absolute().joinpath(input_file)
            input_files.append(str(input_dir))
        else:
            input_files.append(input_file)

    # Get max_ep_len
    env = gym.make("CartPole-v1")
    max_ep_len = env.spec.max_episode_steps
    env.close()

    # Training config
    config = (
        DTConfig()
        .environment(
            env="CartPole-v1",
            clip_actions=False,
            normalize_actions=False,
        )
        .framework("torch")
        .offline_data(
            input_="dataset",
            input_config={
                "format": "json",
                "paths": input_files,
            },
            actions_in_input_normalized=True,
        )
        .training(
            horizon=max_ep_len,  # This needs to be specified for DT to work.
            lr=0.01,
            optimizer={
                "weight_decay": 0.1,
                "betas": [0.9, 0.999],
            },
            train_batch_size=512,
            replay_buffer_config={
                "capacity": 20,
            },
            model={
                "max_seq_len": 3,
            },
            num_layers=1,
            num_heads=1,
            embed_dim=64,
        )
        # Need to do evaluation rollouts for stopping condition.
        .evaluation(
            target_return=200.0,
            evaluation_interval=1,
            evaluation_num_workers=1,
            evaluation_duration=10,
            evaluation_duration_unit="episodes",
            evaluation_parallel_to_training=False,
            evaluation_config=DTConfig.overrides(input_="sampler", explore=False),
        )
        .rollouts(
            num_rollout_workers=0,
        )
        .reporting(
            min_train_timesteps_per_iteration=5000,
        )
        .resources(
            # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
            num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        )
    )
    config = config.to_dict()

    # Configure when to stop training
    # Note that for an offline RL algorithm, we don't do training rollouts,
    # instead we have to rely on evaluation rollouts.
    stop = {
        "evaluation/episode_reward_mean": 200.0,
        "training_iteration": 100,
    }

    print("Training policy until desired reward/iterations. ...")
    tuner = tune.Tuner(
        "DT",
        param_space=config,
        run_config=air.RunConfig(
            stop=stop,
            verbose=Verbosity.V3_TRIAL_DETAILS,
            checkpoint_config=air.CheckpointConfig(
                checkpoint_frequency=1,
                checkpoint_at_end=True,
            ),
        ),
    )
    results = tuner.fit()

    print("Training completed. Restoring new Algorithm for action inference.")
    # Get the last checkpoint from the above training run.
    checkpoint = results.get_best_result().checkpoint
    # Create new Algorithm and restore its state from the last checkpoint.
    algo = Algorithm.from_checkpoint(checkpoint)

    # Create the env to do inference in.
    env = gym.make("CartPole-v1")

    obs, info = env.reset()
    input_dict = algo.get_initial_input_dict(obs)

    num_episodes = 0
    total_rewards = 0.0

    while num_episodes < args.num_episodes_during_inference:
        # Compute an action (`a`).
        a, _, extra = algo.compute_single_action(input_dict=input_dict)
        # Send the computed action `a` to the env.
        obs, reward, terminated, truncated, _ = env.step(a)
        # Add to total rewards.
        total_rewards += reward
        # Is the episode `done`? -> Reset.
        if terminated or truncated:
            print(f"Episode {num_episodes+1} - return: {total_rewards}")
            obs, info = env.reset()
            input_dict = algo.get_initial_input_dict(obs)
            num_episodes += 1
            total_rewards = 0.0
        # Episode is still ongoing -> Continue.
        else:
            input_dict = algo.get_next_input_dict(
                input_dict,
                a,
                reward,
                obs,
                extra,
            )

    env.close()
    ray.shutdown()
