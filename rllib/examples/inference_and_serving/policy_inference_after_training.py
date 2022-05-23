"""
Example showing how you can use your trained policy for inference
(computing actions) in an environment.

Includes options for LSTM-based models (--use-lstm), attention-net models
(--use-attention), and plain (non-recurrent) models.
"""
import argparse
import gym
import os

import ray
from ray import tune
from ray.rllib.agents.registry import get_trainer_class

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run", type=str, default="PPO", help="The RLlib-registered algorithm to use."
)
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "tfe", "torch"],
    default="tf",
    help="The DL framework specifier.",
)
parser.add_argument("--eager-tracing", action="store_true")
parser.add_argument(
    "--stop-iters",
    type=int,
    default=200,
    help="Number of iterations to train before we do inference.",
)
parser.add_argument(
    "--stop-timesteps",
    type=int,
    default=100000,
    help="Number of timesteps to train before we do inference.",
)
parser.add_argument(
    "--stop-reward",
    type=float,
    default=150.0,
    help="Reward at which we stop training before we do inference.",
)
parser.add_argument(
    "--explore-during-inference",
    action="store_true",
    help="Whether the trained policy should use exploration during action "
    "inference.",
)
parser.add_argument(
    "--num-episodes-during-inference",
    type=int,
    default=10,
    help="Number of episodes to do inference over after training.",
)

if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None)

    config = {
        "env": "FrozenLake-v1",
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

    print("Training policy until desired reward/timesteps/iterations. ...")
    results = tune.run(
        args.run,
        config=config,
        stop=stop,
        verbose=2,
        checkpoint_freq=1,
        checkpoint_at_end=True,
    )

    print("Training completed. Restoring new Trainer for action inference.")
    # Get the last checkpoint from the above training run.
    checkpoint = results.get_last_checkpoint()
    # Create new Trainer and restore its state from the last checkpoint.
    trainer = get_trainer_class(args.run)(config=config)
    trainer.restore(checkpoint)

    # Create the env to do inference in.
    env = gym.make("FrozenLake-v1")
    obs = env.reset()

    num_episodes = 0
    episode_reward = 0.0

    while num_episodes < args.num_episodes_during_inference:
        # Compute an action (`a`).
        a = trainer.compute_single_action(
            observation=obs,
            explore=args.explore_during_inference,
            policy_id="default_policy",  # <- default value
        )
        # Send the computed action `a` to the env.
        obs, reward, done, _ = env.step(a)
        episode_reward += reward
        # Is the episode `done`? -> Reset.
        if done:
            print(f"Episode done: Total reward = {episode_reward}")
            obs = env.reset()
            num_episodes += 1
            episode_reward = 0.0

    ray.shutdown()
