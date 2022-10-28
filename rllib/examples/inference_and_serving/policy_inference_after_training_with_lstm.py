"""
Example showing how you can use your trained policy for inference
(computing actions) in an environment.

Includes options for LSTM-based models (--use-lstm), attention-net models
(--use-attention), and plain (non-recurrent) models.
"""
import argparse
import gym
import numpy as np
import os

import ray
from ray import air, tune
from ray.rllib.algorithms.registry import get_algorithm_class

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run", type=str, default="PPO", help="The RLlib-registered algorithm to use."
)
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="tf",
    help="The DL framework specifier.",
)
parser.add_argument(
    "--eager-tracing",
    action="store_true",
    help="Use tf eager tracing to speed up execution in tf2.x. Only supported"
    " for `framework=tf2`.",
)
parser.add_argument(
    "--prev-action",
    action="store_true",
    help="Feed most recent action to the LSTM as part of its input.",
)
parser.add_argument(
    "--prev-reward",
    action="store_true",
    help="Feed most recent reward to the LSTM as part of its input.",
)
parser.add_argument(
    "--stop-iters",
    type=int,
    default=2,
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
    default=0.8,
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
        "model": {
            "use_lstm": True,
            "lstm_cell_size": 256,
            "lstm_use_prev_action": args.prev_action,
            "lstm_use_prev_reward": args.prev_reward,
        },
        "framework": args.framework,
        # Run with tracing enabled for tf2?
        "eager_tracing": args.eager_tracing,
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    print("Training policy until desired reward/timesteps/iterations. ...")
    tuner = tune.Tuner(
        args.run,
        param_space=config,
        run_config=air.RunConfig(
            stop=stop,
            verbose=2,
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
    algo = get_algorithm_class(args.run)(config=config)
    algo.restore(checkpoint)

    # Create the env to do inference in.
    env = gym.make("FrozenLake-v1")
    obs = env.reset()

    # In case the model needs previous-reward/action inputs, keep track of
    # these via these variables here (we'll have to pass them into the
    # compute_actions methods below).
    init_prev_a = prev_a = None
    init_prev_r = prev_r = None

    # Set LSTM's initial internal state.
    lstm_cell_size = config["model"]["lstm_cell_size"]
    # range(2) b/c h- and c-states of the LSTM.
    init_state = state = [np.zeros([lstm_cell_size], np.float32) for _ in range(2)]
    # Do we need prev-action/reward as part of the input?
    if args.prev_action:
        init_prev_a = prev_a = 0
    if args.prev_reward:
        init_prev_r = prev_r = 0.0

    num_episodes = 0

    while num_episodes < args.num_episodes_during_inference:
        # Compute an action (`a`).
        a, state_out, _ = algo.compute_single_action(
            observation=obs,
            state=state,
            prev_action=prev_a,
            prev_reward=prev_r,
            explore=args.explore_during_inference,
            policy_id="default_policy",  # <- default value
        )
        # Send the computed action `a` to the env.
        obs, reward, done, _ = env.step(a)
        # Is the episode `done`? -> Reset.
        if done:
            obs = env.reset()
            num_episodes += 1
            state = init_state
            prev_a = init_prev_a
            prev_r = init_prev_r
        # Episode is still ongoing -> Continue.
        else:
            state = state_out
            if init_prev_a is not None:
                prev_a = a
            if init_prev_r is not None:
                prev_r = reward

    ray.shutdown()
