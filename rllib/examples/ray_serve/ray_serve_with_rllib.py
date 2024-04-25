"""This example script shows how one can use Ray Serve in combination with RLlib.

Here, we serve an already trained PyTorch RLModule to provide action computations
to a Ray Serve client.
"""
import argparse
import atexit
import os

import requests
import subprocess
import time

import gymnasium as gym
from pathlib import Path

import ray
from ray.rllib.algorithms.algorithm import AlgorithmConfig
from ray.rllib.algorithms.ppo import PPOConfig

parser = argparse.ArgumentParser()
parser.add_argument("--train-iters", type=int, default=3)
parser.add_argument("--serve-episodes", type=int, default=2)
parser.add_argument("--no-render", action="store_true")


def train_rllib_rl_module(config: AlgorithmConfig, train_iters: int = 1):
    """Trains a PPO (RLModule) on ALE/MsPacman-v5 for n iterations.

    Saves the trained Algorithm to disk and returns the checkpoint path.

    Args:
        config: The algo config object for the Algorithm.
        train_iters: For how many iterations to train the Algorithm.

    Returns:
        str: The saved checkpoint to restore the RLModule from.
    """
    # Create algorithm from config.
    algo = config.build()

    # Train for n iterations, then save, stop, and return the checkpoint path.
    for _ in range(train_iters):
        print(algo.train())

    # TODO (sven): Change this example to only storing the RLModule checkpoint, NOT
    #  the entire Algorithm.
    checkpoint_result = algo.save()

    algo.stop()

    return checkpoint_result.checkpoint


def kill_proc(proc):
    try:
        proc.terminate()  # Send SIGTERM
        proc.wait(timeout=5)  # Wait for process to terminate
    except subprocess.TimeoutExpired:
        proc.kill()  # Send SIGKILL
        proc.wait()  # Ensure process is dead


if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(num_cpus=8)

    # Config for the served RLlib RLModule/Algorithm.
    config = (
        PPOConfig()
        .api_stack(enable_rl_module_and_learner=True)
        .environment("CartPole-v1")
    )

    # Train the Algorithm for some time, then save it and get the checkpoint path.
    checkpoint = train_rllib_rl_module(config, train_iters=args.train_iters)

    path_of_this_file = Path(__file__).parent
    os.chdir(path_of_this_file)
    # Start the serve app with the trained checkpoint.
    serve_proc = subprocess.Popen(
        [
            "serve",
            "run",
            "classes.cartpole_deployment:rl_module",
            f"checkpoint={checkpoint.path}",
        ]
    )
    # Register our `kill_proc` function to be called on exit to stop Ray Serve again.
    atexit.register(kill_proc, serve_proc)
    # Wait a while to make sure the app is ready to serve.
    time.sleep(20)
    print(f"Started Ray Serve with PID: {serve_proc.pid}")

    try:
        # Create the environment that we would like to receive
        # served actions for.
        env = gym.make("CartPole-v1", render_mode="human")
        obs, info = env.reset()

        num_episodes = 0
        episode_return = 0.0

        while num_episodes < args.serve_episodes:
            # Render env if necessary.
            if not args.no_render:
                env.render()

            # print("-> Requesting action for obs ...")
            # Send a request to serve.
            resp = requests.get(
                "http://localhost:8000/rllib-rlmodule",
                json={"observation": obs.tolist()},
                # timeout=5.0,
            )
            response = resp.json()
            # print("<- Received response {}".format(response))

            # Apply the action in the env.
            action = response["action"]
            obs, reward, done, _, _ = env.step(action)
            episode_return += reward

            # If episode done -> reset to get initial observation of new episode.
            if done:
                print(f"Episode R={episode_return}")
                obs, info = env.reset()
                num_episodes += 1
                episode_return = 0.0

    finally:
        # Make sure to kill the process on script termination
        kill_proc(serve_proc)
