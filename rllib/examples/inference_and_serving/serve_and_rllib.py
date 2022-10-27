"""This example script shows how one can use Ray Serve to serve an already
trained RLlib Policy (and its model) to serve action computations.

For a complete tutorial, also see:
https://docs.ray.io/en/master/serve/tutorials/rllib.html
"""

import argparse
import gym
import requests
from starlette.requests import Request

import ray
import ray.rllib.algorithms.dqn as dqn
from ray.rllib.env.wrappers.atari_wrappers import FrameStack, WarpFrame
from ray import serve

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="tf",
    help="The DL framework specifier.",
)
parser.add_argument("--train-iters", type=int, default=1)
parser.add_argument("--no-render", action="store_true")

args = parser.parse_args()


class ServeRLlibPolicy:
    """Callable class used by Ray Serve to handle async requests.

    All the necessary serving logic is implemented in here:
    - Creation and restoring of the (already trained) RLlib Trainer.
    - Calls to algo.compute_action upon receiving an action request
      (with a current observation).
    """

    def __init__(self, config, checkpoint_path):
        # Create the Trainer.
        self.algo = dqn.DQN(config=config)
        # Load an already trained state for the algo.
        self.algo.restore(checkpoint_path)

    async def __call__(self, request: Request):
        json_input = await request.json()

        # Compute and return the action for the given observation.
        obs = json_input["observation"]
        action = self.algo.compute_single_action(obs)

        return {"action": int(action)}


def train_rllib_policy(config):
    """Trains a DQN on MsPacman-v0 for n iterations.

    Saves the trained Trainer to disk and returns the checkpoint path.

    Returns:
        str: The saved checkpoint to restore DQN from.
    """
    # Create algorithm from config.
    algo = dqn.DQN(config=config)

    # Train for n iterations, then save.
    for _ in range(args.train_iters):
        print(algo.train())
    return algo.save()


if __name__ == "__main__":

    # Config for the served RLlib Policy/Trainer.
    config = {
        "framework": args.framework,
        # local mode -> local env inside Trainer not needed!
        "num_workers": 0,
        "env": "MsPacman-v0",
    }

    # Train the policy for some time, then save it and get the checkpoint path.
    checkpoint_path = train_rllib_policy(config)

    ray.init(num_cpus=8)

    # Start Ray serve (create the RLlib Policy service defined by
    # our `ServeRLlibPolicy` class above).
    client = serve.start()
    client.create_backend("backend", ServeRLlibPolicy, config, checkpoint_path)
    client.create_endpoint(
        "endpoint", backend="backend", route="/mspacman-rllib-policy"
    )

    # Create the environment that we would like to receive
    # served actions for.
    env = FrameStack(WarpFrame(gym.make("MsPacman-v0"), 84), 4)
    obs = env.reset()

    while True:
        print("-> Requesting action for obs ...")
        # Send a request to serve.
        resp = requests.get(
            "http://localhost:8000/mspacman-rllib-policy",
            json={"observation": obs.tolist()},
        )
        response = resp.json()
        print("<- Received response {}".format(response))

        # Apply the action in the env.
        action = response["action"]
        obs, reward, done, _ = env.step(action)

        # If episode done -> reset to get initial observation of new episode.
        if done:
            obs = env.reset()

        # Render if necessary.
        if not args.no_render:
            env.render()
