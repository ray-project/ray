"""
In this example, we train a reinforcement learning model and serve it
using Ray Serve.

We then instantiate an environment and step through it by querying the served model
for actions via HTTP.
"""
import gym
import numpy as np
import requests

from ray.ml.checkpoint import Checkpoint
from ray.ml.examples.rl_example import train_rl_ppo_online
from ray.ml.predictors.integrations.rl.rl_predictor import RLPredictor
from ray.serve.model_wrappers import ModelWrapperDeployment
from ray import serve


def serve_rl_model(checkpoint: Checkpoint, name="RLModel") -> str:
    """Serve a RL model and return deployment URI.

    This function will start Ray Serve and deploy a model wrapper
    that loads the RL checkpoint into a RLPredictor.
    """
    serve.start(detached=True)
    deployment = ModelWrapperDeployment.options(name=name)
    deployment.deploy(RLPredictor, checkpoint)
    return deployment.url


def evaluate_served_policy(endpoint_uri: str, num_episodes: int = 3) -> list:
    """Evaluate a served RL policy on a local environment.

    This function will create an RL environment and step through it.
    To obtain the actions, it will query the deployed RL model.
    """
    env = gym.make("CartPole-v0")

    rewards = []
    for i in range(num_episodes):
        obs = env.reset()
        reward = 0.0
        done = False
        while not done:
            action = query_action(endpoint_uri, obs)
            obs, r, done, _ = env.step(action)
            reward += r
        rewards.append(reward)

    return rewards


def query_action(endpoint_uri: str, obs: np.ndarray):
    """Perform inference on a served RL model.

    This will send a HTTP request to the Ray Serve endpoint of the served
    RL policy model and return the result.
    """
    action_dict = requests.post(endpoint_uri, json={"array": obs.tolist()}).json()
    return action_dict


if __name__ == "__main__":
    num_workers = 2
    use_gpu = False

    # See `rl_example.py` for this minimal online learning example
    result = train_rl_ppo_online(num_workers=num_workers, use_gpu=use_gpu)

    # Serve the model
    endpoint_uri = serve_rl_model(result.checkpoint)

    # Evaluate it on a local environment
    rewards = evaluate_served_policy(endpoint_uri=endpoint_uri)

    # Shut down serve
    serve.shutdown()

    print("Episode rewards:", rewards)
