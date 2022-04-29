import gym
import numpy as np
import requests

from ray.ml.checkpoint import Checkpoint
from ray.ml.examples.rl_example import train_rl_ppo_online
from ray.ml.predictors.integrations.rl.rl_predictor import RLPredictor
from ray.serve.model_wrappers import ModelWrapperDeployment
from ray import serve


def serve_rl_model(checkpoint: Checkpoint, name="RLModel") -> str:
    serve.start(detached=True)
    deployment = ModelWrapperDeployment.options(name=name)
    deployment.deploy(RLPredictor, checkpoint)
    return deployment.url


def evaluate_served_policy(endpoint_uri: str, num_episodes: int = 3) -> list:
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
    action_dict = requests.post(endpoint_uri, json={"array": obs.tolist()}).json()
    return action_dict


if __name__ == "__main__":
    num_workers = 2
    use_gpu = False

    # See `rl_example.py` for this minimal online learning example
    result = train_rl_ppo_online(num_workers=num_workers, use_gpu=use_gpu)

    endpoint_uri = serve_rl_model(result.checkpoint)
    rewards = evaluate_served_policy(endpoint_uri=endpoint_uri)
    serve.shutdown()

    print("Episode rewards:", rewards)
