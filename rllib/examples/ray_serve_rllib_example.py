import argparse
import gym
from gym.spaces import Box, Discrete, Dict
import os

import ray.rllib.agents.ppo as ppo
from ray.rllib.examples.env.random_env import RandomEnv

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework", choices=["tf2", "tf", "tfe", "torch"], default="tf")

if __name__ == "__main__":
    import ray

    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None)

    config = {
        "framework": args.framework,
        "num_workers": 0,  # only 1 "local" worker with an env (not really used here).
        "num_gpus": 0,  # e.g. 1
    }

    trainer = ppo.PPOTrainer(config=config, env="CartPole-v0")
    for _ in range(2):
        trainer.train()
    trainer.save([some checkpoint])

    # Alternatively: Create a new Trainer here.
    trainer.restore([some checkpoint])

    # Create environment to do inference
    env = gym.make("CartPole-v0")
    # Alternatively.
    env = trainer.get_policy().workers.local_worker().env

    env = RandomEnv(observation_space=Dict({"a": Discrete(2), "b": Box(-1.0, 1.0, (1,))}))

    obs = env.reset()
    # obs = {"a": 1, "b": [1.0]}
    while True:
        # obs = torch.Tensor([0.1, 0.1, 0.1, 0.1])  # <- CartPole shape=(4,)
        action = trainer.compute_action(obs, explore=True)
        # Alternatively:
        # obs_batch = [tensor([0.1, 0.1, 0.1, 0.1]), tensor([0.1, 0.1, 0.1, 0.1])]
        # obs_batch = tensor([[0.1, 0.1, 0.1, 0.1], [0.0, 0.0, 0.0, 0.0]])
        # obs_batch = [{"a": 0, "b": [0.0]}, {"a": 1, "b": [-1.0]}, ...]
        actions = trainer.compute_actions(obs_batch, explore=True)
        # actions =
        obs, reward, done, _ = env.step(action)
        if done:
            obs = env.reset()

    ray.shutdown()
