import IPython

import ray
from ray.rllib.agents.dqn.dqn import DQNAgent

if __name__ == "__main__":
    ray.init()

    agent = DQNAgent(env='CartPole-v0')

    IPython.embed()
