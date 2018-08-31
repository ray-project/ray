import unittest

from ray.rllib.agents.ppo import PPOAgent, DEFAULT_CONFIG
import ray

class ModelCatalogTest(unittest.TestCase):
    def testLocal(self):
        ray.init(local_mode=True)
        cf = DEFAULT_CONFIG.copy()
        agent = PPOAgent(cf, "CartPole-v0")
        print(agent.train())

if __name__ == "__main__":
    unittest.main(verbosity=2)
