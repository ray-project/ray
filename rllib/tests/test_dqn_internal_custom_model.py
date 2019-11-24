from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import ray
from ray.rllib.agents.dqn import DQNTrainer
from ray.rllib.agents.ppo import PPOTrainer
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.visionnet_v2 import VisionNetwork
from ray.rllib.models.tf.fcnet_v2 import FullyConnectedNetwork

class DQNCustomModelTest(unittest.TestCase):
    def TestDQNCustomModelTest(self, cls):
        for i, model in enumerate([VisionNetwork, FullyConnectedNetwork]):
            ModelCatalog.register_custom_model("my_model" + str(i), model)
            config = {"model": {"custom_model": "my_model" + str(i)}}
            agent = cls(config=config, env="BreakoutNoFrameskip-v4")
            print(agent.train())

    def testDQNCustomModelTestDQN(self):
        self.TestDQNCustomModelTest(DQNTrainer)

    """this test is to make sure the inheritance of DistribtionQModel in
    the custom model examples does not destroy the functionality
    of other policies"""

    def testDQNCustomModelTestPPO(self):
        self.TestDQNCustomModelTest(PPOTrainer)


if __name__ == "__main__":
    ray.init(num_cpus=5)
    unittest.main(verbosity=2)
