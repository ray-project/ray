from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import ray
from ray.rllib.agents.dqn import DQNTrainer
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.visionnet_v2 import VisionNetwork

class DQNCustomModelTest(unittest.TestCase):
    def testDQNCustomModelTest(self):
        ModelCatalog.register_custom_model("my_model", VisionNetwork)
        config = {'model': {
            "custom_model": "my_model",
            "custom_options": {},  # extra options to pass to your model
        }}
        ray.init()
        agent = DQNTrainer(config=config, env="BreakoutNoFrameskip-v4")
        print(agent.train())
if __name__ == "__main__":
    unittest.main(verbosity=2)
