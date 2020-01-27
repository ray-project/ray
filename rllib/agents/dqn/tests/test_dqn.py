import numpy as np
import unittest

import ray
import ray.rllib.agents.dqn as dqn

class TestPDQN(unittest.TestCase):

    ray.init()

    def test_dqn_compilation(self):
        """Test whether a DQNTrainer can be built with both frameworks."""
        config = dqn.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.

        # tf.
        trainer = dqn.DQNTrainer(config=config, env="CartPole-v0")

        num_iterations = 2
        for i in range(num_iterations):
            trainer.train()

        # Torch.
        #config["use_pytorch"] = True
        #trainer = dqn.DQNTrainer(config=config, env="CartPole-v0")
        #for i in range(num_iterations):
        #    trainer.train()
