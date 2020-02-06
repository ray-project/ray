import unittest

import ray.rllib.agents.ddpg as ddpg
from ray.rllib.utils.framework import try_import_tf

tf = try_import_tf()


class TestDDPG(unittest.TestCase):
    def test_ddpg_compilation(self):
        """Test whether a DDPGTrainer can be built with both frameworks."""
        config = ddpg.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["eager"] = True

        # tf.
        trainer = ddpg.DDPGTrainer(config=config, env="Pendulum-v0")

        num_iterations = 2
        for i in range(num_iterations):
            results = trainer.train()
            print(results)
