import unittest

import ray.rllib.agents.ddpg as ddpg
from ray.rllib.utils.framework import try_import_tf

tf = try_import_tf()


class TestDDPG(unittest.TestCase):
    def test_ddpg_compilation(self):
        """Test whether a DDPGTrainer can be built with both frameworks."""
        config = ddpg.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        #config["eager"] = True
        config["exploration"] = {
            "type": "GaussianActionNoise",
            "initial_stddev": 0.1,
            "final_stddev": 0.02,
            "timesteps": 10000,
        }

        # tf.
        trainer = ddpg.DDPGTrainer(config=config, env="Pendulum-v0")

        num_iterations = 2
        for i in range(num_iterations):
            results = trainer.train()
            print(results)
