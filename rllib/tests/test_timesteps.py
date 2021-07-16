import numpy as np
import unittest

import ray
import ray.rllib.agents.pg as pg
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.utils.test_utils import framework_iterator


class TestTimeSteps(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_timesteps(self):
        """Test whether a PGTrainer can be built with both frameworks."""
        config = pg.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["model"]["fcnet_hiddens"] = [1]
        config["model"]["fcnet_activation"] = None

        obs = np.array(1)
        obs_one_hot = np.array([[0.0, 1.0]])

        for _ in framework_iterator(config):
            trainer = pg.PGTrainer(config=config, env=RandomEnv)
            policy = trainer.get_policy()

            for i in range(1, 21):
                trainer.compute_single_action(obs)
                self.assertEqual(policy.global_timestep, i)
            for i in range(1, 21):
                policy.compute_actions(obs_one_hot)
                self.assertEqual(policy.global_timestep, i + 20)

            # Artificially set ts to 100Bio, then keep computing actions and
            # train.
            crazy_timesteps = int(1e11)
            policy.global_timestep = crazy_timesteps
            # Run for 10 more ts.
            for i in range(1, 11):
                policy.compute_actions(obs_one_hot)
                self.assertEqual(policy.global_timestep, i + crazy_timesteps)
            trainer.train()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
