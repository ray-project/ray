import numpy as np
import unittest

import ray
import ray.rllib.algorithms.pg as pg
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.utils.test_utils import check, framework_iterator


class TestTimeSteps(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_timesteps(self):
        """Test whether PG can be built with both frameworks."""
        config = pg.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["model"]["fcnet_hiddens"] = [1]
        config["model"]["fcnet_activation"] = None

        obs = np.array(1)
        obs_batch = np.array([1])

        for _ in framework_iterator(config):
            algo = pg.PG(config=config, env=RandomEnv)
            policy = algo.get_policy()

            for i in range(1, 21):
                algo.compute_single_action(obs)
                check(policy.global_timestep, i)
            for i in range(1, 21):
                policy.compute_actions(obs_batch)
                check(policy.global_timestep, i + 20)

            # Artificially set ts to 100Bio, then keep computing actions and
            # train.
            crazy_timesteps = int(1e11)
            policy.on_global_var_update({"timestep": crazy_timesteps})
            # Run for 10 more ts.
            for i in range(1, 11):
                policy.compute_actions(obs_batch)
                check(policy.global_timestep, i + crazy_timesteps)
            algo.train()
            algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
