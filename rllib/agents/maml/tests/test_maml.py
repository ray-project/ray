import unittest

import ray
import ray.rllib.agents.maml as maml
from ray.rllib.utils.test_utils import check_compute_single_action, \
    check_train_results, framework_iterator


class TestMAML(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_maml_compilation(self):
        """Test whether a MAMLTrainer can be built with all frameworks."""
        config = maml.DEFAULT_CONFIG.copy()
        config["num_workers"] = 1
        config["horizon"] = 200
        num_iterations = 1

        # Test for tf framework (torch not implemented yet).
        for fw in framework_iterator(config, frameworks=("tf", "torch")):
            for env in [
                    "pendulum_mass.PendulumMassEnv",
                    "cartpole_mass.CartPoleMassEnv"
            ]:
                if fw == "tf" and env.startswith("cartpole"):
                    continue
                print("env={}".format(env))
                env_ = "ray.rllib.examples.env.{}".format(env)
                trainer = maml.MAMLTrainer(config=config, env=env_)
                for i in range(num_iterations):
                    results = trainer.train()
                    check_train_results(results)
                    print(results)
                check_compute_single_action(
                    trainer, include_prev_action_reward=True)
                trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
