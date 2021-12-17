import unittest

import ray
import ray.rllib.agents.alpha_star as alpha_star
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.utils.test_utils import check_compute_single_action, \
    check_train_results, framework_iterator


class Testalpha_star(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True)#TODO

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_alpha_star_compilation(self):
        """Test whether a AlphaStarTrainer can be built with all frameworks."""
        config = alpha_star.DEFAULT_CONFIG.copy()
        config["num_workers"] = 2
        config["env"] = MultiAgentCartPole
        config["env_config"] = {
            "num_agents": 2,
        }
        num_iterations = 2

        for _ in framework_iterator(config, frameworks="tf2"):#TODO
            _config = config.copy()
            trainer = alpha_star.AlphaStarTrainer(config=_config)
            for i in range(num_iterations):
                results = trainer.train()
                check_train_results(results)
                print(results)
            check_compute_single_action(trainer)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
