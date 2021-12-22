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
        # Multi0agent cartpole with 4 agents (IDs: 0, 1, 2, 3).
        config["env"] = MultiAgentCartPole
        config["env_config"] = {"num_agents": 4}
        # Two GPUs -> 2 policies per GPU.
        config["num_gpus"] = 2
        config["_fake_gpus"] = True
        # Let the algo know about our 4 policies.
        config["multiagent"] = {
            "policies": {"p0", "p1", "p2", "p3"},
            # Agent IDs are 0, 1, 2, 3 (ints).
            "policy_mapping_fn":
                lambda aid, eps, worker, **kw: f"p{aid}"
        }

        num_iterations = 2

        for _ in framework_iterator(config, frameworks="torch"):#TODO
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
