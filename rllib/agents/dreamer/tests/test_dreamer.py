import unittest

import ray
from ray import tune
import ray.rllib.agents.dreamer as dreamer
from ray.rllib.examples.env.dm_control_suite import hopper_hop
from ray.rllib.utils.test_utils import check_compute_single_action, \
    framework_iterator


class TestDreamer(unittest.TestCase):
    """Sanity tests for DreamerTrainer."""

    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()

    def test_dreamer_compilation(self):
        """Test whether an DreamerTrainer can be built with all frameworks."""
        config = dreamer.DEFAULT_CONFIG.copy()
        tune.register_env("dm_control_hopper_hop", lambda _: hopper_hop())

        num_iterations = 1

        # Test against all frameworks.
        for _ in framework_iterator(config, frameworks="torch"):
            for env in ["dm_control_hopper_hop"]:
                trainer = dreamer.DREAMERTrainer(config=config, env=env)
                for i in range(num_iterations):
                    results = trainer.train()
                    print(results)
                check_compute_single_action(trainer)
                trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
