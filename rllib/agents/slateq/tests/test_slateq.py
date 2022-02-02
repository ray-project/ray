import unittest

import ray
import ray.rllib.agents.slateq as slateq
from ray.rllib.examples.env.recsim_recommender_system_envs import (
    LongTermSatisfactionRecSimEnv,
)
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)


class TestSlateQ(unittest.TestCase):
    """Sanity tests for Slateq algorithm."""

    def setUp(self):
        ray.init(num_cpus=4)

    def tearDown(self):
        ray.shutdown()

    def test_slateq_compilation(self):
        """Test whether an A2CTrainer can be built with both frameworks."""
        config = {
            "env": LongTermSatisfactionRecSimEnv,
        }

        num_iterations = 1

        # Test only against torch (no other frameworks supported so far).
        for _ in framework_iterator(config, frameworks="torch"):
            trainer = slateq.SlateQTrainer(config=config)
            for i in range(num_iterations):
                results = trainer.train()
                check_train_results(results)
                print(results)
            check_compute_single_action(trainer)
            trainer.stop()

    def test_slateq_loss_function(self):
        pass


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
