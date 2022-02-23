import unittest

import ray
import ray.rllib.agents.slateq as slateq
from ray.rllib.examples.env.recommender_system_envs_with_recsim import (
    InterestEvolutionRecSimEnv,
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
        """Test whether a SlateQTrainer can be built with both frameworks."""
        config = {
            "env": InterestEvolutionRecSimEnv,
            "learning_starts": 1000,
        }

        num_iterations = 1

        for _ in framework_iterator(config, with_eager_tracing=True):
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
