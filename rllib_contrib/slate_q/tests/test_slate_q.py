import unittest

import rllib_slate_q.slate_q.slateq as slateq
from rllib_slate_q.env.recommender_system_envs_with_recsim import (
    InterestEvolutionRecSimEnv,
)

import ray
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)


class TestSlateQ(unittest.TestCase):
    """Sanity tests for Slateq algorithm."""

    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()

    def test_slateq_compilation(self):
        """Test whether SlateQ can be built with both frameworks."""
        config = (
            slateq.SlateQConfig()
            .environment(env=InterestEvolutionRecSimEnv)
            .training(num_steps_sampled_before_learning_starts=1000)
        )

        num_iterations = 1

        for _ in framework_iterator(config, with_eager_tracing=True):
            algo = config.build()
            for i in range(num_iterations):
                results = algo.train()
                check_train_results(results)
                print(results)
            check_compute_single_action(algo)
            algo.stop()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
