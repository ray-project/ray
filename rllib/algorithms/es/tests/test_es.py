import numpy as np
import os
import unittest

import ray
import ray.rllib.algorithms.es as es
from ray.rllib.utils.test_utils import check_compute_single_action, framework_iterator


class TestES(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_es_compilation(self):
        """Test whether an ESAlgorithm can be built on all frameworks."""
        config = (
            es.ESConfig()
            .resources(
                # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
                num_gpus=float(os.environ.get("RLLIB_NUM_GPUS", "0"))
            )
            # Keep it simple.
            .training(
                model={
                    "fcnet_hiddens": [10],
                    "fcnet_activation": None,
                },
                noise_size=2500000,
                episodes_per_batch=10,
                train_batch_size=100,
            )
            .rollouts(num_rollout_workers=1)
            # Test eval workers ("normal" WorkerSet, unlike ES' list of
            # RolloutWorkers used for collecting train batches).
            .evaluation(evaluation_interval=1, evaluation_num_workers=2)
        )

        num_iterations = 1

        for _ in framework_iterator(config):
            for env in ["CartPole-v1", "Pendulum-v1"]:
                algo = config.build(env=env)
                for i in range(num_iterations):
                    results = algo.train()
                    print(results)

                check_compute_single_action(algo)
                algo.stop()
        ray.shutdown()

    def test_es_weights(self):
        """Test whether an ESAlgorithm can be built on all frameworks."""
        config = (
            es.ESConfig()
            .resources(
                # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
                num_gpus=float(os.environ.get("RLLIB_NUM_GPUS", "0"))
            )
            # Keep it simple.
            .training(
                model={
                    "fcnet_hiddens": [10],
                    "fcnet_activation": None,
                },
                noise_size=2500000,
                episodes_per_batch=10,
                train_batch_size=100,
            )
            .rollouts(num_rollout_workers=1)
        )

        for _ in framework_iterator(config):
            algo = config.build(env="CartPole-v1")

            weights = np.zeros_like(algo.get_weights())
            algo.set_weights(weights=weights)
            new_weights = algo.get_weights()

            self.assertTrue(np.array_equal(weights, new_weights))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
