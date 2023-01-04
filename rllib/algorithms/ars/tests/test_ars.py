import os
import unittest

import ray
import ray.rllib.algorithms.ars as ars
from ray.rllib.utils.test_utils import framework_iterator, check_compute_single_action


class TestARS(unittest.TestCase):
    num_gpus = float(os.environ.get("RLLIB_NUM_GPUS", "0"))

    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=3 if not cls.num_gpus else None)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_ars_compilation(self):
        """Test whether an ARSAlgorithm can be built on all frameworks."""
        config = (
            ars.ARSConfig()
            # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
            .resources(num_gpus=self.num_gpus)
            # Keep it simple.
            .training(
                model={
                    "fcnet_hiddens": [10],
                    "fcnet_activation": None,
                },
                noise_size=2500000,
            )
            # Test eval workers ("normal" WorkerSet, unlike ARS' list of
            # RolloutWorkers used for collecting train batches).
            .evaluation(evaluation_interval=1, evaluation_num_workers=1)
        )

        num_iterations = 2

        for _ in framework_iterator(config):
            algo = config.build(env="CartPole-v1")
            for i in range(num_iterations):
                results = algo.train()
                print(results)

            check_compute_single_action(algo)
            algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
