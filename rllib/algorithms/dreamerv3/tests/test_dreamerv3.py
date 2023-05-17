import unittest

import ray
from ray.rllib.algorithms.dreamerv3 import dreamerv3
from ray.rllib.utils.test_utils import framework_iterator


class TestDreamerV3(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_dreamerv3_compilation(self):
        """Test whether DreamerV3 can be built with all frameworks."""

        # Build a DreamerV3Config object.
        config = (
            dreamerv3.DreamerV3Config()
            .training(
                # TODO (sven): Fix having to provide this.
                #  Should be compiled by AlgorithmConfig?
                model={
                    "batch_size_B": 16,
                    "batch_length_T": 64,
                    "horizon_H": 15,
                    "model_dimension": "XS",
                    "gamma": 0.997,
                    "training_ratio": 512,
                    "symlog_obs": True,
                },
                _enable_learner_api=True,
            )
            .resources(
                num_learner_workers=0,
                num_cpus_per_learner_worker=1,
                num_gpus_per_learner_worker=0,
                num_gpus=0,
            )
            .rl_module(_enable_rl_module_api=True)
        )

        num_iterations = 2

        for _ in framework_iterator(
            config, frameworks="tf2"
        ):  # , with_eager_tracing=True):
            for env in ["ALE/MsPacman-v5", "CartPole-v1", "FrozenLake-v1"]:
                print("Env={}".format(env))
                config.environment(env)
                algo = config.build()

                for i in range(num_iterations):
                    results = algo.train()
                    print(results)

                algo.stop()

    def test_dreamerv3_model_sizes(self):
        """Tests whether the different model sizes match the ones in the paper."""
        pass


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
