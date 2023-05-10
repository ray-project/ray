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
        """Test whether DremaerV3 can be built with all frameworks."""

        # Build a DreamerV3Config object.
        config = (
            dreamerv3.DreamerV3Config()
            .training(
                model=dict(),
                _enable_learner_api=True,
            )
            .rl_module(_enable_rl_module_api=True)
        )

        num_iterations = 2

        for _ in framework_iterator(config, frameworks="tf2", with_eager_tracing=True):
            for env in ["FrozenLake-v1", "CartPole-v1", "ALE/MsPacman-v5"]:
                print("Env={}".format(env))
                config.environment(env)
                algo = config.build()

                for i in range(num_iterations):
                    results = algo.train()
                    print(results)

                algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
