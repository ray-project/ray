import unittest

import ray
import ray.rllib.algorithms.leela_zero.leela_zero as lz
from ray.rllib.algorithms.leela_zero.leela_zero_model import LeelaZeroModel
from ray.rllib.examples.env.pettingzoo_chess import MultiAgentChess
from ray.rllib.utils.test_utils import (
    check_train_results,
    framework_iterator,
)
from ray.tune.registry import register_env


class TestAlphaZero(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_leela_zero_compilation(self):
        """Test whether LeelaZero can be built with PyTorch frameworks."""
        register_env('ChessMultiAgent', lambda config: MultiAgentChess())
        config = (
            lz.LeelaZeroConfig()
            .environment(env="ChessMultiAgent")
            .training(model={"custom_model": LeelaZeroModel})
            .resources(num_gpus = 0)
        )
        num_iterations = 1
        # Only working for torch right now.
        for _ in framework_iterator(config, frameworks="torch"):
            algo = config.build()
            for i in range(num_iterations):
                results = algo.train()
                check_train_results(results)
                print(results)
            algo.stop()
        assert True


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
