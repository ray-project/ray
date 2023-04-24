import unittest

import ray
import ray.rllib.algorithms.leela_chess_zero.leela_chess_zero as lz
from ray.rllib.algorithms.leela_chess_zero.leela_chess_zero_model import (
    LeelaChessZeroModel,
)
from ray.rllib.examples.env.pettingzoo_chess import MultiAgentChess
from ray.rllib.utils.test_utils import (
    check_train_results,
    framework_iterator,
)


class TestLeelaChessZero(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_leela_chess_zero_compilation(self):
        """Test whether LeelaChessZero can be built with PyTorch frameworks."""
        config = (
            lz.LeelaChessZeroConfig()
            .environment(env=MultiAgentChess)
            .training(
                sgd_minibatch_size=256,
                train_batch_size=256,
                num_sgd_iter=1,
                model={"custom_model": LeelaChessZeroModel, "max_seq_len": 200},
                mcts_config={"num_simulations": 2},
            )
            .resources(num_gpus=0)
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


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
