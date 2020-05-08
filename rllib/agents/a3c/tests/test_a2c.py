import unittest

import ray
from ray.rllib.agents.a3c import A2CTrainer
from ray.rllib.utils.test_utils import check_compute_action


class TestA2C(unittest.TestCase):
    """Sanity tests for A2C exec impl."""

    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()

    def test_a2c_exec_impl(ray_start_regular):
        trainer = A2CTrainer(
            env="CartPole-v0",
            config={
                "min_iter_time_s": 0,
                "use_exec_api": True
            })
        assert isinstance(trainer.train(), dict)
        check_compute_action(trainer)

    def test_a2c_exec_impl_microbatch(ray_start_regular):
        trainer = A2CTrainer(
            env="CartPole-v0",
            config={
                "min_iter_time_s": 0,
                "microbatch_size": 10,
                "use_exec_api": True,
            })
        assert isinstance(trainer.train(), dict)
        check_compute_action(trainer)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
