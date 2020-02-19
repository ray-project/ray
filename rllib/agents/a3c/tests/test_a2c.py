import unittest

import ray
from ray.rllib.agents.a3c import a2c_pipeline


class TestA2C(unittest.TestCase):
    """Sanity tests for A2CPipeline."""

    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()

    def test_a2c_pipeline(ray_start_regular):
        trainer = a2c_pipeline.A2CPipeline(
            env="CartPole-v0", config={"min_iter_time_s": 0})
        assert isinstance(trainer.train(), dict)

    def test_a2c_pipeline_save_restore(ray_start_regular):
        trainer = a2c_pipeline.A2CPipeline(
            env="CartPole-v0", config={"min_iter_time_s": 0})
        res1 = trainer.train()
        checkpoint = trainer.save()
        res2 = trainer.train()
        assert res2["timesteps_total"] > res1["timesteps_total"], (res1, res2)
        trainer.restore(checkpoint)

        # Should restore the timesteps counter to the same as res2.
        res3 = trainer.train()
        assert res3["timesteps_total"] == res2["timesteps_total"], (res2, res3)

    def test_a2c_pipeline_microbatch(ray_start_regular):
        trainer = a2c_pipeline.A2CPipeline(
            env="CartPole-v0",
            config={
                "min_iter_time_s": 0,
                "microbatch_size": 10
            })
        assert isinstance(trainer.train(), dict)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
