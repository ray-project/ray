import numpy as np
import unittest

import ray
from ray.rllib.agents.a3c import a2c_workflow


class TestA2C(unittest.TestCase):
    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()

    def test_a2c_workflow(ray_start_regular):
        """Sanity test for A2CWorkflow."""
        trainer = a2c_workflow.A2CWorkflow(env="CartPole-v0")
        assert isinstance(trainer.train(), dict)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
