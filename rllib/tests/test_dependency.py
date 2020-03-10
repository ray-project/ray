#!/usr/bin/env python

import os
import unittest


class TestDependency(unittest.TestCase):
    def test_dependency(self):
        # Do not import tf for testing purposes.
        os.environ["RLLIB_TEST_NO_TF_IMPORT"] = "1"
    
        from ray.rllib.agents.a3c import A2CTrainer
        assert "tensorflow" not in sys.modules, \
            "TF initially present, when it shouldn't."
    
        # note: no ray.init(), to test it works without Ray
        trainer = A2CTrainer(
            env="CartPole-v0", config={
                "use_pytorch": True,
                "num_workers": 0
            })
        trainer.train()

        assert "tensorflow" not in sys.modules, "TF should not be imported"

        # Clean up.
        del os.environ["RLLIB_TEST_NO_TF_IMPORT"]


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
