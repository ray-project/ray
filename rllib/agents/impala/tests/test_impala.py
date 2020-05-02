import unittest

import ray
import ray.rllib.agents.impala as impala
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import framework_iterator

tf = try_import_tf()


class TestIMPALA(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_impala_compilation(self):
        """Test whether an ImpalaTrainer can be built with both frameworks."""
        config = impala.DEFAULT_CONFIG.copy()
        num_iterations = 1

        for _ in framework_iterator(config, frameworks=("torch", "tf")):
            local_cfg = config.copy()
            for env in ["CartPole-v0"]:
                # Test w/o LSTM.
                trainer = impala.ImpalaTrainer(config=local_cfg, env=env)
                for i in range(num_iterations):
                    print(trainer.train())

                # Test w/ LSTM.
                local_cfg["model"]["use_lstm"] = True
                trainer = impala.ImpalaTrainer(config=local_cfg, env=env)
                for i in range(num_iterations):
                    print(trainer.train())


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
