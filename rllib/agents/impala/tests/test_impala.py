import unittest

import ray
import ray.rllib.agents.impala as impala
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check_compute_single_action, \
    framework_iterator

tf = try_import_tf()


class TestIMPALA(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_impala_compilation(self):
        """Test whether an ImpalaTrainer can be built with both frameworks."""
        config = impala.DEFAULT_CONFIG.copy()
        num_iterations = 1

        for _ in framework_iterator(config, frameworks=("tf", "torch")):
            local_cfg = config.copy()
            for env in ["Pendulum-v0", "CartPole-v0"]:
                print("Env={}".format(env))
                print("w/o LSTM")
                # Test w/o LSTM.
                local_cfg["model"]["use_lstm"] = False
                local_cfg["num_aggregation_workers"] = 0
                trainer = impala.ImpalaTrainer(config=local_cfg, env=env)
                for i in range(num_iterations):
                    print(trainer.train())
                check_compute_single_action(trainer)
                trainer.stop()

                # Test w/ LSTM.
                print("w/ LSTM")
                local_cfg["model"]["use_lstm"] = True
                local_cfg["num_aggregation_workers"] = 2
                trainer = impala.ImpalaTrainer(config=local_cfg, env=env)
                for i in range(num_iterations):
                    print(trainer.train())
                check_compute_single_action(trainer, include_state=True)
                trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
