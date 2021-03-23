import unittest

import ray
import ray.rllib.agents.impala as impala
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check_compute_single_action, \
    framework_iterator

tf1, tf, tfv = try_import_tf()


class TestIMPALA(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_impala_compilation(self):
        """Test whether an ImpalaTrainer can be built with both frameworks."""
        config = impala.DEFAULT_CONFIG.copy()
        config["model"]["lstm_use_prev_action"] = True
        config["model"]["lstm_use_prev_reward"] = True
        num_iterations = 1
        env = "CartPole-v0"

        for _ in framework_iterator(config):
            local_cfg = config.copy()
            for lstm in [False, True]:
                local_cfg["num_aggregation_workers"] = 0 if not lstm else 1
                local_cfg["model"]["use_lstm"] = lstm
                print("lstm={} aggregation-worker={}".format(
                    lstm, local_cfg["num_aggregation_workers"]))
                # Test with and w/o aggregation workers (this has nothing
                # to do with LSTMs, though).
                trainer = impala.ImpalaTrainer(config=local_cfg, env=env)
                for i in range(num_iterations):
                    print(trainer.train())
                check_compute_single_action(
                    trainer,
                    include_state=lstm,
                    include_prev_action_reward=lstm,
                )
                trainer.stop()

    def test_impala_lr_schedule(self):
        config = impala.DEFAULT_CONFIG.copy()
        config["lr_schedule"] = [
            [0, 0.0005],
            [10000, 0.000001],
        ]
        local_cfg = config.copy()
        trainer = impala.ImpalaTrainer(config=local_cfg, env="CartPole-v0")

        def get_lr(result):
            return result["info"]["learner"][DEFAULT_POLICY_ID]["cur_lr"]

        try:
            r1 = trainer.train()
            r2 = trainer.train()
            assert get_lr(r2) < get_lr(r1), (r1, r2)
        finally:
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
