import unittest

import ray.rllib.agents.dqn as dqn
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check_compute_single_action, \
    framework_iterator

tf1, tf, tfv = try_import_tf()


class TestR2D2(unittest.TestCase):
    def test_r2d2_compilation(self):
        """Test whether a R2D2Trainer can be built on all frameworks."""
        config = dqn.R2D2_DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["model"]["use_lstm"] = True  # Wrap with an LSTM.
        config["model"]["max_seq_len"] = 20
        num_iterations = 2

        for _ in framework_iterator(config, frameworks="torch"):#TODO
            trainer = dqn.R2D2Trainer(config=config, env="CartPole-v0")
            rw = trainer.workers.local_worker()
            for i in range(num_iterations):
                sb = rw.sample()
                assert sb.count == config["rollout_fragment_length"]
                results = trainer.train()
                print(results)

            check_compute_single_action(trainer, include_state=True)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
