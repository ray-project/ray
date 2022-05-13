import unittest

import ray
import ray.rllib.agents.sac as sac
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import check_compute_single_action, framework_iterator

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()


class TestRNNSAC(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_rnnsac_compilation(self):
        """Test whether a R2D2Trainer can be built on all frameworks."""
        config = sac.RNNSAC_DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.

        # Wrap with an LSTM and use a very simple base-model.
        config["model"] = {
            "max_seq_len": 20,
        }
        config["policy_model"] = {
            "use_lstm": True,
            "lstm_cell_size": 64,
            "fcnet_hiddens": [10],
            "lstm_use_prev_action": True,
            "lstm_use_prev_reward": True,
        }
        config["Q_model"] = {
            "use_lstm": True,
            "lstm_cell_size": 64,
            "fcnet_hiddens": [10],
            "lstm_use_prev_action": True,
            "lstm_use_prev_reward": True,
        }

        # Test with MultiAgentPrioritizedReplayBuffer
        config["replay_buffer_config"] = {
            "type": "MultiAgentPrioritizedReplayBuffer",
            "replay_burn_in": 20,
            "zero_init_states": True,
        }

        config["lr"] = 5e-4

        num_iterations = 1

        # Test building an RNNSAC agent in all frameworks.
        for _ in framework_iterator(config, frameworks="torch"):
            trainer = sac.RNNSACTrainer(config=config, env="CartPole-v0")
            for i in range(num_iterations):
                results = trainer.train()
                print(results)

            check_compute_single_action(
                trainer,
                include_state=True,
                include_prev_action_reward=True,
            )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
