import unittest

import ray
from ray.rllib.algorithms import sac
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
        """Test whether RNNSAC can be built on all frameworks."""
        config = (
            sac.RNNSACConfig()
            .rollouts(num_rollout_workers=0)
            .training(
                # Wrap with an LSTM and use a very simple base-model.
                model={"max_seq_len": 20},
                policy_model_config={
                    "use_lstm": True,
                    "lstm_cell_size": 64,
                    "fcnet_hiddens": [10],
                    "lstm_use_prev_action": True,
                    "lstm_use_prev_reward": True,
                },
                q_model_config={
                    "use_lstm": True,
                    "lstm_cell_size": 64,
                    "fcnet_hiddens": [10],
                    "lstm_use_prev_action": True,
                    "lstm_use_prev_reward": True,
                },
                replay_buffer_config={
                    "type": "MultiAgentPrioritizedReplayBuffer",
                    "replay_burn_in": 20,
                    "zero_init_states": True,
                },
                lr=5e-4,
            )
        )
        num_iterations = 1

        # Test building an RNNSAC agent in all frameworks.
        for _ in framework_iterator(config, frameworks="torch"):
            algo = config.build(env="CartPole-v0")
            for i in range(num_iterations):
                results = algo.train()
                print(results)

            check_compute_single_action(
                algo,
                include_state=True,
                include_prev_action_reward=True,
            )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
