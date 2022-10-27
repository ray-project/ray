import unittest

import ray
import ray.rllib.algorithms.r2d2 as r2d2
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()


def check_batch_sizes(train_results):
    """Check if batch sizes are according to what we expect from config."""
    info = train_results["info"]
    learner_info = info[LEARNER_INFO]

    for pid, policy_stats in learner_info.items():
        if pid == "batch_count":
            continue
        # Expect td-errors to be per batch-item.
        configured_b = train_results["config"]["train_batch_size"]
        actual_b = policy_stats["td_error"].shape[0]
        if (configured_b - actual_b) / actual_b > 0.1:
            # Since R2D2 learns on sequences of a fixed length but with variable
            # amount of timesteps that are padded, the batch size is almost never the
            # `train_batch_size`, which is specified in timesteps, but close to it.
            assert 0.8 < (
                abs(
                    configured_b
                    / (
                        train_results["config"]["model"]["max_seq_len"]
                        + train_results["config"]["replay_buffer_config"][
                            "replay_burn_in"
                        ]
                    )
                    / actual_b
                )
                < 1.2
            )


class TestR2D2(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_r2d2_compilation(self):
        """Test whether R2D2 can be built on all frameworks."""
        config = (
            r2d2.R2D2Config()
            .rollouts(num_rollout_workers=0)
            .training(
                model={
                    # Wrap with an LSTM and use a very simple base-model.
                    "use_lstm": True,
                    "max_seq_len": 20,
                    "fcnet_hiddens": [32],
                    "lstm_cell_size": 64,
                },
                dueling=False,
                lr=5e-4,
                zero_init_states=True,
                replay_buffer_config={"replay_burn_in": 20},
                num_steps_sampled_before_learning_starts=0,
            )
            .exploration(exploration_config={"epsilon_timesteps": 100000})
        )

        num_iterations = 1

        # Test building an R2D2 agent in all frameworks.
        for _ in framework_iterator(config, with_eager_tracing=True):
            algo = config.build(env="CartPole-v1")
            for i in range(num_iterations):
                results = algo.train()
                check_train_results(results)
                check_batch_sizes(results)
                print(results)

            check_compute_single_action(algo, include_state=True)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
