import pytest
import unittest

import ray
import ray.rllib.agents.dqn.apex as apex
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO, LEARNER_STATS_KEY
from ray.rllib.utils.test_utils import (
    check,
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)


class TestApexDQN(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=4)

    def tearDown(self):
        ray.shutdown()

    def test_apex_zero_workers(self):
        config = (
            apex.ApexConfig()
            .rollouts(num_rollout_workers=0)
            .resources(num_gpus=0)
            .training(
                replay_buffer_config={
                    "learning_starts": 1000,
                },
                optimizer={
                    "num_replay_buffer_shards": 1,
                },
            )
            .reporting(
                min_sample_timesteps_per_reporting=100,
                min_time_s_per_reporting=1,
            )
        )

        for _ in framework_iterator(config):
            trainer = config.build(env="CartPole-v0")
            results = trainer.train()
            check_train_results(results)
            print(results)
            trainer.stop()

    def test_apex_dqn_compilation_and_per_worker_epsilon_values(self):
        """Test whether an APEX-DQNTrainer can be built on all frameworks."""
        config = (
            apex.ApexConfig()
            .rollouts(num_rollout_workers=3)
            .resources(num_gpus=0)
            .training(
                replay_buffer_config={
                    "learning_starts": 1000,
                },
                optimizer={
                    "num_replay_buffer_shards": 1,
                },
            )
            .reporting(
                min_sample_timesteps_per_reporting=100,
                min_time_s_per_reporting=1,
            )
        )

        for _ in framework_iterator(config, with_eager_tracing=True):
            trainer = config.build(env="CartPole-v0")

            # Test per-worker epsilon distribution.
            infos = trainer.workers.foreach_policy(
                lambda p, _: p.get_exploration_state()
            )
            expected = [0.4, 0.016190862, 0.00065536]
            check([i["cur_epsilon"] for i in infos], [0.0] + expected)

            check_compute_single_action(trainer)

            for i in range(2):
                results = trainer.train()
                check_train_results(results)
                print(results)

            # Test again per-worker epsilon distribution
            # (should not have changed).
            infos = trainer.workers.foreach_policy(
                lambda p, _: p.get_exploration_state()
            )
            check([i["cur_epsilon"] for i in infos], [0.0] + expected)

            trainer.stop()

    def test_apex_lr_schedule(self):
        config = (
            apex.ApexConfig()
            .rollouts(
                num_rollout_workers=1,
                rollout_fragment_length=5,
            )
            .resources(num_gpus=0)
            .training(
                train_batch_size=10,
                optimizer={
                    "num_replay_buffer_shards": 1,
                    # This makes sure learning schedule is checked every 10 timesteps.
                    "max_weight_sync_delay": 10,
                },
                replay_buffer_config={
                    "no_local_replay_buffer": True,
                    "type": "MultiAgentPrioritizedReplayBuffer",
                    "learning_starts": 10,
                    "capacity": 100,
                    "prioritized_replay_alpha": 0.6,
                    # Beta parameter for sampling from prioritized replay buffer.
                    "prioritized_replay_beta": 0.4,
                    # Epsilon to add to the TD errors when updating priorities.
                    "prioritized_replay_eps": 1e-6,
                },
                # Initial lr, doesn't really matter because of the schedule below.
                lr=0.2,
                lr_schedule=[[0, 0.2], [100, 0.001]],
            )
            .reporting(
                min_sample_timesteps_per_reporting=10,
                # 0 metrics reporting delay, this makes sure timestep,
                # which lr depends on, is updated after each worker rollout.
                min_time_s_per_reporting=0,
            )
        )

        def _step_n_times(trainer, n: int):
            """Step trainer n times.

            Returns:
                learning rate at the end of the execution.
            """
            for _ in range(n):
                results = trainer.train()
            return results["info"][LEARNER_INFO][DEFAULT_POLICY_ID][LEARNER_STATS_KEY][
                "cur_lr"
            ]

        for _ in framework_iterator(config):
            trainer = config.build(env="CartPole-v0")

            lr = _step_n_times(trainer, 3)  # 50 timesteps
            # Close to 0.2
            self.assertGreaterEqual(lr, 0.1)

            lr = _step_n_times(trainer, 20)  # 200 timesteps
            # LR Annealed to 0.001
            self.assertLessEqual(lr, 0.0011)

            trainer.stop()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
