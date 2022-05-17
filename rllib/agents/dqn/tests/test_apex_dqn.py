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
        config = apex.APEX_DEFAULT_CONFIG.copy()
        config["num_workers"] = 0
        config["num_gpus"] = 0
        config["replay_buffer_config"] = {
            "learning_starts": 1000,
        }
        config["min_sample_timesteps_per_reporting"] = 100
        config["min_time_s_per_reporting"] = 1
        config["optimizer"]["num_replay_buffer_shards"] = 1
        for _ in framework_iterator(config):
            trainer = apex.ApexTrainer(config=config, env="CartPole-v0")
            results = trainer.train()
            check_train_results(results)
            print(results)
            trainer.stop()

    def test_apex_dqn_compilation_and_per_worker_epsilon_values(self):
        """Test whether an APEX-DQNTrainer can be built on all frameworks."""
        config = apex.APEX_DEFAULT_CONFIG.copy()
        config["num_workers"] = 3
        config["num_gpus"] = 0
        config["replay_buffer_config"] = {
            "learning_starts": 1000,
        }
        config["min_sample_timesteps_per_reporting"] = 100
        config["min_time_s_per_reporting"] = 1
        config["optimizer"]["num_replay_buffer_shards"] = 1

        for _ in framework_iterator(config, with_eager_tracing=True):
            plain_config = config.copy()
            trainer = apex.ApexTrainer(config=plain_config, env="CartPole-v0")

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
        config = apex.APEX_DEFAULT_CONFIG.copy()
        config["num_workers"] = 1
        config["num_gpus"] = 0
        config["train_batch_size"] = 10
        config["rollout_fragment_length"] = 5
        config["replay_buffer_config"] = {
            "no_local_replay_buffer": True,
            "type": "MultiAgentPrioritizedReplayBuffer",
            "learning_starts": 10,
            "capacity": 100,
            "replay_batch_size": 10,
            "prioritized_replay_alpha": 0.6,
            # Beta parameter for sampling from prioritized replay buffer.
            "prioritized_replay_beta": 0.4,
            # Epsilon to add to the TD errors when updating priorities.
            "prioritized_replay_eps": 1e-6,
        }
        config["min_sample_timesteps_per_reporting"] = 10
        # 0 metrics reporting delay, this makes sure timestep,
        # which lr depends on, is updated after each worker rollout.
        config["min_time_s_per_reporting"] = 0
        config["optimizer"]["num_replay_buffer_shards"] = 1
        # This makes sure learning schedule is checked every 10 timesteps.
        config["optimizer"]["max_weight_sync_delay"] = 10
        # Initial lr, doesn't really matter because of the schedule below.
        config["lr"] = 0.2
        lr_schedule = [
            [0, 0.2],
            [100, 0.001],
        ]
        config["lr_schedule"] = lr_schedule

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
            trainer = apex.ApexTrainer(config=config, env="CartPole-v0")

            lr = _step_n_times(trainer, 5)  # 50 timesteps
            # Close to 0.2
            self.assertGreaterEqual(lr, 0.1)

            lr = _step_n_times(trainer, 20)  # 200 timesteps
            # LR Annealed to 0.001
            self.assertLessEqual(lr, 0.0011)

            trainer.stop()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
