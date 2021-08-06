import pytest
import unittest

import ray
import ray.rllib.agents.dqn.apex as apex
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.test_utils import check, check_compute_single_action, \
    framework_iterator


class TestApexDQN(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=4)

    def tearDown(self):
        ray.shutdown()

    def test_apex_zero_workers(self):
        config = apex.APEX_DEFAULT_CONFIG.copy()
        config["num_workers"] = 0
        config["num_gpus"] = 0
        config["learning_starts"] = 1000
        config["prioritized_replay"] = True
        config["timesteps_per_iteration"] = 100
        config["min_iter_time_s"] = 1
        config["optimizer"]["num_replay_buffer_shards"] = 1
        for _ in framework_iterator(config):
            trainer = apex.ApexTrainer(config=config, env="CartPole-v0")
            trainer.train()
            trainer.stop()

    def test_apex_dqn_compilation_and_per_worker_epsilon_values(self):
        """Test whether an APEX-DQNTrainer can be built on all frameworks."""
        config = apex.APEX_DEFAULT_CONFIG.copy()
        config["num_workers"] = 3
        config["num_gpus"] = 0
        config["learning_starts"] = 1000
        config["prioritized_replay"] = True
        config["timesteps_per_iteration"] = 100
        config["min_iter_time_s"] = 1
        config["optimizer"]["num_replay_buffer_shards"] = 1

        for _ in framework_iterator(config):
            plain_config = config.copy()
            trainer = apex.ApexTrainer(config=plain_config, env="CartPole-v0")

            # Test per-worker epsilon distribution.
            infos = trainer.workers.foreach_policy(
                lambda p, _: p.get_exploration_state())
            expected = [0.4, 0.016190862, 0.00065536]
            check([i["cur_epsilon"] for i in infos], [0.0] + expected)

            check_compute_single_action(trainer)

            for i in range(2):
                print(trainer.train())

            # Test again per-worker epsilon distribution
            # (should not have changed).
            infos = trainer.workers.foreach_policy(
                lambda p, _: p.get_exploration_state())
            check([i["cur_epsilon"] for i in infos], [0.0] + expected)

            trainer.stop()

    def test_apex_lr_schedule(self):
        config = apex.APEX_DEFAULT_CONFIG.copy()
        config["lr"] = 0.1
        config["lr_schedule"] = [
            [0, 1e-4],
            [1000, 1e-9]
        ]
        config["num_workers"] = 2
        config["train_batch_size"] = 10
        config["learning_starts"] = 10
        config["timesteps_per_iteration"] = 10
        config["min_iter_time_s"] = 1

        def get_lr(result):
            return result["info"]["learner"][DEFAULT_POLICY_ID]["cur_lr"]

        for fw in framework_iterator(config, frameworks=("tf", "torch")):
            plain_config = config.copy()
            trainer = apex.ApexTrainer(config=plain_config, env="CartPole-v0")
            policy = trainer.get_policy()

            try:
                # first lr should be 1e-4, not 0.1 (ignored)
                if fw == "tf":
                    check(policy._sess.run(policy.cur_lr), 1e-4)
                else:
                    check(policy.cur_lr, 1e-4)
                r1 = trainer.train()
                r2 = trainer.train()
                assert get_lr(r2) < get_lr(r1), \
                    f"cur_lr should have decreased for {fw}. Got {(r1, r2)}"
            finally:
                trainer.stop()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
