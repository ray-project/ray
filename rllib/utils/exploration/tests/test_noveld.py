import unittest

import ray
from ray import tune
from ray.rllib.agents.ppo import ppo
from ray.rllib.algorithms.dqn import dqn
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)
from ray.rllib.utils.exploration.callbacks import NovelDMetricsCallbacks


class TestNovelD(unittest.TestCase):
    """Sanity tests for NovelD exploration implementation."""

    def setUp(self):
        ray.init(local_mode=True)

    def tearDown(self):
        ray.shutdown()

    def create_ppo_test_config(self, normalize=False):
        """Returns a PPO configuration for testing."""
        config = (
            ppo.PPOConfig()
            .training(
                num_sgd_iter=2,
            )
            .rollouts(
                # Note, up-o-date NovelD can not run in parallel.
                num_rollout_workers=0,
            )
            .exploration(
                exploration_config={
                    "type": "NovelD",
                    "embed_dim": 128,
                    "lr": 0.0001,
                    "normalize": normalize,
                    "intrinsic_reward_coeff": 0.005,
                    "sub_exploration": {
                        "type": "StochasticSampling",
                    },
                },
            )
        )
        return config

    def create_dqn_test_config(self, normalize=False):
        config = (
            dqn.DQNConfig()
            .training(
                replay_buffer_config={
                    "learning_starts": 1000,
                },
                optimizer={
                    "num_replay_buffer_shards": 1,
                },
            )
            .rollouts(num_rollout_workers=0)
            .exploration(
                exploration_config={
                    "type": "NovelD",
                    "embed_dim": 128,
                    "lr": 0.0001,
                    "normalize": normalize,
                    "intrinsic_reward_coeff": 0.005,
                    "sub_exploration": {
                        "type": "StochasticSampling",
                    },
                },
            )
        )
        return config

    def test_noveld_compilation(self):
        """Test whether NovelD can be used with different Trainers

        Uses all frameworks for testing.
        """

        # Build trainer config objects.
        config_ppo = self.create_ppo_test_config()
        config_dqn = self.create_dqn_test_config()

        num_iterations = 2
        configs = [config_ppo, config_dqn]
        for config in configs:
            for fw in framework_iterator(config, with_eager_tracing=True):
                print("fw: {}".format(fw))
                for env in ["FrozenLake-v1", "MsPacmanNoFrameskip-v4"]:
                    trainer = config.build(env=env)
                    for i in range(num_iterations):
                        results = trainer.train()
                        check_train_results(results)
                        print(results)

                    check_compute_single_action(
                        trainer, include_prev_action_reward=True, include_state=False
                    )
                    trainer.stop()

    def test_noveld_normalized_intrinsic_rewards(self):
        """Test NovelD with normalized intrinsic rewards.

        Uses all frameworks for testing.
        """

        # Build a PPOConfig object.
        config = self.create_ppo_test_config(normalize=True)

        num_iterations = 2
        for fw in framework_iterator(config, with_eager_tracing=True):
            print("fw: {}".format(fw))
            for env in ["FrozenLake-v1", "MsPacmanNoFrameskip-v4"]:
                trainer = config.build(env=env)
                for i in range(num_iterations):
                    results = trainer.train()
                    check_train_results(results)
                    print(results)

                check_compute_single_action(
                    trainer, include_prev_action_reward=True, include_state=False
                )
                trainer.stop()

    def test_noveld_metrics(self):
        """Test the NovelDMetricsCallbacks.

        Uses all frameworks for testing.
        """

        # Build a PPOConfig object and add the NovelD metrics callbacks.
        config = self.create_ppo_test_config().callbacks(NovelDMetricsCallbacks)

        num_iterations = 2

        for fw in framework_iterator(config, with_eager_tracing=True):
            print("fw: {}".format(fw))
            for env in ["FrozenLake-v1", "MsPacmanNoFrameskip-v4"]:
                print("fw: {}".format(fw))
                # Run with tune to get `ExperimentAnalysis` object.
                config.environment(env=env)
                analysis = tune.run(
                    "PPO",
                    config=config.to_dict(),
                    stop={"training_iteration": num_iterations},
                )
                print(analysis)
                # Check the custom metrics for NovelD metrics.
                custom_metrics = analysis.results[next(iter(analysis.results))].get(
                    "custom_metrics"
                )
                self.assertIn("noveld/intrinsic_reward_mean", custom_metrics)
                self.assertIn("noveld/novelty_mean", custom_metrics)
                self.assertIn("noveld/novelty_next_mean", custom_metrics)
                self.assertIn("noveld/state_counts_total_mean", custom_metrics)
                self.assertIn("noveld/state_counts_avg_mean", custom_metrics)

                # Check the histogram data for NovelD histogram data.
                hist_stats = analysis.results[next(iter(analysis.results))].get(
                    "hist_stats"
                )
                self.assertIn("noveld/intrinsic_reward", hist_stats)
                self.assertIn("noveld/novelty", hist_stats)
                self.assertIn("noveld/novelty_next", hist_stats)
                self.assertIn("noveld/state_counts_total", hist_stats)
                self.assertIn("noveld/state_counts_avg", hist_stats)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
