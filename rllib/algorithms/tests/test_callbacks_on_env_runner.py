from collections import Counter
import unittest

import gymnasium as gym

import ray
from ray import train, tune
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger


class EpisodeAndSampleCallbacks(DefaultCallbacks):
    def __init__(self):
        super().__init__()
        self.counts = Counter()

    def on_environment_created(self, *args, env_runner, metrics_logger, env, **kwargs):

        self.counts.update({"env_created": 1})

    def on_episode_start(self, *args, env_runner, metrics_logger, env, **kwargs):
        assert isinstance(env_runner, EnvRunner)
        assert isinstance(metrics_logger, MetricsLogger)
        assert isinstance(env, gym.Env)
        self.counts.update({"start": 1})

    def on_episode_step(self, *args, env_runner, metrics_logger, env, **kwargs):
        assert isinstance(env_runner, EnvRunner)
        assert isinstance(metrics_logger, MetricsLogger)
        assert isinstance(env, gym.Env)
        self.counts.update({"step": 1})

    def on_episode_end(self, *args, env_runner, metrics_logger, env, **kwargs):
        assert isinstance(env_runner, EnvRunner)
        assert isinstance(metrics_logger, MetricsLogger)
        assert isinstance(env, gym.Env)
        self.counts.update({"end": 1})

    def on_sample_end(self, *args, env_runner, metrics_logger, **kwargs):
        assert isinstance(env_runner, EnvRunner)
        assert isinstance(metrics_logger, MetricsLogger)
        self.counts.update({"sample": 1})


class OnEnvironmentCreatedCallback(DefaultCallbacks):
    def on_environment_created(self, *, env_runner, env, env_context, **kwargs):
        assert isinstance(env_runner, EnvRunner)
        assert isinstance(env, gym.Env)
        assert env_runner.tune_trial_id is not None
        # Create a vector-index-sum property per remote worker.
        if not hasattr(env_runner, "sum_sub_env_vector_indices"):
            env_runner.sum_sub_env_vector_indices = 0
        # Add the sub-env's vector index to the counter.
        env_runner.sum_sub_env_vector_indices += env_context.vector_index
        print(
            f"sub-env {env} created; "
            f"worker={env_runner.worker_index}; "
            f"vector-idx={env_context.vector_index}; "
            f"tune-trial-id={env_runner.tune_trial_id}; "
        )


class OnEpisodeCreatedCallback(DefaultCallbacks):
    def on_episode_created(
        self,
        *,
        episode,
        worker=None,
        env_runner=None,
        metrics_logger=None,
        base_env=None,
        env=None,
        policies=None,
        rl_module=None,
        env_index: int,
        **kwargs,
    ) -> None:
        print("Some code here to test the expected error on new API stack!")


class TestCallbacks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        tune.register_env("multi_cart", lambda _: MultiAgentCartPole({"num_agents": 2}))
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_episode_and_sample_callbacks_batch_mode_truncate_episodes(self):
        config = (
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .environment("CartPole-v1")
            .env_runners(
                num_env_runners=0,
                batch_mode="truncate_episodes",
            )
            .callbacks(EpisodeAndSampleCallbacks)
            .training(
                train_batch_size=50,  # <- rollout_fragment_length=50
                minibatch_size=50,
                num_epochs=1,
            )
        )

        for multi_agent in [False, True]:
            if multi_agent:
                config.multi_agent(
                    policies={"p0", "p1"},
                    policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
                )
                config.environment("multi_cart")
            algo = config.build()
            callback_obj = algo.env_runner._callbacks

            # We must have had exactly one env creation event (already before training).
            self.assertEqual(callback_obj.counts["env_created"], 1)

            # Train one iteration.
            algo.train()
            # We must have has exactly one `sample()` call on our EnvRunner.
            self.assertEqual(callback_obj.counts["sample"], 1)
            # We should have had at least one episode start.
            self.assertGreater(callback_obj.counts["start"], 0)
            # Episode starts must be same or one larger than episode ends.
            self.assertTrue(
                callback_obj.counts["start"] == callback_obj.counts["end"]
                or callback_obj.counts["start"] == callback_obj.counts["end"] + 1
            )
            # We must have taken exactly `train_batch_size` steps.
            self.assertEqual(callback_obj.counts["step"], 50)

            # We are still expecting to only have one env created.
            self.assertEqual(callback_obj.counts["env_created"], 1)

            algo.stop()

    def test_episode_and_sample_callbacks_batch_mode_complete_episodes(self):
        config = (
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .environment("CartPole-v1")
            .env_runners(
                batch_mode="complete_episodes",
                num_env_runners=0,
            )
            .callbacks(EpisodeAndSampleCallbacks)
            .training(
                train_batch_size=50,  # <- rollout_fragment_length=50
                minibatch_size=50,
                num_epochs=1,
            )
        )

        for multi_agent in [False, True]:
            if multi_agent:
                config.multi_agent(
                    policies={"p0", "p1"},
                    policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
                )
                config.environment("multi_cart")

            algo = config.build()
            callback_obj = algo.env_runner._callbacks

            # We must have had exactly one env creation event (already before training).
            self.assertEqual(callback_obj.counts["env_created"], 1)

            # Train one iteration.
            algo.train()
            # We should have had at least one episode start.
            self.assertGreater(callback_obj.counts["start"], 0)
            # Episode starts must be exact same as episode ends (b/c we always complete
            # all episodes).
            self.assertTrue(callback_obj.counts["start"] == callback_obj.counts["end"])
            # We must have taken >= `train_batch_size` steps (b/c we complete all
            # episodes).
            self.assertGreaterEqual(callback_obj.counts["step"], 50)

            # We are still expecting to only have one env created.
            self.assertEqual(callback_obj.counts["env_created"], 1)

            algo.stop()

    def test_overriding_on_episode_created_throws_error_on_new_api_stack(self):
        """Tests whether overriding `on_episode_created` raises error w/ SAEnvRunner."""
        config = (
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .callbacks(OnEpisodeCreatedCallback)
        )
        self.assertRaises(ValueError, lambda: config.validate())

    def test_tune_trial_id_visible_in_callbacks(self):
        config = (
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .environment("multi_cart", env_config={"num_agents": 2})
            .callbacks(OnEnvironmentCreatedCallback)
            .multi_agent(
                policies={"default_policy", "p1"},
                policy_mapping_fn=lambda *a, **kw: "default_policy",
            )
        )
        tune.Tuner(
            trainable=config.algo_class,
            param_space=config,
            run_config=train.RunConfig(stop={"training_iteration": 1}),
        ).fit()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
