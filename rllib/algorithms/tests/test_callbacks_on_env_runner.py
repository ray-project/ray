from collections import Counter
import unittest

import ray
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.test_utils import check
from ray.tune.registry import register_env


class EpisodeAndSampleCallbacks(DefaultCallbacks):
    def __init__(self):
        super().__init__()
        self.counts = Counter()

    def on_environment_created(self, *args, **kwargs):
        self.counts.update({"env_created": 1})

    def on_episode_start(self, *args, **kwargs):
        self.counts.update({"start": 1})

    def on_episode_step(self, *args, **kwargs):
        self.counts.update({"step": 1})

    def on_episode_end(self, *args, **kwargs):
        self.counts.update({"end": 1})

    def on_sample_end(self, *args, **kwargs):
        self.counts.update({"sample": 1})


class OnEnvironmentCreatedCallback(DefaultCallbacks):
    def on_environment_created(
        self,
        *,
        env_runner,
        metrics_logger=None,
        env,
        env_context,
        **kwargs,
    ):
        # Create a vector-index-sum property per remote worker.
        if not hasattr(env_runner, "sum_sub_env_vector_indices"):
            env_runner.sum_sub_env_vector_indices = 0

        # "Tag" the env object to make sure, it remains the same throughout the rest of
        # the test.
        assert not hasattr(env, "_secret_tag")
        env._secret_tag = True

        # Add the sub-env's vector index to the counter.
        env_runner.sum_sub_env_vector_indices += env_context.vector_index
        print(
            f"sub-env {env} created; "
            f"worker={env_runner.worker_index}; "
            f"vector-idx={env_context.vector_index}"
        )

    def on_episode_start(self, *, env, **kwargs):
        assert getattr(env, "_secret_tag", False) is True

    def on_episode_step(self, *, env, **kwargs):
        assert getattr(env, "_secret_tag", False) is True

    def on_episode_end(self, *, env, **kwargs):
        assert getattr(env, "_secret_tag", False) is True


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
        ray.init()
        register_env("ma_cartpole", lambda cfg: MultiAgentCartPole({"num_agents": 2}))

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
                sgd_minibatch_size=50,
                num_sgd_iter=1,
            )
        )
        algo = config.build()
        callback_obj = algo.workers.local_worker()._callbacks

        # We must have had exactly one env creation event (already before training).
        check(callback_obj.counts["env_created"], 1)

        # Train one iteration.
        algo.train()
        # We must have has exactly one `sample()` call on our EnvRunner.
        check(callback_obj.counts["sample"], 1)
        # We should have had at least one episode start.
        self.assertGreater(callback_obj.counts["start"], 0)
        # Episode starts must be same or one larger than episode ends.
        self.assertTrue(
            callback_obj.counts["start"] == callback_obj.counts["end"]
            or callback_obj.counts["start"] == callback_obj.counts["end"] + 1
        )
        # We must have taken exactly `train_batch_size` steps.
        check(callback_obj.counts["step"], 50)

        # We are still expecting to only have one env created.
        check(callback_obj.counts["env_created"], 1)

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
                sgd_minibatch_size=50,
                num_sgd_iter=1,
            )
        )
        algo = config.build()
        callback_obj = algo.workers.local_worker()._callbacks

        # We must have had exactly one env creation event (already before training).
        check(callback_obj.counts["env_created"], 1)

        # Train one iteration.
        algo.train()
        # We must have has exactly one `sample()` call on our EnvRunner.
        check(callback_obj.counts["sample"], 1)
        # We should have had at least one episode start.
        self.assertGreater(callback_obj.counts["start"], 0)
        # Episode starts must be exact same as episode ends (b/c we always complete
        # all episodes).
        check(callback_obj.counts["start"], callback_obj.counts["end"])
        # We must have taken >= `train_batch_size` steps (b/c we complete all
        # episodes).
        self.assertGreaterEqual(callback_obj.counts["step"], 50)

        # We are still expecting to only have one env created.
        check(callback_obj.counts["env_created"], 1)

        algo.stop()

    def test_env_remains_same_throughout_sampling(self):
        """Tests, whether env object remains the same throughout sampling process."""
        # SingleAgentEnvRunner.
        config = (
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .callbacks(OnEnvironmentCreatedCallback)
            .environment("CartPole-v1")
        )
        algo = config.build()
        algo.train()

        # Make sure, env on worker 1 and 2 still have the _secret_tag attribute.
        results = algo.workers.foreach_worker(
            lambda env_runner: env_runner.env._secret_tag,
            local_worker=False,
        )
        check(results, [True, True])
        algo.stop()

        # MultiAgentEnvRunner.
        config.environment("ma_cartpole")
        config.multi_agent(enable_multi_agent=True)
        algo = config.build()
        algo.train()

        # Make sure, env on worker 1 and 2 still have the _secret_tag attribute.
        results = algo.workers.foreach_worker(
            lambda env_runner: env_runner.env._secret_tag,
            local_worker=False,
        )
        check(results, [True, True])
        algo.stop()

    def test_overriding_on_episode_created_throws_error_on_new_api_stack(self):
        """Tests, whether overriding the `on_episode_created` throws an error."""
        config = (
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .callbacks(OnEpisodeCreatedCallback)
        )
        self.assertRaises(ValueError, lambda: config.validate())

        # Currently, this works on multi-agent, though (b/c we are not using vector
        # there yet).
        config.environment("ma_cartpole")
        config.multi_agent(enable_multi_agent=True)
        config.validate()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
