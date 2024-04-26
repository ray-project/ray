from collections import Counter
import unittest

import ray
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.utils.test_utils import framework_iterator


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
    def on_environment_created(self, *, env_runner, env, env_context, **kwargs):
        # Create a vector-index-sum property per remote worker.
        if not hasattr(env_runner, "sum_sub_env_vector_indices"):
            env_runner.sum_sub_env_vector_indices = 0
        # Add the sub-env's vector index to the counter.
        env_runner.sum_sub_env_vector_indices += env_context.vector_index
        print(
            f"sub-env {env} created; "
            f"worker={env_runner.worker_index}; "
            f"vector-idx={env_context.vector_index}"
        )


class OnEpisodeCreatedCallback(DefaultCallbacks):
    def on_episode_created(
        self,
        *,
        episode,
        worker=None,
        env_runner=None,
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

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_episode_and_sample_callbacks_batch_mode_truncate_episodes(self):
        config = (
            PPOConfig()
            .experimental(_enable_new_api_stack=True)
            .environment("CartPole-v1")
            .env_runners(
                num_rollout_workers=0,
                batch_mode="truncate_episodes",
                env_runner_cls=SingleAgentEnvRunner,
            )
            .callbacks(EpisodeAndSampleCallbacks)
            .training(
                train_batch_size=50,  # <- rollout_fragment_length=50
                sgd_minibatch_size=50,
                num_sgd_iter=1,
            )
        )
        for _ in framework_iterator(config, frameworks=("torch", "tf2")):
            algo = config.build()
            callback_obj = algo.workers.local_worker()._callbacks

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
            .experimental(_enable_new_api_stack=True)
            .environment("CartPole-v1")
            .env_runners(
                batch_mode="complete_episodes",
                env_runner_cls=SingleAgentEnvRunner,
                num_rollout_workers=0,
            )
            .callbacks(EpisodeAndSampleCallbacks)
            .training(
                train_batch_size=50,  # <- rollout_fragment_length=50
                sgd_minibatch_size=50,
                num_sgd_iter=1,
            )
        )
        for _ in framework_iterator(config, frameworks=("torch", "tf2")):
            algo = config.build()
            callback_obj = algo.workers.local_worker()._callbacks

            # We must have had exactly one env creation event (already before training).
            self.assertEqual(callback_obj.counts["env_created"], 1)

            # Train one iteration.
            algo.train()
            # We must have has exactly one `sample()` call on our EnvRunner.
            self.assertEqual(callback_obj.counts["sample"], 1)
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
        """Tests, whw"""
        config = (
            PPOConfig()
            .experimental(_enable_new_api_stack=True)
            .env_runners(env_runner_cls=SingleAgentEnvRunner)
            .callbacks(OnEpisodeCreatedCallback)
        )
        self.assertRaises(ValueError, lambda: config.validate())


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
