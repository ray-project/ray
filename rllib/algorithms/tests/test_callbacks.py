from collections import Counter
import unittest

import ray
from ray.rllib.algorithms.callbacks import DefaultCallbacks, MultiCallbacks
import ray.rllib.algorithms.dqn as dqn
from ray.rllib.algorithms.pg import PGConfig
from ray.rllib.evaluation.episode import Episode
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.utils.test_utils import framework_iterator


class EpisodeAndSampleCallbacks(DefaultCallbacks):
    def __init__(self):
        super().__init__()
        self.counts = Counter()

    def on_episode_start(self, *args, **kwargs):
        self.counts.update({"start": 1})

    def on_episode_step(self, *args, **kwargs):
        self.counts.update({"step": 1})

    def on_episode_end(self, *args, **kwargs):
        self.counts.update({"end": 1})

    def on_sample_end(self, *args, **kwargs):
        self.counts.update({"sample": 1})


class OnSubEnvironmentCreatedCallback(DefaultCallbacks):
    def on_sub_environment_created(
        self, *, worker, sub_environment, env_context, **kwargs
    ):
        # Create a vector-index-sum property per remote worker.
        if not hasattr(worker, "sum_sub_env_vector_indices"):
            worker.sum_sub_env_vector_indices = 0
        # Add the sub-env's vector index to the counter.
        worker.sum_sub_env_vector_indices += env_context.vector_index
        print(
            f"sub-env {sub_environment} created; "
            f"worker={worker.worker_index}; "
            f"vector-idx={env_context.vector_index}"
        )


class OnEpisodeCreatedCallback(DefaultCallbacks):
    def __init__(self):
        super().__init__()
        self._reset_counter = 0

    def on_episode_created(
        self, *, worker, base_env, policies, env_index, episode, **kwargs
    ):
        print(f"Sub-env {env_index} is going to be reset.")
        self._reset_counter += 1

        # Make sure the passed in episode is really brand new.
        assert episode.env_id == env_index
        if isinstance(episode, Episode):
            assert episode.length == 0
            assert episode.started is False
        else:
            assert episode.length == -1
        assert episode.worker is worker


class TestCallbacks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_episode_and_sample_callbacks(self):
        config = (
            PGConfig()
            .environment("CartPole-v1")
            .rollouts(num_rollout_workers=0, rollout_fragment_length=50)
            .callbacks(EpisodeAndSampleCallbacks)
            .training(train_batch_size=50)
        )
        for _ in framework_iterator(config, frameworks=("tf", "torch")):
            pg = config.build()
            pg.train()
            pg.train()
            callback_obj = pg.workers.local_worker().callbacks
            self.assertGreater(callback_obj.counts["sample"], 0)
            self.assertGreater(callback_obj.counts["start"], 0)
            self.assertGreater(callback_obj.counts["end"], 0)
            self.assertGreater(callback_obj.counts["step"], 0)
            pg.stop()

    def test_on_sub_environment_created(self):

        config = (
            dqn.DQNConfig().environment("CartPole-v1")
            # Create 4 sub-environments per remote worker.
            # Create 2 remote workers.
            .rollouts(num_envs_per_worker=4, num_rollout_workers=2)
        )

        for callbacks in (
            OnSubEnvironmentCreatedCallback,
            MultiCallbacks([OnSubEnvironmentCreatedCallback]),
        ):
            config.callbacks(callbacks)

            for _ in framework_iterator(config, frameworks=("tf", "torch")):
                algo = config.build()
                # Fake the counter on the local worker (doesn't have an env) and
                # set it to -1 so the below `foreach_worker()` won't fail.
                algo.workers.local_worker().sum_sub_env_vector_indices = -1

                # Get sub-env vector index sums from the 2 remote workers:
                sum_sub_env_vector_indices = algo.workers.foreach_worker(
                    lambda w: w.sum_sub_env_vector_indices
                )
                # Local worker has no environments -> Expect the -1 special
                # value returned by the above lambda.
                self.assertTrue(sum_sub_env_vector_indices[0] == -1)
                # Both remote workers (index 1 and 2) have a vector index counter
                # of 6 (sum of vector indices: 0 + 1 + 2 + 3).
                self.assertTrue(sum_sub_env_vector_indices[1] == 6)
                self.assertTrue(sum_sub_env_vector_indices[2] == 6)
                algo.stop()

    def test_on_sub_environment_created_with_remote_envs(self):
        config = (
            dqn.DQNConfig()
            .environment("CartPole-v1")
            .rollouts(
                # Make each sub-environment a ray actor.
                remote_worker_envs=True,
                # Create 2 remote workers.
                num_rollout_workers=2,
                # Create 4 sub-environments (ray remote actors) per remote
                # worker.
                num_envs_per_worker=4,
            )
        )

        for callbacks in (
            OnSubEnvironmentCreatedCallback,
            MultiCallbacks([OnSubEnvironmentCreatedCallback]),
        ):
            config.callbacks(callbacks)

            for _ in framework_iterator(config, frameworks=("tf", "torch")):
                algo = config.build()
                # Fake the counter on the local worker (doesn't have an env) and
                # set it to -1 so the below `foreach_worker()` won't fail.
                algo.workers.local_worker().sum_sub_env_vector_indices = -1

                # Get sub-env vector index sums from the 2 remote workers:
                sum_sub_env_vector_indices = algo.workers.foreach_worker(
                    lambda w: w.sum_sub_env_vector_indices
                )
                # Local worker has no environments -> Expect the -1 special
                # value returned by the above lambda.
                self.assertTrue(sum_sub_env_vector_indices[0] == -1)
                # Both remote workers (index 1 and 2) have a vector index counter
                # of 6 (sum of vector indices: 0 + 1 + 2 + 3).
                self.assertTrue(sum_sub_env_vector_indices[1] == 6)
                self.assertTrue(sum_sub_env_vector_indices[2] == 6)
                algo.stop()

    def test_on_episode_created(self):
        # 1000 steps sampled (2.5 episodes on each sub-environment) before training
        # starts.
        config = (
            dqn.DQNConfig()
            .environment(
                RandomEnv,
                env_config={
                    "max_episode_len": 200,
                    "p_terminated": 0.0,
                },
            )
            .rollouts(num_envs_per_worker=2, num_rollout_workers=1)
            .callbacks(OnEpisodeCreatedCallback)
        )

        # Test with and without Connectors.
        for connector in [True, False]:
            config.rollouts(enable_connectors=connector)
            algo = config.build()
            algo.train()
            # Two sub-environments share 1000 steps in the first training iteration
            # (min_sample_timesteps_per_iteration = 1000).
            # -> 1000 / 2 [sub-envs] = 500 [per sub-env]
            # -> 1 episode = 200 timesteps
            # -> 2.5 episodes per sub-env
            # -> 3 episodes created [per sub-env] = 6 episodes total
            self.assertEqual(
                6,
                algo.workers.foreach_worker(
                    lambda w: w.callbacks._reset_counter,
                    local_worker=False,
                )[0],
            )
            algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
