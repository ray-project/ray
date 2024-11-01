import gymnasium as gym
import ray
import shutil
import unittest

from pathlib import Path

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.bc import BCConfig
from ray.rllib.core.columns import Columns
from ray.rllib.offline.offline_data import OfflineData, OfflinePreLearner
from ray.rllib.offline.offline_prelearner import SCHEMA
from ray.rllib.policy.sample_batch import MultiAgentBatch


class TestOfflineData(unittest.TestCase):
    def setUp(self) -> None:
        data_path = "tests/data/cartpole/cartpole-v1_large"
        self.base_path = Path(__file__).parents[2]
        self.data_path = "local://" + self.base_path.joinpath(data_path).as_posix()
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_offline_data_load(self):
        """Tests loading the data in `OfflineData`."""

        # Create a simple config.
        config = AlgorithmConfig().offline_data(input_=[self.data_path])
        # Generate an `OfflineData` instance.
        offline_data = OfflineData(config)

        # Sample a single row and assert that we have indeed the data we need.
        single_row = offline_data.data.take_batch(batch_size=1)
        self.assertTrue("obs" in single_row)

    def test_sample_single_learner(self):
        """Tests using sampling using a single learner."""

        # Create a simple config.
        config = (
            BCConfig()
            .environment("CartPole-v1")
            .api_stack(
                enable_env_runner_and_connector_v2=True,
                enable_rl_module_and_learner=True,
            )
            .offline_data(
                input_=[self.data_path],
                dataset_num_iters_per_learner=1,
            )
            .learners(
                num_learners=0,
            )
            .training(
                train_batch_size_per_learner=256,
            )
        )

        # Create an algorithm from the config.
        algo = config.build()

        # Ensure that we have indeed a learner object.
        from ray.rllib.core.learner.learner import Learner

        self.assertIsInstance(algo.offline_data.learner_handles[0], Learner)

        # Now sample a batch from the data and ensure it is a `MultiAgentBatch`.
        batch = algo.offline_data.sample(10)
        self.assertIsInstance(batch, MultiAgentBatch)
        self.assertEqual(batch.env_steps(), 10)

        # Now return an iterator.
        iter = algo.offline_data.sample(num_samples=10, return_iterator=True)
        from ray.data.iterator import _IterableFromIterator

        self.assertIsInstance(iter, _IterableFromIterator)
        # Tear down.
        algo.stop()

    def test_sample_multiple_learners(self):
        """Tests sampling using multiple learners."""

        # Create a simple config.
        config = (
            BCConfig()
            .environment("CartPole-v1")
            .api_stack(
                enable_env_runner_and_connector_v2=True,
                enable_rl_module_and_learner=True,
            )
            .offline_data(
                input_=[self.data_path],
                dataset_num_iters_per_learner=1,
            )
            .learners(
                num_learners=2,
            )
            .training(
                train_batch_size_per_learner=256,
            )
        )

        # Create an algorithm from the config.
        algo = config.build()

        # Ensure we have this time:
        #   (a) actor handles for learners.
        #   (b) locality hints for the learners.
        from ray.actor import ActorHandle

        self.assertEqual(len(algo.offline_data.learner_handles), 2)
        self.assertIsNotNone(algo.offline_data.locality_hints)
        self.assertEqual(len(algo.offline_data.locality_hints), 2)
        for a in algo.offline_data.learner_handles:
            self.assertIsInstance(a, ActorHandle)
        for hint in algo.offline_data.locality_hints:
            self.assertIsInstance(hint, str)

        # Now sample from the data and make sure we get two `StreamSplitDataIterator`
        # instances.
        batch = algo.offline_data.sample(
            num_samples=10, return_iterator=2, num_shards=2
        )
        self.assertIsInstance(batch, list)
        # Ensure we have indeed two such `SStreamSplitDataIterator` instances.
        self.assertEqual(len(batch), 2)
        from ray.data._internal.iterator.stream_split_iterator import (
            StreamSplitDataIterator,
        )

        for s in batch:
            self.assertIsInstance(s, StreamSplitDataIterator)

        # Tear down.
        algo.stop()

    def test_offline_data_with_schema(self):
        """Tests passing in a different schema and sample episodes."""

        # Create some data with a different schema.
        env = gym.make("CartPole-v1")
        obs, _ = env.reset()
        eps_id = 12345
        experiences = []
        for i in range(100):
            action = env.action_space.sample()
            next_obs, reward, terminated, truncated, _ = env.step(action)
            experience = {
                "o_t": obs,
                "a_t": action,
                "r_t": reward,
                "o_tp1": next_obs,
                "d_t": terminated or truncated,
                "episode_id": eps_id,
            }
            experiences.append(experience)
            if terminated or truncated:
                obs, info = env.reset()
                eps_id = eps_id + i
            obs = next_obs

        # Convert to `Dataset`.
        ds = ray.data.from_items(experiences)
        # Store unter the temporary directory.
        dir_path = "/tmp/ray/tests/data/test_offline_data_with_schema/test_data"
        ds.write_parquet(dir_path)

        # Define a config.
        config = AlgorithmConfig()
        config.input_ = [dir_path]
        # Explicitly request to use a different schema.
        config.input_read_schema = {
            Columns.OBS: "o_t",
            Columns.ACTIONS: "a_t",
            Columns.REWARDS: "r_t",
            Columns.NEXT_OBS: "o_tp1",
            Columns.EPS_ID: "episode_id",
            Columns.TERMINATEDS: "d_t",
        }
        # Create the `OfflineData` instance. Note, this tests reading
        # the files.
        offline_data = OfflineData(config)
        # Ensure that the data could be loaded.
        self.assertTrue(hasattr(offline_data, "data"))
        # Take a small batch.
        batch = offline_data.data.take_batch(10)
        self.assertTrue("o_t" in batch.keys())
        self.assertTrue("a_t" in batch.keys())
        self.assertTrue("r_t" in batch.keys())
        self.assertTrue("o_tp1" in batch.keys())
        self.assertTrue("d_t" in batch.keys())
        self.assertTrue("episode_id" in batch.keys())
        # Preprocess the batch to episodes. Note, here we test that the
        # user schema is used.
        episodes = OfflinePreLearner._map_to_episodes(
            is_multi_agent=False, batch=batch, schema=SCHEMA | config.input_read_schema
        )
        self.assertEqual(len(episodes["episodes"]), batch["o_t"].shape[0])
        # Finally, remove the files and folders.
        shutil.rmtree(dir_path)


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
