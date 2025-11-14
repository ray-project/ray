import functools
import shutil
import unittest
from pathlib import Path

import gymnasium as gym

import ray
from ray.rllib.algorithms.bc import BCConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core import COMPONENT_RL_MODULE, Columns
from ray.rllib.env import INPUT_ENV_SPACES
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.offline.offline_prelearner import OfflinePreLearner
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, MultiAgentBatch
from ray.rllib.utils import unflatten_dict


class TestOfflinePreLearner(unittest.TestCase):

    EXPECTED_KEYS = [
        Columns.OBS,
        Columns.NEXT_OBS,
        Columns.ACTIONS,
        Columns.REWARDS,
        Columns.TERMINATEDS,
        Columns.TRUNCATEDS,
        "n_step",
    ]

    def setUp(self) -> None:
        data_path = "tests/data/cartpole/cartpole-v1_large"
        self.base_path = Path(__file__).parents[2]
        self.data_path = "local://" + self.base_path.joinpath(data_path).as_posix()
        # Get the observation and action spaces.
        env = gym.make("CartPole-v1")
        self.observation_space = env.observation_space
        self.action_space = env.action_space
        # Set up the configuration.
        self.config = (
            BCConfig()
            .environment(
                observation_space=self.observation_space,
                action_space=self.action_space,
            )
            .api_stack(
                enable_env_runner_and_connector_v2=True,
                enable_rl_module_and_learner=True,
            )
            .offline_data(
                input_=[self.data_path],
                dataset_num_iters_per_learner=1,
            )
            .training(
                train_batch_size_per_learner=256,
            )
        )
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_offline_prelearner_buffer_class(self):
        """Tests using a user-defined buffer class with kwargs."""

        from ray.rllib.utils.replay_buffers.prioritized_episode_buffer import (
            PrioritizedEpisodeReplayBuffer,
        )

        sample_batch_data_path = self.base_path / "tests/data/cartpole/large.json"

        self.config.offline_data(
            input_=["local://" + sample_batch_data_path.as_posix()],
            # Note, for the data we need to read a JSON file.
            input_read_method="read_json",
            # Note, this has to be set to `True`.
            input_read_sample_batches=True,
            # Use a user-defined `PreLearner` class and kwargs.
            prelearner_buffer_class=PrioritizedEpisodeReplayBuffer,
            prelearner_buffer_kwargs={
                "capacity": 2000,
                "alpha": 0.8,
            },
        )

        # Build the algorithm to get the learner.
        algo = self.config.build()
        # Get the module state from the `Learner`(s).
        module_state = algo.offline_data.learner_handles[0].get_state(
            component=COMPONENT_RL_MODULE,
        )[COMPONENT_RL_MODULE]
        # Set up an `OfflinePreLearner` instance.
        oplr = OfflinePreLearner(
            config=self.config,
            module_spec=algo.offline_data.module_spec,
            module_state=module_state,
        )

        # Ensure we have indeed a `PrioritizedEpisodeReplayBuffer` in the `PreLearner`
        # with the `kwargs` we set.
        self.assertIsInstance(oplr.episode_buffer, PrioritizedEpisodeReplayBuffer)
        self.assertEqual(oplr.episode_buffer.capacity, 2000)
        self.assertEqual(oplr.episode_buffer._alpha, 0.8)

        # Now sample from the dataset and convert the `SampleBatch` in the `PreLearner`
        # and sample episodes.
        batch = algo.offline_data.data.take_batch(10)
        batch = unflatten_dict(oplr(batch))
        # Ensure all transformations worked and we have a `MultiAgentBatch`.
        self.assertIsInstance(batch, dict)
        # Ensure that we have as many environment steps as the train batch size.
        self.assertEqual(
            batch[DEFAULT_POLICY_ID][Columns.REWARDS].shape[0],
            self.config.train_batch_size_per_learner,
        )
        # Ensure all keys are available and the length of each value is the
        # train batch size.
        for key in self.EXPECTED_KEYS:
            self.assertIn(key, batch[DEFAULT_POLICY_ID])
            self.assertEqual(
                len(batch[DEFAULT_POLICY_ID][key]),
                self.config.train_batch_size_per_learner,
            )

    def test_offline_prelearner_convert_to_episodes(self):
        """Tests conversion from column data to episodes."""

        # Create the dataset.
        data = ray.data.read_parquet(self.data_path)

        # Now, take a small batch from the data and conert it to episodes.
        batch = data.take_batch(batch_size=10)
        episodes = OfflinePreLearner._map_to_episodes(False, batch)["episodes"]

        self.assertTrue(len(episodes) == 10)
        self.assertTrue(isinstance(episodes[0], SingleAgentEpisode))

    def test_offline_prelearner_ignore_final_observation(self):
        # Create the dataset.
        data = ray.data.read_parquet(self.data_path)

        # Now, take a small batch from the data and conert it to episodes.
        batch = data.take_batch(batch_size=10)
        episodes = OfflinePreLearner._map_to_episodes(
            False, batch, ignore_final_observation=True
        )["episodes"]

        self.assertTrue(
            all(all(eps.get_observations()[-1] == [0.0] * 4) for eps in episodes)
        )

    def test_offline_prelearner_convert_from_old_sample_batch_to_episodes(self):
        """Tests conversion from `SampleBatch` data to episodes."""

        # Use the old records storing `SampleBatch`es.
        sample_batch_data_path = self.base_path / "tests/data/cartpole/large.json"

        # Create the dataset.
        data = ray.data.read_json(sample_batch_data_path.as_posix())

        # Sample a small batch from the raw data.
        batch = data.take_batch(batch_size=10)
        # Convert `SampleBatch` data to episode data.
        episodes = OfflinePreLearner._map_sample_batch_to_episode(False, batch)[
            "episodes"
        ]
        # Assert that we have sampled episodes.
        self.assertTrue(len(episodes) == 10)
        self.assertTrue(isinstance(episodes[0], SingleAgentEpisode))

    def test_offline_prelearner_in_map_batches(self):
        """Tests using the `OfflinePreLearner` in `map_batches` where it is used
        in `OfflineData`.
        """

        # Create a simple dataset.
        data = ray.data.read_parquet(self.data_path)

        # Generate a batch iterator that uses the `OfflinePreLearner` to convert
        # data to episodes.
        batch_iterator = data.map_batches(
            functools.partial(
                OfflinePreLearner._map_to_episodes,
                False,
            )
        ).iter_batches(
            batch_size=10,
            prefetch_batches=1,
        )

        # Now sample a single batch.
        batch = next(iter(batch_iterator))
        # Assert that we have indeed sampled episodes.
        self.assertTrue("episodes" in batch)
        self.assertTrue(isinstance(batch["episodes"][0], SingleAgentEpisode))

    def test_offline_prelearner_sample_from_old_sample_batch_data(self):
        """Tests sampling from a `SampleBatch` dataset."""

        data_path = self.base_path / "tests/data/cartpole/large.json"

        self.config.offline_data(
            input_=["local://" + data_path.as_posix()],
            # Note, the default is `read_parquet`.
            input_read_method="read_json",
            # Signal that we want to read in old `SampleBatch` data.
            input_read_sample_batches=True,
            # Use a different input batch size b/c each `SampleBatch`
            # contains multiple timesteps.
            input_read_batch_size=50,
        )

        # Build the algorithm to get the learner.
        algo = self.config.build()
        # Get the module state from the `Learner`.
        module_state = algo.offline_data.learner_handles[0].get_state(
            component=COMPONENT_RL_MODULE,
        )[COMPONENT_RL_MODULE]
        # Set up an `OfflinePreLearner` instance.
        oplr = OfflinePreLearner(
            config=self.config,
            module_spec=algo.offline_data.module_spec,
            module_state=module_state,
        )
        # Now, pull a batch of defined size from the dataset.
        batch = algo.offline_data.data.take_batch(
            self.config.train_batch_size_per_learner
        )
        # Pass the batch through the `OfflinePreLearner`. Note, the batch is
        # a batch of `SampleBatch`es and could potentially have more than the
        # defined number of experiences to be used for learning.
        # The `OfflinePreLearner`'s episode buffer should buffer all data
        # and sample the exact size requested by the user, i.e.
        # `train_batch_size_per_learner`
        batch = oplr(batch)

        # Ensure all transformations worked and we have a `MultiAgentBatch`.
        self.assertIsInstance(batch["batch"][0], MultiAgentBatch)
        # Ensure that we have as many environment steps as the train batch size.
        self.assertEqual(
            batch["batch"][0][DEFAULT_POLICY_ID].count,
            self.config.train_batch_size_per_learner,
        )
        # Ensure all keys are available and the length of each value is the
        # train batch size.
        for key in self.EXPECTED_KEYS:
            self.assertIn(key, batch["batch"][0][DEFAULT_POLICY_ID])
            self.assertEqual(
                len(batch["batch"][0][DEFAULT_POLICY_ID][key]),
                self.config.train_batch_size_per_learner,
            )

    def test_offline_prelearner_sample_from_episode_data(self):

        # Store data only temporary.
        data_path = "/tmp/cartpole-v1_episodes/"
        # Configure PPO for recording.
        config = (
            PPOConfig()
            .environment(
                env="CartPole-v1",
            )
            .env_runners(
                batch_mode="complete_episodes",
            )
            .offline_data(
                output=data_path,
                output_write_episodes=True,
            )
        )

        # Record some episodes.
        algo = config.build()
        for _ in range(3):
            algo.train()

        # Reset the input data and the episode read flag.
        self.config.offline_data(
            input_=[data_path],
            input_read_episodes=True,
            input_read_batch_size=50,
        )

        # Build the `BC` algorithm.
        algo = self.config.build()
        # Read in the generated set of episode data.
        episode_ds = ray.data.read_parquet(data_path)
        # Sample a batch of episodes from the episode dataset.
        episode_batch = episode_ds.take_batch(256)
        # Get the module state from the `Learner`.
        module_state = algo.offline_data.learner_handles[0].get_state(
            component=COMPONENT_RL_MODULE,
        )[COMPONENT_RL_MODULE]
        # Set up an `OfflinePreLearner` instance.
        oplr = OfflinePreLearner(
            config=self.config,
            module_spec=algo.offline_data.module_spec,
            module_state=module_state,
            spaces=algo.offline_data.spaces[INPUT_ENV_SPACES],
        )
        # Sample a `MultiAgentBatch`.
        batch = oplr(episode_batch)

        # Assert that we have indeed a batch of `train_batch_size_per_learner`.
        self.assertEqual(
            batch["batch"][0][DEFAULT_POLICY_ID].count,
            self.config.train_batch_size_per_learner,
        )

        # Remove all generated Parquet data from disk.
        shutil.rmtree(data_path)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
