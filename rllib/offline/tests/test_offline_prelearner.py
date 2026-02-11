import shutil
from pathlib import Path

import gymnasium as gym
import pytest

import ray
from ray.rllib.algorithms.bc import BCConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core import COMPONENT_RL_MODULE, Columns
from ray.rllib.env import INPUT_ENV_SPACES
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.offline.offline_prelearner import OfflinePreLearner
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils import unflatten_dict

EXPECTED_KEYS = [
    Columns.OBS,
    Columns.NEXT_OBS,
    Columns.ACTIONS,
    Columns.REWARDS,
    Columns.TERMINATEDS,
    Columns.TRUNCATEDS,
    "n_step",
]

episodes_data_path = "offline/tests/data/cartpole/cartpole-v1_large"
base_path = Path(__file__).parents[2]
EPISODES_DATA_PATH = "local://" + base_path.joinpath(episodes_data_path).as_posix()

sample_batch_data_path = "offline/tests/data/cartpole/large.json"
SAMPLE_BATCH_DATA_PATH = (
    "local://" + base_path.joinpath(sample_batch_data_path).as_posix()
)

ENV = gym.make("CartPole-v1")


@pytest.fixture
def config():
    observation_space = ENV.observation_space
    action_space = ENV.action_space
    # Set up the configuration.
    config = (
        BCConfig()
        .environment(
            observation_space=observation_space,
            action_space=action_space,
        )
        .api_stack(
            enable_env_runner_and_connector_v2=True,
            enable_rl_module_and_learner=True,
        )
        .offline_data(
            input_=[EPISODES_DATA_PATH],
            dataset_num_iters_per_learner=1,
        )
        .training(
            train_batch_size_per_learner=256,
        )
    )
    return config


@pytest.fixture
def offline_prelearner(config):
    algo = config.build()
    offline_prelearner = OfflinePreLearner(
        config=config,
        module_spec=algo.offline_data.module_spec,
        module_state=algo.offline_data.learner_handles[0].get_state(
            component=COMPONENT_RL_MODULE,
        )[COMPONENT_RL_MODULE],
    )
    return offline_prelearner


class TestOfflinePreLearner:
    def test_offline_prelearner_buffer_class(self, config):
        """Tests using a user-defined buffer class with kwargs."""

        from ray.rllib.utils.replay_buffers.prioritized_episode_buffer import (
            PrioritizedEpisodeReplayBuffer,
        )

        config.offline_data(
            input_=[SAMPLE_BATCH_DATA_PATH],
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
        algo = config.build()
        # Get the module state from the `Learner`(s).
        module_state = algo.offline_data.learner_handles[0].get_state(
            component=COMPONENT_RL_MODULE,
        )[COMPONENT_RL_MODULE]
        # Set up an `OfflinePreLearner` instance.
        offline_prelearner = OfflinePreLearner(
            config=config,
            module_spec=algo.offline_data.module_spec,
            module_state=module_state,
        )

        # Ensure we have indeed a `PrioritizedEpisodeReplayBuffer` in the `PreLearner`
        # with the `kwargs` we set.
        assert isinstance(
            offline_prelearner.episode_buffer, PrioritizedEpisodeReplayBuffer
        )
        assert offline_prelearner.episode_buffer.capacity == 2000
        assert offline_prelearner.episode_buffer._alpha == 0.8

        # Now sample from the dataset and convert the `SampleBatch` in the `PreLearner`
        # and sample episodes.
        batch = algo.offline_data.data.take_batch(10)
        batch = unflatten_dict(offline_prelearner(batch))
        # Ensure all transformations worked and we have a `MultiAgentBatch`.
        assert isinstance(batch, dict)
        # Ensure that we have as many environment steps as the train batch size.
        assert (
            batch[DEFAULT_POLICY_ID][Columns.REWARDS].shape[0]
            == config.train_batch_size_per_learner
        )
        # Ensure all keys are available and the length of each value is the
        # train batch size.
        for key in EXPECTED_KEYS:
            assert key in batch[DEFAULT_POLICY_ID]
            assert (
                len(batch[DEFAULT_POLICY_ID][key])
                == config.train_batch_size_per_learner
            )

    def test_offline_prelearner_convert_to_episodes(self, offline_prelearner):
        """Tests conversion from column data to episodes."""

        # Create the dataset.
        data = ray.data.read_parquet(EPISODES_DATA_PATH)

        # Now, take a small batch from the data and conert it to episodes.
        batch = data.take_batch(batch_size=10)
        episodes = offline_prelearner._map_to_episodes(batch)["episodes"]

        assert len(episodes) == 10
        assert isinstance(episodes[0], SingleAgentEpisode)

    def test_offline_prelearner_ignore_final_observation(self, config):
        # Create the dataset.
        data = ray.data.read_parquet(EPISODES_DATA_PATH)

        config.offline_data(
            ignore_final_observation=True,
        )

        algo = config.build()
        module_state = algo.offline_data.learner_handles[0].get_state(
            component=COMPONENT_RL_MODULE,
        )[COMPONENT_RL_MODULE]
        offline_prelearner = OfflinePreLearner(
            config=config,
            module_spec=algo.offline_data.module_spec,
            module_state=module_state,
        )

        # Now, take a small batch from the data and conert it to episodes.
        batch = data.take_batch(batch_size=10)
        episodes = offline_prelearner._map_to_episodes(batch)["episodes"]

        assert all(
            all(eps.get_observations()[-1] == [0.0] * ENV.observation_space.shape[0])
            for eps in episodes
        )

    def test_offline_prelearner_convert_from_old_sample_batch_to_episodes(
        self, offline_prelearner
    ):
        """Tests conversion from `SampleBatch` data to episodes."""
        # Create the dataset.
        data = ray.data.read_json(SAMPLE_BATCH_DATA_PATH)

        # Sample a small batch from the raw data.
        batch = data.take_batch(batch_size=10)
        # Convert `SampleBatch` data to episode data.
        episodes = offline_prelearner._map_sample_batch_to_episode(batch)["episodes"]
        # Assert that we have sampled episodes.
        assert len(episodes) == 10
        assert isinstance(episodes[0], SingleAgentEpisode)

    def test_offline_prelearner_sample_from_old_sample_batch_data(self, config):
        """Tests sampling from a `SampleBatch` dataset."""

        config.offline_data(
            input_=[SAMPLE_BATCH_DATA_PATH],
            # Note, the default is `read_parquet`.
            input_read_method="read_json",
            # Signal that we want to read in old `SampleBatch` data.
            input_read_sample_batches=True,
            # Use a different input batch size b/c each `SampleBatch`
            # contains multiple timesteps.
            input_read_batch_size=50,
        )

        # Build the algorithm to get the learner.
        algo = config.build()
        # Get the module state from the `Learner`.
        module_state = algo.offline_data.learner_handles[0].get_state(
            component=COMPONENT_RL_MODULE,
        )[COMPONENT_RL_MODULE]
        # Set up an `OfflinePreLearner` instance.
        oplr = OfflinePreLearner(
            config=config,
            module_spec=algo.offline_data.module_spec,
            module_state=module_state,
        )
        # Now, pull a batch of defined size from the dataset.
        batch = algo.offline_data.data.take_batch(config.train_batch_size_per_learner)
        # Pass the batch through the `OfflinePreLearner`. Note, the batch is
        # a batch of `SampleBatch`es and could potentially have more than the
        # defined number of experiences to be used for learning.
        # The `OfflinePreLearner`'s episode buffer should buffer all data
        # and sample the exact size requested by the user, i.e.
        # `train_batch_size_per_learner`
        batch = unflatten_dict(oplr(batch))

        # Ensure all transformations worked and we have a `MultiAgentBatch`.
        assert isinstance(batch, dict)
        # Ensure that we have as many environment steps as the train batch size.
        assert (
            batch[DEFAULT_POLICY_ID][Columns.REWARDS].shape[0]
            == config.train_batch_size_per_learner
        )
        # Ensure all keys are available and the length of each value is the
        # train batch size.
        for key in EXPECTED_KEYS:
            assert key in batch[DEFAULT_POLICY_ID]
            assert (
                len(batch[DEFAULT_POLICY_ID][key])
                == config.train_batch_size_per_learner
            )

    def test_offline_prelearner_sample_from_episode_data(self):
        """Test sampling and writing of complete epsidoes.

        Creates episodes and writes them to disk with PPO.
        Reads some episodes from disk and transforms them with the `OfflinePreLearner`.
        Checks that the transformed data is a batch of size `train_batch_size_per_learner`.
        Deletes the generated data on disk after the test.
        """
        episodes_output_path = "/tmp/cartpole-v1_episodes/"
        config = (
            PPOConfig()
            .environment(
                env="CartPole-v1",
            )
            .training(train_batch_size_per_learner=256)
            .env_runners(
                batch_mode="complete_episodes",
                # num_env_runners=1,
            )
            .offline_data(
                output=episodes_output_path,
                output_write_episodes=True,
            )
            .training(
                # Use small batch sizes for the test.
                train_batch_size_per_learner=20,
                minibatch_size=10,
            )
        )

        # Record episodes.
        algo = config.build()
        algo.train()

        # Set input data and the episode read flag.
        config.offline_data(
            input_=[episodes_output_path],
            input_read_episodes=True,
            input_read_batch_size=1,
        )

        algo = config.build()

        episode_ds = ray.data.read_parquet(episodes_output_path)
        episode_batch = episode_ds.take_batch(64)
        module_state = algo.offline_data.learner_handles[0].get_state(
            component=COMPONENT_RL_MODULE,
        )[COMPONENT_RL_MODULE]
        offline_prelearner = OfflinePreLearner(
            config=config,
            module_spec=algo.offline_data.module_spec,
            module_state=module_state,
            spaces=algo.offline_data.spaces[INPUT_ENV_SPACES],
        )
        # Offline Prelearner is expected to map episodes to sample batches.
        batch = unflatten_dict(offline_prelearner(episode_batch))

        # Assert that we have a batch of `train_batch_size_per_learner`.
        assert DEFAULT_POLICY_ID in batch
        assert (
            batch[DEFAULT_POLICY_ID][Columns.REWARDS].shape[0]
            == config.train_batch_size_per_learner
        )

        # Remove all generated Parquet data from disk.
        shutil.rmtree(episodes_output_path)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
