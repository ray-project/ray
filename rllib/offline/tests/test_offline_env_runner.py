import pathlib
import shutil
import unittest

import msgpack
import msgpack_numpy as m

import ray
from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.offline.offline_data import OfflineData
from ray.rllib.offline.offline_env_runner import OfflineSingleAgentEnvRunner


class TestOfflineEnvRunner(unittest.TestCase):
    def setUp(self) -> None:
        self.base_path = pathlib.Path("/tmp/")
        self.config = (
            PPOConfig()
            .env_runners(
                # This defines how many rows per file we will
                # have (given `num_rows_per_file` in the
                # `output_write_method_kwargs` is not set).
                rollout_fragment_length=1000,
                num_env_runners=0,
                # Note, this means that written episodes. if
                # `output_write_episodes=True` will be incomplete
                # in many cases.
                batch_mode="truncate_episodes",
            )
            .environment("CartPole-v1")
            .rl_module(
                # Use a small network for this test.
                model_config=DefaultModelConfig(
                    fcnet_hiddens=[32],
                    fcnet_activation="linear",
                    vf_share_layers=True,
                )
            )
        )
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_offline_env_runner_record_episodes(self):
        """Tests recording of episodes.

        Note, in this case each row of the dataset is an episode
        that could potentially contain hundreds of steps.
        """
        data_dir = "local://" / self.base_path / "cartpole-episodes"
        config = self.config.offline_data(
            output=data_dir.as_posix(),
            # Store experiences in episodes.
            output_write_episodes=True,
        )

        offline_env_runner = OfflineSingleAgentEnvRunner(config=config, worker_index=1)
        # Sample 100 episodes.
        _ = offline_env_runner.sample(
            num_episodes=100,
            random_actions=True,
        )

        data_path = data_dir / self.config.env.lower()
        records = list(data_path.iterdir())

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].name, "run-000001-00001")

        # Now read in episodes.
        config = self.config.offline_data(
            input_=[data_path.as_posix()],
            input_read_episodes=True,
        )
        offline_data = OfflineData(config)
        # Assert the dataset has only 100 rows (each row containing an episode).
        self.assertEqual(offline_data.data.count(), 100)
        # Take a single row and ensure its a `SingleAgentEpisode` instance.
        self.assertIsInstance(
            SingleAgentEpisode.from_state(
                msgpack.unpackb(
                    offline_data.data.take(1)[0]["item"], object_hook=m.decode
                )
            ),
            SingleAgentEpisode,
        )
        # The batch contains now episodes (in a numpy.NDArray).
        episodes = offline_data.data.take_batch(100)["item"]
        # The batch should contain 100 episodes (not 100 env steps).
        self.assertEqual(len(episodes), 100)
        # Remove all data.
        shutil.rmtree(data_dir)

    def test_offline_env_runner_record_column_data(self):
        """Tests recording of single time steps in column format.

        Note, in this case each row in the dataset contains only a single
        timestep of the agent.
        """
        data_dir = "local://" / self.base_path / "cartpole-columns"
        config = self.config.offline_data(
            output=data_dir.as_posix(),
            # Store experiences in episodes.
            output_write_episodes=False,
            # Do not compress columns.
            output_compress_columns=[],
        )

        offline_env_runner = OfflineSingleAgentEnvRunner(config=config, worker_index=1)

        _ = offline_env_runner.sample(
            num_timesteps=100,
            random_actions=True,
        )

        data_path = data_dir / self.config.env.lower()
        records = list(data_path.iterdir())

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].name, "run-000001-00001")

        # Now read in episodes.
        config = self.config.offline_data(
            input_=[data_path.as_posix()],
            input_read_episodes=False,
        )
        offline_data = OfflineData(config)
        # Assert the dataset has only 100 rows.
        self.assertEqual(offline_data.data.count(), 100)
        # The batch contains now episodes (in a numpy.NDArray).
        batch = offline_data.data.take_batch(100)
        # The batch should contain 100 episodes (not 100 env steps).
        self.assertTrue(len(batch[Columns.OBS]) == 100)
        # Remove all data.
        shutil.rmtree(data_dir)

    def test_offline_env_runner_compress_columns(self):
        """Tests recording of timesteps with compressed columns.

        Note, `input_compress_columns` will compress only the columns
        listed. `Columns.OBS` will also compress `Columns.NEXT_OBS`.
        """
        data_dir = "local://" / self.base_path / "cartpole-columns"
        config = self.config.offline_data(
            output=data_dir.as_posix(),
            # Store experiences in episodes.
            output_write_episodes=False,
            # LZ4-compress columns 'obs', 'new_obs', and 'actions' to
            # save disk space and increase performance. Note, this means
            # that you have to use `input_compress_columns` in the same
            # way when using the data for training in `RLlib`.
            output_compress_columns=[Columns.OBS, Columns.ACTIONS],
            # In addition compress the complete file.
            # TODO (simon): This does not work. It looks as if there
            # is an error in the write/read methods for qparquet in
            # ray.data. `arrow_open_stream_args` nor `arrow_parquet_args`
            # do work here.
            # output_write_method_kwargs={
            #     "arrow_open_stream_args": {
            #         "compression": "gzip",
            #     }
            # }
        )

        offline_env_runner = OfflineSingleAgentEnvRunner(config=config, worker_index=1)

        _ = offline_env_runner.sample(
            num_timesteps=100,
            random_actions=True,
        )

        data_path = data_dir / self.config.env.lower()
        records = list(data_path.iterdir())

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].name, "run-000001-00001")

        # Now read in episodes.
        config = self.config.offline_data(
            input_=[(data_path / "run-000001-00001").as_posix()],
            input_read_episodes=False,
            # Also uncompress files and columns.
            # TODO (simon): Activate as soon as the bug is fixed
            # in ray.data.
            # input_read_method_kwargs={
            #     "arrow_open_stream_args": {
            #         "compression": "gzip",
            #     }
            # },
            input_compress_columns=[Columns.OBS, Columns.ACTIONS],
        )
        offline_data = OfflineData(config)
        # Assert the dataset has only 100 rows.
        self.assertEqual(offline_data.data.count(), 100)
        # The batch contains now episodes (in a numpy.NDArray).
        batch = offline_data.data.take_batch(100)
        # The batch should contain 100 episodes (not 100 env steps).
        self.assertTrue(len(batch[Columns.OBS]) == 100)
        # Remove all data.
        shutil.rmtree(data_dir)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
