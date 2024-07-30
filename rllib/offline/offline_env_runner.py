import logging
import ray

from pathlib import Path
from typing import List

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core.columns import Columns
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.annotations import override
from ray.rllib.utils.compression import pack_if_needed
from ray.rllib.utils.typing import EpisodeType

logger = logging.Logger(__file__)

# TODO (simon): This class can be agnostic to the episode type as it
# calls only get_state.


class OfflineSingleAgentEnvRunner(SingleAgentEnvRunner):
    """The environment runner to record the single agent case."""

    @override(SingleAgentEnvRunner)
    def __init__(self, config: AlgorithmConfig, **kwargs):
        # Initialize the parent.
        super().__init__(config, **kwargs)

        # Set the output write method.
        self.data_write_method = self.config.output_data_write_method
        self.data_write_method_kwargs = self.config.output_data_write_method_kwargs

        # Set the filesystem.
        self.filesystem = self.config.output_filesystem
        self.filesystem_kwargs = self.config.output_filesystem_kwargs
        # Set the output base path.
        self.output_path = self.config.output
        # Set the subdir (environment specific).
        self.subdir_path = self.config.env.lower()
        # Set the worker-specific path name. Note, this is
        # specifically to enable multi-threaded writing into
        # the same directory.
        self.worker_path = "run-" + f"{self.worker_index}".zfill(6) + "-"

        # If a specific filesystem is given, set it up. Note, this could
        # be `gcsfs` for GCS, `pyarrow` for S3 or `adlfs` for Azure Blob Storage.
        if self.filesystem:
            if self.filesystem == "gcs":
                import gcsfs

                self.filesystem_object = gcsfs.GCSFileSystem(**self.filesystem_kwargs)
            elif self.filesystem == "s3":
                from pyarrow import fs

                self.filesystem_object = fs.S3FileSystem(**self.filesystem_kwargs)
            elif self.filesystem == "abs":
                import adlfs

                self.filesystem_object = adlfs.AzureBlobFileSystem(
                    **self.filesystem_kwargs
                )
            else:
                raise ValueError(
                    f"Unknown filesystem: {self.filesystem}. Filesystems can be "
                    "'gcs' for GCS, "
                    "'s3' for S3, or 'abs'"
                )
            # Add the filesystem object to the write method kwargs.
            self.data_write_method_kwargs.update(
                {
                    "filesystem": self.filesystem_object,
                }
            )

        # If we should store `SingleAgentEpisodes` or column data.
        self.output_write_episodes = self.config.output_write_episodes
        self.output_compress_columns = self.config.output_compress_columns

        # Buffer these many rows before writing to file.
        self.output_max_rows_per_file = self.config.output_max_rows_per_file
        if self.output_max_rows_per_file:
            self.write_data_this_iter = False
        else:
            self.write_data_this_iter = True

        # Counts how often `sample` is called to define the output path for
        # each file.
        self._sample_counter = 0
        self._num_env_steps = 0

        if self.output_write_episodes:
            self._samples = []
        else:
            self._samples_data = []

    @override(SingleAgentEnvRunner)
    def sample(
        self,
        *,
        num_timesteps: int = None,
        num_episodes: int = None,
        explore: bool = None,
        random_actions: bool = False,
        force_reset: bool = False,
    ) -> List[SingleAgentEpisode]:

        # Call the super sample method.
        self._sample_counter += 1

        samples = super().sample(
            num_timesteps=num_timesteps,
            num_episodes=num_episodes,
            explore=explore,
            random_actions=random_actions,
            force_reset=force_reset,
        )

        # Increase env step counter.
        self._num_env_steps += sum(eps.env_steps() for eps in samples)

        # Add data to the buffers.
        if self.output_write_episodes:
            self._samples.extend(samples)
        else:
            self._map_episodes_to_data(samples)

        # Start the recording of data.
        if self.output_max_rows_per_file:
            if self._num_env_steps >= self.output_max_rows_per_file:
                self.write_data_this_iter = True

        if self.write_data_this_iter:
            # Write episodes as objects.
            if self.output_write_episodes:
                # TODO (simon): Complete.
                pass
            # Otherwise store as column data.
            else:
                if self.output_max_rows_per_file:
                    self.write_data_this_iter = False

                samples_to_write = self._samples_data[: self.output_max_rows_per_file]
                self._samples_data = self._samples_data[self.output_max_rows_per_file :]
                self._num_env_steps = len(self._samples_data)
                samples_ds = ray.data.from_items(samples_to_write)
                try:
                    path = (
                        Path(self.output_path)
                        .joinpath(self.subdir_path)
                        .joinpath(
                            self.worker_path + f"-{self._sample_counter}".zfill(6)
                        )
                    )
                    getattr(samples_ds, self.data_write_method)(
                        path.as_posix(), **self.data_write_method_kwargs
                    )
                    logger.info("Wrote samples to storage.")
                except Exception as e:
                    logger.error(e)

        # Finally return the samples as usual.
        return samples

    def _map_episodes_to_data(self, samples: List[EpisodeType]) -> None:
        """Converts list of episodes to list of single dict experiences.

        Note, this method also appends all sampled experiences to the
        buffer.

        Args:
            samples: List of episodes to be converted.
        """
        # Loop through all sampled episodes.
        for sample in samples:
            # Loop through all items of the episode.
            for i in range(len(sample)):
                sample_data = {
                    Columns.EPS_ID: sample.id_,
                    Columns.AGENT_ID: sample.agent_id,
                    Columns.MODULE_ID: sample.module_id,
                    # Compress observations, if requested.
                    Columns.OBS: pack_if_needed(sample.get_observations(i))
                    if Columns.OBS in self.output_compress_columns
                    else sample.get_observations(i),
                    # Compress actions, if requested.
                    Columns.ACTIONS: pack_if_needed(sample.get_actions(i))
                    if Columns.OBS in self.output_compress_columns
                    else sample.get_actions(i),
                    Columns.REWARDS: sample.get_rewards(i),
                    # Compress next observations, if requested.
                    Columns.NEXT_OBS: pack_if_needed(sample.get_observations(i + 1))
                    if Columns.OBS in self.output_compress_columns
                    else sample.get_observations(i + 1),
                    Columns.TERMINATEDS: False
                    if i < len(sample) - 1
                    else sample.is_terminated,
                    Columns.TRUNCATEDS: False
                    if i < len(sample) - 1
                    else sample.is_truncated,
                    **{
                        # Compress any extra model output, if requested.
                        k: pack_if_needed(sample.get_extra_model_outputs(k, i))
                        if k in self.output_compress_columns
                        else sample.get_extra_model_outputs(k, i)
                        for k in sample.extra_model_outputs.keys()
                    },
                }
                # Finally append to the data buffer.
                self._samples_data.append(sample_data)
