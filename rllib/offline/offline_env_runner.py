import logging
import ray

from pathlib import Path
from typing import List

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core.columns import Columns
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
    OverrideToImplementCustomLogic,
)
from ray.rllib.utils.compression import pack_if_needed
from ray.rllib.utils.spaces.space_utils import to_jsonable_if_needed
from ray.rllib.utils.typing import EpisodeType
from ray.util.debug import log_once
from ray.util.annotations import PublicAPI

logger = logging.Logger(__file__)

# TODO (simon): This class can be agnostic to the episode type as it
#  calls only get_state.


@PublicAPI(stability="alpha")
class OfflineSingleAgentEnvRunner(SingleAgentEnvRunner):
    """The environment runner to record the single agent case."""

    @override(SingleAgentEnvRunner)
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def __init__(self, *, config: AlgorithmConfig, **kwargs):
        # Initialize the parent.
        super().__init__(config=config, **kwargs)

        # Get the data context for this `EnvRunner`.
        data_context = ray.data.DataContext.get_current()
        # Limit the resources for Ray Data to the CPUs given to this `EnvRunner`.
        data_context.execution_options.resource_limits = (
            data_context.execution_options.resource_limits.copy(
                cpu=config.num_cpus_per_env_runner
            )
        )

        # Set the output write method.
        self.output_write_method = self.config.output_write_method
        self.output_write_method_kwargs = self.config.output_write_method_kwargs

        # Set the filesystem.
        self.filesystem = self.config.output_filesystem
        self.filesystem_kwargs = self.config.output_filesystem_kwargs
        self.filesystem_object = None

        # Set the output base path.
        self.output_path = self.config.output
        # Set the subdir (environment specific).
        self.subdir_path = self.config.env.lower()
        # Set the worker-specific path name. Note, this is
        # specifically to enable multi-threaded writing into
        # the same directory.
        self.worker_path = "run-" + f"{self.worker_index}".zfill(6)

        # If a specific filesystem is given, set it up. Note, this could
        # be `gcsfs` for GCS, `pyarrow` for S3 or `adlfs` for Azure Blob Storage.
        # this filesystem is specifically needed, if a session has to be created
        # with the cloud provider.

        if self.filesystem == "gcs":
            import gcsfs

            self.filesystem_object = gcsfs.GCSFileSystem(**self.filesystem_kwargs)
        elif self.filesystem == "s3":
            from pyarrow import fs

            self.filesystem_object = fs.S3FileSystem(**self.filesystem_kwargs)
        elif self.filesystem == "abs":
            import adlfs

            self.filesystem_object = adlfs.AzureBlobFileSystem(**self.filesystem_kwargs)
        elif self.filesystem is not None:
            raise ValueError(
                f"Unknown filesystem: {self.filesystem}. Filesystems can be "
                "'gcs' for GCS, 's3' for S3, or 'abs'"
            )
        # Add the filesystem object to the write method kwargs.
        self.output_write_method_kwargs.update(
            {
                "filesystem": self.filesystem_object,
            }
        )

        # If we should store `SingleAgentEpisodes` or column data.
        self.output_write_episodes = self.config.output_write_episodes
        # Which columns should be compressed in the output data.
        self.output_compress_columns = self.config.output_compress_columns

        # Buffer these many rows before writing to file.
        self.output_max_rows_per_file = self.config.output_max_rows_per_file
        # If the user defines a maximum number of rows per file, set the
        # event to `False` and check during sampling.
        if self.output_max_rows_per_file:
            self.write_data_this_iter = False
        # Otherwise the event is always `True` and we write always sampled
        # data immediately to disk.
        else:
            self.write_data_this_iter = True

        # If the remaining data should be stored. Note, this is only
        # relevant in case `output_max_rows_per_file` is defined.
        self.write_remaining_data = self.config.output_write_remaining_data

        # Counts how often `sample` is called to define the output path for
        # each file.
        self._sample_counter = 0

        # Define the buffer for experiences stored until written to disk.
        self._samples = []

    @override(SingleAgentEnvRunner)
    @OverrideToImplementCustomLogic
    def sample(
        self,
        *,
        num_timesteps: int = None,
        num_episodes: int = None,
        explore: bool = None,
        random_actions: bool = False,
        force_reset: bool = False,
    ) -> List[SingleAgentEpisode]:
        """Samples from environments and writes data to disk."""

        # Call the super sample method.
        samples = super().sample(
            num_timesteps=num_timesteps,
            num_episodes=num_episodes,
            explore=explore,
            random_actions=random_actions,
            force_reset=force_reset,
        )

        self._sample_counter += 1

        # Add data to the buffers.
        if self.output_write_episodes:

            import msgpack
            import msgpack_numpy as mnp

            if log_once("msgpack"):
                logger.info(
                    "Packing episodes with `msgpack` and encode array with "
                    "`msgpack_numpy` for serialization. This is needed for "
                    "recording episodes."
                )
            # Note, we serialize episodes with `msgpack` and `msgpack_numpy` to
            # ensure version compatibility.
            self._samples.extend(
                [msgpack.packb(eps.get_state(), default=mnp.encode) for eps in samples]
            )
        else:
            self._map_episodes_to_data(samples)

        # If the user defined the maximum number of rows to write.
        if self.output_max_rows_per_file:
            # Check, if this number is reached.
            if len(self._samples) >= self.output_max_rows_per_file:
                # Start the recording of data.
                self.write_data_this_iter = True

        if self.write_data_this_iter:
            # If the user wants a maximum number of experiences per file,
            # cut the samples to write to disk from the buffer.
            if self.output_max_rows_per_file:
                # Reset the event.
                self.write_data_this_iter = False
                # Ensure that all data ready to be written is released from
                # the buffer. Note, this is important in case we have many
                # episodes sampled and a relatively small `output_max_rows_per_file`.
                while len(self._samples) >= self.output_max_rows_per_file:
                    # Extract the number of samples to be written to disk this
                    # iteration.
                    samples_to_write = self._samples[: self.output_max_rows_per_file]
                    # Reset the buffer to the remaining data. This only makes sense, if
                    # `rollout_fragment_length` is smaller `output_max_rows_per_file` or
                    # a 2 x `output_max_rows_per_file`.
                    self._samples = self._samples[self.output_max_rows_per_file :]
                    samples_ds = ray.data.from_items(samples_to_write)
            # Otherwise, write the complete data.
            else:
                samples_ds = ray.data.from_items(self._samples)
            try:
                # Setup the path for writing data. Each run will be written to
                # its own file. A run is a writing event. The path will look
                # like. 'base_path/env-name/00000<WorkerID>-00000<RunID>'.
                path = (
                    Path(self.output_path)
                    .joinpath(self.subdir_path)
                    .joinpath(self.worker_path + f"-{self._sample_counter}".zfill(6))
                )
                getattr(samples_ds, self.output_write_method)(
                    path.as_posix(), **self.output_write_method_kwargs
                )
                logger.info(f"Wrote samples to storage at {path}.")
            except Exception as e:
                logger.error(e)

        self.metrics.log_value(
            key="recording_buffer_size",
            value=len(self._samples),
        )

        # Finally return the samples as usual.
        return samples

    @override(EnvRunner)
    @OverrideToImplementCustomLogic
    def stop(self) -> None:
        """Writes the reamining samples to disk

        Note, if the user defined `max_rows_per_file` the
        number of rows for the remaining samples could be
        less than the defined maximum row number by the user.
        """
        # If there are samples left over we have to write htem to disk. them
        # to a dataset.
        if self._samples and self.write_remaining_data:
            # Convert them to a `ray.data.Dataset`.
            samples_ds = ray.data.from_items(self._samples)
            # Increase the sample counter for the folder/file name.
            self._sample_counter += 1
            # Try to write the dataset to disk/cloud storage.
            try:
                # Setup the path for writing data. Each run will be written to
                # its own file. A run is a writing event. The path will look
                # like. 'base_path/env-name/00000<WorkerID>-00000<RunID>'.
                path = (
                    Path(self.output_path)
                    .joinpath(self.subdir_path)
                    .joinpath(self.worker_path + f"-{self._sample_counter}".zfill(6))
                )
                getattr(samples_ds, self.output_write_method)(
                    path.as_posix(), **self.output_write_method_kwargs
                )
                logger.info(
                    f"Wrote final samples to storage at {path}. Note "
                    "Note, final samples could be smaller in size than "
                    f"`max_rows_per_file`, if defined."
                )
            except Exception as e:
                logger.error(e)

        logger.debug(f"Experience buffer length: {len(self._samples)}")

    @OverrideToImplementCustomLogic
    def _map_episodes_to_data(self, samples: List[EpisodeType]) -> None:
        """Converts list of episodes to list of single dict experiences.

        Note, this method also appends all sampled experiences to the
        buffer.

        Args:
            samples: List of episodes to be converted.
        """
        # Loop through all sampled episodes.
        obs_space = self.env.observation_space
        action_space = self.env.action_space
        for sample in samples:
            # Loop through all items of the episode.
            for i in range(len(sample)):
                sample_data = {
                    Columns.EPS_ID: sample.id_,
                    Columns.AGENT_ID: sample.agent_id,
                    Columns.MODULE_ID: sample.module_id,
                    # Compress observations, if requested.
                    Columns.OBS: pack_if_needed(
                        to_jsonable_if_needed(sample.get_observations(i), obs_space)
                    )
                    if Columns.OBS in self.output_compress_columns
                    else to_jsonable_if_needed(sample.get_observations(i), obs_space),
                    # Compress actions, if requested.
                    Columns.ACTIONS: pack_if_needed(
                        to_jsonable_if_needed(sample.get_actions(i), action_space)
                    )
                    if Columns.ACTIONS in self.output_compress_columns
                    else to_jsonable_if_needed(sample.get_actions(i), action_space),
                    Columns.REWARDS: sample.get_rewards(i),
                    # Compress next observations, if requested.
                    Columns.NEXT_OBS: pack_if_needed(
                        to_jsonable_if_needed(sample.get_observations(i + 1), obs_space)
                    )
                    if Columns.OBS in self.output_compress_columns
                    else to_jsonable_if_needed(
                        sample.get_observations(i + 1), obs_space
                    ),
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
                self._samples.append(sample_data)
