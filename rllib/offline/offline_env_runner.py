import logging
import ray

from pathlib import Path
from typing import List

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.annotations import PublicAPI, override

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
        self.subdir_path = self.config.env
        # Set the worker-specific path name. Note, this is
        # specifically to enable multi-threaded writing into
        # the same directory.
        self.worker_path = f"00000{self.worker_index}-"

        # If a specific filesystem is given, set it up. Note, this could
        # be `gcsfs` for GCS, `pyarrow` for S3 or `adlfs` for Azure Blob Storage.
        if self.filesystem:
            if self.filesystem == "gcs":
                import gcsfs
                self.filesystem_object = gcsfs.GCSFileSystem(**self.filesystem_kwargs)
            elif self.filesystem =="s3":
                from pyarrow import fs
                self.filesystem_object = fs.S3FileSystem(**self.filesystem_kwargs)
            elif self.filesystem == "abs":
                import adlfs
                self.filesystem_object = adlfs.AzureBlobFileSystem(**self.filesystem_kwargs)
            else:
                raise ValueError(
                    f"Unknown filesystem: {self.filesystem}. Filesystems can be 'gcs' for GCS, "
                    "'s3' for S3, or 'abs'"
                )

        # If we should store `SingleAgentEpisodes` or column data.
        self.output_write_episodes = self.config.output_write_episodes

        # Counts how often `sample` is called to define the output path for
        # each file.
        self.sample_counter = 0

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
        self.sample_counter += 1
        samples = super().sample()
        
        # TODO (simon): Implement a num_rows_per_file.
        # Write episodes as objects.
        if self.output_write_episodes:
            pass
        # Otherwise store as column data.
        else:
            try:
                path = (
                    Path(self.output_path)
                    .joinpath(self.subdir_path)
                    .joinpath(self.worker_path)
                    .joinpath(f"-{self.sample_counter}")
                )
                getattr(ray.data, self.data_write_method)(
                    path.as_posix(), **self.data_write_method_kwargs
                )

                logger.info("Wrote samples to storage.")
            except Exception as e:
                logger.error(e)

        # Finally return the samples as usual.
        return samples

        

