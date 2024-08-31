import logging
from pathlib import Path
import ray

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core import COMPONENT_RL_MODULE
from ray.rllib.env import INPUT_ENV_SPACES
from ray.rllib.offline.offline_prelearner import OfflinePreLearner
from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)

logger = logging.getLogger(__name__)


@ExperimentalAPI
class OfflineData:
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def __init__(self, config: AlgorithmConfig):

        self.config = config
        self.is_multi_agent = config.is_multi_agent()
        self.path = (
            config.input_ if isinstance(config.input_, list) else Path(config.input_)
        )
        # Use `read_parquet` as default data read method.
        self.data_read_method = config.input_read_method
        # Override default arguments for the data read method.
        self.data_read_method_kwargs = (
            self.default_read_method_kwargs | config.input_read_method_kwargs
        )

        # Set the filesystem.
        self.filesystem = self.config.output_filesystem
        self.filesystem_kwargs = self.config.output_filesystem_kwargs
        self.filesystem_object = None

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
        self.data_read_method_kwargs.update(
            {
                "filesystem": self.filesystem_object,
            }
        )

        try:
            # Load the dataset.
            self.data = getattr(ray.data, self.data_read_method)(
                self.path, **self.data_read_method_kwargs
            )
            logger.info("Reading data from {}".format(self.path))
            logger.info(self.data.schema())
        except Exception as e:
            logger.error(e)
        # Avoids reinstantiating the batch iterator each time we sample.
        self.batch_iterator = None
        self.map_batches_kwargs = (
            self.default_map_batches_kwargs | self.config.map_batches_kwargs
        )
        self.iter_batches_kwargs = (
            self.default_iter_batches_kwargs | self.config.iter_batches_kwargs
        )
        # Defines the prelearner class. Note, this could be user-defined.
        self.prelearner_class = self.config.prelearner_class or OfflinePreLearner
        # For remote learner setups.
        self.locality_hints = None
        self.learner_handles = None
        self.module_spec = None

    @OverrideToImplementCustomLogic
    def sample(
        self,
        num_samples: int,
        return_iterator: bool = False,
        num_shards: int = 1,
    ):
        if (
            not return_iterator
            or return_iterator
            and num_shards <= 1
            and not self.batch_iterator
        ):
            # If no iterator should be returned, or if we want to return a single
            # batch iterator, we instantiate the batch iterator once, here.
            # TODO (simon, sven): The iterator depends on the `num_samples`, i.e.abs
            # sampling later with a different batch size would need a
            # reinstantiation of the iterator.

            self.batch_iterator = self.data.map_batches(
                self.prelearner_class,
                fn_constructor_kwargs={
                    "config": self.config,
                    "learner": self.learner_handles[0],
                    "spaces": self.spaces[INPUT_ENV_SPACES],
                },
                batch_size=num_samples,
                **self.map_batches_kwargs,
            ).iter_batches(
                batch_size=num_samples,
                **self.iter_batches_kwargs,
            )

        # Do we want to return an iterator or a single batch?
        if return_iterator:
            # In case of multiple shards, we return multiple
            # `StreamingSplitIterator` instances.
            if num_shards > 1:
                # Call here the learner to get an up-to-date module state.
                # TODO (simon): This is a workaround as along as learners cannot
                # receive any calls from another actor.
                module_state = ray.get(
                    self.learner_handles[0].get_state.remote(
                        component=COMPONENT_RL_MODULE
                    )
                )
                return self.data.map_batches(
                    # TODO (cheng su): At best the learner handle passed in here should
                    # be the one from the learner that is nearest, but here we cannot
                    # provide locality hints.
                    self.prelearner_class,
                    fn_constructor_kwargs={
                        "config": self.config,
                        "learner": self.learner_handles,
                        "spaces": self.spaces["__env__"],
                        "locality_hints": self.locality_hints,
                        "module_spec": self.module_spec,
                        "module_state": module_state,
                    },
                    batch_size=num_samples,
                    **self.map_batches_kwargs,
                ).streaming_split(
                    n=num_shards, equal=False, locality_hints=self.locality_hints
                )

            # Otherwise, we return a simple batch `DataIterator`.
            else:
                return self.batch_iterator
        else:
            # Return a single batch from the iterator.
            return next(iter(self.batch_iterator))["batch"][0]

    @property
    def default_read_method_kwargs(self):
        return {
            "override_num_blocks": max(self.config.num_learners * 2, 2),
        }

    @property
    def default_map_batches_kwargs(self):
        return {
            "concurrency": max(2, self.config.num_learners),
            "zero_copy_batch": True,
        }

    @property
    def default_iter_batches_kwargs(self):
        return {
            "prefetch_batches": 2,
            "local_shuffle_buffer_size": self.config.train_batch_size_per_learner
            or (self.config.train_batch_size // max(1, self.config.num_learners)) * 4,
        }
