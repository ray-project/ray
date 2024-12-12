import logging
from pathlib import Path
import pyarrow.fs
import ray
import time
import types

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core import COMPONENT_RL_MODULE
from ray.rllib.env import INPUT_ENV_SPACES
from ray.rllib.offline.offline_prelearner import OfflinePreLearner
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class OfflineData:
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def __init__(self, config: AlgorithmConfig):

        self.config = config
        self.is_multi_agent = self.config.is_multi_agent()
        self.path = (
            self.config.input_
            if isinstance(config.input_, list)
            else Path(config.input_)
        )
        # Use `read_parquet` as default data read method.
        self.data_read_method = self.config.input_read_method
        # Override default arguments for the data read method.
        self.data_read_method_kwargs = (
            self.default_read_method_kwargs | self.config.input_read_method_kwargs
        )
        # In case `EpisodeType` or `BatchType` batches are read the size
        # could differ from the final `train_batch_size_per_learner`.
        self.data_read_batch_size = self.config.input_read_batch_size

        # If data should be materialized.
        self.materialize_data = config.materialize_data
        # If mapped data should be materialized.
        self.materialize_mapped_data = config.materialize_mapped_data
        # Flag to identify, if data has already been mapped with the
        # `OfflinePreLearner`.
        self.data_is_mapped = False

        # Set the filesystem.
        self.filesystem = self.config.input_filesystem
        self.filesystem_kwargs = self.config.input_filesystem_kwargs
        self.filesystem_object = None

        # If a specific filesystem is given, set it up. Note, this could
        # be `gcsfs` for GCS, `pyarrow` for S3 or `adlfs` for Azure Blob Storage.
        # this filesystem is specifically needed, if a session has to be created
        # with the cloud provider.
        if self.filesystem == "gcs":
            import gcsfs

            self.filesystem_object = gcsfs.GCSFileSystem(**self.filesystem_kwargs)
        elif self.filesystem == "s3":
            self.filesystem_object = pyarrow.fs.S3FileSystem(**self.filesystem_kwargs)
        elif self.filesystem == "abs":
            import adlfs

            self.filesystem_object = adlfs.AzureBlobFileSystem(**self.filesystem_kwargs)
        elif isinstance(self.filesystem, pyarrow.fs.FileSystem):
            self.filesystem_object = self.filesystem
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
            start_time = time.perf_counter()
            self.data = getattr(ray.data, self.data_read_method)(
                self.path, **self.data_read_method_kwargs
            )
            if self.materialize_data:
                self.data = self.data.materialize()
            stop_time = time.perf_counter()
            logger.debug(f"Time for loading dataset: {stop_time - start_time}s.")
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
        # Materialize the mapped data, if necessary. This runs for all the
        # data the `OfflinePreLearner` logic and maps them to `MultiAgentBatch`es.
        # TODO (simon, sven): This would never update the module nor the
        #   the connectors. If this is needed we have to check, if we give
        #   (a) only an iterator and let the learner and OfflinePreLearner
        #       communicate through the object storage. This only works when
        #       not materializing.
        #   (b) Rematerialize the data every couple of iterations. This is
        #       is costly.
        if not self.data_is_mapped:
            # Constructor `kwargs` for the `OfflinePreLearner`.
            fn_constructor_kwargs = {
                "config": self.config,
                "learner": self.learner_handles[0],
                "spaces": self.spaces[INPUT_ENV_SPACES],
            }
            # If we have multiple learners, add to the constructor `kwargs`.
            if num_shards > 1:
                # Call here the learner to get an up-to-date module state.
                # TODO (simon): This is a workaround as along as learners cannot
                # receive any calls from another actor.
                module_state = ray.get(
                    self.learner_handles[0].get_state.remote(
                        component=COMPONENT_RL_MODULE
                    )
                )
                # Add constructor `kwargs` when using remote learners.
                fn_constructor_kwargs.update(
                    {
                        "learner": self.learner_handles,
                        "locality_hints": self.locality_hints,
                        "module_spec": self.module_spec,
                        "module_state": module_state,
                    }
                )

            self.data = self.data.map_batches(
                self.prelearner_class,
                fn_constructor_kwargs=fn_constructor_kwargs,
                batch_size=self.data_read_batch_size or num_samples,
                **self.map_batches_kwargs,
            )
            # Set the flag to `True`.
            self.data_is_mapped = True
            # If the user wants to materialize the data in memory.
            if self.materialize_mapped_data:
                self.data = self.data.materialize()
        # Build an iterator, if necessary.
        if (not self.batch_iterator and (not return_iterator or num_shards <= 1)) or (
            return_iterator and isinstance(self.batch_iterator, types.GeneratorType)
        ):
            # If no iterator should be returned, or if we want to return a single
            # batch iterator, we instantiate the batch iterator once, here.
            # TODO (simon, sven): The iterator depends on the `num_samples`, i.e.abs
            # sampling later with a different batch size would need a
            # reinstantiation of the iterator.
            self.batch_iterator = self.data.iter_batches(
                # This is important. The batch size is now 1, because the data
                # is already run through the `OfflinePreLearner` and a single
                # instance is a single `MultiAgentBatch` of size `num_samples`.
                batch_size=1,
                **self.iter_batches_kwargs,
            )

            if not return_iterator:
                self.batch_iterator = iter(self.batch_iterator)

        # Do we want to return an iterator or a single batch?
        if return_iterator:
            # In case of multiple shards, we return multiple
            # `StreamingSplitIterator` instances.
            if num_shards > 1:
                # TODO (simon): Check, if we should use `iter_batches_kwargs` here
                #   as well.
                logger.debug("===> [OfflineData]: Return streaming_split ... ")
                return self.data.streaming_split(
                    n=num_shards,
                    # Note, `equal` must be `True`, i.e. the batch size must
                    # be the same for all batches b/c otherwise remote learners
                    # could block each others.
                    equal=True,
                    locality_hints=self.locality_hints,
                )

            # Otherwise, we return a simple batch `DataIterator`.
            else:
                return self.batch_iterator
        else:
            # Return a single batch from the iterator.
            try:
                return next(self.batch_iterator)["batch"][0]
            except StopIteration:
                # If the batch iterator is exhausted, reinitiate a new one.
                logger.debug(
                    "===> [OfflineData]: Batch iterator exhausted. Reinitiating ..."
                )
                self.batch_iterator = None
                return self.sample(
                    num_samples=num_samples,
                    return_iterator=return_iterator,
                    num_shards=num_shards,
                )

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
