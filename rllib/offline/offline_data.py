import logging
import time
import types
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict

import numpy as np
import pyarrow.fs

import ray
from ray.rllib.core import COMPONENT_RL_MODULE
from ray.rllib.env import INPUT_ENV_SPACES
from ray.rllib.offline.offline_prelearner import OfflinePreLearner
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils import force_list, unflatten_dict
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class OfflineData:
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def __init__(self, config: "AlgorithmConfig"):

        # TODO (simon): Define self.spaces here.
        self.config = config
        self.is_multi_agent = self.config.is_multi_agent
        self.path = (
            self.config.input_
            if isinstance(config.input_, list)
            else Path(config.input_)
        )
        # Use `read_parquet` as default data read method.
        self.data_read_method = self.config.input_read_method
        # Override default arguments for the data read method.
        self.data_read_method_kwargs = self.config.input_read_method_kwargs
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
                f"Unknown `config.input_filesystem` {self.filesystem}! Filesystems "
                "can be None for local, any instance of `pyarrow.fs.FileSystem`, "
                "'gcs' for GCS, 's3' for S3, or 'abs' for adlfs.AzureBlobFileSystem."
            )
        # Add the filesystem object to the write method kwargs.
        if self.filesystem_object:
            self.data_read_method_kwargs.update(
                {
                    "filesystem": self.filesystem_object,
                }
            )

        # Load the dataset.
        start_time = time.perf_counter()
        self.data = getattr(ray.data, self.data_read_method)(
            self.path, **self.data_read_method_kwargs
        )
        if self.materialize_data:
            self.data = self.data.materialize()
        stop_time = time.perf_counter()
        logger.debug(
            f"Time to load offline data from {self.path}: {stop_time - start_time:.2f}s."
        )

        # Avoids reinstantiating the batch iterator each time we sample.
        self.batch_iterators = None
        self.map_batches_kwargs = (
            self.default_map_batches_kwargs | self.config.map_batches_kwargs
        )
        self.iter_batches_kwargs = (
            self.default_iter_batches_kwargs | self.config.iter_batches_kwargs
        )
        self.returned_streaming_split = False
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
        module_state: Dict[str, Any] = None,
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

            if not module_state:
                # Get the RLModule state from learners.
                if num_shards >= 1:
                    # Call here the learner to get an up-to-date module state.
                    # TODO (simon): This is a workaround as along as learners cannot
                    # receive any calls from another actor.
                    module_state = ray.get(
                        self.learner_handles[0].get_state.remote(
                            component=COMPONENT_RL_MODULE,
                        )
                    )[COMPONENT_RL_MODULE]
                    # Provide the `Learner`(s) GPU devices, if needed.
                    # if not self.map_batches_uses_gpus(self.config) and self.config._validate_config:
                    #     devices = ray.get(self.learner_handles[0].get_device.remote())
                    #     devices = [devices] if not isinstance(devices, list) else devices
                    #     device_strings = [
                    #         f"{device.type}:{str(device.index)}"
                    #         if device.type == "cuda"
                    #         else device.type
                    #         for device in devices
                    #     ]
                    # # Otherwise, set the GPU strings to `None`.
                    # # TODO (simon): Check inside 'OfflinePreLearner'.
                    # else:
                    #     device_strings = None
                else:
                    # Get the module state from the `Learner`(S).
                    module_state = self.learner_handles[0].get_state(
                        component=COMPONENT_RL_MODULE,
                    )[COMPONENT_RL_MODULE]
                    # Provide the `Learner`(s) GPU devices, if needed.
                    # if not self.map_batches_uses_gpus(self.config) and self.config._validate_config:
                    #     device = self.learner_handles[0].get_device()
                    #     device_strings = [
                    #         f"{device.type}:{str(device.index)}"
                    #         if device.type == "cuda"
                    #         else device.type
                    #     ]
                    # else:
                    #     device_strings = None
            # Constructor `kwargs` for the `OfflinePreLearner`.
            fn_constructor_kwargs = {
                "config": self.config,
                "spaces": self.spaces[INPUT_ENV_SPACES],
                "module_spec": self.module_spec,
                "module_state": module_state,
                # "device_strings": self.get_devices(),
            }

            # Map the data to run the `OfflinePreLearner`s in the data pipeline
            # for training.
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
        # Build an iterator, if necessary. Note, in case that an iterator should be
        # returned now and we have already generated from the iterator, i.e.
        # `isinstance(self.batch_iterators, types.GeneratorType) == True`, we need
        # to create here a new iterator.
        if not self.batch_iterators or (
            return_iterator and isinstance(self.batch_iterators, types.GeneratorType)
        ):
            # If we have more than one learner create an iterator for each of them
            # by splitting the data stream.
            if num_shards > 1:
                # In case of multiple shards, we return multiple
                # `StreamingSplitIterator` instances.
                self.batch_iterators = self.data.streaming_split(
                    n=num_shards,
                    # Note, `equal` must be `True`, i.e. the batch size must
                    # be the same for all batches b/c otherwise remote learners
                    # could block each others.
                    equal=True,
                    locality_hints=self.locality_hints,
                )
            # Otherwise we create a simple iterator and - if necessary - initialize
            # it here.
            else:
                # Should an iterator be returned?
                if return_iterator:
                    self.batch_iterators = self.data.iterator()
                # Otherwise, the user wants batches returned.
                else:
                    # Define a collate (last-mile) transformation that maps batches
                    # to RLlib's `MultiAgentBatch`.
                    def _collate_fn(_batch: Dict[str, np.ndarray]) -> MultiAgentBatch:
                        _batch = unflatten_dict(_batch)
                        return MultiAgentBatch(
                            {
                                module_id: SampleBatch(module_data)
                                for module_id, module_data in _batch.items()
                            },
                            env_steps=sum(
                                len(next(iter(module_data.values())))
                                for module_data in _batch.values()
                            ),
                        )

                    # If no iterator should be returned, or if we want to return a single
                    # batch iterator, we instantiate the batch iterator once, here.
                    self.batch_iterators = self.data.iter_batches(
                        batch_size=num_samples,
                        _collate_fn=_collate_fn,
                        **self.iter_batches_kwargs,
                    )
                    self.batch_iterators = iter(self.batch_iterators)

        # Do we want to return an iterator or a single batch?
        if return_iterator:
            return force_list(self.batch_iterators)
        else:
            # Return a single batch from the iterator.
            try:
                return next(self.batch_iterators)
            except StopIteration:
                # If the batch iterator is exhausted, reinitiate a new one.
                logger.debug("Batch iterator exhausted. Reinitiating ...")
                self.batch_iterators = None
                return self.sample(
                    num_samples=num_samples,
                    return_iterator=return_iterator,
                    num_shards=num_shards,
                )

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
        }
