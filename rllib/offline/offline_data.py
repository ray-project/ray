import logging
from pathlib import Path
import ray

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core import COMPONENT_RL_MODULE
from ray.rllib.core.columns import Columns
from ray.rllib.offline.offline_prelearner import OfflinePreLearner
from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)

logger = logging.getLogger(__name__)

# This is the default schema used if no `input_read_schema` is set in
# the config. If a user passes in a schema into `input_read_schema`
# this user-defined schema has to comply with the keys of `SCHEMA`,
# while values correspond to the columns in the user's dataset. Note
# that only the user-defined values will be overridden while all
# other values from SCHEMA remain as defined here.
SCHEMA = {
    Columns.EPS_ID: Columns.EPS_ID,
    Columns.AGENT_ID: Columns.AGENT_ID,
    Columns.MODULE_ID: Columns.MODULE_ID,
    Columns.OBS: Columns.OBS,
    Columns.ACTIONS: Columns.ACTIONS,
    Columns.REWARDS: Columns.REWARDS,
    Columns.INFOS: Columns.INFOS,
    Columns.NEXT_OBS: Columns.NEXT_OBS,
    Columns.TERMINATEDS: Columns.TERMINATEDS,
    Columns.TRUNCATEDS: Columns.TRUNCATEDS,
    Columns.T: Columns.T,
    # TODO (simon): Add remove as soon as we are new stack only.
    "agent_index": "agent_index",
    "dones": "dones",
    "unroll_id": "unroll_id",
}


@ExperimentalAPI
class OfflineData:
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def __init__(self, config: AlgorithmConfig):

        self.config = config
        self.is_multi_agent = config.is_multi_agent()
        self.path = (
            config.get("input_")
            if isinstance(config.get("input_"), list)
            else Path(config.get("input_"))
        )
        # Use `read_json` as default data read method.
        self.data_read_method = config.input_read_method
        # Override default arguments for the data read method.
        self.data_read_method_kwargs = (
            self.default_read_method_kwargs | config.input_read_method_kwargs
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
        # Defines the prelearner class. Note, this could be user-defined.
        self.prelearner_class = self.config.get("prelearner_class", OfflinePreLearner)
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
                },
                concurrency=2,
                batch_size=num_samples,
            ).iter_batches(
                batch_size=num_samples,
                prefetch_batches=2,
                local_shuffle_buffer_size=num_samples * 10,
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
                        "locality_hints": self.locality_hints,
                        "module_spec": self.module_spec,
                        "module_state": module_state,
                    },
                    concurrency=num_shards,
                    batch_size=num_samples,
                    zero_copy_batch=True,
                ).streaming_split(
                    n=num_shards, equal=False, locality_hints=self.locality_hints
                )

            # Otherwise, we return a simple batch `DataIterator`.
            else:
                return self.batch_iterator
        else:
            # Return a single batch from the iterator.
            return next(iter(self.batch_iterator))["batch"][0]

    @OverrideToImplementCustomLogic
    @property
    def default_read_method_kwargs(self):
        return {
            "override_num_blocks": max(self.config.num_learners * 2, 2),
        }
