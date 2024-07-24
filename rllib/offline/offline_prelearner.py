import numpy as np
import random
import ray
from ray.actor import ActorHandle
from typing import Any, Dict, List, Optional, Union

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner import Learner
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.compression import unpack_if_needed
from ray.rllib.utils.typing import EpisodeType, ModuleID

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
class OfflinePreLearner:
    """Class that coordinates data transformation from dataset to learner.

    This class is an essential part of the new `Offline RL API` of `RLlib`.
    It is a callable class that is run in `ray.data.Dataset.map_batches`
    when iterating over batches for training. It's basic function is to
    convert data in batch from rows to episodes (`SingleAGentEpisode`s
    for now) and to then run the learner connector pipeline to convert
    further to trainable batches. These batches are used directly in the
    `Learner`'s `update` method.

    The main reason to run these transformations inside of `map_batches`
    is for better performance. Batches can be pre-fetched in `ray.data`
    and therefore batch trransformation can be run highly parallelized to
    the `Learner''s `update`.

    This class can be overridden to implement custom logic for transforming
    batches and make them 'Learner'-ready. When deriving from this class
    the `__call__` method and `_map_to_episodes` can be overridden to induce
    custom logic for the complete transformation pipeline (`__call__`) or
    for converting to episodes only ('_map_to_episodes`). For an example
    how this class can be sued to also compute values and advantages see
    `rllib.algorithm.marwil.marwil_prelearner.MAWRILOfflinePreLearner`.

    Custom `OfflinePreLearner` classes can be passed into
    `AlgorithmConfig.offline`'s `prelearner_class`. The `OfflineData` class
    will then use the custom class in its data pipeline.
    """

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def __init__(
        self,
        config: AlgorithmConfig,
        learner: Union[Learner, list[ActorHandle]],
        locality_hints: Optional[list] = None,
        module_spec: Optional[MultiAgentRLModuleSpec] = None,
        module_state: Optional[Dict[ModuleID, Any]] = None,
    ):

        self.config = config
        # We need this learner to run the learner connector pipeline.
        # If it is a `Learner` instance, the `Learner` is local.
        if isinstance(learner, Learner):
            self._learner = learner
            self.learner_is_remote = False
            self._module = self._learner._module
        # Otherwise we have remote `Learner`s.
        else:
            # TODO (simon): Check with the data team how to get at
            # initialization the data block location.
            node_id = ray.get_runtime_context().get_node_id()
            # Shuffle indices such that not each data block syncs weights
            # with the same learner in case there are multiple learners
            # on the same node like the `PreLearner`.
            indices = list(range(len(locality_hints)))
            random.shuffle(indices)
            locality_hints = [locality_hints[i] for i in indices]
            learner = [learner[i] for i in indices]
            # Choose a learner from the same node.
            for i, hint in enumerate(locality_hints):
                if hint == node_id:
                    self._learner = learner[i]
            # If no learner has been chosen, there is none on the same node.
            if not self._learner:
                # Then choose a learner randomly.
                self._learner = learner[random.randint(0, len(learner) - 1)]
            self.learner_is_remote = True
            # Build the module from spec. Note, this will be a MARL module.
            self._module = module_spec.build()
            self._module.set_state(module_state)
        # Build the learner connector pipeline.
        self._learner_connector = self.config.build_learner_connector(
            input_observation_space=None,
            input_action_space=None,
        )
        # Cache the policies to be trained to update weights only for these.
        self._policies_to_train = self.config.policies_to_train
        self._is_multi_agent = config.is_multi_agent()
        # Set the counter to zero.
        self.iter_since_last_module_update = 0
        # self._future = None

    @OverrideToImplementCustomLogic
    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, List[EpisodeType]]:
        # Map the batch to episodes.
        episodes = self._map_to_episodes(
            self._is_multi_agent, batch, schema=SCHEMA | self.config.input_read_schema
        )
        # TODO (simon): Make synching work. Right now this becomes blocking or never
        # receives weights. Learners appear to be non accessable via other actors.
        # Increase the counter for updating the module.
        # self.iter_since_last_module_update += 1

        # if self._future:
        #     refs, _ = ray.wait([self._future], timeout=0)
        #     print(f"refs: {refs}")
        #     if refs:
        #         module_state = ray.get(self._future)
        #
        #         self._module.set_state(module_state)
        #         self._future = None

        # # Synch the learner module, if necessary. Note, in case of a local learner
        # # we have a reference to the module and therefore an up-to-date module.
        # if self.learner_is_remote and self.iter_since_last_module_update
        # > self.config.prelearner_module_synch_period:
        #     # Reset the iteration counter.
        #     self.iter_since_last_module_update = 0
        #     # Request the module weights from the remote learner.
        #     self._future =
        # self._learner.get_module_state.remote(inference_only=False)
        #     # module_state =
        # ray.get(self._learner.get_module_state.remote(inference_only=False))
        #     # self._module.set_state(module_state)

        # Run the `Learner`'s connector pipeline.
        batch = self._learner_connector(
            rl_module=self._module,
            data={},
            episodes=episodes["episodes"],
            shared_data={},
        )
        # Convert to `MultiAgentBatch`.
        batch = MultiAgentBatch(
            {
                module_id: SampleBatch(module_data)
                for module_id, module_data in batch.items()
            },
            # TODO (simon): This can be run once for the batch and the
            # metrics, but we run it twice: here and later in the learner.
            env_steps=sum(e.env_steps() for e in episodes["episodes"]),
        )
        # Remove all data from modules that should not be trained. We do
        # not want to pass around more data than necessaty.
        for module_id in list(batch.policy_batches.keys()):
            if not self._should_module_be_updated(module_id, batch):
                del batch.policy_batches[module_id]

        # TODO (simon): Log steps trained for metrics (how?). At best in learner
        # and not here. But we could precompute metrics here and pass it to the learner
        # for logging. Like this we do not have to pass around episode lists.

        # TODO (simon): episodes are only needed for logging here.
        return {"batch": [batch]}

    def _should_module_be_updated(self, module_id, multi_agent_batch=None):
        """Checks which modules in a MARL module should be updated."""
        if not self._policies_to_train:
            # In case of no update information, the module is updated.
            return True
        elif not callable(self._policies_to_train):
            return module_id in set(self._policies_to_train)
        else:
            return self._policies_to_train(module_id, multi_agent_batch)

    @OverrideToImplementCustomLogic
    @staticmethod
    def _map_to_episodes(
        is_multi_agent: bool,
        batch: Dict[str, np.ndarray],
        schema: Dict[str, str] = SCHEMA,
    ) -> Dict[str, List[EpisodeType]]:
        """Maps a batch of data to episodes."""

        episodes = []
        # TODO (simon): Give users possibility to provide a custom schema.
        for i, obs in enumerate(batch[schema[Columns.OBS]]):

            # If multi-agent we need to extract the agent ID.
            # TODO (simon): Check, what happens with the module ID.
            if is_multi_agent:
                agent_id = (
                    batch[schema[Columns.AGENT_ID]][i]
                    if Columns.AGENT_ID in batch
                    # The old stack uses "agent_index" instead of "agent_id".
                    # TODO (simon): Remove this as soon as we are new stack only.
                    else (
                        batch[schema["agent_index"]][i]
                        if schema["agent_index"] in batch
                        else None
                    )
                )
            else:
                agent_id = None

            if is_multi_agent:
                # TODO (simon): Add support for multi-agent episodes.
                pass
            else:
                # Build a single-agent episode with a single row of the batch.
                episode = SingleAgentEpisode(
                    id_=batch[schema[Columns.EPS_ID]][i],
                    agent_id=agent_id,
                    observations=[
                        unpack_if_needed(obs),
                        unpack_if_needed(batch[schema[Columns.NEXT_OBS]][i]),
                    ],
                    infos=[
                        {},
                        batch[schema[Columns.INFOS]][i]
                        if schema[Columns.INFOS] in batch
                        else {},
                    ],
                    actions=[batch[schema[Columns.ACTIONS]][i]],
                    rewards=[batch[schema[Columns.REWARDS]][i]],
                    terminated=batch[
                        schema[Columns.TERMINATEDS]
                        if schema[Columns.TERMINATEDS] in batch
                        else "dones"
                    ][i],
                    truncated=batch[schema[Columns.TRUNCATEDS]][i]
                    if schema[Columns.TRUNCATEDS] in batch
                    else False,
                    # TODO (simon): Results in zero-length episodes in connector.
                    # t_started=batch[Columns.T if Columns.T in batch else
                    # "unroll_id"][i][0],
                    # TODO (simon): Single-dimensional columns are not supported.
                    extra_model_outputs={
                        k: [v[i]]
                        for k, v in batch.items()
                        if (k not in schema and k not in schema.values())
                    },
                    len_lookback_buffer=0,
                )
            episodes.append(episode)
        # Note, `map_batches` expects a `Dict` as return value.
        return {"episodes": episodes}
