import gymnasium as gym
import logging
import numpy as np
import random
from typing import Any, Dict, List, Optional, Union, Tuple, TYPE_CHECKING

import ray
from ray.actor import ActorHandle
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner import Learner
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.compression import unpack_if_needed
from ray.rllib.utils.spaces.space_utils import from_jsonable_if_needed
from ray.rllib.utils.typing import EpisodeType, ModuleID

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

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

logger = logging.getLogger(__name__)


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
    how this class can be used to also compute values and advantages see
    `rllib.algorithm.marwil.marwil_prelearner.MAWRILOfflinePreLearner`.

    Custom `OfflinePreLearner` classes can be passed into
    `AlgorithmConfig.offline`'s `prelearner_class`. The `OfflineData` class
    will then use the custom class in its data pipeline.
    """

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def __init__(
        self,
        config: "AlgorithmConfig",
        learner: Union[Learner, list[ActorHandle]],
        spaces: Optional[Tuple[gym.Space, gym.Space]] = None,
        locality_hints: Optional[list] = None,
        module_spec: Optional[MultiRLModuleSpec] = None,
        module_state: Optional[Dict[ModuleID, Any]] = None,
    ):

        self.config = config
        self.input_read_episodes = self.config.input_read_episodes
        self.input_read_sample_batches = self.config.input_read_sample_batches
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
            # Build the module from spec. Note, this will be a MultiRLModule.
            self._module = module_spec.build()
            self._module.set_state(module_state)

        # Store the observation and action space if defined, otherwise we
        # set them to `None`. Note, if `None` the `convert_from_jsonable`
        # will not convert the input space samples.
        self.observation_space, self.action_space = spaces or (None, None)

        # Build the learner connector pipeline.
        self._learner_connector = self.config.build_learner_connector(
            input_observation_space=self.observation_space,
            input_action_space=self.action_space,
        )
        # Cache the policies to be trained to update weights only for these.
        self._policies_to_train = self.config.policies_to_train
        self._is_multi_agent = config.is_multi_agent()
        # Set the counter to zero.
        self.iter_since_last_module_update = 0
        # self._future = None

        # Set up an episode buffer, if the module is stateful or we sample from
        # `SampleBatch` types.
        if (
            self.input_read_sample_batches
            or self._module.is_stateful()
            or self.input_read_episodes
        ):
            # Either the user defined a buffer class or we fall back to the default.
            prelearner_buffer_class = (
                self.config.prelearner_buffer_class
                or self.default_prelearner_buffer_class
            )
            prelearner_buffer_kwargs = (
                self.default_prelearner_buffer_kwargs
                | self.config.prelearner_buffer_kwargs
            )
            # Initialize the buffer.
            self.episode_buffer = prelearner_buffer_class(
                **prelearner_buffer_kwargs,
            )

    @OverrideToImplementCustomLogic
    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, List[EpisodeType]]:

        # If we directly read in episodes we just convert to list.
        if self.input_read_episodes:
            # Import `msgpack` for decoding.
            import msgpack
            import msgpack_numpy as mnp

            # Read the episodes and decode them.
            episodes = [
                SingleAgentEpisode.from_state(
                    msgpack.unpackb(state, object_hook=mnp.decode)
                )
                for state in batch["item"]
            ]
            self.episode_buffer.add(episodes)
            episodes = self.episode_buffer.sample(
                num_items=self.config.train_batch_size_per_learner,
                # TODO (simon): This can be removed as soon as DreamerV3 has been
                # cleaned up, i.e. can use episode samples for training.
                sample_episodes=True,
                finalize=True,
            )
        # Else, if we have old stack `SampleBatch`es.
        elif self.input_read_sample_batches:
            episodes = OfflinePreLearner._map_sample_batch_to_episode(
                self._is_multi_agent,
                batch,
                finalize=True,
                schema=SCHEMA | self.config.input_read_schema,
                input_compress_columns=self.config.input_compress_columns,
            )["episodes"]
            self.episode_buffer.add(episodes)
            episodes = self.episode_buffer.sample(
                num_items=self.config.train_batch_size_per_learner,
                # TODO (simon): This can be removed as soon as DreamerV3 has been
                # cleaned up, i.e. can use episode samples for training.
                sample_episodes=True,
                finalize=True,
            )
        # Otherwise we map the batch to episodes.
        else:
            episodes = self._map_to_episodes(
                self._is_multi_agent,
                batch,
                schema=SCHEMA | self.config.input_read_schema,
                finalize=True,
                input_compress_columns=self.config.input_compress_columns,
                observation_space=self.observation_space,
                action_space=self.action_space,
            )["episodes"]

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
            batch={},
            episodes=episodes,
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
            env_steps=sum(e.env_steps() for e in episodes),
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

    @property
    def default_prelearner_buffer_class(self):
        from ray.rllib.utils.replay_buffers.episode_replay_buffer import (
            EpisodeReplayBuffer,
        )

        # Return the buffer.
        return EpisodeReplayBuffer

    @property
    def default_prelearner_buffer_kwargs(self):
        return {
            "capacity": self.config.train_batch_size_per_learner * 10,
            "batch_size_B": self.config.train_batch_size_per_learner,
        }

    def _should_module_be_updated(self, module_id, multi_agent_batch=None):
        """Checks which modules in a MultiRLModule should be updated."""
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
        batch: Dict[str, Union[list, np.ndarray]],
        schema: Dict[str, str] = SCHEMA,
        finalize: bool = False,
        input_compress_columns: Optional[List[str]] = None,
        observation_space: gym.Space = None,
        action_space: gym.Space = None,
    ) -> Dict[str, List[EpisodeType]]:
        """Maps a batch of data to episodes."""

        # Set to empty list, if `None`.
        input_compress_columns = input_compress_columns or []

        # If spaces are given, we can use the space-specific
        # conversion method to convert space samples.
        if observation_space and action_space:
            convert = from_jsonable_if_needed
        # Otherwise we use an identity function.
        else:

            def convert(sample, space):
                return sample

        episodes = []
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
                NotImplementedError
            else:
                # Build a single-agent episode with a single row of the batch.
                episode = SingleAgentEpisode(
                    id_=str(batch[schema[Columns.EPS_ID]][i]),
                    agent_id=agent_id,
                    # Observations might be (a) serialized and/or (b) converted
                    # to a JSONable (when a composite space was used). We unserialize
                    # and then reconvert from JSONable to space sample.
                    observations=[
                        convert(unpack_if_needed(obs), observation_space)
                        if Columns.OBS in input_compress_columns
                        else convert(obs, observation_space),
                        convert(
                            unpack_if_needed(batch[schema[Columns.NEXT_OBS]][i]),
                            observation_space,
                        )
                        if Columns.OBS in input_compress_columns
                        else convert(
                            batch[schema[Columns.NEXT_OBS]][i], observation_space
                        ),
                    ],
                    infos=[
                        {},
                        batch[schema[Columns.INFOS]][i]
                        if schema[Columns.INFOS] in batch
                        else {},
                    ],
                    # Actions might be (a) serialized and/or (b) converted to a JSONable
                    # (when a composite space was used). We unserializer and then
                    # reconvert from JSONable to space sample.
                    actions=[
                        convert(
                            unpack_if_needed(batch[schema[Columns.ACTIONS]][i]),
                            action_space,
                        )
                        if Columns.ACTIONS in input_compress_columns
                        else convert(batch[schema[Columns.ACTIONS]][i], action_space)
                    ],
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
                    # Extra model outputs might be serialized. We unserialize them here
                    # if needed.
                    # TODO (simon): Check, if we need here also reconversion from
                    # JSONable in case of composite spaces.
                    extra_model_outputs={
                        k: [
                            unpack_if_needed(v[i])
                            if k in input_compress_columns
                            else v[i]
                        ]
                        for k, v in batch.items()
                        if (
                            k not in schema
                            and k not in schema.values()
                            and k not in ["dones", "agent_index", "type"]
                        )
                    },
                    len_lookback_buffer=0,
                )

                if finalize:
                    episode.finalize()
                episodes.append(episode)
        # Note, `map_batches` expects a `Dict` as return value.
        return {"episodes": episodes}

    def _map_sample_batch_to_episode(
        is_multi_agent: bool,
        batch: Dict[str, Union[list, np.ndarray]],
        schema: Dict[str, str] = SCHEMA,
        finalize: bool = False,
        input_compress_columns: Optional[List[str]] = None,
    ) -> Dict[str, List[EpisodeType]]:
        """Maps an old stack `SampleBatch` to new stack episodes."""

        # Set `input_compress_columns` to an empty `list` if `None`.
        input_compress_columns = input_compress_columns or []

        # TODO (simon): CHeck, if needed. It could possibly happen that a batch contains
        #   data from different episodes. Merging and resplitting the batch would then
        # be the solution.
        # Check, if batch comes actually from multiple episodes.
        # episode_begin_indices = np.where(np.diff(np.hstack(batch["eps_id"])) != 0) + 1

        # Define a container to collect episodes.
        episodes = []
        # Loop over `SampleBatch`es in the `ray.data` batch (a dict).
        for i, obs in enumerate(batch[schema[Columns.OBS]]):

            # If multi-agent we need to extract the agent ID.
            # TODO (simon): Check, what happens with the module ID.
            if is_multi_agent:
                agent_id = (
                    # The old stack uses "agent_index" instead of "agent_id".
                    batch[schema["agent_index"]][i][0]
                    if schema["agent_index"] in batch
                    else None
                )
            else:
                agent_id = None

            if is_multi_agent:
                # TODO (simon): Add support for multi-agent episodes.
                NotImplementedError
            else:
                # Unpack observations, if needed.
                obs = (
                    unpack_if_needed(obs.tolist())
                    if schema[Columns.OBS] in input_compress_columns
                    else obs.tolist()
                )
                # Append the last `new_obs` to get the correct length of observations.
                obs.append(
                    unpack_if_needed(batch[schema[Columns.NEXT_OBS]][i][-1])
                    if schema[Columns.OBS] in input_compress_columns
                    else batch[schema[Columns.NEXT_OBS]][i][-1]
                )
                # Create a `SingleAgentEpisode`.
                episode = SingleAgentEpisode(
                    id_=str(batch[schema[Columns.EPS_ID]][i][0]),
                    agent_id=agent_id,
                    observations=obs,
                    infos=(
                        batch[schema[Columns.INFOS]][i]
                        if schema[Columns.INFOS] in batch
                        else [{}] * len(obs)
                    ),
                    # Actions might be (a) serialized. We unserialize them here.
                    actions=(
                        unpack_if_needed(batch[schema[Columns.ACTIONS]][i])
                        if Columns.ACTIONS in input_compress_columns
                        else batch[schema[Columns.ACTIONS]][i]
                    ),
                    rewards=batch[schema[Columns.REWARDS]][i],
                    terminated=(
                        any(batch[schema[Columns.TERMINATEDS]][i])
                        if schema[Columns.TERMINATEDS] in batch
                        else any(batch["dones"][i])
                    ),
                    truncated=(
                        any(batch[schema[Columns.TRUNCATEDS]][i])
                        if schema[Columns.TRUNCATEDS] in batch
                        else False
                    ),
                    # TODO (simon): Results in zero-length episodes in connector.
                    # t_started=batch[Columns.T if Columns.T in batch else
                    # "unroll_id"][i][0],
                    # TODO (simon): Single-dimensional columns are not supported.
                    # Extra model outputs might be serialized. We unserialize them here
                    # if needed.
                    # TODO (simon): Check, if we need here also reconversion from
                    # JSONable in case of composite spaces.
                    extra_model_outputs={
                        k: unpack_if_needed(v[i])
                        if k in input_compress_columns
                        else v[i]
                        for k, v in batch.items()
                        if (
                            k not in schema
                            and k not in schema.values()
                            and k not in ["dones", "agent_index", "type"]
                        )
                    },
                    len_lookback_buffer=0,
                )
                # Finalize, if necessary.
                # TODO (simon, sven): Check, if we should convert all data to lists
                # before. Right now only obs are lists.
                if finalize:
                    episode.finalize()
                episodes.append(episode)
        # Note, `map_batches` expects a `Dict` as return value.
        return {"episodes": episodes}
