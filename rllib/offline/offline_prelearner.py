import copy
import logging
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple, Union

import gymnasium as gym
import numpy as np
import tree

from ray._common.deprecation import deprecation_warning
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModule
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils import flatten_dict, try_import_torch
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.compression import unpack_if_needed
from ray.rllib.utils.replay_buffers.episode_replay_buffer import EpisodeReplayBuffer
from ray.rllib.utils.typing import EpisodeType, ModuleID
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

torch, _ = try_import_torch()

#: This is the default schema used if no `input_read_schema` is set in
#: the config. If a user passes in a schema into `input_read_schema`
#: this user-defined schema has to comply with the keys of `SCHEMA`,
#: while values correspond to the columns in the user's dataset. Note
#: that only the user-defined values will be overridden while all
#: other values from SCHEMA remain as defined here.
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


def _validate_deprecated_map_args(
    kwargs: dict, config: "AlgorithmConfig"
) -> Tuple[bool, Dict, List]:
    """Handles deprecated args for OfflinePreLearner's map functions

    If a user of this API tries to use deprecated arguments, we print a deprecation
    messages.

    Args:
        kwargs: The kwargs for the map function to check for deprecated args

    Returns:
        The validated arguments
    """
    if "is_multi_agent" in kwargs:
        deprecation_warning(
            old="OfflinePreLearner._map_sample_batch_to_episode(is_multi_agent)",
            new="Is True if `AlgorithmConfig.is_multi_agent=True`",
            error=False,
        )
        is_multi_agent = kwargs["is_multi_agent"]
    else:
        is_multi_agent = config.is_multi_agent

    if "schema" in kwargs:
        deprecation_warning(
            old="OfflinePreLearner._map_sample_batch_to_episode(schema)",
            new="AlgorithmConfig.offline(input_read_schema)",
            error=False,
        )
        schema = kwargs["schema"]
    else:
        schema = SCHEMA | config.input_read_schema

    if "input_compress_columns" in kwargs:
        deprecation_warning(
            old="OfflinePreLearner._map_sample_batch_to_episode(input_compress_columns)",
            new="AlgorithmConfig.offline(input_compress_columns)",
            error=False,
        )
        input_compress_columns = kwargs["input_compress_columns"] or []
    else:
        input_compress_columns = config.input_compress_columns or []

    return is_multi_agent, schema, input_compress_columns


@PublicAPI(stability="alpha")
class OfflinePreLearner:
    """Maps raw ingested data to episodes and runs the Learner pipeline.

    OfflinePreLearner is meant to be used by RLlib to build a Ray Data pipeline
    using `ray.data.Dataset.map_batches` when ingesting data for offline training.
    Ray data is thereby used under the hood to parallelize the data transformation.

    It's basic function is to:
    (1) Convert the dataset into RLlib's native episode
    format (`SingleAgentEpisode`, since `MultiAgentEpisode` is not supported yet).
    (2) Apply the learner connector pipeline to episodes to create batches that are
    ready to be trained on (can be passed in `Learner.update` methods).

    OfflinePreLearner can be overridden to implement custom logic and passed into
    `AlgorithmConfig.offline(prelearner_class)`.
    """

    default_prelearner_buffer_class = EpisodeReplayBuffer

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def __init__(
        self,
        *,
        config: "AlgorithmConfig",
        spaces: Optional[Tuple[gym.Space, gym.Space]] = None,
        module_state: Optional[Dict[ModuleID, Any]] = None,
        **kwargs: Dict[str, Any],
    ):
        if "module_spec" in kwargs:
            deprecation_warning(
                old="OfflinePreLearner(module_spec=..)",
                new="OfflinePreLearner(config=AlgorithmConfig().rl_module(rl_module_spec=..))",
                error=False,
            )
            rl_module_spec = kwargs["module_spec"]
        else:
            rl_module_spec = config.rl_module_spec

        self.config: AlgorithmConfig = config
        self._module: MultiRLModule = rl_module_spec.build()
        if module_state:
            self._module.set_state(module_state)

        self.observation_space, self.action_space = spaces or (None, None)
        self._learner_connector = self.config.build_learner_connector(
            input_observation_space=self.observation_space,
            input_action_space=self.action_space,
        )

        if (
            self.config.input_read_sample_batches
            or self._module.is_stateful()
            or self.config.input_read_episodes
        ):
            prelearner_buffer_class = (
                self.config.prelearner_buffer_class
                or self.default_prelearner_buffer_class
            )
            prelearner_buffer_kwargs = {
                "capacity": self.config.train_batch_size_per_learner * 10,
                "batch_size_B": self.config.train_batch_size_per_learner,
            } | self.config.prelearner_buffer_kwargs
            self.episode_buffer = prelearner_buffer_class(
                **prelearner_buffer_kwargs,
            )

    @OverrideToImplementCustomLogic
    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        """Maps raw ingested data to episodes and runs the Learner pipeline.

        Args:
            batch: A dictionary of numpy arrays where each numpy array represents a column of the dataset.

        Returns:
            A flattened dict representing a batch that can be passed to `Learner.update` methods.
        """
        if self.config.input_read_episodes:
            import msgpack
            import msgpack_numpy as mnp

            # Read the episodes and decode them.
            episodes: List[SingleAgentEpisode] = [
                SingleAgentEpisode.from_state(
                    msgpack.unpackb(state, object_hook=mnp.decode)
                )
                for state in batch["item"]
            ]
            episodes = self._postprocess_and_sample(episodes)

        elif self.config.input_read_sample_batches:
            episodes: List[SingleAgentEpisode] = self._map_sample_batch_to_episode(
                batch,
                to_numpy=True,
            )["episodes"]
            episodes = self._postprocess_and_sample(episodes)
        else:
            episodes: List[SingleAgentEpisode] = self._map_to_episodes(
                batch,
                to_numpy=True,
            )["episodes"]

        # TODO (simon): Sync learner connector state
        # TODO (sven): Add MetricsLogger to non-Learner components that have a LearnerConnector pipeline.

        # Run the `Learner`'s connector pipeline.
        batch = self._learner_connector(
            rl_module=self._module,
            batch={},
            episodes=episodes,
            shared_data={},
            metrics=None,
        )
        # Remove all data from modules that should not be trained. We do
        # not want to pass around more data than necessary.
        for module_id in batch:
            if not self._should_module_be_updated(module_id, batch):
                del batch[module_id]

        # Flatten the dictionary to increase serialization performance.
        return flatten_dict(batch)

    def _validate_episodes(
        self, episodes: List[SingleAgentEpisode]
    ) -> Set[SingleAgentEpisode]:
        """Validate episodes .

        Validates that all episodes are done and no duplicates are in the batch.
        Validates that there are no duplicate episodes.

        Args:
            episodes: A list of `SingleAgentEpisode` instances to validate.

        Returns:
            A set of validated `SingleAgentEpisode` instances.

        Raises:
            ValueError: If not all episodes are `done`.
        """
        # Ensure that episodes are all done.
        if not all(eps.is_done for eps in episodes):
            raise ValueError(
                "When sampling from episodes (`input_read_episodes=True`) all "
                "recorded episodes must be done (i.e. either `terminated=True`) "
                "or `truncated=True`)."
            )
        # Ensure that episodes do not contain duplicates. Note, this can happen
        # if the dataset is small and pulled batches contain multiple episodes.
        unique_episode_ids = set()
        cleaned_episodes = set()
        for eps in episodes:
            if (
                eps.id_ not in unique_episode_ids
                and eps.id_ not in self.episode_buffer.episode_id_to_index
            ):
                unique_episode_ids.add(eps.id_)
                cleaned_episodes.add(eps)
        return cleaned_episodes

    def _postprocess_and_sample(
        self, episodes: List[SingleAgentEpisode]
    ) -> List[SingleAgentEpisode]:
        """Postprocesses episodes and samples from the buffer.

        Args:
            episodes: A list of `SingleAgentEpisode` instances.

        Returns:
            A list of `SingleAgentEpisode` instances sampled from the buffer.
        """
        # Ensure that all episodes are done and no duplicates are in the batch.
        episodes = self._validate_episodes(episodes)

        if (
            self._module.is_stateful()
            and not self.config.prelearner_use_recorded_module_states
        ):
            for eps in episodes:
                if Columns.STATE_OUT in eps.extra_model_outputs:
                    del eps.extra_model_outputs[Columns.STATE_OUT]
                if Columns.STATE_IN in eps.extra_model_outputs:
                    del eps.extra_model_outputs[Columns.STATE_IN]

        # Add the episodes to the buffer.
        self.episode_buffer.add(episodes)

        # Sample from the buffer.
        batch_length_T = (
            self.config.model_config.get("max_seq_len", 0)
            if self._module.is_stateful()
            else None
        )

        return self.episode_buffer.sample(
            num_items=self.config.train_batch_size_per_learner,
            batch_length_T=batch_length_T,
            n_step=self.config.get("n_step", 1),
            # TODO (simon): This can be removed as soon as DreamerV3 has been
            # cleaned up, i.e. can use episode samples for training.
            sample_episodes=True,
            to_numpy=True,
            lookback=self.config.episode_lookback_horizon,
            min_batch_length_T=getattr(self.config, "burnin_len", 0),
        )

    def _should_module_be_updated(self, module_id, multi_agent_batch=None) -> bool:
        """Checks which modules in a MultiRLModule should be updated."""
        policies_to_train = self.config.policies_to_train
        if not policies_to_train:
            # In case of no update information, the module is updated.
            return True
        elif not callable(policies_to_train):
            return module_id in set(policies_to_train)
        else:
            return policies_to_train(module_id, multi_agent_batch)

    @OverrideToImplementCustomLogic
    def _map_to_episodes(
        self,
        batch: Dict[str, Union[list, np.ndarray]],
        to_numpy: bool = False,
        **kwargs: Dict[str, Any],
    ) -> Dict[str, List[EpisodeType]]:
        """Maps a batch of data to episodes."""

        is_multi_agent, schema, input_compress_columns = _validate_deprecated_map_args(
            kwargs, self.config
        )

        episodes = []
        for i, obs in enumerate(batch[schema[Columns.OBS]]):
            if is_multi_agent:
                # TODO (simon): Add support for multi-agent episodes.
                raise NotImplementedError(
                    "Loading multi-agent episodes is currently not supported."
                )
            else:
                unpacked_obs = (
                    unpack_if_needed(obs)
                    if Columns.OBS in input_compress_columns
                    else obs
                )

                if self.config.ignore_final_observation:
                    unpacked_next_obs = tree.map_structure(
                        lambda x: np.zeros_like(x), copy.deepcopy(unpacked_obs)
                    )
                else:
                    unpacked_next_obs = (
                        unpack_if_needed(batch[schema[Columns.NEXT_OBS]][i])
                        if Columns.OBS in input_compress_columns
                        else batch[schema[Columns.NEXT_OBS]][i]
                    )

                # Build a single-agent episode with a single row of the batch.
                episode = SingleAgentEpisode(
                    id_=str(batch[schema[Columns.EPS_ID]][i])
                    if schema[Columns.EPS_ID] in batch
                    else uuid.uuid4().hex,
                    agent_id=None,
                    # Observations might be (a) serialized and/or (b) converted
                    # to a JSONable (when a composite space was used). We unserialize
                    # and then reconvert from JSONable to space sample.
                    observations=[unpacked_obs, unpacked_next_obs],
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
                        unpack_if_needed(batch[schema[Columns.ACTIONS]][i])
                        if Columns.ACTIONS in input_compress_columns
                        else batch[schema[Columns.ACTIONS]][i]
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

                if to_numpy:
                    episode.to_numpy()
                episodes.append(episode)
        # Note, `map_batches` expects a `Dict` as return value.
        return {"episodes": episodes}

    @OverrideToImplementCustomLogic
    def _map_sample_batch_to_episode(
        self,
        batch: Dict[str, Union[list, np.ndarray]],
        to_numpy: bool = False,
        **kwargs: Dict[str, Any],
    ) -> Dict[str, List[EpisodeType]]:
        """Maps an old stack `SampleBatch` to new stack episodes."""

        is_multi_agent, schema, input_compress_columns = _validate_deprecated_map_args(
            kwargs, self.config
        )

        # Set `input_compress_columns` to an empty `list` if `None`.
        input_compress_columns = input_compress_columns or []

        # TODO (simon): Check, if needed. It could possibly happen that a batch contains
        #   data from different episodes. Merging and resplitting the batch would then
        # be the solution.
        # Check, if batch comes actually from multiple episodes.
        # episode_begin_indices = np.where(np.diff(np.hstack(batch["eps_id"])) != 0) + 1

        # Define a container to collect episodes.
        episodes = []
        # Loop over `SampleBatch`es in the `ray.data` batch (a dict).
        for i, obs in enumerate(batch[schema[Columns.OBS]]):
            if is_multi_agent:
                # TODO (simon): Add support for multi-agent episodes.
                raise NotImplementedError(
                    "Loading multi-agent episodes from sample batches is currently not supported."
                )
            else:
                # Unpack observations, if needed. Note, observations could
                # be either compressed in their entirety (the column) or individually (each row).
                if isinstance(obs, str):
                    # Decompress the observations if we have a string, i.e.
                    # observations are compressed in their entirety.
                    obs = unpack_if_needed(obs)
                    # Convert to a list of arrays. This is needed as input by
                    # the `SingleAgentEpisode`.
                    obs = [obs[i, ...] for i in range(obs.shape[0])]
                # Otherwise observations are only compressed inside of the
                # batch column (if at all).
                elif isinstance(obs, np.ndarray):
                    # Unpack observations, if they are compressed otherwise we
                    # simply convert to a list, which is needed by the
                    # `SingleAgentEpisode`.
                    obs = (
                        unpack_if_needed(obs.tolist())
                        if schema[Columns.OBS] in input_compress_columns
                        else obs.tolist()
                    )
                else:
                    raise TypeError(
                        f"Unknown observation type: {type(obs)}. When mapping "
                        "from old recorded `SampleBatches` batched "
                        "observations should be either of type `np.array` "
                        "or - if the column is compressed - of `str` type."
                    )

                if schema[Columns.NEXT_OBS] in batch:
                    # Append the last `new_obs` to get the correct length of
                    # observations.
                    obs.append(
                        unpack_if_needed(batch[schema[Columns.NEXT_OBS]][i][-1])
                        if schema[Columns.OBS] in input_compress_columns
                        else batch[schema[Columns.NEXT_OBS]][i][-1]
                    )
                else:
                    # Otherwise we duplicate the last observation.
                    obs.append(obs[-1])

                if (
                    schema[Columns.TRUNCATEDS] in batch
                    and schema[Columns.TERMINATEDS] in batch
                ):
                    truncated = batch[schema[Columns.TRUNCATEDS]][i][-1]
                    terminated = batch[schema[Columns.TERMINATEDS]][i][-1]
                elif (
                    schema[Columns.TRUNCATEDS] in batch
                    and schema[Columns.TERMINATEDS] not in batch
                ):
                    truncated = batch[schema[Columns.TRUNCATEDS]][i][-1]
                    terminated = False
                elif (
                    schema[Columns.TRUNCATEDS] not in batch
                    and schema[Columns.TERMINATEDS] in batch
                ):
                    terminated = batch[schema[Columns.TERMINATEDS]][i][-1]
                    truncated = False
                elif "done" in batch:
                    terminated = batch["done"][i][-1]
                    truncated = False
                # Otherwise, if no `terminated`, nor `truncated` nor `done`
                # is given, we consider the episode as terminated.
                else:
                    terminated = True
                    truncated = False

                episode = SingleAgentEpisode(
                    # If the recorded episode has an ID we use this ID,
                    # otherwise we generate a new one.
                    id_=str(batch[schema[Columns.EPS_ID]][i][0])
                    if schema[Columns.EPS_ID] in batch
                    else uuid.uuid4().hex,
                    agent_id=None,
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
                    terminated=terminated,
                    truncated=truncated,
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
                # TODO (simon, sven): Check, if we should convert all data to lists
                # before. Right now only obs are lists.
                if to_numpy:
                    episode.to_numpy()
                episodes.append(episode)

        # Note: `ray.data.Dataset.map_batches` expects a `Dict`
        return {"episodes": episodes}
