import copy
import gymnasium as gym
import numpy as np
import tree
import uuid

from typing import Any, Dict, List, Optional, Union

from ray.rllib.core.columns import Columns
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.offline.offline_prelearner import OfflinePreLearner, SCHEMA
from ray.rllib.utils import flatten_dict
from ray.rllib.utils.annotations import OverrideToImplementCustomLogic
from ray.rllib.utils.compression import unpack_if_needed
from ray.rllib.utils.spaces.space_utils import from_jsonable_if_needed
from ray.rllib.utils.typing import EpisodeType


class MinariOfflinePreLearner(OfflinePreLearner):
    @OverrideToImplementCustomLogic
    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        # If we directly read in episodes we just convert to list.
        if self.input_read_episodes:
            # Import `msgpack` for decoding.
            import msgpack
            import msgpack_numpy as mnp

            # Read the episodes and decode them.
            episodes: List[SingleAgentEpisode] = [
                SingleAgentEpisode(
                    # Note, we use here a generated ID as episode IDs in Minari repeat
                    # themselves and the EpisodeReplayBuffer would error out otherwise.
                    id_=uuid.uuid4().hex,  # state["episode"]["id"],
                    observations=msgpack.unpackb(
                        state[Columns.OBS], object_hook=mnp.decode
                    ),
                    actions=msgpack.unpackb(
                        state[Columns.ACTIONS], object_hook=mnp.decode
                    ),
                    rewards=msgpack.unpackb(
                        state[Columns.REWARDS], object_hook=mnp.decode
                    ),
                    infos=msgpack.unpackb(state[Columns.INFOS], object_hook=mnp.decode),
                    terminated=state[Columns.TERMINATEDS],
                    truncated=state[Columns.TRUNCATEDS],
                    len_lookback_buffer=0,
                )
                for state in batch["episodes"]
            ]
            # Ensure that all episodes are done and no duplicates are in the batch.
            episodes = self._validate_episodes(episodes)
            # Add the episodes to the buffer.
            self.episode_buffer.add(episodes)
            # TODO (simon): Refactor into a single code block for both cases.
            episodes = self.episode_buffer.sample(
                num_items=self.config.train_batch_size_per_learner,
                batch_length_T=self.config.model_config.get("max_seq_len", 0)
                if self._module.is_stateful()
                else None,
                n_step=self.config.get("n_step", 1) or 1,
                # TODO (simon): This can be removed as soon as DreamerV3 has been
                # cleaned up, i.e. can use episode samples for training.
                sample_episodes=True,
                to_numpy=True,
            )
        # Otherwise we map the batch to episodes.
        # TODO (simon): Implement this.
        else:
            episodes: List[SingleAgentEpisode] = self._map_to_episodes(
                self._is_multi_agent,
                batch,
                schema=SCHEMA | self.config.input_read_schema,
                to_numpy=False,
                input_compress_columns=self.config.input_compress_columns,
                observation_space=self.observation_space,
                action_space=self.action_space,
            )["episodes"]

        # Run the `Learner`'s connector pipeline.
        batch = self._learner_connector(
            rl_module=self._module,
            batch={},
            episodes=episodes,
            shared_data={},
            # TODO (sven): Add MetricsLogger to non-Learner components that have a
            #  LearnerConnector pipeline.
            metrics=None,
        )
        # Remove all data from modules that should not be trained. We do
        # not want to pass around more data than necessary.
        for module_id in batch:
            if not self._should_module_be_updated(module_id, batch):
                del batch[module_id]

        # Flatten the dictionary to increase serialization performance.
        return flatten_dict(batch)

    @OverrideToImplementCustomLogic
    @staticmethod
    def _map_to_episodes(
        is_multi_agent: bool,
        batch: Dict[str, Union[list, np.ndarray]],
        schema: Dict[str, str] = SCHEMA,
        to_numpy: bool = False,
        input_compress_columns: Optional[List[str]] = None,
        ignore_final_observation: Optional[bool] = False,
        observation_space: gym.Space = None,
        action_space: gym.Space = None,
        **kwargs: Dict[str, Any],
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
                pass
            else:
                # Unpack observations, if needed.
                unpacked_obs = (
                    convert(unpack_if_needed(obs), observation_space)
                    if Columns.OBS in input_compress_columns
                    else convert(obs, observation_space)
                )
                # Set the next observation.
                if ignore_final_observation:
                    unpacked_next_obs = tree.map_structure(
                        lambda x: 0 * x, copy.deepcopy(unpacked_obs)
                    )
                else:
                    unpacked_next_obs = (
                        convert(
                            unpack_if_needed(batch[schema[Columns.NEXT_OBS]][i]),
                            observation_space,
                        )
                        if Columns.OBS in input_compress_columns
                        else convert(
                            batch[schema[Columns.NEXT_OBS]][i], observation_space
                        )
                    )
                # Build a single-agent episode with a single row of the batch.
                episode = SingleAgentEpisode(
                    id_=str(batch[schema[Columns.EPS_ID]][i])
                    if schema[Columns.EPS_ID] in batch
                    else uuid.uuid4().hex,
                    agent_id=agent_id,
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

                if to_numpy:
                    episode.to_numpy()
                episodes.append(episode)
        # Note, `map_batches` expects a `Dict` as return value.
        return {"episodes": episodes}
