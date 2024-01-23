from collections import defaultdict
from queue import Queue
from typing import Any, Collection, DefaultDict, Dict, List, Optional, Set, Union
import uuid

import gymnasium as gym
import numpy as np
import tree

from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.env.utils.infinite_lookback_buffer import InfiniteLookbackBuffer
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.error import MultiAgentEnvError
from ray.rllib.utils.typing import AgentID, ModuleID, MultiAgentDict


# TODO (simon): Include cases in which the number of agents in an
# episode are shrinking or growing during the episode itself.
# Note, recorded `terminateds`/`truncateds` come as simple
# `MultiAgentDict`s and have no assingment to a certain timestep.
# Instead we assign it to the last observation recorded.
# Theoretically, there could occur edge cases in some environments
# where an agent receives partial rewards and then terminates without
# a last observation. In these cases, we duplicate the last observation.
# If no initial observation, but only partial rewards occurred,
# we delete the agent data b/c there is nothing to learn.
class MultiAgentEpisodeV2:
    """Stores multi-agent episode data.

    The central attribute of the class is the timestep mapping
    `self.env_t_to_agent_t` that maps the environment steps (across all
    stepping agents) to the individual agents' own scale/timesteps.

    The `MultiAgentEpisode` is based on the `SingleAgentEpisode`s
    for each agent, stored in `MultiAgentEpisode.agent_episodes`.
    """

    SKIP_ENV_TS_TAG = "S"
    OUT_OF_BOUNDS_TS_TAG = "O"

    def __init__(
        self,
        id_: Optional[str] = None,
        agent_ids: Optional[Collection[AgentID]] = None,
        agent_episode_ids: Optional[Dict[AgentID, str]] = None,
        *,
        observations: Optional[List[MultiAgentDict]] = None,
        observation_space: Optional[gym.Space] = None,
        infos: Optional[List[MultiAgentDict]] = None,
        actions: Optional[List[MultiAgentDict]] = None,
        action_space: Optional[gym.Space] = None,
        rewards: Optional[List[MultiAgentDict]] = None,
        terminateds: Union[MultiAgentDict, bool] = False,
        truncateds: Union[MultiAgentDict, bool] = False,
        render_images: Optional[List[np.ndarray]] = None,
        extra_model_outputs: Optional[List[MultiAgentDict]] = None,
        env_t_started: Optional[int] = None,
        len_lookback_buffer: Union[int, str] = "auto",
    ) -> "MultiAgentEpisode":
        """Initializes a `MultiAgentEpisode`.

        Args:
            id_: Optional. Either a string to identify an episode or None.
                If None, a hexadecimal id is created. In case of providing
                a string, make sure that it is unique, as episodes get
                concatenated via this string.
            agent_ids: A list of strings containing the agent IDs.
                These have to be provided at initialization.
            agent_episode_ids: An optional dictionary mapping agent IDs
                to their corresponding `SingleAgentEpisode`. If None, each
                `SingleAgentEpisode` in `MultiAgentEpisode.agent_episodes`
                will generate a hexadecimal code. If a dictionary is provided
                make sure that IDs are unique as agents' `SingleAgentEpisode`s
                get concatenated or recreated by it.
            observations: A list of dictionaries mapping agent IDs to observations.
                Can be None. If provided, should match all other episode data
                (actions, rewards, etc.) in terms of list lengths and agent IDs.
            observation_space: An optional gym.spaces.Dict mapping agent IDs to
                individual agents' spaces, which all (individual agents') observations
                should abide to. If not None and this MultiAgentEpisode is finalized
                (via the `self.finalize()` method), and data is appended or set, the new
                data will be checked for correctness.
            infos: A list of dictionaries mapping agent IDs to info dicts.
                Can be None. If provided, should match all other episode data
                (observations, rewards, etc.) in terms of list lengths and agent IDs.
            actions: A list of dictionaries mapping agent IDs to actions.
                Can be None. If provided, should match all other episode data
                (observations, rewards, etc.) in terms of list lengths and agent IDs.
            action_space: An optional gym.spaces.Dict mapping agent IDs to
                individual agents' spaces, which all (individual agents') actions
                should abide to. If not None and this MultiAgentEpisode is finalized
                (via the `self.finalize()` method), and data is appended or set, the new
                data will be checked for correctness.
            rewards: A list of dictionaries mapping agent IDs to rewards.
                Can be None. If provided, should match all other episode data
                (actions, rewards, etc.) in terms of list lengths and agent IDs.
            terminateds: A boolean defining if an environment has
                terminated OR a MultiAgentDict mapping individual agent ids
                to boolean flags indicating whether individual agents have terminated.
                A special __all__ key in these dicts indicates, whether the episode
                is terminated for all agents.
                The default is `False`, i.e. the episode has not been terminated.
            truncateds: A boolean defining if the environment has been
                truncated OR a MultiAgentDict mapping individual agent ids
                to boolean flags indicating whether individual agents have been
                truncated. A special __all__ key in these dicts indicates, whether the
                episode is truncated for all agents.
                The default is `False`, i.e. the episode has not been truncated.
            render_images: A list of RGB uint8 images from rendering
                the multi-agent environment.
            extra_model_outputs: A list of dictionaries mapping agent IDs to their
                corresponding extra model outputs. Each of these "outputs" is a dict
                mapping keys (str) to model output values, for example for
                `key=STATE_OUT`, the values would be the internal state outputs for
                that agent.
            env_t_started: The env timestep (int) that defines the starting point
                of the episode. This is only larger zero, if an already ongoing episode
                chunk is being created, for example by slicing an ongoing episode or
                by calling the `cut()` method on an ongoing episode.
            len_lookback_buffer: The size of the lookback buffers to keep in
                front of this Episode for each type of data (observations, actions,
                etc..). If larger 0, will interpret the first `len_lookback_buffer`
                items in each type of data as NOT part of this actual
                episode chunk, but instead serve as "historical" record that may be
                viewed and used to derive new data from. For example, it might be
                necessary to have a lookback buffer of four if you would like to do
                observation frame stacking and your episode has been cut and you are now
                operating on a new chunk (continuing from the cut one). Then, for the
                first 3 items, you would have to be able to look back into the old
                chunk's data.
                If `len_lookback_buffer` is "auto" (default), will interpret all
                provided data in the constructor as part of the lookback buffers.
        """
        self.id_: str = id_ or uuid.uuid4().hex

        # Lookback buffer length is not provided. Interpret all provided data as
        # lookback buffer.
        if len_lookback_buffer == "auto":
            len_lookback_buffer = len(rewards or [])

        self.observation_space = observation_space or {}
        self.action_space = action_space or {}

        # Agent ids must be provided if data is provided. The Episode cannot
        # know how many agents are in the environment. Also the number of agents
        # can grow or shrink.
        self._agent_ids: Set[AgentID] = set([] if agent_ids is None else agent_ids)
        # Container class to keep information on which agent maps to which module
        # (always for the duration of this episode).
        self.agent_to_module_map: Dict[AgentID, ModuleID] = {}

        # The global last timestep of the episode and the timesteps when this chunk
        # started (excluding a possible lookback buffer).
        self.env_t_started = env_t_started or 0
        self.env_t = (
            (len(rewards) if rewards is not None else 0)
            - len_lookback_buffer
            + self.env_t_started
        )

        # Keeps track of the correspondence between agent steps and environment steps.
        self.env_t_to_agent_t: DefaultDict[AgentID, InfiniteLookbackBuffer] = (
            defaultdict(InfiniteLookbackBuffer)
        )

        #self.ts_carriage_return: int = self.env_t_started - (
        #    len(rewards) if rewards is not None else 0
        #)

        # In the `MultiAgentEpisode` we need these buffers to keep track of actions,
        # that happen when an agent got observations and acted, but did not receive
        # a next observation, yet. In this case we buffer the action, add the rewards,
        # and record `is_terminated/is_truncated` until the next observation is
        # received.
        self._agent_buffered_actions = {}
        self._agent_buffered_extra_model_outputs = defaultdict(dict)
        self._agent_buffered_rewards = defaultdict(float)
        #self.agent_buffers: MultiAgentDict = {
        #    agent_id: {
        #        "actions": Queue(maxsize=1),
        #        "rewards": Queue(maxsize=1),
        #        "extra_model_outputs": Queue(maxsize=1),
        #    }
        #    for agent_id in self._agent_ids
        #}
        # Initialize buffers.
        #for buffer in self.agent_buffers.values():
        #    # Default reward for accumulation is zero.
        #    buffer["rewards"].put_nowait(0.0)
        #    # Default extra_model_output is None.
        #    buffer["extra_model_outputs"].put_nowait(None)

        # This is needed to reconstruct global action structures for
        # orphane actions, i.e. actions that miss a 'next observation' at
        # their occurrence.
        #self.global_actions_t = self._generate_global_actions_t(
        #    actions, len_lookback_buffer
        #)
        ## These are needed to reconstruct (see `self.get_rewards()`) reward
        ## structures if agents received rewards without observations. This
        ## is specific to multi-agent environemnts.
        #self.partial_rewards = {agent_id: [] for agent_id in self._agent_ids}
        #self.partial_rewards_t = defaultdict(
        #    lambda: InfiniteLookbackEnvToAgentTsMapping(
        #        lookback=len_lookback_buffer, t_started=self.env_t_started
        #    )
        #)
        # TODO (simon): remove as soon as `InfiniteLookbackEnvToAgentTsMapping` has
        # been fully tested.
        # self.partial_rewards_t = {
        #     agent_id: _IndexMapping() for agent_id in self._agent_ids
        # }

        # If this is an ongoing episode than the last `__all__` should be `False`
        self.is_terminated: bool = (
            terminateds
            if isinstance(terminateds, bool)
            else terminateds.get("__all__", False)
        )

        # If this is an ongoing episode than the last `__all__` should be `False`
        self.is_truncated: bool = (
            truncateds
            if isinstance(truncateds, bool)
            else truncateds.get("__all__", False)
        )

        # The individual agent SingleAgentEpisode objects.
        self.agent_episodes: Dict[AgentID, SingleAgentEpisode] = {}
        self._init_single_agent_episodes(
            agent_episode_ids=agent_episode_ids,
            observations=observations,
            infos=infos,
            actions=actions,
            rewards=rewards,
            terminateds=terminateds,
            truncateds=truncateds,
            extra_model_outputs=extra_model_outputs,
            len_lookback_buffer=len_lookback_buffer,
        )

        # RGB uint8 images from rendering the env; the images include the corresponding
        # rewards.
        assert render_images is None or observations is not None
        self.render_images: Union[List[np.ndarray], List[object]] = (
            [] if render_images is None else render_images
        )

        # Validate ourselves.
        self.validate()

    #def concat_episode(self, episode_chunk: "MultiAgentEpisode") -> None:
    #    """Adds the given `episode_chunk` to the right side of self.

    #    For concatenating episodes the following rules hold:
    #    - IDs are identical.
    #    - timesteps match (`env_t` of `self` matches `env_t_started` of
    #      `episode_chunk`).

    #    Args:
    #        episode_chunk: `MultiAgentEpsiode` instance that should be concatenated
    #            to `self` (resulting in `self` being extended).
    #    """
    #    assert episode_chunk.id_ == self.id_
    #    assert not self.is_done
    #    # Make sure the timesteps match.
    #    assert self.env_t == episode_chunk.env_t_started

    #    # TODO (simon): Write `validate()` method.

    #    # Make sure, end matches `episode_chunk`'s beginning for all agents.
    #    # Note, we have to assert for the last local observations as for
    #    # each agent alive, we need in the successor an initial observation.
    #    observations: MultiAgentDict = {
    #        agent_id: agent_obs
    #        for agent_id, agent_obs in self.get_observations(indices_in_env_steps=False).items()
    #        if not self.agent_episodes[agent_id].is_done
    #    }
    #    for agent_id, agent_obs in episode_chunk.get_observations(indices=0).items():
    #        # Make sure that the same agents stepped at both timesteps.
    #        assert agent_id in observations
    #        assert observations[agent_id] == agent_obs
    #
    #    # Call the `SingleAgentEpisode`'s `concat_episode()` method for all agents.
    #    for agent_id, agent_eps in self.agent_episodes.items():
    #        if not agent_eps.is_done:
    #            agent_eps.concat_episode(episode_chunk.agent_episodes[agent_id])
    #            # Update our timestep mapping.
    #            # As with observations we need the global timestep mappings to overlap.
    #            assert (
    #                self.env_t_to_agent_t[agent_id][-1]
    #                == episode_chunk.env_t_to_agent_t[agent_id][0]
    #            )
    #            self.env_t_to_agent_t[agent_id] += episode_chunk.env_t_to_agent_t[
    #                agent_id
    #            ][1:]
    #            # TODO (simon): Check, if this works always.
    #            # We have to take care of cases where a successor took over full action
    #            # buffers, b/c it then also took over the last timestep of the global
    #            # action timestep mapping.
    #            if (
    #                self.global_actions_t[agent_id][-1]
    #                == episode_chunk.global_actions_t[agent_id][0]
    #            ):
    #                self.global_actions_t[agent_id] += episode_chunk.global_actions_t[
    #                    agent_id
    #                ][1:]
    #            # If the action buffer was empty before the successor was created, we
    #            # can concatenate all values.
    #            else:
    #                self.global_actions_t[agent_id] += episode_chunk.global_actions_t[
    #                    agent_id
    #                ]
    #
    #            indices_for_partial_rewards = episode_chunk.partial_rewards_t[
    #                agent_id
    #            ].find_indices_right(self.env_t)
    #            # We use the `map()` here with `__getitem__` for the case of empty
    #            # lists.
    #            self.partial_rewards_t[agent_id] += list(
    #                map(
    #                    episode_chunk.partial_rewards_t[agent_id].__getitem__,
    #                    indices_for_partial_rewards,
    #                )
    #            )
    #            self.partial_rewards[agent_id] += list(
    #                map(
    #                    episode_chunk.partial_rewards[agent_id].__getitem__,
    #                    indices_for_partial_rewards,
    #                )
    #            )
    #
    #    # Copy the agent buffers over.
    #    self._copy_buffer(episode_chunk)
    #
    #    self.env_t = episode_chunk.env_t
    #    if episode_chunk.is_terminated:
    #        self.is_terminated = True
    #    if episode_chunk.is_truncated:
    #        self.is_truncated = True
    #
    #    # Validate.
    #    self.validate()

    def get_observations(
        self,
        indices: Optional[Union[int, List[int], slice]] = None,
        agent_ids: Optional[List[AgentID]] = None,
        *,
        env_steps: bool = True,
        #global_indices: bool = False,
        neg_indices_left_of_zero: bool = False,
        fill: Optional[float] = None,
        one_hot_discrete: bool = False,
        as_list: bool = False,
    ) -> Union[MultiAgentDict, List[MultiAgentDict]]:
        """Returns agents' observations or batched ranges thereof from this episode.

        Args:
            indices: A single int is interpreted as an index, from which to return the
                individual observation stored at this index.
                A list of ints is interpreted as a list of indices from which to gather
                individual observations in a batch of size len(indices).
                A slice object is interpreted as a range of observations to be returned.
                Thereby, negative indices by default are interpreted as "before the end"
                unless the `neg_indices_left_of_zero=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
                If None, will return all observations (from ts=0 to the end).
            agent_ids: An optional list/tuple of agent IDs to get observations for.
                If None, will return observations for all agents in this episode.
            env_steps: Whether `indices` should be interpreted as environment time steps
                (True) or per-agent timesteps (False).
            #global_indices: Only relevant for continued episode chunks (e.g. created
            #    via the `cut()` method. If True, given `indices` will be interpreted as
            #    global timesteps (starting from the very beginning of the episode as 0).
            #    Thus, if `self` is a continuation chunk with `self.env_t_start=10` and
            #    `indices=9` and `global_indices=True`, will try to return the last
            #    timestep in the lookback buffer (at env ts=9).
            neg_indices_left_of_zero: If True, negative values in `indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with agent A's observations
                [4, 5, 6,  7, 8, 9], where [4, 5, 6] is the lookback buffer range
                (ts=0 item is 7), will respond to `get_observations(-1, agent_ids=[A],
                neg_indices_left_of_zero=True)` with {A: `6`} and to
                `get_observations(slice(-2, 1), agent_ids=[A],
                neg_indices_left_of_zero=True)` with {A: `[5, 6,  7]`}.
            fill: An optional float value to use for filling up the returned results at
                the boundaries. This filling only happens if the requested index range's
                start/stop boundaries exceed the episode's boundaries (including the
                lookback buffer on the left side). This comes in very handy, if users
                don't want to worry about reaching such boundaries and want to zero-pad.
                For example, an episode with agent A' observations [10, 11,  12, 13, 14]
                and lookback buffer size of 2 (meaning observations `10` and `11` are
                part of the lookback buffer) will respond to
                `get_observations(slice(-7, -2), agent_ids=[A], fill=0.0)` with
                `{A: [0.0, 0.0, 10, 11, 12]}`.
            one_hot_discrete: If True, will return one-hot vectors (instead of
                int-values) for those sub-components of a (possibly complex) observation
                space that are Discrete or MultiDiscrete.  Note that if `fill=0` and the
                requested `indices` are out of the range of our data, the returned
                one-hot vectors will actually be zero-hot (all slots zero).
            as_list: TODO

        Returns:
            A dictionary mapping agent IDs to observations (at the given
            `indices`). If `env_steps` is True, only agents that have stepped
            (were ready) at the given env step `indices` are returned (i.e. not all
            agent IDs are necessarily in the keys).
            If `as_list` is True, returns a list of MultiAgentDicts (mapping agent IDs
            to observations) instead.
        """
        agent_ids = agent_ids or self._agent_ids

        ret = {}

        # User specified agent timesteps (indices) -> Simply delegate everything
        # to the individual agents' SingleAgentEpisodes.
        if env_steps is False:
            for agent_id, sa_episode in self.agent_episodes.items():
                if agent_id not in agent_ids:
                    continue
                ret[agent_id] = sa_episode.get_observations(
                    indices=indices,
                    neg_indices_left_of_zero=neg_indices_left_of_zero,
                    fill=fill,
                    one_hot_discrete=one_hot_discrete,
                )
            return ret

        # User specified env timesteps (indices) -> We need to translate them for each
        # agent into agent-timesteps.
        for agent_id, sa_episode in self.agent_episodes.items():
            if agent_id not in agent_ids:
                continue
            agent_indices = self.env_t_to_agent_t.get(
                indices,
                neg_indices_left_of_zero=neg_indices_left_of_zero,
                fill=self.OUT_OF_BOUNDS_TS_TAG if fill is not None else None,
            )
            # If there are self.SKIP_TS_TAG items in `agent_indices` and user
            # wants to fill these (together with outside-episode-bounds indices) ->
            # Provide these skipped timesteps as filled values.
            if self.SKIP_TS_TAG in agent_indices and fill is not None:
                single_fill_value = sa_episode.get_observations(
                    indices=1000000000000,
                    neg_indices_left_of_zero=neg_indices_left_of_zero,
                    fill=fill,
                    one_hot_discrete=one_hot_discrete,
                )
                ret[agent_id] = []
                for i in agent_indices:
                    if i == self.SKIP_ENV_TS_TAG or i == self.OUT_OF_BOUNDS_TS_TAG:
                        ret[agent_id].append(single_fill_value)
                    else:
                        ret[agent_id].append(sa_episode.get_observations(
                            indices=i,
                            neg_indices_left_of_zero=neg_indices_left_of_zero,
                            fill=fill,
                            one_hot_discrete=one_hot_discrete,
                        ))
                if self.is_finalized:
                    ret[agent_id] = batch(ret[agent_id])
            else:
                # Filter these indices out up front.
                agent_indices = [i for i in agent_indices if i != self.SKIP_TS_TAG]
                ret[agent_id] = sa_episode.get_observations(
                    indices=agent_indices,
                    neg_indices_left_of_zero=neg_indices_left_of_zero,
                    fill=fill,
                    one_hot_discrete=one_hot_discrete,
                )

    # TODO (simon): This should replace all getter logic. Refactor in the next commits.
    def _get_data_by_indices(
        self,
        attr: str,
        indices: Optional[Union[int, List[int], slice]] = None,
        indices_in_env_steps: bool = True,
        neg_indices_left_of_zero: bool = False,
        fill: Optional[float] = None,
        one_hot_discrete: bool = False,
        as_list: bool = False,
        shift: int = 0,
    ) -> Union[MultiAgentDict, List[MultiAgentDict]]:
        # User wants to have global timesteps.
        if indices_in_env_steps:
            # Get the corresponding local timesteps from the timestep mappings.
            indices = {
                agent_id: agent_map.get_agent_timesteps(
                    indices,
                    neg_timesteps_left_of_zero=neg_indices_left_of_zero,
                    fill=fill,
                    # Return `None` for timesteps not found, if user wants list.
                    return_none=as_list,
                    t=self.env_t,
                    shift=shift,
                )
                for agent_id, agent_map in self.env_t_to_agent_t.items()
            }

        # User wants to receive results in a list of `MultiAgentDict`s.
        if as_list:
            # We only return as a list if global timesteps are requested.
            if not indices_in_env_steps:
                RuntimeError(
                    f"Cannot return {attr} as a list when local timesteps are "
                    "requested."
                )

            # Get the number of requested timesteps. Note
            num_indices = len(next(iter(indices.values())))

            # Return the values.
            return [
                {
                    agent_id: agent_eps.get_observations(
                        indices[agent_id][idx],
                        neg_indices_left_of_zero=neg_indices_left_of_zero,
                        fill=fill,
                    )
                    for agent_id, agent_eps in self.agent_episodes.items()
                    # Only include in `MultiAgentDict`, if index was found
                    # via `get_agent_timesteps`.
                    if indices[agent_id][idx]
                }
                for idx in range(num_indices)
            ]
        # User wants a `MultiAgentDict` with agent results as `list`.
        else:
            return {
                agent_id: getattr(agent_eps, "get_" + attr)(
                    indices[agent_id],
                    neg_indices_left_of_zero=neg_indices_left_of_zero,
                    fill=fill,
                    one_hot_discrete=one_hot_discrete,
                )
                for agent_id, agent_eps in self.agent_episodes.items()
                if indices[agent_id]
            }

    def get_infos(
        self,
        indices: Optional[Union[int, List[int], slice]] = None,
        indices_in_env_steps: bool = True,
        neg_indices_left_of_zero: bool = False,
        fill: Optional[float] = None,
        one_hot_discrete: bool = False,
    ) -> MultiAgentDict:
        """Returns multi-agent infos for requested indices."""
        if indices_in_env_steps:
            indices = {
                agent_id: agent_map.get_agent_timesteps(
                    indices,
                    neg_timesteps_left_of_zero=neg_indices_left_of_zero,
                    fill=fill,
                    t=self.env_t,
                )
                for agent_id, agent_map in self.env_t_to_agent_t.items()
            }

        return {
            agent_id: agent_eps.get_infos(
                indices[agent_id],
                neg_indices_left_of_zero=neg_indices_left_of_zero,
                fill=fill,
                one_hot_discrete=one_hot_discrete,
            )
            for agent_id, agent_eps in self.agent_episodes.items()
            if indices[agent_id]
        }

    # TODO (simon): Add buffered actions.
    # You might take a look into old getter. But this time the
    # timestep mapping has to take of requesting the indices right of
    # a requested one.
    def get_actions(
        self,
        indices: Optional[Union[int, List[int], slice]] = None,
        indices_in_env_steps: bool = True,
        neg_indices_left_of_zero: bool = False,
        fill: Optional[float] = None,
        one_hot_discrete: bool = False,
    ) -> MultiAgentDict:
        """Returns multi-agent actions for requested indices."""

        if indices_in_env_steps:
            indices = {
                agent_id: agent_map.get_agent_timesteps(
                    indices,
                    neg_timesteps_left_of_zero=neg_indices_left_of_zero,
                    fill=fill,
                    t=self.env_t,
                    shift=-1,
                )
                for agent_id, agent_map in self.env_t_to_agent_t.items()
            }

        return {
            agent_id: agent_eps.get_actions(
                indices[agent_id],
                neg_indices_left_of_zero=neg_indices_left_of_zero,
                fill=fill,
                one_hot_discrete=one_hot_discrete,
            )
            for agent_id, agent_eps in self.agent_episodes.items()
            if indices[agent_id]
        }

    def get_extra_model_outputs(
        self,
        key: str,
        indices: Optional[Union[int, List[int], slice]] = None,
        indices_in_env_steps: bool = True,
        neg_indices_left_of_zero: bool = False,
        fill: Optional[float] = None,
        one_hot_discrete: bool = False,
    ) -> MultiAgentDict:
        """Returns multi-agent extra model outputs for requested indices."""
        if not key:
            raise RuntimeError(
                "No `key` specified for extra model outputs. To get extra model "
                "model outputs a `key` needs to be specified."
            )

        if indices_in_env_steps:
            indices = {
                agent_id: agent_map.get_agent_timesteps(
                    indices,
                    neg_timesteps_left_of_zero=neg_indices_left_of_zero,
                    fill=fill,
                    t=self.env_t,
                    shift=-1,
                )
                for agent_id, agent_map in self.env_t_to_agent_t.items()
            }

        return {
            agent_id: agent_eps.get_extra_model_outputs(
                key,
                indices=indices[agent_id],
                neg_indices_left_of_zero=neg_indices_left_of_zero,
                fill=fill,
                one_hot_discrete=one_hot_discrete,
            )
            for agent_id, agent_eps in self.agent_episodes.items()
            if indices[agent_id]
        }

    # TODO (simon): Make sure that users always give in sorted lists.
    # Because of the way we check the indices we cannot guarantee the order of
    # indices, specifically, if we want to insert buffered actions.
    # TODO (simon): Users might want to receive only actions that have a
    # corresponding 'next observation' (i.e. no buffered actions). Take care of this.
    # Also in the `extra_model_outputs`.

    # def get_actions(
    #     self,
    #     indices: Union[int, List[int]] = -1,
    #     indices_in_env_steps: bool = True,
    #     as_list: bool = False,
    # ) -> Union[MultiAgentDict, List[MultiAgentDict]]:
    #     """Gets actions for all agents that stepped in the last timesteps.

    #     Note that actions are only returned for agents that stepped
    #     during the given index range.

    #     Args:
    #         indices: Either a single index or a list of indices. The indices
    #             can be reversed (e.g. [-1, -2]) or absolute (e.g. [98, 99]).
    #             This defines the time indices for which the actions
    #             should be returned.
    #         indices_in_env_steps: Boolean that defines, if the indices should be considered
    #             environment (`True`) or agent (`False`) steps.

    #     Returns: A dictionary mapping agent ids to actions (of different
    #         timesteps). Only for agents that have stepped (were ready) at a
    #         timestep, actions are returned (i.e. not all agent ids are
    #         necessarily in the keys).
    #     """
    #     buffered_actions = {}

    #     if indices_in_env_steps:
    #         # Check, if the indices are iterable.
    #         if isinstance(indices, list):
    #             indices = [
    #                 (self.env_t + idx + 1 if idx < 0 else idx + self.ts_carriage_return)
    #                 for idx in indices
    #             ]
    #         # If not make them iterable.
    #         else:
    #             indices = (
    #                 [self.env_t + indices + 1]
    #                 if indices < 0
    #                 else [indices + self.ts_carriage_return]
    #             )
    #     else:
    #         if not isinstance(indices, list):
    #             indices = [indices]
    #     # Check now, if one of these indices is the last in the global
    #     # action timestep mapping.
    #     for agent_id, agent_global_action_t in self.global_actions_t.items():
    #         if agent_global_action_t:
    #             last_action_index = (
    #                 agent_global_action_t[-1]
    #                 if indices_in_env_steps
    #                 else len(agent_global_action_t) - 1
    #             )
    #         # We consider only timesteps that are in the requested indices and
    #         # check then, if for these the buffer is full.
    #         if (
    #             agent_global_action_t
    #             and (last_action_index in indices or -1 in indices)
    #             and self.agent_buffers[agent_id]["actions"].full()
    #         ):
    #             # Then the buffer must be full and needs to be accessed.
    #             # Note, we do not want to empty the buffer, but only read it.
    #             buffered_actions[agent_id] = [
    #                 self.agent_buffers[agent_id]["actions"].queue[0]
    #             ]
    #         else:
    #             buffered_actions[agent_id] = []

    #     # Now, get the actions.
    #     actions = self._getattr_by_index(
    #         "actions",
    #         indices=indices,
    #         has_initial_value=True,
    #         indices_in_env_steps=indices_in_env_steps,
    #         indices_in_env_steps_mapping=self.global_actions_t,
    #         # shift=1,
    #         as_list=as_list,
    #         buffered_values=buffered_actions,
    #     )

    #     return actions

    # def get_extra_model_outputs(
    #     self,
    #     indices: Union[int, List[int]] = -1,
    #     indices_in_env_steps: bool = True,
    #     as_list: bool = False,
    # ) -> Union[MultiAgentDict, List[MultiAgentDict]]:
    #     """Gets actions for all agents that stepped in the last timesteps.

    #     Note that actions are only returned for agents that stepped
    #     during the given index range.

    #     Args:
    #         indices: Either a single index or a list of indices. The indices
    #             can be reversed (e.g. [-1, -2]) or absolute (e.g. [98, 99]).
    #             This defines the time indices for which the actions
    #             should be returned.
    #         indices_in_env_steps: Boolean that defines, if the indices should be considered
    #             environment (`True`) or agent (`False`) steps.

    #     Returns: A dictionary mapping agent ids to actions (of different
    #         timesteps). Only for agents that have stepped (were ready) at a
    #         timestep, actions are returned (i.e. not all agent ids are
    #         necessarily in the keys).
    #     """
    #     buffered_outputs = {}

    #     if indices_in_env_steps:
    #         # Check, if the indices are iterable.
    #         if isinstance(indices, list):
    #             indices = [(self.env_t + idx + 1 if idx < 0 else idx) for idx in indices]
    #         # If not make them iterable.
    #         else:
    #             indices = [self.env_t + indices + 1] if indices < 0 else [indices]
    #     else:
    #         if not isinstance(indices, list):
    #             indices = [indices]
    #     # Check now, if one of these indices is the last in the global
    #     # action timestep mapping.
    #     for agent_id, agent_global_action_t in self.global_actions_t.items():
    #         if agent_global_action_t:
    #             last_action_index = (
    #                 agent_global_action_t[-1]
    #                 if indices_in_env_steps
    #                 else len(agent_global_action_t) - 1
    #             )
    #         # We consider only timesteps that are in the requested indices and
    #         # check then, if for these the buffer is full. Note, we cannot use
    #         # the extra model outputs buffer as this is by default `None`.
    #         if (
    #             agent_global_action_t
    #             and (last_action_index in indices or -1 in indices)
    #             and self.agent_buffers[agent_id]["actions"].full()
    #         ):
    #             # Then the buffer must be full and needs to be accessed.
    #             # Note, we do not want to empty the buffer, but only read it.
    #             buffered_outputs[agent_id] = [
    #                 self.agent_buffers[agent_id]["extra_model_outputs"].queue[0]
    #             ]
    #         else:
    #             buffered_outputs[agent_id] = []

    #     # Now, get the actions.
    #     extra_model_outputs = self._getattr_by_index(
    #         "extra_model_outputs",
    #         indices=indices,
    #         has_initial_value=True,
    #         indices_in_env_steps=indices_in_env_steps,
    #         indices_in_env_steps_mapping=self.global_actions_t,
    #         # shift=1,
    #         as_list=as_list,
    #         buffered_values=buffered_outputs,
    #     )

    #     return extra_model_outputs

    # TODO (simon): Add the buffered rewards and maybe add functionality
    #  or extra method to retrieve partial rewards.
    # get_partial_rewards()
    def get_rewards(
        self,
        indices: Optional[Union[int, List[int], slice]] = None,
        indices_in_env_steps: bool = True,
        neg_indices_left_of_zero: bool = False,
        fill: Optional[float] = None,
        one_hot_discrete: bool = False,
    ) -> MultiAgentDict:
        """Returns multi-agent actions for requested indices."""

        if indices_in_env_steps:
            indices = {
                agent_id: agent_map.get_agent_timesteps(
                    indices,
                    neg_timesteps_left_of_zero=neg_indices_left_of_zero,
                    fill=fill,
                    t=self.env_t,
                    shift=-1,
                )
                for agent_id, agent_map in self.env_t_to_agent_t.items()
            }

        return {
            agent_id: agent_eps.get_rewards(
                indices[agent_id],
                neg_indices_left_of_zero=neg_indices_left_of_zero,
                fill=fill,
                one_hot_discrete=one_hot_discrete,
            )
            for agent_id, agent_eps in self.agent_episodes.items()
            if indices[agent_id]
        }

    # def get_rewards(
    #     self,
    #     indices: Union[int, List[int]] = -1,
    #     indices_in_env_steps: bool = True,
    #     as_list: bool = False,
    #     partial: bool = True,
    #     consider_buffer: bool = True,
    # ) -> Union[MultiAgentDict, List[MultiAgentDict]]:
    #     """Gets rewards for all agents that stepped in the last timesteps.

    #     Note that rewards are only returned for agents that stepped
    #     during the given index range.

    #     Args:
    #         indices: Either a single index or a list of indices. The indices
    #             can be reversed (e.g. [-1, -2]) or absolute (e.g. [98, 99]).
    #             This defines the time indices for which the rewards
    #             should be returned.
    #         indices_in_env_steps: Boolean that defines, if the indices should be considered
    #             environment (`True`) or agent (`False`) steps.

    #     Returns: A dictionary mapping agent ids to rewards (of different
    #         timesteps). Only for agents that have stepped (were ready) at a
    #         timestep, rewards are returned (i.e. not all agent ids are
    #         necessarily in the keys).
    #     """

    #     if indices_in_env_steps:
    #         # Check, if the indices are iterable.
    #         if isinstance(indices, list):
    #             indices = [
    #                 (self.env_t - self.ts_carriage_return + idx + 1 if idx < 0 else idx)
    #                 for idx in indices
    #             ]
    #         # If not make them iterable.
    #         else:
    #             indices = (
    #                 [self.env_t - self.ts_carriage_return + indices + 1]
    #                 if indices < 0
    #                 else [indices]
    #             )
    #     else:
    #         if not isinstance(indices, list):
    #             indices = [indices]

    #     if not partial and consider_buffer:
    #         buffered_rewards = {}
    #         timestep_mapping = {}

    #         for agent_id, agent_env_t_to_agent_t in
    # self.env_t_to_agent_t.items():
    #             # If the agent had an initial observation.
    #             if agent_env_t_to_agent_t:
    #                 # If the agent received rewards after the last observation.
    #                 if (
    #                     self.partial_rewards_t[agent_id]
    #                     and self.partial_rewards_t[agent_id][-1]
    #                     > agent_env_t_to_agent_t[-1]
    #                     and self.partial_rewards_t[agent_id][-1] <= max(indices)
    #                 ):
    #                     indices_at_or_after_last_obs = [
    #                         agent_env_t_to_agent_t[-1]
    #                     ] + sorted(
    #                         [
    #                             idx
    #                             for idx in indices
    #                             if idx > agent_env_t_to_agent_t[-1]
    #                         ]
    #                     )
    #                     (
    #                         buffered_rewards[agent_id],
    #                         indices_wih_rewards,
    #                     ) = self.accumulate_partial_rewards(
    #                         agent_id,
    #                         indices_at_or_after_last_obs,
    #                         return_indices=True,
    #                     )
    #                     # Note, the global timestep mapping begins at zero
    #                     # with the initial observation. Rewards start at
    #                     # timestep 1.
    #                     timestep_mapping[agent_id] = _IndexMapping(
    #                         agent_env_t_to_agent_t[1:]
    #                         + list(
    #                             range(
    #                                 agent_env_t_to_agent_t[-1] + 1,
    #                                 agent_env_t_to_agent_t[-1]
    #                                 + 1
    #                                 + len(indices_wih_rewards),
    #                             )
    #                         )
    #                     )
    #                 # There are no partial rewards in the range of requested indices.
    #                 else:
    #                     buffered_rewards[agent_id] = []
    #                     # Note, we need here at least the timestep mapping for the
    #                     # recorded timesteps as they have recorded any rewards before.
    #                     # TODO (simon): Allow slicing for _IndexMapping. ALso rename
    #                     # to
    #                     # TimestepMapping.
    #                     # TODO (simon): Check, if we can simply use the `
    #                     # has_initial_value`instead of slicing.
    #                     timestep_mapping[agent_id] = _IndexMapping(
    #                         agent_env_t_to_agent_t[1:]
    #                     )

    #             # If the agent had no initial observation, yet.
    #             else:
    #                 # Has the agent received partial rewards, yet and if yes, has
    #                 # any one received before the last requested index.
    #                 if self.partial_rewards_t[agent_id] and self.partial_rewards_t[
    #                     agent_id
    #                 ][0] < max(indices):
    #                     # Then assign the partial rewards to the requested indices.
    #                     # Note, the function accumulates from the second index on, so
    #                     # we add a zero.
    #                     # TODO (simon): Check, if we need here maybe a sifting for
    #                     # the first index in partial_rewards_t.
    #                     (
    #                         buffered_rewards[agent_id],
    #                         indices_with_rewards,
    #                     ) = self.accumulate_partial_rewards(
    #                         agent_id,
    #                         [0] + sorted(indices),
    #                         return_indices=True,
    #                     )
    #                     # TODO (simon): This should be all indices at or below which
    #                     # rewards existed.
    #                     timestep_mapping[agent_id] = _IndexMapping(
    #                         range(1, len(indices_with_rewards) + 1)
    #                     )
    #                     # timestep_mapping[agent_id] = _IndexMapping(
    #                     #     [
    #                     #         idx
    #                     #         for idx in sorted(indices)
    #                     #         if self.partial_rewards_t[
    #                     #             "agent_1"
    #                     #         ].find_indices_left_equal(idx)
    #                     #     ]
    #                     # )
    #                 # Else, no partial rewards have to be assigned.
    #                 else:
    #                     buffered_rewards[agent_id] = []
    #                     timestep_mapping[agent_id] = _IndexMapping()

    #     # Now request the rewards.
    #     if partial:
    #         # Here we simply apply the logic from `_getattr_by_index`, however
    #         # use simply the the `partial_rewards` dict. Check for indices
    #         # correspondingly.
    #         if as_list:
    #             return [
    #                 {
    #                     agent_id: self.partial_rewards[agent_id][
    #                         self.partial_rewards_t[agent_id].find_indices(
    #                             [idx], shift=0
    #                         )[0]
    #                     ]
    #                     for agent_id, agent_eps in self.agent_episodes.items()
    #                     if self.partial_rewards_t[agent_id].find_indices([idx],
    #                       shift=0)
    #                 }
    #                 for idx in indices
    #             ]
    #         else:
    #             return {
    #                 agent_id: list(
    #                     map(
    #                         self.partial_rewards[agent_id].__getitem__,
    #                         self.partial_rewards_t[agent_id].find_indices(
    #                             indices, shift=0
    #                         ),
    #                     )
    #                 )
    #                 for agent_id in self._agent_ids
    #                 if self.partial_rewards_t[agent_id].find_indices(indices, shift=0)
    #             }

    #     else:
    #         if consider_buffer:
    #             # Note, we do not consider initial values here as the indices are
    #             # already positive.
    #             return self._getattr_by_index(
    #                 "rewards",
    #                 indices,
    #                 indices_in_env_steps=indices_in_env_steps,
    #                 indices_in_env_steps_mapping=timestep_mapping,
    #                 buffered_values=buffered_rewards,
    #                 as_list=as_list,
    #             )
    #         else:
    #             # Note, if we use the global timestep mapping (of observations), the
    #             # mapping starts at 0 (because observations do), therefore we have to
    #             # set `has_initial_value` to `True`.
    #             return self._getattr_by_index(
    #                 "rewards",
    #                 indices,
    #                 has_initial_value=True,
    #                 indices_in_env_steps=indices_in_env_steps,
    #                 as_list=as_list,
    #                 shift=-1,
    #             )

    def accumulate_partial_rewards(
        self,
        agent_id: Union[str, int],
        indices: List[int],
        return_indices: bool = False,
    ):
        """Accumulates rewards along the interval between two indices.

        Assumes the indices are sorted ascendingly. Opeartes along the
        half-open interval (last_index, index].

        Args:
            agent_id: Either string or integer. The unique id of the agent in
                the `MultiAgentEpisode`.
            indices: List of integers. The ascendingly sorted indices for which
                the rewards should be accumulated.

        Returns: A list of accumulated rewards for the indices `1:len(indices)`.
        """
        if return_indices:
            index_interval_map = {
                indices[indices.index(idx) + 1]: self.partial_rewards_t[
                    agent_id
                ].find_indices_between_right_equal(idx, indices[indices.index(idx) + 1])
                for idx in indices[:-1]
                if self.partial_rewards_t[agent_id].find_indices_between_right_equal(
                    idx, indices[indices.index(idx) + 1]
                )
            }
            return [
                sum(map(self.partial_rewards[agent_id].__getitem__, v))
                for v in index_interval_map.values()
            ], list(index_interval_map.keys())
        else:
            return [
                sum(
                    [
                        self.partial_rewards[agent_id][i]
                        for i in self.partial_rewards_t[
                            agent_id
                        ].find_indices_between_right_equal(
                            idx, indices[indices.index(idx) + 1]
                        )
                    ]
                )
                for idx in indices[:-1]
                if self.partial_rewards_t[agent_id].find_indices_between_right_equal(
                    idx, indices[indices.index(idx) + 1]
                )
            ]

    def get_terminateds(self) -> MultiAgentDict:
        """Gets the terminateds at given indices."""
        terminateds = {
            agent_id: self.agent_episodes[agent_id].is_terminated
            for agent_id in self._agent_ids
        }
        terminateds.update({"__all__": self.is_terminated})
        return terminateds

    def get_truncateds(self) -> MultiAgentDict:
        truncateds = {
            agent_id: self.agent_episodes[agent_id].is_truncated
            for agent_id in self._agent_ids
        }
        truncateds.update({"__all__": self.is_terminated})
        return truncateds

    def add_env_reset(
        self,
        *,
        observations: MultiAgentDict,
        infos: Optional[MultiAgentDict] = None,
        render_image: Optional[np.ndarray] = None,
    ) -> None:
        """Stores initial observation.

        Args:
            observations: A dictionary mapping agent IDs to initial observations.
                Note that some agents may not have an initial observation.
            infos: A dictionary mapping agent IDs to initial info dicts.
                Note that some agents may not have an initial info dict. If not None,
                the agent IDs in `infos` must be a subset of those in `observations`
                meaning it would not be allowed to have an agent with an info dict,
                but not with an observation.
            render_image: A (global) RGB uint8 image from rendering the environment
                (for all agents).
        """
        assert not self.is_done
        # Assume that this episode is completely empty and has not stepped yet.
        # Leave self.env_t (and self.env_t_started) at 0.
        assert self.env_t == self.env_t_started == 0
        infos = infos or {}

        # Note that we store the render images into the `MultiAgentEpisode`
        # instead into each `SingleAgentEpisode`.
        if render_image is not None:
            self.render_images.append(render_image)

        # Note, all agents will have an initial observation, some may have an initial
        # info dict as well.
        for agent_id, agent_obs in observations.items():
            # Create SingleAgentEpisode, if necessary.
            if agent_id not in self.agent_episodes:
                self.agent_episodes[agent_id] = SingleAgentEpisode(
                    observation_space=self.observation_space.get(agent_id),
                    action_space=self.action_space.get(agent_id),
                )
            # Add initial observations (and infos) to the agent's episode.
            self.agent_episodes[agent_id].add_env_reset(
                observation=agent_obs,
                infos=infos.get(agent_id),
            )

    def add_env_step(
        self,
        observations: MultiAgentDict,
        actions: MultiAgentDict,
        rewards: MultiAgentDict,
        *,
        infos: Optional[MultiAgentDict] = None,
        terminateds: Optional[MultiAgentDict] = None,
        truncateds: Optional[MultiAgentDict] = None,
        render_image: Optional[np.ndarray] = None,
        extra_model_outputs: Optional[MultiAgentDict] = None,
    ) -> None:
        """Adds a timestep to the episode.

        Args:
            observations: A dictionary mapping agent ids to their corresponding
                observations. Note that some agents may not have stepped at this
                timestep.
            actions: Mandatory. A dictionary mapping agent ids to their
                corresponding actions. Note that some agents may not have stepped at
                this timestep.
            rewards: Mandatory. A dictionary mapping agent ids to their
                corresponding observations. Note that some agents may not have stepped
                at this timestep.
            infos: A dictionary mapping agent ids to their
                corresponding info. Note that some agents may not have stepped at this
                timestep.
            terminateds: A dictionary mapping agent ids to their `terminated` flags,
                indicating, whether the environment has been terminated for them.
                A special `__all__` key indicates that the episode is terminated for
                all agent ids.
            terminateds: A dictionary mapping agent ids to their `truncated` flags,
                indicating, whether the environment has been truncated for them.
                A special `__all__` key indicates that the episode is `truncated` for
                all agent ids.
            render_image: An RGB uint8 image from rendering the environment.
            extra_model_outputs: Optional. A dictionary mapping agent ids to their
                corresponding specific model outputs (also in a dictionary; e.g.
                `vf_preds` for PPO).
        """
        # Cannot add data to an already done episode.
        if self.is_done:
            raise MultiAgentEnvError(
                "Cannot call `add_env_step` on a MultiAgentEpisode that is already "
                "done!"
            )

        terminateds = terminateds or {}
        truncateds = truncateds or {}

        # Increase (global) env step by one.
        self.env_t += 1

        # TODO (sven, simon): Will there still be an `__all__` that is
        #  terminated or truncated?
        # TODO (simon): Maybe allow user to not provide this and then `__all__` is
        #  False?
        self.is_terminated = terminateds.get("__all__", False)
        self.is_truncated = truncateds.get("__all__", False)

        # Note that we store the render images into the `MultiAgentEpisode`
        # instead of storing them into each `SingleAgentEpisode`.
        if render_image is not None:
            self.render_images.append(render_image)

        # For all agents that are not stepping in this env step, but that are not done
        # yet -> Add a None to their env- to agent-step mappings.
        stepping_agent_ids = set(actions.keys())
        for agent_id in self._agent_ids:
            if agent_id not in stepping_agent_ids:
                self.env_t_to_agent_t[agent_id].append(self.SKIP_ENV_TS_TAG)

        # Loop through all agent IDs that we received data for in this step:
        # Those found in observations, actions, and rewards.
        agent_ids_with_data = (
            set(observations.keys()) | set(actions.keys()) | set(rewards.keys())
        )
        for agent_id in agent_ids_with_data:
            if agent_id not in self.agent_episodes:
                self.agent_episodes[agent_id] = SingleAgentEpisode(
                    observation_space=self.observation_space.get(agent_id),
                    action_space=self.action_space.get(agent_id),
                )
            sa_episode: SingleAgentEpisode = self.agent_episodes[agent_id]

            # Collect value to be passed (at end of for-loop) into `add_env_step()`
            # call.
            _observation = observations.get(agent_id)
            _action = actions.get(agent_id)
            _reward = rewards.get(agent_id)
            _infos = infos.get(agent_id)
            _terminated = terminateds.get(agent_id, False) or self.is_terminated
            _truncated = truncateds.get(agent_id, False) or self.is_truncated
            _extra_model_outputs = extra_model_outputs.get(agent_id)

            # The value to place into the env- to agent-step map for this agent ID.
            _agent_step = self.SKIP_ENV_TS_TAG

            # Agents, whose SingleAgentEpisode had already been done before this
            # step should NOT have received any data in this step.
            if (
                sa_episode.is_done
                and any(v is not None for v in [
                    _observation, _action, _reward, _infos, _extra_model_outputs
                ])
            ):
                raise MultiAgentEnvError(
                    f"Agent {agent_id} already had its `SingleAgentEpisode.is_done` set "
                    f"to True, but still received data in a following step! "
                    f"obs={_observation} act={_action} rew={_reward} info={_infos} "
                    f"extra_model_outputs={_extra_model_outputs}."
                )
            _reward = _reward or 0.0

            # CASE 1: A complete agent step is available (in one env step).
            # -------------------------------------------------------------
            # We have an observation and an action for this agent ->
            # Add the agent step to the single agent episode.
            # ... action -> next obs + reward ...
            if _observation is not None and _action is not None:
                if agent_id not in rewards:
                    raise MultiAgentEnvError(
                        f"Agent {agent_id} acted (and received next obs), but did NOT "
                        f"receive any reward from the env!"
                    )

                _agent_step = len(sa_episode)

            # CASE 2: Step gets completed with a buffered action.
            # ---------------------------------------------------
            # We have an observation, but no action ->
            # a) Action (and extra model outputs) must be buffered already. Also use
            # collected buffered rewards.
            # b) The observation is the first observation for this agent ID.
            elif _observation is not None and _action is None:
                _action = self._agent_buffered_actions.pop(agent_id, None)

                # We have a buffered action (the agent had acted after the previous
                # observation, but the env had not responded - until now - with another
                # observation).
                # ...[buffered action] ... ... -> next obs + (reward)? ...
                if _action is not None:
                    # Get the extra model output if available.
                    _extra_model_outputs = (
                        self._agent_buffered_extra_model_outputs.pop(agent_id, None)
                    )
                    _reward = (
                        self._agent_buffered_rewards.pop(agent_id, 0) + _reward
                    )
                    _agent_step = len(sa_episode)
                # First observation for this agent, we have no buffered action.
                # ... [done]? ... -> [1st obs for agent ID]
                else:
                    # The agent is already done -> The agent thus has never stepped once
                    # and we do not have to create a SingleAgentEpisode for it.
                    if _terminated or _truncated:
                        self._erase_buffered_data(agent_id)
                        continue
                    # This must be the agent's initial observation.
                    else:
                        if agent_id in self._agent_buffered_rewards:
                            raise MultiAgentEnvError(
                                f"Agent {agent_id} seems to have received a reward ("
                                f"{self._agent_buffered_rewards[agent_id]}) before it "
                                f"even received its first observation!"
                            )
                        # Make `add_env_reset` call and continue with next agent.
                        sa_episode.add_env_reset(observation=_observation, infos=_infos)
                        continue

            # CASE 3: Step is started (by an action), but not completed (no next obs).
            # ------------------------------------------------------------------------
            # We have no observation, but we have an action to be buffered (and used
            # when we do receive the next obs for this agent in the future).
            elif agent_id not in observations and agent_id in actions:
                # Agent got truncated -> Error b/c we would need a last (truncation)
                # observation for this (otherwise, e.g. bootstrapping would not work).
                # [previous obs] [action] (to be buffered) ... ... [truncated]
                if _truncated:
                    raise MultiAgentEnvError(
                        f"Agent {agent_id} acted and then got truncated, but did NOT "
                        "receive a last (truncation) observation, required for e.g. "
                        "value function bootstrapping!"
                    )
                # Agent got terminated.
                # [previous obs] [action] (to be buffered) ... ... [terminated]
                elif _terminated:
                    # If the agent was terminated and no observation is provided,
                    # duplicate the previous one (this is a technical "fix" to properly
                    # complete the single agent episode; this last observation is never
                    # used for learning anyway).
                    _observation = sa_episode.get_observations(-1)
                    _infos = sa_episode.get_infos(-1)
                    _agent_step = len(sa_episode)
                # Agent is still alive.
                # [previous obs] [action] (to be buffered) ...
                else:
                    # Buffer action, reward, and extra_model_outputs.
                    assert agent_id not in self._agent_buffered_actions
                    self._agent_buffered_actions[agent_id] = _action
                    self._agent_buffered_rewards[agent_id] = _reward
                    self._agent_buffered_extra_model_outputs[agent_id] = (
                        _extra_model_outputs
                    )

            # CASE 4: Step has started in the past and is still ongoing (no observation,
            # no action).
            # --------------------------------------------------------------------------
            # Record reward and terminated/truncated flags.
            else:
                _action = self._agent_buffered_actions.get(agent_id)

                # Agent is done.
                if _terminated or _truncated:
                    # If the agent has NOT stepped, we treat it as not being
                    # part of this episode.
                    # ... ... [other agents doing stuff] ... ... [agent done]
                    if _action is None:
                        self._erase_buffered_data(agent_id)
                        continue

                    # Agent got truncated -> Error b/c we would need a last (truncation)
                    # observation for this (otherwise, e.g. bootstrapping would not
                    # work).
                    if _truncated:
                        raise MultiAgentEnvError(
                            f"Agent {agent_id} acted and then got truncated, but did "
                            "NOT receive a last (truncation) observation, required "
                            "for e.g. value function bootstrapping!"
                        )

                    # [obs] ... ... [buffered action] ... ... [done]
                    # If the agent was terminated and no observation is provided,
                    # duplicate the previous one (this is a technical "fix" to properly
                    # complete the single agent episode; this last observation is never
                    # used for learning anyway).
                    _observation = sa_episode.get_observations(-1)
                    _infos = sa_episode.get_infos(-1)
                    # `_action` is already `get` above. We don't need to pop out from
                    # the buffer as it gets wiped out anyway below b/c the agent is
                    # done. 
                    _extra_model_outputs = (
                        self._agent_buffered_extra_model_outputs.pop(agent_id, None)
                    )
                    _reward = (
                        self._agent_buffered_rewards.pop(agent_id, 0) + _reward
                    )
                    _agent_step = len(sa_episode)
                # The agent is still alive, just add current reward to buffer.
                else:
                    self._agent_buffered_rewards[agent_id] += _reward

            # Update the env- to agent-step mapping.
            self.env_t_to_agent_t[agent_id].append(_agent_step)

            # If agent is stepping, add timestep to `SingleAgentEpisode`.
            if _agent_step != self.SKIP_ENV_TS_TAG:
                sa_episode.add_env_step(
                    observation=_observation,
                    action=_action,
                    reward=_reward,
                    infos=_infos,
                    terminated=_terminated,
                    truncated=_truncated,
                    extra_model_outputs=_extra_model_outputs,
                )

            # Agent is also done. -> Erase all buffered values for this agent
            # (they should be empty at this point anyways).
            if _terminated or _truncated:
                self._erase_buffered_data(agent_id)

    def validate(self) -> None:
        """Validates the episode's data.

        This function ensures that the data stored to a `MultiAgentEpisode` is
        in order (e.g. that the correct number of observations, actions, rewards
        are there).
        """
        for eps in self.agent_episodes.values():
            eps.validate()

        # TODO (sven): Validate MultiAgentEpisode specifics, like the timestep mappings,
        #  action/reward buffers, etc..

    @property
    def is_finalized(self) -> bool:
        """True, if the data in this episode is already stored as numpy arrays."""
        is_finalized = next(iter(self.agent_episodes.values())).is_finalized
        # Make sure that all single agent's episodes' `finalized` flags are the same.
        if not all(
            eps.is_finalized is is_finalized for eps in self.agent_episodes.values()
        ):
            raise RuntimeError(
                f"Only some SingleAgentEpisode objects in {self} are finalized (others "
                f"are not)!"
            )
        return is_finalized

    @property
    def is_done(self):
        """Whether the episode is actually done (terminated or truncated).

        A done episode cannot be continued via `self.add_env_step()` or being
        concatenated on its right-side with another episode chunk or being
        succeeded via `self.cut()`.

        Note that in a multi-agent environment this does not necessarily
        correspond to single agents having terminated or being truncated.

        `self.is_terminated` should be `True`, if all agents are terminated and
        `self.is_truncated` should be `True`, if all agents are truncated. If
        only one or more (but not all!) agents are `terminated/truncated the
        `MultiAgentEpisode.is_terminated/is_truncated` should be `False`. This
        information about single agent's terminated/truncated states can always
        be retrieved from the `SingleAgentEpisode`s inside the 'MultiAgentEpisode`
        one.

        If all agents are either terminated or truncated, but in a mixed fashion,
        i.e. some are terminated and others are truncated: This is currently
        undefined and could potentially be a problem (if a user really implemented
        such a multi-agent env that behaves this way).

        Returns:
            Boolean defining if an episode has either terminated or truncated.
        """
        return self.is_terminated or self.is_truncated

    def finalize(self) -> "MultiAgentEpisode":
        """Converts this Episode's list attributes to numpy arrays.

        This means in particular that this episodes' lists (per single agent) of
        (possibly complex) data (e.g. an agent having a dict obs space) will be
        converted to (possibly complex) structs, whose leafs are now numpy arrays.
        Each of these leaf numpy arrays will have the same length (batch dimension)
        as the length of the original lists.

        Note that SampleBatch.INFOS are NEVER numpy'ized and will remain a list
        (normally, a list of the original, env-returned dicts). This is due to the
        herterogenous nature of INFOS returned by envs, which would make it unwieldy to
        convert this information to numpy arrays.

        After calling this method, no further data may be added to this episode via
        the `self.add_env_step()` method.

        Examples:

        .. testcode::

            import numpy as np

            from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
            from ray.rllib.env.tests.test_multi_agent_episode import (
                TestMultiAgentEpisode
            )

            # Create some multi-agent episode data.
            (
                observations,
                actions,
                rewards,
                terminateds,
                truncateds,
                infos,
            ) = TestMultiAgentEpisode._mock_multi_agent_records()
            # Define the agent ids.
            agent_ids = ["agent_1", "agent_2", "agent_3", "agent_4", "agent_5"]

            episode = MultiAgentEpisode(
                agent_ids=agent_ids,
                observations=observations,
                actions=actions,
                rewards=rewards,
                # Note: terminated/truncated have nothing to do with an episode
                # being `finalized` or not (via the `self.finalize()` method)!
                terminateds=terminateds,
                truncateds=truncateds,
                infos=infos,
            )

            # Episode has not been finalized (numpy'ized) yet.
            assert not episode.is_finalized
            # We are still operating on lists.
            assert (
                episode.get_observations(
                    indices=[1],
                    indices_in_env_steps=True,
                    agent_ids="agent_1",
                ) == [1]
            )

            # Let's finalize the episode.
            episode.finalize()
            assert episode.is_finalized

            # Everything is now numpy arrays (with 0-axis of size
            # B=[len of requested slice]).
            assert (
                isinstance(episode.get_observations(
                    indices=[1],
                    indices_in_env_steps=True,
                    agent_ids="agent_1",
                ), np.ndarray)
            )

        Returns:
             This `MultiAgentEpisode` object with the converted numpy data.
        """
        for agent_id, agent_eps in self.agent_episodes.items():
            agent_eps.finalize()

        return self

    def cut(self, len_lookback_buffer: int = 0) -> "MultiAgentEpisode":
        assert not self.is_done and len_lookback_buffer >= 0

        # Initialize this episode chunk with the most recent observations
        # and infos (even if lookback is zero). Similar to an initial `env.reset()`
        indices_obs_and_infos = slice(-len_lookback_buffer - 1, None)
        # TODO (simon): Note, the lookback for the other data is trickier as there
        # could be partial rewards or actions in the buffer.
        indices_rest = (
            slice(-len_lookback_buffer, None)
            if len_lookback_buffer > 0
            else slice(None, 0)
        )

        # TODO (simon): Here we need the `as_list` argument in the getters.
        successor = MultiAgentEpisode(
            # Same ID.
            id_=self.id_,
            # Same agent IDs.
            agent_id=self._agent_ids,
            # Same single agents' episode IDs.
            agent_episode_ids=self.single_agent_episode_ids,
            observations=self.get_observations(
                indices=indices_obs_and_infos, as_list=True
            ),
            infos=self.get_infos(indices=indices_obs_and_infos, as_list=True),
            actions=self.get_actions(indices=indices_rest, as_list=True),
            # TODO (simon): Here we need actually the partial rewards.
            rewards=self.get_rewards(indices=indices_rest, as_list=True),
            extra_model_outputs=self.get_extra_model_outputs(
                indices=indices_rest, as_list=True
            ),
            # Continue with `self`'s current timestep.
            env_t_started=self.env_t,
            len_lookback_buffer=len_lookback_buffer,
        )

        # TODO (simon): Copy over the buffers, here.

        return successor

    @property
    def agent_ids(self) -> MultiAgentDict:
        """Returns the agent ids."""
        return self._agent_ids

    @property
    def single_agent_episode_ids(self) -> MultiAgentDict:
        """Returns ids from each agent's `SIngleAgentEpisode`."""

        return {
            agent_id: agent_eps.id_
            for agent_id, agent_eps in self.agent_episodes.items()
        }

    # TODO (sven, simon): We are taking over dead agents to the successor
    #  is this intended or should we better check during concatenation, if
    #  the union of agents from both episodes is included? Next issue.
    # def cut(self) -> "MultiAgentEpisode":
    #     """Returns a successor episode chunk (of len=0) continuing from this Episode.

    #     The successor will have the same ID as `self` and starts at the timestep where
    #     it's predecessor `self` left off. The current observations and infos
    #     are carried over from the predecessor as initial observations/infos.

    #     Returns: A MultiAgentEpisode starting at the timestep where its predecessor
    #         stopped.
    #     """
    #     assert not self.is_done

    #     successor = MultiAgentEpisode(
    #         id_=self.id_,
    #         agent_ids=self._agent_ids,
    #         agent_episode_ids={
    #             agent_id: agent_eps.id_
    #             for agent_id, agent_eps in self.agent_episodes.items()
    #         },
    #         terminateds=self.is_terminated,
    #         truncateds=self.is_truncated,
    #         env_t_started=self.env_t,
    #     )

    #     for agent_id, agent_eps in self.agent_episodes.items():
    #         # Call the `SingleAgentEpisode.create_successor` method for
    #         # all agents that are still alive.
    #         if not agent_eps.is_done and agent_eps.observations:
    #             # Build a successor for each agent that is not done, yet.
    #             successor.agent_episodes[agent_id] = agent_eps.cut()
    #             # Record the initial observation in the global timestep mapping.
    #             successor.env_t_to_agent_t[agent_id] = _IndexMapping(
    #                 [self.env_t_to_agent_t[agent_id][-1]]
    #             )
    #         # For agents that are done or have no observation yet, create empty
    #         # instances.
    #         else:
    #             successor.agent_episodes[agent_id] = SingleAgentEpisode(
    #                 id_=agent_eps.id_,
    #                 terminated=agent_eps.is_terminated,
    #                 truncated=agent_eps.is_truncated,
    #             )
    #             successor.env_t_to_agent_t[agent_id] = _IndexMapping()

    #     # Copy the agent buffers to the successor. These remain the same as
    #     # no agent has stepped, yet.
    #     successor._copy_buffer(self)

    #     # Build the global action timestep mapping for buffered actions.
    #     # Note, this mapping tracks orhpane actions in the episode before and
    #     # gives it a timestep `successor.env_t`, i.e. calling `get_actions(indices=0)`
    #     # will return these orphane actions from the predecessor.
    #     # TODO (simon): This might lead to information loss when concatenating.
    #     # as the action was made before when the agent had its last observation.
    #     # This observation might help to avoid the loss.
    #     successor.global_actions_t = self._generate_action_timestep_mappings()

    #     # Add the not-yet recorded partial rewards for each agent.
    #     # TODO (simon): Check, if get_rewards can help here (with indices_in_env_steps=False)
    #     successor = self._add_partial_rewards(successor)

    #     # Return the successor.
    #     return successor

    def get_state(self) -> Dict[str, Any]:
        """Returns the state of a multi-agent episode.

        Note that from an episode's state the episode itself can
        be recreated.

        Returns: A dicitonary containing pickable data fro a
            `MultiAgentEpisode`.
        """
        # TODO (simon): Add the buffers.
        return list(
            {
                "id_": self.id_,
                "agent_ids": self._agent_ids,
                "env_t_to_agent_t": self.env_t_to_agent_t,
                "global_actions_t": self.global_actions_t,
                "partial_rewards_t": self.partial_rewards_t,
                "partial_rewards": self.partial_rewards,
                "agent_episodes": list(
                    {
                        agent_id: agent_eps.get_state()
                        for agent_id, agent_eps in self.agent_episodes.items()
                    }.items()
                ),
                "env_t_started": self.env_t_started,
                "env_t": self.env_t,
                "ts_carriage_return": self.ts_carriage_return,
                "is_terminated": self.is_terminated,
                "is_truncated": self.is_truncated,
            }.items()
        )

    @staticmethod
    def from_state(state) -> None:
        """Creates a multi-agent episode from a state dictionary.

        See `MultiAgentEpisode.get_state()` for creating a state for
        a `MultiAgentEpisode` pickable state. For recreating a
        `MultiAgentEpisode` from a state, this state has to be complete,
        i.e. all data must have been stored in the state.
        """
        # TODO (simon): Add the buffers.
        episode = MultiAgentEpisode(id=state[0][1])
        episode._agent_ids = state[1][1]
        episode.env_t_to_agent_t = state[2][1]
        episode.global_actions_t = state[3][1]
        episode.partial_rewards_t = state[4][1]
        episode.partial_rewards = state[5][1]
        episode.agent_episodes = {
            agent_id: SingleAgentEpisode.from_state(agent_state)
            for agent_id, agent_state in state[6][1]
        }
        episode.env_t_started = state[7][1]
        episode.env_t = state[8][1]
        episode.ts_carriage_return = state[9][1]
        episode.is_terminated = state[10][1]
        episode.is_trcunated = state[11][1]
        return episode

    def get_sample_batch(self) -> MultiAgentBatch:
        """Converts this `MultiAgentEpisode` into a `MultiAgentBatch`.

        Each `SingleAgentEpisode` instances in `MultiAgentEpisode.agent_epiosdes`
        will be converted into a `SampleBatch` and the environment timestep will be
        passed as the returned MultiAgentBatch's `env_steps`.

        Returns:
            A MultiAgentBatch containing all of this episode's data.
        """
        # TODO (simon): Check, if timesteps should be converted into global
        # timesteps instead of agent steps.
        # Note, only agents that have stepped are included into the batch.
        return MultiAgentBatch(
            policy_batches={
                agent_id: agent_eps.get_sample_batch()
                for agent_id, agent_eps in self.agent_episodes.items()
                if agent_eps.t - agent_eps.t_started > 0
            },
            env_steps=self.env_t - self.env_t_started,
        )

    def get_return(self, consider_buffer=False) -> float:
        """Get the all-agent return.

        Args:
            consider_buffer; Boolean. Defines, if we should also consider
                buffered rewards wehn calculating return. Agents might
                have received partial rewards, i.e. rewards without an
                observation. These are stored to the buffer and added up
                until the next observation is received by an agent.
        Returns: A float. The aggregate return from all agents.
        """

        assert (
            sum(len(agent_map) for agent_map in self.env_t_to_agent_t.values()) > 0
        ), (
            "ERROR: Cannot determine return of episode that hasn't started, yet!"
            "Call `MultiAgentEpisode.add_env_reset(observations=)` "
            "first (after which `get_return(MultiAgentEpisode)` will be 0)."
        )
        env_return = sum(
            agent_eps.get_return() for agent_eps in self.agent_episodes.values()
        )
        # If we should consider buffered partial rewards for agents.
        if consider_buffer:
            buffered_rewards = 0
            for agent_buffer in self.agent_buffers.values():
                if agent_buffer["rewards"].full():
                    agent_buffered_rewards = agent_buffer["rewards"].get_nowait()
                    buffered_rewards += agent_buffered_rewards
                    # Write the agent rewards back to the buffer.
                    agent_buffer["rewards"].put_nowait(agent_buffered_rewards)

            return env_return + buffered_rewards
        # Otherwise return the sum of recorded rewards from `SingleAgentEpisode`s.
        else:
            return env_return

    # def _generate_action_timestep_mappings(self):
    #    # Note, here we use the indices that are from the predecessor, i.e.
    #    # these timesteps do not occur in the successor. This will have
    #    # the consequences that `get_actions()` will not list any actions
    #    # that come from the buffers of the predecessor.
    #    return {
    #        agent_id: _IndexMapping([self.global_actions_t[agent_id][-1]])
    #        if agent_buffer["actions"].full()
    #        else _IndexMapping()
    #        for agent_id, agent_buffer in self.agent_buffers.items()
    #    }

    # def _add_partial_rewards(self, successor):
    #    # TODO (simon): This could be made simpler with a reward buffer that
    #    # is longer than 1 and does not add, but append. Then we just collect
    #    # the buffer.
    #    for agent_id, agent_partial_rewards in self.partial_rewards.items():
    #        # If a global timestep mapping exists for the agent use it.
    #        if self.env_t_to_agent_t[agent_id]:
    #            # The successor episode only need the partial rewards that
    #            # have not yet recorded in the `SingleAgentEpisode`.
    #            indices_to_keep = self.partial_rewards_t[agent_id].find_indices_right(
    #                self.env_t_to_agent_t[agent_id][-1],
    #            )
    #        # Otherwise, use the partial rewards timestep mapping.
    #        else:
    #            # The partial rewards timestep mapping does only exist for agents that
    #            # have stepped or have not stepped and received at least a single
    #            # partial reward.
    #            if self.partial_rewards_t[agent_id]:
    #                indices_to_keep = list(range(len(self.partial_rewards_t[agent_id])))
    #            # Otherwise return an empty index list.
    #            else:
    #                indices_to_keep = []

    #        successor.partial_rewards_t[agent_id] = _IndexMapping(
    #            map(self.partial_rewards_t[agent_id].__getitem__, indices_to_keep)
    #        )
    #        successor.partial_rewards[agent_id] = list(
    #            map(agent_partial_rewards.__getitem__, indices_to_keep)
    #        )
    #        # Leave partial results after the last observation in the buffer
    #        # as long as the agent is not temrinated/truncated.
    #        # Note, we still need to consider the `partial_rewards` and
    #        # `partial_rewards_t` for two reasons:
    #        #   1. The agent is terminated in the last step before creating
    #        #       this successor.
    #        #   2. We might want to concatenate the successor to its
    #        #       predecessor and therefore need structs for each agent
    #        #       (dead or alive).
    #        if not self.agent_episodes[agent_id].is_done:
    #            successor.agent_buffers[agent_id]["rewards"].get_nowait()
    #            successor.agent_buffers[agent_id]["rewards"].put_nowait(
    #                sum(successor.partial_rewards[agent_id])
    #            )

    #    return successor

    #def _generate_global_actions_t(self, actions, len_lookback_buffer):
    #    # Only, if we have agent ids we can provide the action timestep mappings.
    #    if self._agent_ids:
    #        # Only if we have actions provided we can build the action timestep
    #        # mappings.
    #        if actions:
    #            agent_ts_maps = defaultdict(
    #                lambda: InfiniteLookbackEnvToAgentTsMapping(
    #                    lookback=len_lookback_buffer, t_started=self.env_t_started
    #                )
    #            )
    #            for ts, action in enumerate(actions):
    #                for agent_id in action:
    #                    if agent_id in action:
    #                        # Note, actions start at timestep 1.
    #                        agent_ts_maps[agent_id].append(
    #                            ts + 1
    #                        )  # + ts_carriage_return
    #            # Return the agents' action timestep mappings.
    #            return agent_ts_maps
    #
    #        else:
    #            return defaultdict(InfiniteLookbackEnvToAgentTsMapping)
    #    else:
    #        return defaultdict(InfiniteLookbackEnvToAgentTsMapping)

    # TODO (sven, simon): This function can only deal with data if it does not contain
    #  terminated or truncated agents (i.e. you have to provide ONLY alive agents in the
    #  agent_ids in the constructor - the episode does not deduce the agents).
    def _init_single_agent_episodes(
        self,
        *,
        agent_episode_ids: Optional[Dict[str, str]] = None,
        observations: Optional[List[MultiAgentDict]] = None,
        actions: Optional[List[MultiAgentDict]] = None,
        rewards: Optional[List[MultiAgentDict]] = None,
        infos: Optional[List[MultiAgentDict]] = None,
        terminateds: Union[MultiAgentDict, bool] = False,
        truncateds: Union[MultiAgentDict, bool] = False,
        extra_model_outputs: Optional[List[MultiAgentDict]] = None,
        len_lookback_buffer: int,
    ) -> Dict[AgentID, SingleAgentEpisode]:

        if observations is None:
            return

        # Infos and extra_model_outputs are allowed to be None -> Fill them with
        # proper dummy values, if so.
        if infos is None:
            infos = [{} for _ in range(len(observations))]
        if extra_model_outputs is None:
            extra_model_outputs = [{} for _ in range(len(observations))]

        observations_per_agent = defaultdict(list)
        infos_per_agent = defaultdict(list)
        actions_per_agent = defaultdict(list)
        rewards_per_agent = defaultdict(list)
        extra_model_outputs_per_agent = defaultdict(list)
        done_per_agent = defaultdict(bool)
        len_lookback_buffer_per_agent = defaultdict(int)
        t_started_per_agent = defaultdict(int)

        all_agent_ids = set()

        # env_t_to_agent_t[agentID]:
        # agent that has received an obs after (MA)env.reset()
        # ag1: [0, None, None, 1, 2, None, 3, None, 4, None]

        # 7) New agents:
        # agent that has NOT received an obs after (MA)env.reset()
        # ag2: [0, 1, None, 2, 3, 4, None, 5, None, None] <- w/ offset n
        # -> means if you do a get(m) on the above buffer, the method internally gets m-n
        # Alternatively, one could prepend the buffer with n Nones.

        # Step through all observations and interpret these as the (global) env steps.
        env_t = self.env_t - len_lookback_buffer
        for data_idx, (obs, inf) in enumerate(zip(observations, infos)):
            if data_idx < len(observations) - 1:
                act, extra_outs, rew = (
                    actions[data_idx],
                    extra_model_outputs[data_idx],
                    rewards[data_idx],
                )
            else:
                act = extra_outs = rew = {}

            for agent_id, agent_obs in obs.items():
                all_agent_ids.add(agent_id)

                observations_per_agent[agent_id].append(agent_obs)
                infos_per_agent[agent_id].append(inf.get(agent_id, {}))

                # Pull out buffered action (if not first obs for this agent) and complete
                # step for agent.
                if len(observations_per_agent[agent_id]) > 1:
                    actions_per_agent[agent_id].append(self._agent_buffered_actions.pop(agent_id))
                    extra_model_outputs_per_agent[agent_id].append(self._agent_buffered_extra_model_outputs.pop(agent_id))
                    rewards_per_agent[agent_id].append(self._agent_buffered_rewards.pop(agent_id))

                # Agent is still continuing (has an action for the next step).
                if agent_id in act:
                    # Always push actions/extra outputs into buffer, then remove them
                    # from there, once the next observation comes in. Same for rewards.
                    self._agent_buffered_actions[agent_id] = act[agent_id]
                    self._agent_buffered_extra_model_outputs[agent_id] = extra_outs.get(agent_id, {})
                    self._agent_buffered_rewards[agent_id] += rew.get(agent_id, 0.0)

                    # Update env_t_to_agent_t mapping.
                    self.env_t_to_agent_t[agent_id].append(
                        len(observations_per_agent[agent_id]) - 1
                    )
                # Agent is done (has no action for the next step).
                elif agent_id in terminateds or agent_id in truncateds:
                    done_per_agent[agent_id] = True
                    # Update env_t_to_agent_t mapping.
                    self.env_t_to_agent_t[agent_id].append(
                        len(observations_per_agent[agent_id]) - 1
                    )
                # This is the last obs (no further action/reward data).
                else:
                    assert data_idx == len(observations) - 1

            # Those agents that did NOT step get None added to their mapping.
            for agent_id in (all_agent_ids - set(obs.keys())):
                if agent_id not in done_per_agent:
                    self.env_t_to_agent_t[agent_id].append(self.SKIP_ENV_TS_TAG)

            # Update per-agent lookback buffer and t_started counters.
            for agent_id in all_agent_ids:
                if env_t < len_lookback_buffer:
                    if agent_id not in done_per_agent:
                        len_lookback_buffer_per_agent[agent_id] += 1
                elif agent_id not in t_started_per_agent:
                    # Search last non-skip entry and add one to it.
                    for i in range(1, len(self.env_t_to_agent_t[agent_id])):
                        if self.env_t_to_agent_t[agent_id][-i] != self.SKIP_ENV_TS_TAG:
                            t_started_per_agent[agent_id] = self.env_t_to_agent_t[agent_id][-i]
                            break

            # Increase env timestep by one.
            env_t += 1

        # Validate per-agent data.
        # Fix lookback buffers of env_t_to_agent_t mappings.
        for agent_id, buf in self.env_t_to_agent_t.items():
            assert (
                len(observations_per_agent[agent_id])
                == len(infos_per_agent[agent_id])
                == len(actions_per_agent[agent_id]) + 1
                == len(extra_model_outputs_per_agent[agent_id]) + 1
                == len(rewards_per_agent[agent_id]) + 1
            )
            buf.lookback = len_lookback_buffer_per_agent[agent_id]

        # Now create the individual episodes from the collected per-agent data.
        for agent_id, agent_obs in observations_per_agent.items():
            sa_episode = SingleAgentEpisode(
                id_=(
                    agent_episode_ids.get(agent_id)
                    if agent_episode_ids is not None else None
                ),
                observations=agent_obs,
                observation_space=self.observation_space.get(agent_id),
                infos=infos_per_agent[agent_id],
                actions=actions_per_agent[agent_id],
                action_space=self.action_space.get(agent_id),
                rewards=rewards_per_agent[agent_id],
                extra_model_outputs=tree.map_structure(
                    lambda *s: list(s), *extra_model_outputs_per_agent[agent_id]
                ),
                terminated=terminateds.get(agent_id, False),
                truncated=truncateds.get(agent_id, False),
                t_started=t_started_per_agent[agent_id],
                len_lookback_buffer=len_lookback_buffer_per_agent[agent_id],
            )
            self.agent_episodes[agent_id] = sa_episode

    def _erase_buffered_data(self, agent_id):
        self._agent_buffered_actions.pop(agent_id, None)
        self._agent_buffered_extra_model_outputs.pop(agent_id, None)
        self._agent_buffered_rewards.pop(agent_id, None)

    def _getattr_by_index(
        self,
        attr: str = "observations",
        indices: Union[int, List[int]] = -1,
        key: Optional[str] = None,
        has_initial_value=False,
        indices_in_env_steps: bool = True,
        indices_in_env_steps_mapping: Optional[MultiAgentDict] = None,
        shift: int = 0,
        as_list: bool = False,
        buffered_values: MultiAgentDict = None,
    ) -> MultiAgentDict:
        """Returns values in the form of indices: [-1, -2]."""
        # TODO (simon): Does not give yet indices that are in between.
        # TODO (sven): Do we even need indices in between? This is very
        # tricky.

        if not indices_in_env_steps_mapping:
            indices_in_env_steps_mapping = self.env_t_to_agent_t

        # First for indices_in_env_steps = True:
        if indices_in_env_steps:
            # Check, if the indices are iterable.
            if isinstance(indices, list):
                indices = [
                    (self.env_t + idx + int(has_initial_value))
                    if idx < 0
                    else idx + self.ts_carriage_return
                    for idx in indices
                ]
            # If not make them iterable.
            else:
                indices = (
                    [
                        self.env_t
                        # - self.ts_carriage_return
                        + indices
                        + int(has_initial_value)
                    ]
                    if indices < 0
                    # else [indices + 1]
                    else [indices + self.ts_carriage_return]
                )

            # If a list should be returned.
            if as_list:
                if buffered_values:
                    # Note, for addition we have to ensure that both elements are lists
                    # and terminated/truncated agents have numpy arrays.
                    return [
                        {
                            agent_id: (
                                getattr(agent_eps, attr).get(key)
                                + buffered_values[agent_id]
                            )[indices_in_env_steps_mapping[agent_id].find_indices([idx])[0]]
                            for agent_id, agent_eps in self.agent_episodes.items()
                            if indices_in_env_steps_mapping[agent_id].find_indices([idx], shift)
                        }
                        for idx in indices
                    ]
                else:
                    return [
                        {
                            agent_id: (getattr(agent_eps, attr))[
                                indices_in_env_steps_mapping[agent_id].find_indices([idx], shift)[
                                    0
                                ]
                            ]
                            for agent_id, agent_eps in self.agent_episodes.items()
                            if indices_in_env_steps_mapping[agent_id].find_indices([idx], shift)
                        }
                        for idx in indices
                    ]
            # Otherwise we return a dictionary.
            else:
                if buffered_values:
                    # Note, for addition we have to ensure that both elements are lists
                    # and terminated/truncated agents have numpy arrays.
                    return {
                        agent_id: list(
                            map(
                                (
                                    getattr(agent_eps, attr).get(key)
                                    + buffered_values[agent_id]
                                ).__getitem__,
                                indices_in_env_steps_mapping[agent_id].find_indices(
                                    indices, shift
                                ),
                            )
                        )
                        for agent_id, agent_eps in self.agent_episodes.items()
                        # Only include agent data for agents that stepped.
                        if indices_in_env_steps_mapping[agent_id].find_indices(indices, shift)
                    }
                else:
                    return {
                        agent_id: list(
                            map(
                                getattr(agent_eps, attr).__getitem__,
                                indices_in_env_steps_mapping[agent_id].find_indices(
                                    indices, shift
                                ),
                            )
                        )
                        for agent_id, agent_eps in self.agent_episodes.items()
                        # Only include agent data for agents that stepped.
                        if indices_in_env_steps_mapping[agent_id].find_indices(indices, shift)
                    }
        # Otherwise just look for the timesteps in the `SingleAgentEpisode`s
        # directly.
        else:
            # Check, if the indices are iterable.
            if not isinstance(indices, list):
                indices = [indices]

            # If we have buffered values for the attribute we want to concatenate
            # while searching for the indices.
            if buffered_values:
                return {
                    agent_id: list(
                        map(
                            (
                                getattr(agent_eps, attr).get(key)
                                + buffered_values[agent_id]
                            ).__getitem__,
                            set(indices).intersection(
                                set(
                                    range(
                                        -len(indices_in_env_steps_mapping[agent_id]),
                                        len(indices_in_env_steps_mapping[agent_id]),
                                    )
                                )
                            ),
                        )
                    )
                    for agent_id, agent_eps in self.agent_episodes.items()
                    if set(indices).intersection(
                        set(
                            range(
                                -len(indices_in_env_steps_mapping[agent_id]),
                                len(indices_in_env_steps_mapping[agent_id]),
                            )
                        )
                    )
                }
            else:
                return {
                    agent_id: list(
                        map(
                            getattr(agent_eps, attr).__getitem__,
                            set(indices).intersection(
                                set(
                                    range(
                                        -len(indices_in_env_steps_mapping[agent_id]),
                                        len(indices_in_env_steps_mapping[agent_id]),
                                    )
                                )
                            ),
                        )
                    )
                    for agent_id, agent_eps in self.agent_episodes.items()
                    if set(indices).intersection(
                        set(
                            range(
                                -len(indices_in_env_steps_mapping[agent_id]),
                                len(indices_in_env_steps_mapping[agent_id]),
                            )
                        )
                    )
                }

    def _get_single_agent_data(
        self,
        agent_id: str,
        ma_data: List[MultiAgentDict],
        use_env_t_to_agent_t: bool = True,
        start_index: int = 0,
        end_index: Optional[int] = None,
        shift: int = 0,
    ) -> List[Any]:
        """Returns single agent data from multi-agent data.

        Args:
            agent_id: A string identifying the agent for which the
                data should be extracted.
            ma_data: A List of dictionaries, each containing multi-agent
                data, i.e. mapping from agent ids to timestep data.
            start_index: An integer defining the start point of the
                extration window. The default starts at the beginning of the
                the `ma_data` list.
            end_index: Optional. An integer defining the end point of the
                extraction window. If `None`, the extraction window will be
                until the end of the `ma_data` list.g
            shift: An integer that defines by which amount to shift the
                running index for extraction. This is for example needed
                when we extract data that started at index 1.

        Returns: A list containing single-agent data from the multi-agent
            data provided.
        """

        # Should we search along the global timestep mapping, e.g. for observations,
        # or infos.
        #if use_env_t_to_agent_t:
        #    # Return all single agent data along the global timestep.
        #    return [
        #        singleton[agent_id]
        #        for singleton in list(
        #            map(
        #                ma_data.__getitem__,
        #                [
        #                    i + shift
        #                    for i in self.env_t_to_agent_t[agent_id][
        #                        start_index:end_index
        #                    ]
        #                ],
        #            )
        #        )
        #        if agent_id in singleton.keys()
        #    ]
        # Use all. This makes sense in multi-agent games where rewards could be given,
        # even to agents that receive no observation in a timestep.
        #else:
        #    return [
        #        singleton[agent_id] for singleton in ma_data if agent_id in singleton
        #    ]

    def _copy_buffer(self, episode: "MultiAgentEpisode") -> None:
        """Writes values from one buffer to another."""

        for agent_id, agent_buffer in episode.agent_buffers.items():
            for buffer_name, buffer in agent_buffer.items():
                # If the buffer is full write over the values.
                if buffer.full():
                    # Get the item from them buffer.
                    item = buffer.get_nowait()
                    # Flush the destination buffer, if it is full.
                    if self.agent_buffers[agent_id][buffer_name].full():
                        self.agent_buffers[agent_id][buffer_name].get_nowait()
                    # Write it to the destination buffer.
                    self.agent_buffers[agent_id][buffer_name].put_nowait(item)
                    # The source buffer might still need the item.
                    buffer.put_nowait(item)
                # If the buffer is empty, empty the destination buffer.
                else:
                    if self.agent_buffers[agent_id][buffer_name].full():
                        self.agent_buffers[agent_id][buffer_name].get_nowait()

    def __len__(self):
        """Returns the length of an `MultiAgentEpisode`.

        Note that the length of an episode is defined by the difference
        between its actual timestep and the starting point.

        Returns: An integer defining the length of the episode or an
            error if the episode has not yet started.
        """
        assert (
            sum(len(agent_map) for agent_map in self.env_t_to_agent_t.values()) > 0
        ), (
            "ERROR: Cannot determine length of episode that hasn't started, yet!"
            "Call `MultiAgentEpisode.add_env_reset(observations=)` "
            "first (after which `len(MultiAgentEpisode)` will be 0)."
        )
        return self.env_t - self.env_t_started


class _IndexMapping(list):
    """Provides lists with a method to find multiple elements.

    This class is used for the timestep mapping which is central to
    the multi-agent episode. For each agent the timestep mapping is
    implemented with an `IndexMapping`.

    The `IndexMapping.find_indices` method simplifies the search for
    multiple environment timesteps at which some agents have stepped.
    See for example `MultiAgentEpisode.get_observations()`.
    """

    def find_indices(self, indices_to_find: List[int], shift: int = 0):
        """Returns global timesteps at which an agent stepped.

        The function returns for a given list of indices the ones
        that are stored in the `IndexMapping`.

        Args:
            indices_to_find: A list of indices that should be
                found in the `IndexMapping`.

        Returns:
            A list of indices at which to find the `indices_to_find`
            in `self`. This could be empty if none of the given
            indices are in `IndexMapping`.
        """
        indices = []
        for num in indices_to_find:
            # To avoid duplicate indices in rare cases we have to test
            # for positive values. Note, `shift` could be negative.
            if num in self and self.index(num) + shift >= 0:
                indices.append(self.index(num) + shift)
        return indices

    def find_indices_right(self, threshold: int, shift: bool = 0):
        indices = []
        for num in reversed(self):
            # To avoid duplicate indices in rare cases we have to test
            # for positive values. Note, `shift` could be negative.
            if num <= threshold:
                # `_IndexMapping` is always ordered. Avoid to run through
                # all timesteps (could be thousands).
                break
            # Avoid negative indices, but as list is reversed: continue.
            elif self.index(num) + shift < 0:
                continue
            else:
                indices.append(max(self.index(num) + shift, 0))
        return list(reversed(indices))

    def find_indices_right_equal(self, threshold: int, shift: bool = 0):
        indices = []
        for num in reversed(self):
            # To avoid duplicate indices in rare cases we have to test
            # for positive values. Note, `shift` could be negative.
            if num < threshold:
                # `_IndexMapping` is always ordered. Avoid to run through
                # all timesteps (could be thousands).
                break
            # Avoid negative indices, but as list is reversed: continue.
            elif self.index(num) + shift < 0:
                continue
            else:
                indices.append(max(self.index(num) + shift, 0))
        return list(reversed(indices))

    def find_indices_left_equal(self, threshold: int, shift: bool = 0):
        indices = []
        for num in self:
            if num > threshold or (self.index(num) + shift) < 0:
                # `_IndexMapping` is always ordered. Avoid to run through
                # all timesteps (could be thousands).
                break
            else:
                indices.append(self.index(num))
        return indices

    def find_indices_between_right_equal(
        self, threshold_left: int, threshold_right: int, shift: int = 0
    ):
        indices = []
        for num in self:
            if num > threshold_right:
                break
            elif num <= threshold_left or self.index(num) + shift < 0:
                continue
            else:
                indices.append(self.index(num) + shift)
        return indices
