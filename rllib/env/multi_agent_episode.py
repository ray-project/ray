from collections import defaultdict
from queue import Queue
from typing import Any, Dict, List, Optional, Set, Union
import uuid

import numpy as np

from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.typing import MultiAgentDict


# TODO (simon): Include cases in which the number of agents in an
# episode are shrinking or growing during the episode itself.
class MultiAgentEpisode:
    """Stores multi-agent episode data.

    The central attribute of the class is the timestep mapping
    `global_t_to_local_t` that maps the global (environment)
    timestep to the local (agent) timesteps.

    The `MultiAgentEpisode` is based on the `SingleAgentEpisode`s
    for each agent, stored in `MultiAgentEpisode.agent_episodes`.
    """

    def __init__(
        self,
        id_: Optional[str] = None,
        agent_ids: List[str] = None,
        agent_episode_ids: Optional[Dict[str, str]] = None,
        *,
        observations: Optional[List[MultiAgentDict]] = None,
        infos: Optional[List[MultiAgentDict]] = None,
        actions: Optional[List[MultiAgentDict]] = None,
        rewards: Optional[List[MultiAgentDict]] = None,
        terminateds: Union[MultiAgentDict, bool] = False,
        truncateds: Union[MultiAgentDict, bool] = False,
        render_images: Optional[List[np.ndarray]] = None,
        extra_model_outputs: Optional[List[MultiAgentDict]] = None,
        t_started: Optional[int] = None,
    ) -> "MultiAgentEpisode":
        """Initializes a `MultiAgentEpisode`.

        Args:
            id_: Optional. Either a string to identify an episode or None.
                If None, a hexadecimal id is created. In case of providing
                a string, make sure that it is unique, as episodes get
                concatenated via this string.
            agent_ids: A list of strings containing the agent ids.
                These have to be provided at initialization.
            agent_episode_ids: Either a dictionary mapping agent ids
                corresponding `SingleAgentEpisode` or None. If None, each
                `SingleAgentEpisode` in `MultiAgentEpisode.agent_episodes`
                will generate a hexadecimal code. If a dictionary is provided
                make sure that ids are unique as agents'  `SingleAgentEpisode`s
                get concatenated or recreated by it.
            observations: A list of dictionaries mapping agent IDs to observations.
                Can be None. If provided, should match all other episode data
                (actions, rewards, etc.) in terms of list lengths and agent IDs.
            infos: A list of dictionaries mapping agent IDs to info dicts.
                Can be None. If provided, should match all other episode data
                (observations, rewards, etc.) in terms of list lengths and agent IDs.
            actions: A list of dictionaries mapping agent IDs to actions.
                Can be None. If provided, should match all other episode data
                (observations, rewards, etc.) in terms of list lengths and agent IDs.
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
            t_started: The timestep (int) that defines the starting point
                of the episode. This is only larger zero, if an ongoing episode is
                created, for example by slicing an ongoing episode or by calling
                the `cut()` method on an ongoing episode.
        """

        self.id_: str = id_ or uuid.uuid4().hex

        # Agent ids must be provided if data is provided. The Episode cannot
        # know how many agents are in the environment. Also the number of agents
        # can grwo or shrink.
        self._agent_ids: Union[Set[str], Set[object]] = (
            set() if agent_ids is None else set(agent_ids)
        )

        # The global last timestep of the episode and the timesteps when this chunk
        # started.
        self.t = self.t_started = (
            t_started
            if t_started is not None
            else (len(observations) - 1 if observations is not None else 0)
        )

        self.ts_carriage_return = self.t_started - (
            len(observations) - 1 if observations else 0
        )
        # Keeps track of the correspondence between agent steps and environment steps.
        # This is a mapping from agents to `_IndexMapping`. The latter keeps
        # track of the global timesteps at which an agent stepped.
        # Note, global (env) timesteps are values, while local (agent) steps are the
        # indices at which these global steps are recorded.
        self.global_t_to_local_t: Dict[str, List[int]] = self._generate_ts_mapping(
            observations
        )

        # In the `MultiAgentEpisode` we need these buffers to keep track of actions,
        # that happen when an agent got observations and acted, but did not receive
        # a next observation, yet. In this case we buffer the action, add the rewards,
        # and record `is_terminated/is_truncated` until the next observation is
        # received.
        self.agent_buffers = {
            agent_id: {
                "actions": Queue(maxsize=1),
                "rewards": Queue(maxsize=1),
                "extra_model_outputs": Queue(maxsize=1),
            }
            for agent_id in self._agent_ids
        }
        # Initialize buffers.
        for buffer in self.agent_buffers.values():
            # Default reward for accumulation is zero.
            buffer["rewards"].put_nowait(0.0)
            # Default extra_mdoel_output is None.
            buffer["extra_model_outputs"].put_nowait(None)

        # This is needed to reconstruct global action structures for
        # orphane actions, i.e. actions that miss a 'next observation' at
        # their occurrence.
        self.global_actions_t = self._generate_global_actions_t(actions)
        # These are needed to reconstruct (see `self.get_rewards()`) reward
        # structures if agents received rewards without observations. This
        # is specific to multi-agent environemnts.
        self.partial_rewards = {agent_id: [] for agent_id in self._agent_ids}
        self.partial_rewards_t = {
            agent_id: _IndexMapping() for agent_id in self._agent_ids
        }

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

        # Note that all attributes will be recorded along the global timestep
        # in an multi-agent environment. `SingleAgentEpisodes`
        self.agent_episodes: MultiAgentDict = {
            agent_id: self._generate_single_agent_episode(
                agent_id,
                agent_episode_ids,
                observations,
                actions,
                rewards,
                infos,
                terminateds,
                truncateds,
                extra_model_outputs,
            )
            for agent_id in self._agent_ids
        }

        # RGB uint8 images from rendering the env; the images include the corresponding
        # rewards.
        assert render_images is None or observations is not None
        self.render_images: Union[List[np.ndarray], List[object]] = (
            [] if render_images is None else render_images
        )

    def concat_episode(self, episode_chunk: "MultiAgentEpisode") -> None:
        """Adds the given `episode_chunk` to the right side of self.

        For concatenating episodes the following rules hold:
            - IDs are identical.
            - timesteps match (`t` of `self` matches `t_started` of `episode_chunk`).

        Args:
            episode_chunk: `MultiAgentEpsiode` instance that should be concatenated
                to `self`.
        """
        assert episode_chunk.id_ == self.id_
        assert not self.is_done
        # Make sure the timesteps match.
        assert self.t == episode_chunk.t_started

        # TODO (simon): Write `validate()` method.

        # Make sure, end matches `episode_chunk`'s beginning for all agents.
        # Note, we have to assert for the last local observations as for
        # each agent alive, we need in the successor an initial observation.
        observations: MultiAgentDict = {
            agent_id: agent_obs
            for agent_id, agent_obs in self.get_observations(global_ts=False).items()
            if not self.agent_episodes[agent_id].is_done
        }
        for agent_id, agent_obs in episode_chunk.get_observations(indices=0).items():
            # Make sure that the same agents stepped at both timesteps.
            assert agent_id in observations
            assert observations[agent_id] == agent_obs

        # Call the `SingleAgentEpisode`'s `concat_episode()` method for all agents.
        for agent_id, agent_eps in self.agent_episodes.items():
            if not agent_eps.is_done:
                agent_eps.concat_episode(episode_chunk.agent_episodes[agent_id])
                # Update our timestep mapping.
                # As with observations we need the global timestep mappings to overlap.
                assert (
                    self.global_t_to_local_t[agent_id][-1]
                    == episode_chunk.global_t_to_local_t[agent_id][0]
                )
                self.global_t_to_local_t[agent_id] += episode_chunk.global_t_to_local_t[
                    agent_id
                ][1:]
                # TODO (simon): Check, if this works always.
                # We have to take care of cases where a successor took over full action
                # buffers, b/c it then also took over the last timestep of the global
                # action timestep mapping.
                if (
                    self.global_actions_t[agent_id][-1]
                    == episode_chunk.global_actions_t[agent_id][0]
                ):
                    self.global_actions_t[agent_id] += episode_chunk.global_actions_t[
                        agent_id
                    ][1:]
                # If the action buffer was empty before the successor was created, we
                # can concatenate all values.
                else:
                    self.global_actions_t[agent_id] += episode_chunk.global_actions_t[
                        agent_id
                    ]

                indices_for_partial_rewards = episode_chunk.partial_rewards_t[
                    agent_id
                ].find_indices_right(self.t)
                # We use the `map()` here with `__getitem__` for the case of empty
                # lists.
                self.partial_rewards_t[agent_id] += list(
                    map(
                        episode_chunk.partial_rewards_t[agent_id].__getitem__,
                        indices_for_partial_rewards,
                    )
                )
                self.partial_rewards[agent_id] += list(
                    map(
                        episode_chunk.partial_rewards[agent_id].__getitem__,
                        indices_for_partial_rewards,
                    )
                )

        # Copy the agent buffers over.
        self._copy_buffer(episode_chunk)

        self.t = episode_chunk.t
        if episode_chunk.is_terminated:
            self.is_terminated = True
        if episode_chunk.is_truncated:
            self.is_truncated = True

        # Validate
        # TODO (simon): Write validate function.
        # self.validate()

    # TODO (simon): Maybe adding agent axis. We might need only some agent observations.
    # Then also add possibility to get all obs (or None)
    # Write many test cases (numbered obs).
    def get_observations(
        self,
        indices: Union[int, List[int]] = -1,
        global_ts: bool = True,
        as_list: bool = False,
    ) -> Union[MultiAgentDict, List[MultiAgentDict]]:
        """Gets observations for all agents that stepped in the last timesteps.

        Note that observations are only returned for agents that stepped
        during the given index range.

        Args:
            indices: Either a single index or a list of indices. The indices
                can be reversed (e.g. [-1, -2]) or absolute (e.g. [98, 99]).
                This defines the time indices for which the observations
                should be returned.
            global_ts: Boolean that defines, if the indices should be considered
                environment (`True`) or agent (`False`) steps.

        Returns: A dictionary mapping agent ids to observations (of different
            timesteps). Only for agents that have stepped (were ready) at a
            timestep, observations are returned (i.e. not all agent ids are
            necessarily in the keys).
        """
        return self._getattr_by_index(
            "observations",
            indices,
            has_initial_value=True,
            global_ts=global_ts,
            as_list=as_list,
        )

    # TODO (simon): Make sure that users always give in sorted lists.
    # Because of the way we check the indices we cannot guarantee the order of
    # indices, specifically, if we want to insert buffered actions.
    # TODO (simon): Users might want to receive only actions that have a
    # corresponding 'next observation' (i.e. no buffered actions). Take care of this.
    # Also in the `extra_model_outputs`.

    def get_actions(
        self,
        indices: Union[int, List[int]] = -1,
        global_ts: bool = True,
        as_list: bool = False,
    ) -> Union[MultiAgentDict, List[MultiAgentDict]]:
        """Gets actions for all agents that stepped in the last timesteps.

        Note that actions are only returned for agents that stepped
        during the given index range.

        Args:
            indices: Either a single index or a list of indices. The indices
                can be reversed (e.g. [-1, -2]) or absolute (e.g. [98, 99]).
                This defines the time indices for which the actions
                should be returned.
            global_ts: Boolean that defines, if the indices should be considered
                environment (`True`) or agent (`False`) steps.

        Returns: A dictionary mapping agent ids to actions (of different
            timesteps). Only for agents that have stepped (were ready) at a
            timestep, actions are returned (i.e. not all agent ids are
            necessarily in the keys).
        """
        buffered_actions = {}

        if global_ts:
            # Check, if the indices are iterable.
            if isinstance(indices, list):
                indices = [
                    (self.t + idx + 1 if idx < 0 else idx + self.ts_carriage_return)
                    for idx in indices
                ]
            # If not make them iterable.
            else:
                indices = (
                    [self.t + indices + 1]
                    if indices < 0
                    else [indices + self.ts_carriage_return]
                )
        else:
            if not isinstance(indices, list):
                indices = [indices]
        # Check now, if one of these indices is the last in the global
        # action timestep mapping.
        for agent_id, agent_global_action_t in self.global_actions_t.items():
            if agent_global_action_t:
                last_action_index = (
                    agent_global_action_t[-1]
                    if global_ts
                    else len(agent_global_action_t) - 1
                )
            # We consider only timesteps that are in the requested indices and
            # check then, if for these the buffer is full.
            if (
                agent_global_action_t
                and (last_action_index in indices or -1 in indices)
                and self.agent_buffers[agent_id]["actions"].full()
            ):
                # Then the buffer must be full and needs to be accessed.
                # Note, we do not want to empty the buffer, but only read it.
                buffered_actions[agent_id] = [
                    self.agent_buffers[agent_id]["actions"].queue[0]
                ]
            else:
                buffered_actions[agent_id] = []

        # Now, get the actions.
        actions = self._getattr_by_index(
            "actions",
            indices=indices,
            has_initial_value=True,
            global_ts=global_ts,
            global_ts_mapping=self.global_actions_t,
            # shift=1,
            as_list=as_list,
            buffered_values=buffered_actions,
        )

        return actions

    def get_extra_model_outputs(
        self,
        indices: Union[int, List[int]] = -1,
        global_ts: bool = True,
        as_list: bool = False,
    ) -> Union[MultiAgentDict, List[MultiAgentDict]]:
        """Gets actions for all agents that stepped in the last timesteps.

        Note that actions are only returned for agents that stepped
        during the given index range.

        Args:
            indices: Either a single index or a list of indices. The indices
                can be reversed (e.g. [-1, -2]) or absolute (e.g. [98, 99]).
                This defines the time indices for which the actions
                should be returned.
            global_ts: Boolean that defines, if the indices should be considered
                environment (`True`) or agent (`False`) steps.

        Returns: A dictionary mapping agent ids to actions (of different
            timesteps). Only for agents that have stepped (were ready) at a
            timestep, actions are returned (i.e. not all agent ids are
            necessarily in the keys).
        """
        buffered_outputs = {}

        if global_ts:
            # Check, if the indices are iterable.
            if isinstance(indices, list):
                indices = [(self.t + idx + 1 if idx < 0 else idx) for idx in indices]
            # If not make them iterable.
            else:
                indices = [self.t + indices + 1] if indices < 0 else [indices]
        else:
            if not isinstance(indices, list):
                indices = [indices]
        # Check now, if one of these indices is the last in the global
        # action timestep mapping.
        for agent_id, agent_global_action_t in self.global_actions_t.items():
            if agent_global_action_t:
                last_action_index = (
                    agent_global_action_t[-1]
                    if global_ts
                    else len(agent_global_action_t) - 1
                )
            # We consider only timesteps that are in the requested indices and
            # check then, if for these the buffer is full. Note, we cannot use
            # the extra model outputs buffer as this is by default `None`.
            if (
                agent_global_action_t
                and (last_action_index in indices or -1 in indices)
                and self.agent_buffers[agent_id]["actions"].full()
            ):
                # Then the buffer must be full and needs to be accessed.
                # Note, we do not want to empty the buffer, but only read it.
                buffered_outputs[agent_id] = [
                    self.agent_buffers[agent_id]["extra_model_outputs"].queue[0]
                ]
            else:
                buffered_outputs[agent_id] = []

        # Now, get the actions.
        extra_model_outputs = self._getattr_by_index(
            "extra_model_outputs",
            indices=indices,
            has_initial_value=True,
            global_ts=global_ts,
            global_ts_mapping=self.global_actions_t,
            # shift=1,
            as_list=as_list,
            buffered_values=buffered_outputs,
        )

        return extra_model_outputs

    def get_rewards(
        self,
        indices: Union[int, List[int]] = -1,
        global_ts: bool = True,
        as_list: bool = False,
        partial: bool = True,
        consider_buffer: bool = True,
    ) -> Union[MultiAgentDict, List[MultiAgentDict]]:
        """Gets rewards for all agents that stepped in the last timesteps.

        Note that rewards are only returned for agents that stepped
        during the given index range.

        Args:
            indices: Either a single index or a list of indices. The indices
                can be reversed (e.g. [-1, -2]) or absolute (e.g. [98, 99]).
                This defines the time indices for which the rewards
                should be returned.
            global_ts: Boolean that defines, if the indices should be considered
                environment (`True`) or agent (`False`) steps.

        Returns: A dictionary mapping agent ids to rewards (of different
            timesteps). Only for agents that have stepped (were ready) at a
            timestep, rewards are returned (i.e. not all agent ids are
            necessarily in the keys).
        """

        if global_ts:
            # Check, if the indices are iterable.
            if isinstance(indices, list):
                indices = [
                    (self.t - self.ts_carriage_return + idx + 1 if idx < 0 else idx)
                    for idx in indices
                ]
            # If not make them iterable.
            else:
                indices = (
                    [self.t - self.ts_carriage_return + indices + 1]
                    if indices < 0
                    else [indices]
                )
        else:
            if not isinstance(indices, list):
                indices = [indices]

        if not partial and consider_buffer:
            buffered_rewards = {}
            timestep_mapping = {}

            for agent_id, agent_global_t_to_local_t in self.global_t_to_local_t.items():
                # If the agent had an initial observation.
                if agent_global_t_to_local_t:
                    # If the agent received rewards after the last observation.
                    if (
                        self.partial_rewards_t[agent_id]
                        and self.partial_rewards_t[agent_id][-1]
                        > agent_global_t_to_local_t[-1]
                        and self.partial_rewards_t[agent_id][-1] <= max(indices)
                    ):
                        indices_at_or_after_last_obs = [
                            agent_global_t_to_local_t[-1]
                        ] + sorted(
                            [
                                idx
                                for idx in indices
                                if idx > agent_global_t_to_local_t[-1]
                            ]
                        )
                        (
                            buffered_rewards[agent_id],
                            indices_wih_rewards,
                        ) = self.accumulate_partial_rewards(
                            agent_id,
                            indices_at_or_after_last_obs,
                            return_indices=True,
                        )
                        # Note, the global timestep mapping begins at zero
                        # with the initial observation. Rewards start at
                        # timestep 1.
                        timestep_mapping[agent_id] = _IndexMapping(
                            agent_global_t_to_local_t[1:]
                            + list(
                                range(
                                    agent_global_t_to_local_t[-1] + 1,
                                    agent_global_t_to_local_t[-1]
                                    + 1
                                    + len(indices_wih_rewards),
                                )
                            )
                        )
                    # There are no partial rewards in the range of requested indices.
                    else:
                        buffered_rewards[agent_id] = []
                        # Note, we need here at least the timestep mapping for the
                        # recorded timesteps as they have recorded any rewards before.
                        # TODO (simon): Allow slicing for _IndexMapping. ALso rename to
                        # TimestepMapping.
                        # TODO (simon): Check, if we can simply use the `
                        # has_initial_value`instead of slicing.
                        timestep_mapping[agent_id] = _IndexMapping(
                            agent_global_t_to_local_t[1:]
                        )

                # If the agent had no initial observation, yet.
                else:
                    # Has the agent received partial rewards, yet and if yes, has
                    # any one received before the last requested index.
                    if self.partial_rewards_t[agent_id] and self.partial_rewards_t[
                        agent_id
                    ][0] < max(indices):
                        # Then assign the partial rewards to the requested indices.
                        # Note, the function accumulates from the second index on, so
                        # we add a zero.
                        # TODO (simon): Check, if we need here maybe a sifting for
                        # the first index in partial_rewards_t.
                        (
                            buffered_rewards[agent_id],
                            indices_with_rewards,
                        ) = self.accumulate_partial_rewards(
                            agent_id,
                            [0] + sorted(indices),
                            return_indices=True,
                        )
                        # TODO (simon): This should be all indices at or below which
                        # rewards existed.
                        timestep_mapping[agent_id] = _IndexMapping(
                            range(1, len(indices_with_rewards) + 1)
                        )
                        # timestep_mapping[agent_id] = _IndexMapping(
                        #     [
                        #         idx
                        #         for idx in sorted(indices)
                        #         if self.partial_rewards_t[
                        #             "agent_1"
                        #         ].find_indices_left_equal(idx)
                        #     ]
                        # )
                    # Else, no partial rewards have to be assigned.
                    else:
                        buffered_rewards[agent_id] = []
                        timestep_mapping[agent_id] = _IndexMapping()

        # Now request the rewards.
        if partial:
            # Here we simply apply the logic from `_getattr_by_index`, however
            # use simply the the `partial_rewards` dict. Check for indices
            # correspondingly.
            if as_list:
                return [
                    {
                        agent_id: self.partial_rewards[agent_id][
                            self.partial_rewards_t[agent_id].find_indices(
                                [idx], shift=0
                            )[0]
                        ]
                        for agent_id, agent_eps in self.agent_episodes.items()
                        if self.partial_rewards_t[agent_id].find_indices([idx], shift=0)
                    }
                    for idx in indices
                ]
            else:
                return {
                    agent_id: list(
                        map(
                            self.partial_rewards[agent_id].__getitem__,
                            self.partial_rewards_t[agent_id].find_indices(
                                indices, shift=0
                            ),
                        )
                    )
                    for agent_id in self._agent_ids
                    if self.partial_rewards_t[agent_id].find_indices(indices, shift=0)
                }

        else:
            if consider_buffer:
                # Note, we do not consider initial values here as the indices are
                # already positive.
                return self._getattr_by_index(
                    "rewards",
                    indices,
                    global_ts=global_ts,
                    global_ts_mapping=timestep_mapping,
                    buffered_values=buffered_rewards,
                    as_list=as_list,
                )
            else:
                # Note, if we use the global timestep mapping (of observations), the
                # mapping starts at 0 (because observations do), therefore we have to
                # set `has_initial_value` to `True`.
                return self._getattr_by_index(
                    "rewards",
                    indices,
                    has_initial_value=True,
                    global_ts=global_ts,
                    as_list=as_list,
                    shift=-1,
                )

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

    def get_infos(
        self,
        indices: Union[int, List[int]] = -1,
        global_ts: bool = True,
        as_list: bool = False,
    ) -> Union[MultiAgentDict, List[MultiAgentDict]]:
        """Gets infos for all agents that stepped in the last timesteps.

        Note that infos are only returned for agents that stepped
        during the given index range.

        Args:
            indices: Either a single index or a list of indices. The indices
                can be reversed (e.g. [-1, -2]) or absolute (e.g. [98, 99]).
                This defines the time indices for which the infos
                should be returned.
            global_ts: Boolean that defines, if the indices should be considered
                environment (`True`) or agent (`False`) steps.

        Returns: A dictionary mapping agent ids to infos (of different
            timesteps). Only for agents that have stepped (were ready) at a
            timestep, infos are returned (i.e. not all agent ids are
            necessarily in the keys).
        """
        return self._getattr_by_index(
            "infos",
            indices,
            has_initial_value=True,
            global_ts=global_ts,
            as_list=as_list,
        )

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
            observations: A dictionary mapping agent ids to initial observations.
                Note that some agents may not have an initial observation.
            infos: A dictionary mapping agent ids to initial info dicts.
                Note that some agents may not have an initial info dict.
            render_image: An RGB uint8 image from rendering the environment.
        """
        assert not self.is_done
        # Assume that this episode is completely empty and has not stepped yet.
        # Leave self.t (and self.t_started) at 0.
        assert self.t == self.t_started == 0
        infos = infos or {}

        # TODO (simon): After clearing with sven for initialization of timesteps
        # this might be removed.
        if len(self.global_t_to_local_t) == 0:
            self.global_t_to_local_t = {
                agent_id: _IndexMapping() for agent_id in self._agent_ids
            }

        # Note that we store the render images into the `MultiAgentEpisode`
        # instead into each `SingleAgentEpisode`.
        if render_image is not None:
            self.render_images.append(render_image)

        # Note, all agents will have an initial observation.
        for agent_id in observations.keys():
            # Add initial timestep for each agent to the timestep mapping.
            self.global_t_to_local_t[agent_id].append(self.t)
            # Add initial observations to the agent's episode.
            self.agent_episodes[agent_id].add_env_reset(
                # Note, initial observation has to be provided.
                observation=observations[agent_id],
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
        assert not self.is_done

        terminateds = terminateds or {}
        truncateds = truncateds or {}

        # Environment step.
        self.t += 1

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

        # Add data to agent episodes.
        for agent_id in self._agent_ids:
            # Skip agents that have been terminated or truncated.
            if self.agent_episodes[agent_id].is_done:
                continue

            agent_is_terminated = terminateds.get(agent_id, False) or self.is_terminated
            agent_is_truncated = truncateds.get(agent_id, False) or self.is_truncated

            # CASE 1: observation, no action.
            # If we have an observation, but no action, we might have a buffered action,
            # or an initial agent observation.
            if agent_id in observations and agent_id not in actions:
                # We have a buffered action.
                if self.agent_buffers[agent_id]["actions"].full():
                    # Get the action from the buffer.
                    agent_action = self.agent_buffers[agent_id]["actions"].get_nowait()
                    # Get the extra model output if available.
                    agent_extra_model_output = self.agent_buffers[agent_id][
                        "extra_model_outputs"
                    ].get_nowait()
                    # Reset the buffer to default value.
                    self.agent_buffers[agent_id]["extra_model_outputs"].put_nowait(None)

                    # Note, the reward buffer will be always full with at least the
                    # default of zero reward.
                    agent_reward = self.agent_buffers[agent_id]["rewards"].get_nowait()
                    # We might also got some reward in this episode.
                    if agent_id in rewards:
                        agent_reward += rewards[agent_id]
                        # Also add to the global reward list.
                        self.partial_rewards[agent_id].append(rewards[agent_id])
                        # And add to the global reward timestep mapping.
                        self.partial_rewards_t[agent_id].append(self.t)

                    # Refill the reward buffer with the default value of zero.
                    self.agent_buffers[agent_id]["rewards"].put_nowait(0.0)

                    # It could be the last timestep of the agent.
                    # Note, in this case the agent is in `is_terminated` and
                    # `is_truncated` b/c it has an observation.
                    if agent_is_terminated or agent_is_truncated:
                        # Then, flush the buffers.
                        # TODO (simon): You can simply not refill them above.
                        self.agent_buffers[agent_id]["rewards"].get_nowait()
                        self.agent_buffers[agent_id]["extra_model_outputs"].get_nowait()
                    # If the agent stepped we need to keep track in the timestep
                    # mapping.
                    self.global_t_to_local_t[agent_id].append(self.t)
                    # Add data to `SingleAgentEpisode.
                    self.agent_episodes[agent_id].add_env_step(
                        observation=observations[agent_id],
                        action=agent_action,
                        reward=agent_reward,
                        infos=infos.get(agent_id),
                        terminated=agent_is_terminated,
                        truncated=agent_is_truncated,
                        extra_model_outputs=agent_extra_model_output,
                    )
                # We have no buffered action.
                else:
                    # The agent might have been terminated/truncated.
                    # if agent_id in is_terminated or agent_id in is_truncated:
                    if agent_is_terminated or agent_is_truncated:
                        # If the agent has never stepped, we treat it as not being
                        # part of this episode.
                        # Delete all of the agent's registers.
                        # del self._agent_ids[self._agent_ids.index(agent_id)]
                        # self._agent_ids.discard(agent_id)
                        # del self.agent_episodes[agent_id]
                        # del self.agent_buffers[agent_id]
                        # del self.global_t_to_local_t[agent_id]
                        # del self.global_actions_t[agent_id]
                        # del self.partial_rewards[agent_id]
                        # del self.partial_rewards_t[agent_id]
                        # Then continue with the next agent.
                        continue
                    # Then this must be the agent's initial observation.
                    else:
                        # If this was the agent's first step, record the step in the
                        # global timestep mapping.
                        self.global_t_to_local_t[agent_id].append(self.t)
                        # The agent might have got a reward.
                        # TODO (simon): Refactor to a function `record_rewards`.
                        if agent_id in rewards:
                            # Add the reward to the one in the buffer.
                            self.agent_buffers[agent_id]["rewards"].put_nowait(
                                self.agent_buffers[agent_id]["rewards"].get_nowait()
                                + rewards[agent_id]
                            )
                            # Add the reward to the partial rewards of this agent.
                            self.partial_rewards[agent_id].append(rewards[agent_id])
                            self.partial_rewards_t[agent_id].append(self.t)

                        self.agent_episodes[agent_id].add_env_reset(
                            observation=observations[agent_id],
                            infos=infos.get(agent_id),
                        )
            # CASE 2: No observation, but action.
            # We have no observation, but we have an action. This must be an orphane
            # action and we need to buffer it.
            elif agent_id not in observations and agent_id in actions:
                # Maybe the agent got terminated.
                if agent_is_terminated or agent_is_truncated:
                    # If this was indeed the agent's last step, we need to record it
                    # in the timestep mapping.
                    self.global_t_to_local_t[agent_id].append(self.t)
                    # Also flush all default values from the buffers.
                    self.agent_buffers[agent_id]["rewards"].get_nowait()
                    self.agent_buffers[agent_id]["extra_model_outputs"].get_nowait()
                    # TODO (simon): When agents die, shall we remove them from the
                    # agent_id list?
                    # If the agent was terminated and no observation is provided,
                    # take the last one.
                    self.agent_episodes[agent_id].add_env_step(
                        observation=self.agent_episodes[agent_id].observations[-1],
                        action=actions[agent_id],
                        reward=0.0 if agent_id not in rewards else rewards[agent_id],
                        infos=self.agent_episodes[agent_id].infos[-1],
                        terminated=agent_is_terminated,
                        truncated=agent_is_truncated,
                        extra_model_outputs=None
                        if agent_id not in extra_model_outputs
                        else extra_model_outputs[agent_id],
                    )
                # Agent is still alive.
                else:
                    # TODO (simon): Maybe add a shift mapping that keeps track on
                    # original action timestep (global one). Right now the
                    # `global_reward_t` might serve here.
                    # Buffer the action.
                    self.agent_buffers[agent_id]["actions"].put_nowait(
                        actions[agent_id]
                    )
                    # Record the timestep for the action.
                    self.global_actions_t[agent_id].append(self.t)
                    # If available, buffer also reward. Note, if the agent is terminated
                    # or truncated, we finish the `SingleAgentEpisode`.
                    if agent_id in rewards:
                        # Add the reward to the existing one in the buffer. Note, the
                        # default value is zero.
                        # TODO (simon): Refactor to `record_rewards()`.
                        self.agent_buffers[agent_id]["rewards"].put_nowait(
                            self.agent_buffers[agent_id]["rewards"].get_nowait()
                            + rewards[agent_id]
                        )
                        # Add to the global reward list.
                        self.partial_rewards[agent_id].append(rewards[agent_id])
                        # Add also to the global reward timestep mapping.
                        self.partial_rewards_t[agent_id].append(self.t)
                    # If the agent got any extra model outputs, buffer them, too.
                    if extra_model_outputs and agent_id in extra_model_outputs:
                        # Flush the default `None` from buffer.
                        self.agent_buffers[agent_id]["extra_model_outputs"].get_nowait()
                        # STore the extra model outputs into the buffer.
                        self.agent_buffers[agent_id]["extra_model_outputs"].put_nowait(
                            extra_model_outputs[agent_id]
                        )

            # CASE 3: No observation and no action.
            # We have neither observation nor action. Then, we could have `reward`,
            # `is_terminated` or `is_truncated` and should record it.
            elif agent_id not in observations and agent_id not in actions:
                # The agent could be is_terminated
                if agent_is_terminated or agent_is_truncated:
                    # If the agent has never stepped, we treat it as not being
                    # part of this episode.
                    if len(self.agent_episodes[agent_id].observations) == 0:
                        # Delete all of the agent's registers.
                        # del self._agent_ids[self._agent_ids.index(agent_id)]
                        # self._agent_ids.discard(agent_id)
                        # del self.agent_episodes[agent_id]
                        # del self.agent_buffers[agent_id]
                        # del self.global_t_to_local_t[agent_id]
                        # del self.global_actions_t[agent_id]
                        # del self.partial_rewards[agent_id]
                        # del self.partial_rewards_t[agent_id]
                        # Then continue with the next agent.
                        continue

                    # If no observation and no action is available and the agent had
                    # stepped before the buffer must be full b/c after each
                    # observation the agent does step and if no observation followed
                    # to write the record away, the action gets buffered.
                    agent_action = self.agent_buffers[agent_id]["actions"].get_nowait()
                    # Note, this is initialized as `None` if agents do not have
                    # extra model outputs.
                    agent_extra_model_output = self.agent_buffers[agent_id][
                        "extra_model_outputs"
                    ].get_nowait()
                    # Get the reward from the buffer. Note, this is always available
                    # as it is initialized as a zero reward.
                    agent_reward = self.agent_buffers[agent_id]["rewards"].get_nowait()
                    # If a reward is received at this timestep record it.
                    if agent_id in rewards:
                        # TODO (simon): Refactor to `record_rewards()`.
                        agent_reward += rewards[agent_id]
                        # Add to the global reward list.
                        self.partial_rewards[agent_id].append(rewards[agent_id])
                        # Add also to the global reward timestep mapping.
                        self.partial_rewards_t[agent_id].append(self.t)

                    # If this was indeed the agent's last step, we need to record
                    # it in the timestep mapping.
                    self.global_t_to_local_t[agent_id].append(self.t)
                    # Finish the agent's episode.
                    self.agent_episodes[agent_id].add_env_step(
                        observation=self.agent_episodes[agent_id].observations[-1],
                        action=agent_action,
                        reward=agent_reward,
                        infos=infos.get(agent_id),
                        terminated=agent_is_terminated,
                        truncated=agent_is_truncated,
                        extra_model_outputs=agent_extra_model_output,
                    )
                # The agent is still alive.
                else:
                    # If the agent received an reward (triggered by actions of
                    # other agents) we collect it and add it to the one in the
                    # buffer.
                    if agent_id in rewards:
                        self.agent_buffers[agent_id]["rewards"].put_nowait(
                            self.agent_buffers[agent_id]["rewards"].get_nowait()
                            + rewards[agent_id]
                        )
                        # Add to the global reward list.
                        self.partial_rewards[agent_id].append(rewards[agent_id])
                        # Add also to the global reward timestep mapping.
                        self.partial_rewards_t[agent_id].append(self.t)
            # CASE 4: Observation and action.
            # We have an observation and an action. Then we can simply add the
            # complete information to the episode.
            else:
                # In this case the agent id is also in the `is_terminated` and
                # `is_truncated` dictionaries.
                if agent_is_terminated or agent_is_truncated:
                    # If the agent is also done in this timestep, flush the default
                    # values from the buffers. Note, the agent has an action, i.e.
                    # it has stepped before, so we have no recorded partial rewards
                    # to add here.
                    self.agent_buffers[agent_id]["rewards"].get_nowait()
                    self.agent_buffers[agent_id]["extra_model_outputs"].get_nowait()
                # If the agent stepped we need to keep track in the timestep mapping.
                self.global_t_to_local_t[agent_id].append(self.t)
                # Record the action to the global action timestep mapping.
                self.global_actions_t[agent_id].append(self.t)
                if agent_id in rewards:
                    # Also add to the global reward list.
                    self.partial_rewards[agent_id].append(rewards[agent_id])
                    # And add to the global reward timestep mapping.
                    self.partial_rewards_t[agent_id].append(self.t)
                # Add timestep to `SingleAgentEpisode`.
                self.agent_episodes[agent_id].add_env_step(
                    observation=observations[agent_id],
                    action=actions[agent_id],
                    reward=0.0 if agent_id not in rewards else rewards[agent_id],
                    infos=infos.get(agent_id),
                    terminated=agent_is_terminated,
                    truncated=agent_is_truncated,
                    extra_model_outputs=None
                    if extra_model_outputs is None
                    else extra_model_outputs[agent_id],
                )

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

    # TODO (sven, simon): We are taking over dead agents to the successor
    #  is this intended or should we better check during concatenation, if
    #  the union of agents from both episodes is included? Next issue.
    def cut(self) -> "MultiAgentEpisode":
        """Returns a successor episode chunk (of len=0) continuing from this Episode.

        The successor will have the same ID as `self` and starts at the timestep where
        it's predecessor `self` left off. The current observations and infos
        are carried over from the predecessor as initial observations/infos.

        Returns: A MultiAgentEpisode starting at the timestep where its predecessor
            stopped.
        """
        assert not self.is_done

        successor = MultiAgentEpisode(
            id_=self.id_,
            agent_ids=self._agent_ids,
            agent_episode_ids={
                agent_id: agent_eps.id_
                for agent_id, agent_eps in self.agent_episodes.items()
            },
            terminateds=self.is_terminated,
            truncateds=self.is_truncated,
            t_started=self.t,
        )

        for agent_id, agent_eps in self.agent_episodes.items():
            # Call the `SingleAgentEpisode.create_successor` method for
            # all agents that are still alive.
            if not agent_eps.is_done and agent_eps.observations:
                # Build a successor for each agent that is not done, yet.
                successor.agent_episodes[agent_id] = agent_eps.cut()
                # Record the initial observation in the global timestep mapping.
                successor.global_t_to_local_t[agent_id] = _IndexMapping(
                    [self.global_t_to_local_t[agent_id][-1]]
                )
            # For agents that are done or have no observation yet, create empty
            # instances.
            else:
                successor.agent_episodes[agent_id] = SingleAgentEpisode(
                    id_=agent_eps.id_,
                    terminated=agent_eps.is_terminated,
                    truncated=agent_eps.is_truncated,
                )
                successor.global_t_to_local_t[agent_id] = _IndexMapping()

        # Copy the agent buffers to the successor. These remain the same as
        # no agent has stepped, yet.
        successor._copy_buffer(self)

        # Build the global action timestep mapping for buffered actions.
        # Note, this mapping tracks orhpane actions in the episode before and
        # gives it a timestep `successor.t`, i.e. calling `get_actions(indices=0)`
        # will return these orphane actions from the predecessor.
        # TODO (simon): This might lead to information loss when concatenating.
        # as the action was made before when the agent had its last observation.
        # This observation might help to avoid the loss.
        successor.global_actions_t = self._generate_action_timestep_mappings()

        # Add the not-yet recorded partial rewards for each agent.
        # TODO (simon): Check, if get_rewards can help here (with global_ts=False)
        successor = self._add_partial_rewards(successor)

        # Return the successor.
        return successor

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
                "global_t_to_local_t": self.global_t_to_local_t,
                "global_actions_t": self.global_actions_t,
                "partial_rewards_t": self.partial_rewards_t,
                "partial_rewards": self.partial_rewards,
                "agent_episodes": list(
                    {
                        agent_id: agent_eps.get_state()
                        for agent_id, agent_eps in self.agent_episodes.items()
                    }.items()
                ),
                "t_started": self.t_started,
                "t": self.t,
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
        episode.global_t_to_local_t = state[2][1]
        episode.global_actions_t = state[3][1]
        episode.partial_rewards_t = state[4][1]
        episode.partial_rewards = state[5][1]
        episode.agent_episodes = {
            agent_id: SingleAgentEpisode.from_state(agent_state)
            for agent_id, agent_state in state[6][1]
        }
        episode.t_started = state[7][1]
        episode.t = state[8][1]
        episode.ts_carriage_return = state[9][1]
        episode.is_terminated = state[10][1]
        episode.is_trcunated = state[11][1]
        return episode

    def to_sample_batch(self) -> MultiAgentBatch:
        """Converts a `MultiAgentEpisode` into a `MultiAgentBatch`.

        Each `SingleAgentEpisode` instances in
        `MultiAgentEpisode.agent_epiosdes` will be converted into
        a `SampleBatch` and the environment timestep will be passed
        towards the `MultiAgentBatch`'s `count`.

        Returns: A `MultiAgentBatch` instance.
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
            env_steps=self.t - self.t_started,
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
            sum(len(agent_map) for agent_map in self.global_t_to_local_t.values()) > 0
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

    def _generate_action_timestep_mappings(self):
        # Note, here we use the indices that are from the predecessor, i.e.
        # these timesteps do not occur in the successor. This will have
        # the consequences that `get_actions()` will not list any actions
        # that come from the buffers of the predecessor.
        return {
            agent_id: _IndexMapping([self.global_actions_t[agent_id][-1]])
            if agent_buffer["actions"].full()
            else _IndexMapping()
            for agent_id, agent_buffer in self.agent_buffers.items()
        }

    def _add_partial_rewards(self, successor):
        # TODO (simon): This could be made simpler with a reward buffer that
        # is longer than 1 and does not add, but append. Then we just collect
        # the buffer.
        for agent_id, agent_partial_rewards in self.partial_rewards.items():
            # If a global timestep mapping exists for the agent use it.
            if self.global_t_to_local_t[agent_id]:
                # The successor episode only need the partial rewards that
                # have not yet recorded in the `SingleAgentEpisode`.
                indices_to_keep = self.partial_rewards_t[agent_id].find_indices_right(
                    self.global_t_to_local_t[agent_id][-1],
                )
            # Otherwise, use the partial rewards timestep mapping.
            else:
                # The partial rewards timestep mapping does only exist for agents that
                # have stepped or have not stepped and received at least a single
                # partial reward.
                if self.partial_rewards_t[agent_id]:
                    indices_to_keep = list(range(len(self.partial_rewards_t[agent_id])))
                # Otherwise return an empty index list.
                else:
                    indices_to_keep = []

            successor.partial_rewards_t[agent_id] = _IndexMapping(
                map(self.partial_rewards_t[agent_id].__getitem__, indices_to_keep)
            )
            successor.partial_rewards[agent_id] = list(
                map(agent_partial_rewards.__getitem__, indices_to_keep)
            )
            # Leave partial results after the last observation in the buffer
            # as long as the agent is not temrinated/truncated.
            # Note, we still need to consider the `partial_rewards` and
            # `partial_rewards_t` for two reasons:
            #   1. The agent is terminated in the last step before creating
            #       this successor.
            #   2. We might want to concatenate the successor to its
            #       predecessor and therefore need structs for each agent
            #       (dead or alive).
            if not self.agent_episodes[agent_id].is_done:
                successor.agent_buffers[agent_id]["rewards"].get_nowait()
                successor.agent_buffers[agent_id]["rewards"].put_nowait(
                    sum(successor.partial_rewards[agent_id])
                )

        return successor

    def _generate_ts_mapping(
        self, observations: List[MultiAgentDict]
    ) -> MultiAgentDict:
        """Generates a timestep mapping to local agent timesteps.

        This helps us to keep track of which agent stepped at
        which global (environment) timestep.
        Note that the local (agent) timestep is given by the index
        of the list for each agent.

        Args:
            observations: A list of observations.Each observations maps agent
                ids to their corresponding observation.

        Returns: A dictionary mapping agents to time index lists. The latter
            contain the global (environment) timesteps at which the agent
            stepped (was ready).
        """
        # Only if agent ids have been provided we can build the timestep mapping.
        if len(self._agent_ids) > 0:
            global_t_to_local_t = {agent: _IndexMapping() for agent in self._agent_ids}

            # We need the observations to create the timestep mapping.
            if observations:
                for t, agent_map in enumerate(observations):
                    for agent_id in agent_map:
                        # If agent stepped add the timestep to the timestep mapping.
                        global_t_to_local_t[agent_id].append(
                            t + self.ts_carriage_return
                        )
            # Otherwise, set to an empoty dict (when creating an empty episode).
            else:
                global_t_to_local_t = {}
        # Otherwise, set to an empoty dict (when creating an empty episode).
        else:
            # TODO (sven, simon): Shall we return an empty dict or an agent dict with
            # empty lists?
            global_t_to_local_t = {}
        # Return the index mapping.
        return global_t_to_local_t

    def _generate_global_actions_t(self, actions):
        global_actions_t = {agent_id: _IndexMapping() for agent_id in self._agent_ids}
        if actions:
            for t, action in enumerate(actions):
                for agent_id in action:
                    # Note, actions start at timestep 1, i.e. no initial actions.
                    # TODO (simon): Check, if need here also the `ts_carriage_return`.
                    global_actions_t[agent_id].append(t + self.ts_carriage_return + 1)

        return global_actions_t

    # TODO (sven, simon): This function can only deal with data if it does not contain
    # terminated or truncated agents (i.e. you have to provide ONLY alive agents in the
    # agent_ids in the constructor - the episode does not deduce the agents).
    def _generate_single_agent_episode(
        self,
        agent_id: str,
        agent_episode_ids: Optional[Dict[str, str]] = None,
        observations: Optional[List[MultiAgentDict]] = None,
        actions: Optional[List[MultiAgentDict]] = None,
        rewards: Optional[List[MultiAgentDict]] = None,
        infos: Optional[List[MultiAgentDict]] = None,
        terminateds: Union[MultiAgentDict, bool] = False,
        truncateds: Union[MultiAgentDict, bool] = False,
        extra_model_outputs: Optional[List[MultiAgentDict]] = None,
    ) -> SingleAgentEpisode:
        """Generates a SingleAgentEpisode from multi-agent data.

        Note, if no data is provided an empty `SingleAgentEpiosde`
        will be returned that starts at `SingleAgentEpisode.t_started=0`.
        """

        # If an episode id for an agent episode was provided assign it.
        episode_id = None if agent_episode_ids is None else agent_episode_ids[agent_id]
        # We need the timestep mapping to create single agent's episode.
        if len(self.global_t_to_local_t) > 0:
            # Set to None if not provided.
            agent_observations = (
                None
                if observations is None
                else self._get_single_agent_data(
                    agent_id, observations, shift=-self.ts_carriage_return
                )
            )

            agent_actions = (
                None
                if actions is None
                else self._get_single_agent_data(
                    agent_id,
                    actions,
                    use_global_t_to_local_t=False,
                )
            )

            # Rewards are complicated in multi-agent scenarios, as agents could receive
            # a reward even though they did not get an observation or stepped at a
            # certain timestep.
            agent_rewards = (
                None
                if rewards is None
                else self._get_single_agent_data(
                    agent_id,
                    rewards,
                    use_global_t_to_local_t=False,
                )
            )
            # Like observations, infos start at timestep `t=0`, so we do not need to
            # shift or start later when using the global timestep mapping. But we
            # need to use the timestep carriage in case the starting timestep is
            # different from the length of observations-after-initialization.
            agent_infos = (
                None
                if infos is None
                else self._get_single_agent_data(
                    agent_id, infos, shift=-self.ts_carriage_return
                )
            )

            _agent_extra_model_outputs = (
                None
                if extra_model_outputs is None
                else self._get_single_agent_data(
                    agent_id,
                    extra_model_outputs,
                    use_global_t_to_local_t=False,
                )
            )
            # Convert `extra_model_outputs` for this agent from list of dicts to dict
            # of lists.
            agent_extra_model_outputs = defaultdict(list)
            for _model_out in _agent_extra_model_outputs:
                for key, val in _model_out.items():
                    agent_extra_model_outputs[key].append(val)

            agent_is_terminated = terminateds.get(agent_id, False)
            agent_is_truncated = truncateds.get(agent_id, False)

            # If there are as many actions as observations we have to buffer.
            if (
                agent_actions
                and agent_observations
                and len(agent_observations) == len(agent_actions)
            ):
                # Assert then that the other data is in order.
                if agent_extra_model_outputs:
                    assert all(
                        len(v) == len(agent_actions)
                        for v in agent_extra_model_outputs.values()
                    ), (
                        f"Agent {agent_id} doesn't have the same number of "
                        "`extra_model_outputs` as it has actions "
                        f"({len(agent_actions)})."
                    )
                    # Put the last extra model outputs into the buffer.
                    self.agent_buffers[agent_id]["extra_model_outputs"].get_nowait()
                    self.agent_buffers[agent_id]["extra_model_outputs"].put_nowait(
                        {k: v.pop() for k, v in agent_extra_model_outputs.items()}
                    )

                # Put the last action into the buffer.
                self.agent_buffers[agent_id]["actions"].put_nowait(agent_actions.pop())

            # TODO (simon): Check, if this can be refactored to a
            # `_generate_partial_rewards` method and can be done where
            # the global timestep  and global action timestep
            # mappings are created (__init__).
            # We have to take care of partial rewards when generating the agent rewards:
            #   1. Rewards between different observations -> added up and
            #       assigned to next observation.
            #   2. Rewards after the last observation -> get buffered and added up
            #       in the buffer for the next observation.
            #   3. Rewards before the initial observation -> get buffered
            #       and added to the next observation.
            # All partial rewards are recorded in `partial_rewards` together
            # with their corresponding timesteps in `partial_rewards_t`.
            if agent_rewards and observations:
                partial_agent_rewards_t = _IndexMapping()
                partial_agent_rewards = []
                agent_rewards = []
                agent_reward = 0.0
                for t, reward in enumerate(rewards):
                    if agent_id in reward:
                        # Add the rewards
                        partial_agent_rewards.append(reward[agent_id])
                        # Then add the reward.
                        agent_reward += reward[agent_id]
                        # Note, rewards start at timestep 1 (there are no initial ones).
                        # TODO (simon): Check, if we need to use here also
                        # `ts_carriage_return`.
                        partial_agent_rewards_t.append(t + self.ts_carriage_return + 1)
                        if (t + 1) in self.global_t_to_local_t[agent_id][1:]:
                            agent_rewards.append(agent_reward)
                            agent_reward = 0.0

                # If the agent reward is not zero, we must have rewards that came
                # after the last observation. Then we buffer this reward.
                self.agent_buffers[agent_id]["rewards"].put_nowait(
                    self.agent_buffers[agent_id]["rewards"].get_nowait() + agent_reward
                )
                # Now save away the original rewards and the reward timesteps.
                self.partial_rewards_t[agent_id] = partial_agent_rewards_t
                self.partial_rewards[agent_id] = partial_agent_rewards

            return SingleAgentEpisode(
                id_=episode_id,
                observations=agent_observations,
                actions=agent_actions,
                rewards=agent_rewards,
                infos=agent_infos,
                terminated=agent_is_terminated,
                truncated=agent_is_truncated,
                extra_model_outputs=agent_extra_model_outputs,
            )
        # Otherwise return empty `SingleAgentEpisode`.
        else:
            return SingleAgentEpisode(id_=episode_id)

    def _getattr_by_index(
        self,
        attr: str = "observations",
        indices: Union[int, List[int]] = -1,
        has_initial_value=False,
        global_ts: bool = True,
        global_ts_mapping: Optional[MultiAgentDict] = None,
        shift: int = 0,
        as_list: bool = False,
        buffered_values: MultiAgentDict = None,
    ) -> MultiAgentDict:
        """Returns values in the form of indices: [-1, -2]."""
        # TODO (simon): Does not give yet indices that are in between.
        # TODO (sven): Do we even need indices in between? This is very
        # tricky.

        if not global_ts_mapping:
            global_ts_mapping = self.global_t_to_local_t

        # First for global_ts = True:
        if global_ts:
            # Check, if the indices are iterable.
            if isinstance(indices, list):
                indices = [
                    (self.t + idx + int(has_initial_value))
                    if idx < 0
                    else idx + self.ts_carriage_return
                    for idx in indices
                ]
            # If not make them iterable.
            else:
                indices = (
                    [
                        self.t
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
                                list(getattr(agent_eps, attr))
                                + buffered_values[agent_id]
                            )[global_ts_mapping[agent_id].find_indices([idx], shift)[0]]
                            for agent_id, agent_eps in self.agent_episodes.items()
                            if global_ts_mapping[agent_id].find_indices([idx], shift)
                        }
                        for idx in indices
                    ]
                else:
                    return [
                        {
                            agent_id: (getattr(agent_eps, attr))[
                                global_ts_mapping[agent_id].find_indices([idx], shift)[
                                    0
                                ]
                            ]
                            for agent_id, agent_eps in self.agent_episodes.items()
                            if global_ts_mapping[agent_id].find_indices([idx], shift)
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
                                    list(getattr(agent_eps, attr))
                                    + buffered_values[agent_id]
                                ).__getitem__,
                                global_ts_mapping[agent_id].find_indices(
                                    indices, shift
                                ),
                            )
                        )
                        for agent_id, agent_eps in self.agent_episodes.items()
                        # Only include agent data for agents that stepped.
                        if global_ts_mapping[agent_id].find_indices(indices, shift)
                    }
                else:
                    return {
                        agent_id: list(
                            map(
                                getattr(agent_eps, attr).__getitem__,
                                global_ts_mapping[agent_id].find_indices(
                                    indices, shift
                                ),
                            )
                        )
                        for agent_id, agent_eps in self.agent_episodes.items()
                        # Only include agent data for agents that stepped.
                        if global_ts_mapping[agent_id].find_indices(indices, shift)
                    }
        # Otherwise just look for the timesteps in the `SingleAgentEpisode`s
        # directly.
        else:
            # Check, if the indices are iterable.
            if not isinstance(indices, list):
                indices = [indices]

            if buffered_values:
                return {
                    agent_id: list(
                        map(
                            (
                                getattr(agent_eps, attr) + buffered_values[agent_id]
                            ).__getitem__,
                            set(indices).intersection(
                                set(
                                    range(
                                        -len(global_ts_mapping[agent_id]),
                                        len(global_ts_mapping[agent_id]),
                                    )
                                )
                            ),
                        )
                    )
                    for agent_id, agent_eps in self.agent_episodes.items()
                    if set(indices).intersection(
                        set(
                            range(
                                -len(global_ts_mapping[agent_id]),
                                len(global_ts_mapping[agent_id]),
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
                                        -len(global_ts_mapping[agent_id]),
                                        len(global_ts_mapping[agent_id]),
                                    )
                                )
                            ),
                        )
                    )
                    for agent_id, agent_eps in self.agent_episodes.items()
                    if set(indices).intersection(
                        set(
                            range(
                                -len(global_ts_mapping[agent_id]),
                                len(global_ts_mapping[agent_id]),
                            )
                        )
                    )
                }

    def _get_single_agent_data(
        self,
        agent_id: str,
        ma_data: List[MultiAgentDict],
        use_global_t_to_local_t: bool = True,
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
        if use_global_t_to_local_t:
            # Return all single agent data along the global timestep.
            return [
                singleton[agent_id]
                for singleton in list(
                    map(
                        ma_data.__getitem__,
                        [
                            i + shift
                            for i in self.global_t_to_local_t[agent_id][
                                start_index:end_index
                            ]
                        ],
                    )
                )
                if agent_id in singleton.keys()
            ]
        # Use all. This makes sense in multi-agent games where rewards could be given,
        # even to agents that receive no observation in a timestep.
        else:
            return [
                singleton[agent_id] for singleton in ma_data if agent_id in singleton
            ]

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
            sum(len(agent_map) for agent_map in self.global_t_to_local_t.values()) > 0
        ), (
            "ERROR: Cannot determine length of episode that hasn't started, yet!"
            "Call `MultiAgentEpisode.add_env_reset(observations=)` "
            "first (after which `len(MultiAgentEpisode)` will be 0)."
        )
        return self.t - self.t_started


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
