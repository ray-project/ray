import copy
from collections import defaultdict
import numpy as np
import scipy
from typing import Dict, List, Optional, Tuple, Union

from ray.rllib.core.columns import Columns
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.replay_buffers.episode_replay_buffer import EpisodeReplayBuffer
from ray.rllib.utils import force_list
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import batch
from ray.rllib.utils.typing import SampleBatchType


class MultiAgentEpisodeReplayBuffer(EpisodeReplayBuffer):
    def __init__(
        self,
        capacity: int = 10000,
        *,
        batch_size_B: int = 16,
        batch_length_T: int = 64,
        **kwargs,
    ):
        # Initialize the base episode replay buffer.
        super().__init__(
            capacity=capacity,
            batch_size_B=batch_size_B,
            batch_length_T=batch_length_T,
            **kwargs,
        )

        # Stores indices of module (single-agent) timesteps.
        self._module_indices: Dict[str, List[Tuple[int]]] = defaultdict(list)
        # Stores for each module the episode indices.
        self.module_episode_id_to_index: Dict[str, Dict[str, int]] = defaultdict(dict)

        # Stores the number of single-agent timesteps in the buffer.
        self._num_agent_timesteps: int = 0
        # Stores the number of single-agent timesteps per module.
        self._num_module_timesteps: Dict[str, int] = defaultdict(int)

        # Stores the number of added single-agent timesteps over the
        # lifetime of the buffer.
        self._num_agent_timesteps_added: int = 0
        # Stores the number of added single-agent timesteps per module
        # over the lifetime of the buffer.
        self._num_module_timesteps_added: Dict[str, int] = defaultdict(int)

        self._num_module_episodes: Dict[str, int] = defaultdict(int)
        # Stores the number of module episodes evicted. Note, this is
        # important for indexing.
        self._num_module_episodes_evicted: Dict[str, int] = defaultdict(int)

    @override(EpisodeReplayBuffer)
    def add(
        self,
        episodes: Union[List["MultiAgentEpisode"], "MultiAgentEpisode"],
    ) -> None:

        episodes: List["MultiAgentEpisode"] = force_list(episodes)

        new_episode_ids: List[str] = []
        for eps in episodes:
            new_episode_ids.append(eps.id_)
            self._num_timesteps += eps.env_steps()
            self._num_timesteps_added += eps.env_steps()

        # Evict old episodes.
        eps_evicted: List["MultiAgentEpisode"] = []
        eps_evicted_ids: List[Union[str, int]] = []
        eps_evicted_idxs: List[int] = []
        while (
            self._num_timesteps > self.capacity
            and self._num_remaining_episodes(new_episode_ids, eps_evicted_ids) != 1
        ):
            # Evict episode.
            eps_evicted.append(self.episodes.popleft())
            eps_evicted_ids.append(eps_evicted[-1].id_)
            eps_evicted_idxs.append(self.episode_id_to_index.pop(eps_evicted_ids[-1]))
            # If this episode has a new chunk in the new episodes added,
            # we subtract it again.
            # TODO (sven, simon): Should we just treat such an episode chunk
            # as a new episode?
            if eps_evicted_ids[-1] in new_episode_ids:
                new_eps_to_evict = episodes[new_episode_ids.index(eps_evicted_ids[-1])]
                self._num_timesteps -= new_eps_to_evict.env_steps()
                self._num_timesteps_added -= new_eps_to_evict.env_steps()
                episodes.remove(new_eps_to_evict)
            # Remove the timesteps of the evicted episode from the counter.
            self._num_timesteps -= eps_evicted[-1].env_steps()
            self._num_agent_timesteps -= eps_evicted[-1].agent_steps()
            self._num_episodes_evicted += 1
            # Remove the module timesteps of the evicted episode from the counters.
            self._evict_module_episodes(eps_evicted[-1])

        # Add agent and module steps.
        for eps in episodes:
            self._num_agent_timesteps += eps.agent_steps()
            self._num_agent_timesteps_added += eps.agent_steps()
            # Update the module counters by the module timesteps.
            self._update_module_counters(eps)

        # Remove corresponding indices, if episodes were evicted.
        if eps_evicted_idxs:
            new_indices = []
            for idx_tuple in self._indices:
                # If episode index is not from an evicted episode, keep it.
                if idx_tuple[0] not in eps_evicted_idxs:
                    new_indices.append(idx_tuple)
            # Assign the new list of indices.
            self._indices = new_indices
            # Also remove corresponding module indices.
            for module_id, module_indices in self._module_indices.items():
                new_module_indices = []
                for idx_quad in module_indices:
                    if idx_quad[0] not in eps_evicted_idxs:
                        new_module_indices.append(idx_quad)
                self._module_indices[module_id] = new_module_indices

        for eps in episodes:
            eps = copy.deepcopy(eps)
            # If the episode is part of an already existing episode, concatenate.
            if eps.id_ in self.episode_id_to_index:
                eps_idx = self.episode_id_to_index[eps.id_]
                existing_eps = self.episodes[eps_idx]
                existing_len = len(existing_eps)
                self._indices.extend(
                    [
                        (
                            eps_idx,
                            # Note, we add 1 b/c the first timestep is
                            # never sampled.
                            existing_len + i + 1,
                        )
                        for i in range(len(eps))
                    ]
                )
                # Add new module indices.
                self._add_new_module_indices(eps, eps_idx)
                # Concatenate the episode chunk.
                existing_eps.concat_episode(eps)
            # Otherwise, create a new entry.
            else:
                # New episode.
                self.episodes.append(eps)
                eps_idx = len(self.episodes) - 1 + self._num_episodes_evicted
                self.episode_id_to_index[eps.id_] = eps_idx
                self._indices.extend([(eps_idx, i + 1) for i in range(len(eps))])
                # Add new module indices.
                self._add_new_module_indices(eps, eps_idx)

    @override(EpisodeReplayBuffer)
    def sample(
        self,
        num_items: Optional[int] = None,
        *,
        batch_size_B: Optional[int] = None,
        batch_length_T: Optional[int] = None,
        n_step: Optional[Union[int, Tuple]] = None,
        gamma: float = 0.99,
        include_infos: bool = False,
        include_extra_model_outputs: bool = False,
        replay_mode: str = "independent",
        modules_to_sample: Optional[List[Union[str, int]]] = None,
    ) -> SampleBatchType:

        if num_items is not None:
            assert batch_size_B is None, (
                "Cannot call `sample()` with both `num_items` and `batch_size_B` "
                "provided! Use either one."
            )
            batch_size_B = num_items

        # Use our default values if no sizes/lengths provided.
        batch_size_B = batch_size_B or self.batch_size_B
        # TODO (simon): Implement trajectory sampling for RNNs.
        batch_length_T = batch_length_T or self.batch_length_T

        # Sample for each module independently.
        if replay_mode == "independent":
            return self._sample_independent(
                batch_size_B=batch_size_B,
                batch_length_T=batch_length_T,
                n_step=n_step,
                gamma=gamma,
                include_infos=include_infos,
                include_extra_model_outputs=include_extra_model_outputs,
                modules_to_sample=modules_to_sample,
            )
        else:
            return self._sample_synchonized(
                batch_size_B=batch_size_B,
                batch_length_T=batch_length_T,
                n_step=n_step,
                gamma=gamma,
                include_infos=include_infos,
                include_extra_model_outputs=include_extra_model_outputs,
                modules_to_sample=modules_to_sample,
            )

    def _sample_independent(
        self,
        batch_size_B: Optional[int],
        batch_length_T: Optional[int],
        n_step: Optional[Union[int, Tuple]],
        gamma: float,
        include_infos: bool,
        include_extra_model_outputs: bool,
        modules_to_sample: Optional[List[Union[str, int]]],
    ) -> SampleBatchType:

        # Sample the n-step if necessary.
        if isinstance(n_step, tuple):
            # Use random n-step sampling.
            random_n_step = True
        else:
            actual_n_step = n_step or 1
            random_n_step = False

        ret = {}
        # TODO (simon): Ensure that the module has data and if not, skip it.
        #  TODO (sven): Should we then error out or skip? I think the Learner
        #  should handle this case when a module has no train data.
        for module_id in modules_to_sample or self._module_indices.keys():
            # Rows to return.
            observations = [[] for _ in range(batch_size_B)]
            next_observations = [[] for _ in range(batch_size_B)]
            actions = [[] for _ in range(batch_size_B)]
            rewards = [[] for _ in range(batch_size_B)]
            is_terminated = [False for _ in range(batch_size_B)]
            is_truncated = [False for _ in range(batch_size_B)]
            weights = [1.0 for _ in range(batch_size_B)]
            n_steps = [[] for _ in range(batch_size_B)]
            # If `info` should be included, construct also a container for them.
            if include_infos:
                infos = [[] for _ in range(batch_size_B)]
            # If `extra_model_outputs` should be included, construct a container for
            # them.
            if include_extra_model_outputs:
                extra_model_outputs = [[] for _ in range(batch_size_B)]
            B = 0
            while B < batch_size_B:
                # Now sample from the single-agent timesteps.
                index_tuple = self._module_indices[module_id][
                    self.rng.integers(len(self._module_indices[module_id]))
                ]

                # This will be an agent timestep (not env timestep).
                # TODO (simon, sven): Maybe deprecate sa_episode_idx (_) in the index
                #   quads. Is there any need for it?
                ma_episode_idx, _, agent_id, sa_episode_ts = (
                    index_tuple[0] - self._num_episodes_evicted,
                    index_tuple[1] - self._num_module_episodes_evicted[module_id],
                    index_tuple[2],
                    index_tuple[3],
                )
                # If we cannnot make the n-step, we resample.
                if sa_episode_ts - n_step < 0:
                    continue
                # If we use random n-step sampling, draw the n-step for this item.
                if random_n_step:
                    actual_n_step = int(self.rng.integers(n_step[0], n_step[1]))
                # If we are at the end of an episode, continue.
                # Note, priority sampling got us `o_(t+n)` and we need for the loss
                # calculation in addition `o_t`.
                # TODO (simon): Maybe introduce a variable `num_retries` until the
                # while loop should break when not enough samples have been collected
                # to make n-step possible.
                if sa_episode_ts - actual_n_step < 0:
                    continue
                else:
                    n_steps[B] = actual_n_step
                # Get the multi-agent episode.
                ma_episode = self.episodes[ma_episode_idx]
                # Retrieve the single-agent episode for filtering.
                sa_episode = ma_episode.agent_episodes[agent_id]
                # Ensure that each row contains a tuple of the form:
                #   (o_t, a_t, sum(r_(t:t+n_step)), o_(t+n_step))
                # TODO (simon): Implement version for sequence sampling when using RNNs.
                sa_eps_observation = sa_episode.get_observations(
                    slice(sa_episode_ts - actual_n_step, sa_episode_ts + 1)
                )
                # Note, the reward that is collected by transitioning from `o_t` to
                # `o_(t+1)` is stored in the next transition in `SingleAgentEpisode`.
                sa_eps_rewards = sa_episode.get_rewards(
                    slice(sa_episode_ts - actual_n_step, sa_episode_ts)
                )
                observations[B] = sa_eps_observation[0]
                next_observations[B] = sa_eps_observation[-1]
                # Note, this will be the reward after executing action
                # `a_(episode_ts-n_step+1)`. For `n_step>1` this will be the sum of
                # all rewards that were collected over the last n steps.
                rewards[B] = scipy.signal.lfilter(
                    [1], [1, -gamma], sa_eps_rewards[::-1], axis=0
                )[-1]
                # Note, `SingleAgentEpisode` stores the action that followed
                # `o_t` with `o_(t+1)`, therefore, we need the next one.
                actions[B] = sa_episode.get_actions(sa_episode_ts - actual_n_step)
                if include_infos:
                    # If infos are included we include the ones from the last timestep
                    # as usually the info contains additional values about the last
                    # state.
                    infos[B] = sa_episode.get_infos(sa_episode_ts)
                if include_extra_model_outputs:
                    # If `extra_model_outputs` are included we include the ones from the
                    # first timestep as usually the `extra_model_outputs` contain
                    # additional values from the forward pass that produced the action
                    # at the first timestep.
                    # Note, we extract them into single row dictionaries similar to the
                    # infos, in a connector we can then extract these into single batch
                    # rows.
                    extra_model_outputs[B] = {
                        k: sa_episode.get_extra_model_outputs(
                            k, sa_episode_ts - actual_n_step
                        )
                        for k in sa_episode.extra_model_outputs.keys()
                    }
                # If the sampled time step is the episode's last time step check, if
                # the episode is terminated or truncated.
                if sa_episode_ts == sa_episode.t:
                    is_terminated[B] = sa_episode.is_terminated
                    is_truncated[B] = sa_episode.is_truncated

                # Increase counter.
                B += 1

            ret[module_id] = {
                # Note, observation and action spaces could be complex. `batch`
                # takes care of these.
                Columns.OBS: batch(observations),
                Columns.ACTIONS: batch(actions),
                Columns.REWARDS: np.array(rewards),
                Columns.NEXT_OBS: batch(next_observations),
                Columns.TERMINATEDS: np.array(is_terminated),
                Columns.TRUNCATEDS: np.array(is_truncated),
                "weights": np.array(weights),
                "n_steps": np.array(n_steps),
            }
            # Include infos if necessary.
            if include_infos:
                ret[module_id].update(
                    {
                        Columns.INFOS: infos,
                    }
                )
            # Include extra model outputs, if necessary.
            if include_extra_model_outputs:
                ret[module_id].update(
                    # These could be complex, too.
                    batch(extra_model_outputs)
                )
        return ret

    def _sample_synchonized(
        self,
        batch_size_B: Optional[int],
        batch_length_T: Optional[int],
        n_step: Optional[Union[int, Tuple]],
        gamma: float,
        include_infos: bool,
        include_extra_model_outputs: bool,
        modules_to_sample: Optional[List[Union[str, int]]],
    ) -> SampleBatchType:

        # Sample the n-step if necessary.
        if isinstance(n_step, tuple):
            # Use random n-step sampling.
            random_n_step = True
        else:
            actual_n_step = n_step or 1
            random_n_step = False

        observations = defaultdict(list)
        next_observations = defaultdict(list)
        actions = defaultdict(list)
        rewards = defaultdict(list)
        is_terminated = defaultdict(list)
        is_truncated = defaultdict(list)
        weights = defaultdict(list)
        n_steps = defaultdict(list)
        # If `info` should be included, construct also a container for them.
        if include_infos:
            infos = defaultdict(list)
        # If `extra_model_outputs` should be included, construct a container for them.
        if include_extra_model_outputs:
            extra_model_outputs = defaultdict(list)

        B = 0
        # TODO (simon): This is lockstep sampling. Implement independent
        # sampling next.
        while B < batch_size_B:
            index_tuple = self._indices[self.rng.integers(len(self._indices))]

            # This will be an env timestep (not agent timestep)
            episode_idx, episode_ts = (
                index_tuple[0] - self._num_episodes_evicted,
                index_tuple[1],
            )
            # If we use random n-step sampling, draw the n-step for this item.
            if random_n_step:
                actual_n_step = int(self.rng.integers(n_step[0], n_step[1]))
            # If we are at the end of an episode, continue.
            # Note, priority sampling got us `o_(t+n)` and we need for the loss
            # calculation in addition `o_t`.
            # TODO (simon): Maybe introduce a variable `num_retries` until the
            # while loop should break when not enough samples have been collected
            # to make n-step possible.
            if episode_ts - actual_n_step < 0:
                continue

            # Retrieve the multi-agent episode.
            episode = self.episodes[episode_idx]

            # Ensure that each row contains a tuple of the form:
            #   (o_t, a_t, sum(r_(t:t+n_step)), o_(t+n_step))
            # TODO (simon): Implement version for sequence sampling when using RNNs.
            eps_observation = episode.get_observations(
                slice(episode_ts - actual_n_step, episode_ts + 1),
                return_list=True,
            )
            # Note, `MultiAgentEpisode` stores the action that followed
            # `o_t` with `o_(t+1)`, therefore, we need the next one.
            # TODO (sven): Something appears to be wrong with the action indexing.
            #   At least we have an inconsistency here between SAE and MAE. In the
            #   MAE I get the action from one step earlier in the SAE I get the
            #   the action from the timestep at which this observation was taken.
            #   Tests are failing.
            eps_actions = episode.get_actions(episode_ts - actual_n_step)
            # Make sure that at least a single agent should have full transition.
            # TODO (simon): Filter for the `modules_to_sample`.
            agents_to_sample = self._agents_with_full_transitions(
                eps_observation,
                eps_actions,
            )
            # If not, we resample.
            if not agents_to_sample:
                continue
            # TODO (simon, sven): Do we need to include the common agent rewards?
            # Note, the reward that is collected by transitioning from `o_t` to
            # `o_(t+1)` is stored in the next transition in `MultiAgentEpisode`.
            eps_rewards = episode.get_rewards(
                slice(episode_ts - actual_n_step, episode_ts),
                return_list=True,
            )
            # TODO (simon, sven): Do we need to include the common infos? And are
            # there common extra model outputs?
            if include_infos:
                # If infos are included we include the ones from the last timestep
                # as usually the info contains additional values about the last state.
                eps_infos = episode.get_infos(episode_ts)
            if include_extra_model_outputs:
                # If `extra_model_outputs` are included we include the ones from the
                # first timestep as usually the `extra_model_outputs` contain additional
                # values from the forward pass that produced the action at the first
                # timestep.
                # Note, we extract them into single row dictionaries similar to the
                # infos, in a connector we can then extract these into single batch
                # rows.
                eps_extra_model_outputs = {
                    k: episode.get_extra_model_outputs(k, episode_ts - actual_n_step)
                    for k in episode.extra_model_outputs.keys()
                }
            # If the sampled time step is the episode's last time step check, if
            # the episode is terminated or truncated.
            episode_terminated = False
            episode_truncated = False
            if episode_ts == episode.env_t:
                episode_terminated = episode.is_terminated
                episode_truncated = episode.is_truncated
            # TODO (simon): Filter for the `modules_to_sample`.
            # TODO (sven, simon): We could here also sample for all agents in the
            # `modules_to_sample` and then adapt the `n_step` for agents that
            # have not a full transition.
            for agent_id in agents_to_sample:
                # Map our agent to the corresponding module we want to
                # train.
                module_id = episode._agent_to_module_mapping[agent_id]
                # TODO (simon, sven): Here we could skip for modules not
                # to be sampled in `modules_to_sample`.
                observations[module_id].append(eps_observation[0][agent_id])
                next_observations[module_id].append(eps_observation[-1][agent_id])
                # Fill missing rewards with zeros.
                agent_rewards = [r[agent_id] or 0.0 for r in eps_rewards]
                rewards[module_id].append(
                    scipy.signal.lfilter([1], [1, -gamma], agent_rewards[::-1], axis=0)[
                        -1
                    ]
                )
                # Note, this should exist, as we filtered for agents with full
                # transitions.
                actions[module_id].append(eps_actions[agent_id])
                if include_infos:
                    infos[module_id].append(eps_infos[agent_id])
                if include_extra_model_outputs:
                    extra_model_outputs[module_id].append(
                        {
                            k: eps_extra_model_outputs[agent_id][k]
                            for k in eps_extra_model_outputs[agent_id].keys()
                        }
                    )
                # If sampled observation is terminal for the agent. Either MAE
                # episode is truncated/terminated or SAE episode is truncated/
                # terminated at this ts.
                # TODO (simon, sven): Add method agent_alive(ts) to MAE.
                # or add slicing to get_terminateds().
                agent_ts = episode.env_t_to_agent_t[agent_id][episode_ts]
                agent_eps = episode.agent_episodes[agent_id]
                agent_terminated = agent_ts == agent_eps.t and agent_eps.is_terminated
                agent_truncated = (
                    agent_ts == agent_eps.t
                    and agent_eps.is_truncated
                    and not agent_eps.is_terminated
                )
                if episode_terminated or agent_terminated:
                    is_terminated[module_id].append(True)
                    is_truncated[module_id].append(False)
                elif episode_truncated or agent_truncated:
                    is_truncated[module_id].append(True)
                    is_terminated[module_id].append(False)
                else:
                    is_terminated[module_id].append(False)
                    is_truncated[module_id].append(False)

            # Increase counter.
            B += 1

        # Should be convertible to MultiAgentBatch.
        ret = {
            **{
                module_id: {
                    Columns.OBS: batch(observations[module_id]),
                    Columns.ACTIONS: batch(actions[module_id]),
                    Columns.REWARDS: np.array(rewards[module_id]),
                    Columns.NEXT_OBS: batch(next_observations[module_id]),
                    Columns.TERMINATEDS: np.array(is_terminated[module_id]),
                    Columns.TRUNCATEDS: np.array(is_truncated[module_id]),
                    "weights": np.array(weights[module_id]),
                    "n_steps": np.array(n_steps[module_id]),
                }
                for module_id in observations.keys()
            }
        }

        # Return multi-agent dictionary.
        return ret

    def _num_remaining_episodes(self, new_eps, evicted_eps):
        """Calculates the number of remaining episodes.

        When adding episodes and evicting them in the `add()` method
        this function calculates iteratively the number of remaining
        episodes.

        Args:
            new_eps: List of new episode IDs.
            evicted_eps: List of evicted episode IDs.

        Returns:
            Number of episodes remaining after evicting the episodes in
            `evicted_eps` and adding the episode in `new_eps`.
        """
        return len(
            set(self.episode_id_to_index.keys()).union(set(new_eps)) - set(evicted_eps)
        )

    def _evict_module_episodes(self, multi_agent_eps: MultiAgentEpisode) -> None:
        """Evicts the module episodes from the buffer adn updates all counters.

        Args:
            multi_agent_eps: The multi-agent episode to evict from the buffer.
        """

        # Note we need to take the agent ids from the evicted episode because
        # different episodes can have different agents and module mappings.
        for agent_id in multi_agent_eps.agent_ids:
            # Retrieve the corresponding module ID and module episode.
            module_id = multi_agent_eps._agent_to_module_mapping[agent_id]
            module_eps = multi_agent_eps.agent_episodes[agent_id]
            # Remove the episode index from the module episode index map.
            self.module_episode_id_to_index[module_id].pop(module_eps.id_)
            # Update all counters.
            self._num_module_timesteps[module_id] -= module_eps.env_steps()
            self._num_module_timesteps_added[module_id] -= module_eps.env_steps()
            self._num_module_episodes[module_id] -= 1
            self._num_module_episodes_evicted[module_id] += 1

    def _update_module_counters(self, multi_agent_episode: MultiAgentEpisode) -> None:
        """Updates the module counters after adding an episode.

        Args:
            multi_agent_episode: The multi-agent episode to update the module counters
                for.
        """
        for agent_id in multi_agent_episode.agent_ids:
            agent_steps = multi_agent_episode.agent_episodes[agent_id].env_steps()
            # Only add if the agent has stepped in the episode (chunk).
            if agent_steps > 0:
                # Receive the corresponding module ID.
                # TODO (sven, simon): Is there always a mapping? What if not?
                # Is then module_id == agent_id?
                module_id = multi_agent_episode._agent_to_module_mapping[agent_id]
                self._num_module_timesteps[module_id] += agent_steps
                self._num_module_timesteps_added[module_id] += agent_steps
                # Also add to the module episode counter.
                self._num_module_episodes[module_id] += 1

    def _add_new_module_indices(
        self, multi_agent_episode: MultiAgentEpisode, episode_idx: int
    ) -> None:
        """Adds the module indices for new episode chunks.

        Args:
            multi_agent_episode: The multi-agent episode to add the module indices for.
            episode_idx: The index of the episode in the `self.episodes`.
        """

        for agent_id in multi_agent_episode.agent_ids:
            # Get the corresponding module id.
            module_id = multi_agent_episode._agent_to_module_mapping[agent_id]
            # Get the module episode.
            module_eps = multi_agent_episode.agent_episodes[agent_id]
            if module_eps.id_ in self.module_episode_id_to_index[module_id]:
                module_eps_idx = self.module_episode_id_to_index[module_id][
                    module_eps.id_
                ]
                existing_eps_len = len(
                    self.episodes[episode_idx].agent_episodes[agent_id]
                )
            else:
                module_eps_idx = (
                    self._num_module_episodes[module_id]
                    - 1
                    + self._num_module_episodes_evicted[module_id]
                )
                existing_eps_len = 0
                # Add the module episode ID to the index map.
                self.module_episode_id_to_index[module_id][
                    module_eps.id_
                ] = module_eps_idx
            # Add new module indices.
            self._module_indices[module_id].extend(
                [
                    (
                        # Keep the MAE index for sampling
                        episode_idx,
                        module_eps_idx,
                        agent_id,
                        existing_eps_len + i + 1,
                    )
                    for i in range(len(module_eps))
                ]
            )

    def _agents_with_full_transitions(self, observations, actions):
        """Filters for agents that have full transitions.

        Args:
            observations: The observations of the episode.
            actions: The actions of the episode.

        Returns:
            List of agent IDs that have full transitions.
        """
        agents_to_sample = []
        for agent_id in observations[0].keys():
            # Only if the agent has an action at the first and an observation
            # at the first and last timestep of the n-step transition, we can sample it.
            if agent_id in actions and agent_id in observations[-1]:
                agents_to_sample.append(agent_id)
        return agents_to_sample
