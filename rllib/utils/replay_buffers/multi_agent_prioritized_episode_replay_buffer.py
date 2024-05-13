import copy
import numpy as np
import scipy

from collections import defaultdict, deque
from numpy.typing import NDArray
from typing import Dict, List, Optional, Tuple, Union
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils import force_list
from ray.rllib.utils.annotations import override
from ray.rllib.utils.replay_buffers.multi_agent_episode_replay_buffer import (
    MultiAgentEpisodeReplayBuffer,
)
from ray.rllib.utils.replay_buffers.prioritized_episode_replay_buffer import (
    PrioritizedEpisodeReplayBuffer,
)
from ray.rllib.utils.typing import ModuleID
from ray.rllib.execution.segment_tree import MinSegmentTree, SumSegmentTree


class MultiAgentPrioritizedEpisodeReplayBuffer(
    MultiAgentEpisodeReplayBuffer, PrioritizedEpisodeReplayBuffer
):
    def __init__(
        self,
        capacity: int = 10000,
        *,
        batch_size_B: int = 16,
        batch_length_T: int = 1,
        alpha: float = 1.0,
        **kwargs,
    ):
        MultiAgentEpisodeReplayBuffer.__init__(
            self,
            capacity=capacity,
            batch_size_B=batch_size_B,
            batch_length_T=batch_length_T,
            **kwargs,
        )
        PrioritizedEpisodeReplayBuffer.__init__(
            self,
            capacity=capacity,
            batch_size_B=batch_size_B,
            batch_length_T=batch_length_T,
            alpha=alpha,
            **kwargs,
        )

        self._sample_idx_to_tree_idx = {}
        # Initialize segment trees for the priority weights per module. Note, b/c
        # the trees are binary we need for them a capacity that is an exponential
        # of 2. Double it to enable temporary buffer overflow (we need then free
        # nodes in the trees).
        tree_capacity = int(2 ** np.ceil(np.log2(self.capacity)))

        self._module_to_max_priority = defaultdict(lambda: 1.0)
        self._module_to_sum_segment = defaultdict(
            lambda: SumSegmentTree(2 * tree_capacity)
        )
        self._module_to_min_segment = defaultdict(
            lambda: MinSegmentTree(2 * tree_capacity)
        )
        # At initialization all nodes are free.
        self._module_to_free_nodes = defaultdict(
            lambda: deque(list(range(2 * tree_capacity)), maxlen=2 * tree_capacity)
        )
        # Keep track of the maximum index used from the trees. This helps
        # to not traverse the complete trees.
        self._module_to_max_idx = defaultdict(lambda: 0)
        # Map from tree indices to sample indices (i.e. `self._indices`).
        self._module_to_tree_idx_to_sample_idx = defaultdict(lambda: {})

        self._module_to_last_sampled_indices = defaultdict(lambda: [])

    @override(MultiAgentEpisodeReplayBuffer)
    def add(
        self,
        episodes: Union[List["MultiAgentEpisode"], "MultiAgentEpisode"],
        weight: Optional[float] = None,
    ) -> None:

        weight_per_module = {}
        if weight is None:
            weight = self._max_priority
        elif isinstance(dict, weight):
            weight_per_module = weight
            weight = np.mean(weight.values())

        episodes = force_list(episodes)

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
            evicted_episode = self.episodes.popleft()
            eps_evicted.append(evicted_episode)
            eps_evicted_ids.append(evicted_episode.id_)
            eps_evicted_idxs.append(self.episode_id_to_index.pop(evicted_episode.id_))
            # If this episode has a new chunk in the new episodes added,
            # we subtract it again.
            # TODO (sven, simon): Should we just treat such an episode chunk
            # as a new episode?
            if evicted_episode.id_ in new_episode_ids:
                new_eps_to_evict = episodes[new_episode_ids.index(evicted_episode.id_)]
                self._num_timesteps -= new_eps_to_evict.env_steps()
                self._num_timesteps_added -= new_eps_to_evict.env_steps()
                episodes.remove(new_eps_to_evict)
            # Remove the timesteps of the evicted episode from the counter.
            self._num_timesteps -= evicted_episode.env_steps()
            self._num_agent_timesteps -= evicted_episode.agent_steps()
            self._num_episodes_evicted += 1
            # Remove the module timesteps of the evicted episode from the counters.
            self._evict_module_episodes(evicted_episode)

        # Add agent and module steps.
        for eps in episodes:
            self._num_agent_timesteps += eps.agent_steps()
            self._num_agent_timesteps_added += eps.agent_steps()
            # Update the module counters by the module timesteps.
            self._update_module_counters(eps)

        # Remove corresponding indices, if episodes were evicted.
        if eps_evicted_idxs:
            new_indices = []
            # Each index 2-tuple is of the form (ma_episode_idx, timestep) and
            # refers to a certain environment timestep in a certain multi-agent
            # episode.
            i = 0
            for idx_tuple in self._indices:
                # If episode index is not from an evicted episode, keep it.
                if idx_tuple[0] in eps_evicted_idxs:
                    # Here we need the index of a multi-agent sample in the segment
                    # tree.
                    # TODO (simon): Adapt the correct index here.
                    self._free_nodes.appendleft(idx_tuple[2])
                    # Remove also the potentially maximum index.
                    self._max_idx -= 1 if self._max_idx == idx_tuple[2] else 0
                    self._sum_segment[idx_tuple[2]] = 0.0
                    self._min_segment[idx_tuple[2]] = float("inf")
                    # TODO (simon): Check, if this does harm performance.
                    sample_idx = self._tree_idx_to_sample_idx[idx_tuple[2]]
                    self._tree_idx_to_sample_idx.pop(idx_tuple[2])
                    self._sample_idx_to_tree_idx.pop(sample_idx)
                else:
                    new_indices.append(idx_tuple)
                    self._tree_idx_to_sample_idx[idx_tuple[2]] = i
                    self._sample_idx_to_tree_idx[i] = idx_tuple[2]
                    i += 1
            # Assign the new list of indices.
            self._indices = new_indices
            # Also remove corresponding module indices.
            for module_id, module_indices in self._module_to_indices.items():
                new_module_indices = []
                # Each index 4-tuple is of the form
                # (ma_episode_idx, agent_id, timestep, segtree_idx) and refers to a
                # certain agent timestep in a certain multi-agent episode.
                i = 0
                for idx_quadlet in module_indices:
                    if idx_quadlet[0] in eps_evicted_idxs:
                        # Here we need the index of a multi-agent sample in the segment
                        # tree.
                        self._module_to_free_nodes[module_id].appendleft(idx_quadlet[3])
                        # Remove also the potentially maximum index per module.
                        self._module_to_max_idx[module_id] -= (
                            1
                            if self._module_to_max_idx[module_id] == idx_quadlet[3]
                            else 0
                        )
                        self._module_to_sum_segment[module_id][idx_quadlet[3]] = 0.0
                        self._module_to_min_segment[module_id][idx_quadlet[3]] = float(
                            "inf"
                        )
                    else:
                        new_module_indices.append(idx_quadlet)
                        self._module_to_tree_idx_to_sample_idx[module_id][
                            idx_quadlet[3]
                        ] = i
                # Assign the new list of indices for the module.
                self._module_to_indices[module_id] = new_module_indices

        j = len(self._indices)
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
                            existing_len + i,
                            # Get the index in the segment trees.
                            self._get_free_node_and_assign(j + i, weight),
                        )
                        for i in range(len(eps))
                    ]
                )
                # Add new module indices.
                self._add_new_module_indices(eps, eps_idx, True, weight_per_module)
                # Concatenate the episode chunk.
                existing_eps.concat_episode(eps)
            # Otherwise, create a new entry.
            else:
                # New episode.
                self.episodes.append(eps)
                eps_idx = len(self.episodes) - 1 + self._num_episodes_evicted
                self.episode_id_to_index[eps.id_] = eps_idx
                self._indices.extend(
                    [
                        (eps_idx, i, self._get_free_node_and_assign(j + i, weight))
                        for i in range(len(eps))
                    ]
                )
                # Add new module indices.
                self._add_new_module_indices(eps, eps_idx, False, weight_per_module)
            # Increase index to the new length of `self._indices`.
            j = len(self._indices)

    @override(MultiAgentEpisodeReplayBuffer)
    def _add_new_module_indices(
        self,
        ma_episode: MultiAgentEpisode,
        episode_idx: int,
        exists: bool = True,
        weight: Optional[Union[float, Dict[ModuleID, float]]] = None,
    ) -> None:
        """Adds the module indices for new episode chunks.

        Args:
            multi_agent_episode: The multi-agent episode to add the module indices for.
            episode_idx: The index of the episode in the `self.episodes`.
        """

        for agent_id in ma_episode.agent_ids:
            # Get the corresponding module id.
            module_id = ma_episode._agent_to_module_mapping[agent_id]
            # Get the module episode.
            module_eps = ma_episode.agent_episodes[agent_id]
            # Check if the module episode is already in the buffer.
            if exists:
                old_ma_episode = self.episodes[
                    self.episode_id_to_index[ma_episode.id_]
                    - self._num_episodes_evicted
                ]
                # Is the agent episode already in the buffer?
                sa_episode_in_buffer = agent_id in old_ma_episode.agent_episodes
            else:
                # This agent episode is new. The agent might have just entered
                # the environment.
                sa_episode_in_buffer = False
            if sa_episode_in_buffer:
                existing_eps_len = len(
                    self.episodes[episode_idx].agent_episodes[agent_id]
                )
            else:
                existing_eps_len = 0
            # Add new module indices.
            module_weight = weight.get(
                module_id, self._module_to_max_priority[module_id]
            )
            self._module_to_indices[module_id].extend(
                [
                    (
                        # Keep the MAE index for sampling
                        episode_idx,
                        agent_id,
                        existing_eps_len + i,
                        # Get the index in the segment trees.
                        self._get_free_node_per_module_and_assign(
                            module_id,
                            existing_eps_len + i,
                            module_weight,
                        ),
                    )
                    for i in range(len(module_eps))
                ]
            )

    @override(PrioritizedEpisodeReplayBuffer)
    def _get_free_node_and_assign(self, sample_index, weight: float = 1.0) -> int:
        """Gets the next free node in the segment trees.

        In addition the initial priorities for a new transition are added
        to the segment trees and the index of the nodes is added to the
        index mapping.

        Args:
            sample_index: The index of the sample in the `self._indices` list.
            weight: The initial priority weight to be used in sampling for
                the item at index `sample_index`.

        Returns:
            The index in the segment trees `self._sum_segment` and
            `self._min_segment` for the item at index `sample_index` in
            ``self._indices`.
        """
        # Get an index from the free nodes in the segment trees.
        idx = self._free_nodes.popleft()
        self._max_idx = idx if idx > self._max_idx else self._max_idx
        # Add the weight to the segments.
        self._sum_segment[idx] = weight**self._alpha
        self._min_segment[idx] = weight**self._alpha
        # Map the index in the trees to the index in `self._indices`.
        self._tree_idx_to_sample_idx[idx] = sample_index
        self._sample_idx_to_tree_idx[sample_index] = idx
        # Return the index.
        return idx
    
    def _get_free_node_per_module_and_assign(
        self, module_id: ModuleID, sample_index, weight: float = 1.0
    ) -> int:
        """Gets the next free node in the segment trees.

        In addition the initial priorities for a new transition are added
        to the segment trees and the index of the nodes is added to the
        index mapping.

        Args:
            sample_index: The index of the sample in the `self._indices` list.
            weight: The initial priority weight to be used in sampling for
                the item at index `sample_index`.

        Returns:
            The index in the segment trees `self._sum_segment` and
            `self._min_segment` for the item at index `sample_index` in
            ``self._indices`.
        """
        # Get an index from the free nodes in the segment trees.
        idx = self._module_to_free_nodes[module_id].popleft()
        self._module_to_max_idx[module_id] = (
            idx
            if idx > self._module_to_max_idx[module_id]
            else self._module_to_max_idx[module_id]
        )
        # Add the weight to the segments.
        # TODO (simon): Allow alpha to be chosen per module.
        self._module_to_sum_segment[module_id][idx] = weight**self._alpha
        self._module_to_min_segment[idx] = weight**self._alpha
        # Map the index in the trees to the index in `self._indices`.
        self._module_to_tree_idx_to_sample_idx[module_id][idx] = sample_index
        # Return the index.
        return idx

    def sample(
        self,
        num_items: Optional[int] = None,
        *,
        batch_size_B: Optional[int] = None,
        batch_length_T: Optional[int] = None,
        n_step: Optional[Union[int, Tuple]] = 1,
        gamma: float = 0.99,
        include_infos: bool = False,
        include_extra_model_outputs: bool = False,
        replay_mode: str = "independent",
        modules_to_sample: Optional[List[ModuleID]] = None,
        beta: float = 0.0,
        **kwargs,
    ) -> Union[List["MultiAgentEpisode"], List["SingleAgentEpisode"]]:

        assert beta >= 0.0

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
                beta=beta,
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

    @override(MultiAgentEpisodeReplayBuffer)
    def _sample_independent(
        self,
        batch_size_B: Optional[int],
        batch_length_T: Optional[int],
        n_step: Optional[Union[int, Tuple]],
        gamma: float,
        include_infos: bool,
        include_extra_model_outputs: bool,
        modules_to_sample: Optional[List[ModuleID]],
        beta: Optional[float],
    ) -> Union[List["MultiAgentEpisode"], List["SingleAgentEpisode"]]:
        """Samples a batch of independent multi-agent transitions."""

        actual_n_step = n_step or 1
        # Sample the n-step if necessary.
        if isinstance(n_step, tuple):
            # Use random n-step sampling.
            random_n_step = True
        else:
            random_n_step = False

        # Keep track of the indices that were sampled last for updating the
        # weights later (see `ray.rllib.utils.replay_buffer.utils.
        # update_priorities_in_episode_replay_buffer`).
        # self._last_sampled_indices = defaultdict(lambda: [])

        sampled_episodes = []
        # TODO (simon): Ensure that the module has data and if not, skip it.
        #  TODO (sven): Should we then error out or skip? I think the Learner
        #  should handle this case when a module has no train data.
        for module_id in modules_to_sample or self._module_to_indices.keys():
            # Sample proportionally from the replay buffer's module segments using the
            # respective weights.
            module_total_segment_sum = self._module_to_sum_segment[module_id].sum()
            module_p_min = self._min_segment.min() / module_total_segment_sum
            # TODO (simon): Allow individual betas per module.
            module_max_weight = (module_p_min * self.get_num_timesteps()) ** (-beta)
            B = 0
            while B < batch_size_B:
                # First, draw a random sample from Uniform(0, sum over all weights).
                # Note, transitions with higher weight get sampled more often (as
                # more random draws fall into larger intervals).
                module_random_sum = (
                    self.rng.random() * self._module_to_sum_segment[module_id].sum()
                )
                # Get the highest index in the sum-tree for which the sum is
                # smaller or equal the random sum sample.
                # Note, we sample `o_(t + n_step)` as this is the state that
                # brought the information contained in the TD-error (see Schaul
                # et al. (2018), Algorithm 1).
                module_idx = self._module_to_sum_segment[module_id].find_prefixsum_idx(
                    module_random_sum
                )
                # Get the theoretical probability mass for drawing this sample.
                module_p_sample = (
                    self._sum_segment[module_idx] / module_total_segment_sum
                )
                # Compute the importance sampling weight.
                module_weight = (
                    module_p_sample * self.get_num_timesteps(module_id)
                ) ** (-beta)
                # Now, get the transition stored at this index.
                index_quadlet = self._module_to_indices[module_id][
                    self._module_to_tree_idx_to_sample_idx[module_id][module_idx]
                ]

                # This will be an agent timestep (not env timestep).
                # TODO (simon, sven): Maybe deprecate sa_episode_idx (_) in the index
                #   quads. Is there any need for it?
                ma_episode_idx, agent_id, sa_episode_ts, = (
                    index_quadlet[0] - self._num_episodes_evicted,
                    index_quadlet[1],
                    index_quadlet[2],
                )

                # Get the multi-agent episode.
                ma_episode = self.episodes[ma_episode_idx]
                # Retrieve the single-agent episode for filtering.
                sa_episode = ma_episode.agent_episodes[agent_id]

                # If we use random n-step sampling, draw the n-step for this item.
                if random_n_step:
                    actual_n_step = int(self.rng.integers(n_step[0], n_step[1]))
                # If we cannnot make the n-step, we resample.
                if sa_episode_ts + actual_n_step > len(sa_episode):
                    continue
                # Note, this will be the reward after executing action
                # `a_(episode_ts)`. For `n_step>1` this will be the sum of
                # all rewards that were collected over the last n steps.
                sa_raw_rewards = sa_episode.get_rewards(
                    slice(sa_episode_ts, sa_episode_ts + actual_n_step)
                )
                sa_rewards = scipy.signal.lfilter(
                    [1], [1, -gamma], sa_raw_rewards[::-1], axis=0
                )[-1]

                sampled_sa_episode = SingleAgentEpisode(
                    id_=sa_episode.id_,
                    # Provide the IDs for the learner connector.
                    agent_id=sa_episode.agent_id,
                    module_id=sa_episode.module_id,
                    multi_agent_episode_id=ma_episode.id_,
                    # Ensure that each episode contains a tuple of the form:
                    #   (o_t, a_t, sum(r_(t:t+n_step)), o_(t+n_step))
                    # Two observations (t and t+n).
                    observations=[
                        sa_episode.get_observations(sa_episode_ts),
                        sa_episode.get_observations(sa_episode_ts + actual_n_step),
                    ],
                    observation_space=sa_episode.observation_space,
                    infos=(
                        [
                            sa_episode.get_infos(sa_episode_ts),
                            sa_episode.get_infos(sa_episode_ts + actual_n_step),
                        ]
                        if include_infos
                        else None
                    ),
                    actions=[sa_episode.get_actions(sa_episode_ts)],
                    action_space=sa_episode.action_space,
                    rewards=[sa_rewards],
                    # If the sampled single-agent episode is the single-agent episode's
                    # last time step, check, if the single-agent episode is terminated
                    # or truncated.
                    terminated=(
                        False
                        if sa_episode_ts + actual_n_step < len(sa_episode)
                        else sa_episode.is_terminated
                    ),
                    truncated=(
                        False
                        if sa_episode_ts + actual_n_step < len(sa_episode)
                        else sa_episode.is_truncated
                    ),
                    extra_model_outputs={
                        "weights": [
                            module_weight / module_max_weight * 1
                        ],  # actual_size=1
                        "n_step": [actual_n_step],
                        **(
                            {
                                k: [
                                    sa_episode.get_extra_model_outputs(k, sa_episode_ts)
                                ]
                                for k in sa_episode.extra_model_outputs.keys()
                            }
                            if include_extra_model_outputs
                            else {}
                        ),
                    },
                    # TODO (sven): Support lookback buffers.
                    len_lookback_buffer=0,
                    t_started=sa_episode_ts,
                )
                # Append single-agent episode to the list of sampled episodes.
                sampled_episodes.append(sampled_sa_episode)

                # Increase counter.
                B += 1
                # Keep track of sampled indices for updating priorities later for each
                # module.
                self._module_to_last_sampled_indices[module_id].append(module_idx)

            # Increase the per module timesteps counter.
            self.sampled_timesteps_per_module[module_id] += B

        # Increase the counter for environment timesteps.
        self.sampled_timesteps += batch_size_B
        # Return multi-agent dictionary.
        return sampled_episodes

    @override(PrioritizedEpisodeReplayBuffer)
    def update_priorities(self, priorities: Union[NDArray, Dict[ModuleID, NDArray]]) -> None:
        """Update the priorities of items at corresponding indices.

        Usually, incoming priorities are TD-errors.

        Args:
            priorities: Numpy array containing the new priorities to be used
                in sampling for the items in the last sampled batch.
        """
        if isinstance(priorities, dict):
            assert set(priorities.keys()) == set(self._module_to_last_sampled_indices.keys())
            ma_episode_indices = []
            for module_id, module_priorities in priorities.items():
                assert len(module_priorities) == len(self._module_to_last_sampled_indices[module_id])
                for idx, priority in zip(self._module_to_last_sampled_indices[module_id], module_priorities):
                    sample_idx = self._module_to_tree_idx_to_sample_idx[module_id][idx]
                    ma_episode_idx = self._module_to_indices[module_id][sample_idx][0] - self._num_episodes_evicted
                    
                    ma_episode_indices.append(ma_episode_idx)
                    # Note, TD-errors come in as absolute values or results from
                    # cross-entropy loss calculations.
                    # assert priority > 0, f"priority was {priority}"
                    priority = max(priority, 1e-12)
                    assert 0 <= idx < self._module_to_sum_segment[module_id].capacity
                    # TODO (simon): Create metrics.
                    # delta = priority**self._alpha - self._sum_segment[idx]
                    # Update the priorities in the segment trees.
                    self._module_to_sum_segment[module_id][idx] = priority**self._alpha
                    self._module_to_min_segment[module_id][idx] = priority**self._alpha
                    # Update the maximal priority.
                    self._module_to_max_priority[module_id] = max(
                        self._module_to_max_priority[module_id], priority
                    )
                # Clear the corresponding index list for the module.
                self._module_to_last_sampled_indices[module_id].clear()
            
            # for ma_episode_idx in ma_episode_indices:
            #     ma_episode_tree_idx = self._sample_idx_to_tree_idx(ma_episode_idx)
            #     ma_episode_idx = 

            #     # Update the weights 
                # self._sum_segment[tree_idx] = sum(
                #     self._module_to_sum_segment[module_id][idx]
                #     for module_id, idx in self._tree_idx_to_sample_idx[tree_idx]
                # )
                # self._min_segment[tree_idx] = min(
                #     self._module_to_min_segment[module_id][idx]
                #     for module_id, idx in self._tree_idx_to_sample_idx[tree_idx]
                # )

    

            
                

        
