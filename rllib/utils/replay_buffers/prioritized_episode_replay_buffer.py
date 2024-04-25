import copy
import numpy as np
import scipy

from collections import deque
from numpy.typing import NDArray
from typing import Any, Dict, List, Optional, Tuple, Union

from ray.rllib.core.columns import Columns
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.execution.segment_tree import MinSegmentTree, SumSegmentTree
from ray.rllib.utils import force_list
from ray.rllib.utils.replay_buffers.episode_replay_buffer import EpisodeReplayBuffer
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import batch
from ray.rllib.utils.typing import SampleBatchType


class PrioritizedEpisodeReplayBuffer(EpisodeReplayBuffer):
    """Prioritized Replay Buffer that stores episodes by their ID.

    This replay buffer stores episode data (more specifically `SingleAgentEpisode`
    objects) and implements prioritized experience replay first proposed
    in the paper by Schaul et al. (2016, https://arxiv.org/abs/1511.05952).

    Implementation is based on segment trees as suggested by the authors of
    the cited paper, i.e. we use proportional prioritization with an order
    of O(log N) in updating and sampling.

    Each "row" (a slot in a deque) in the buffer is occupied by one episode. If an
    incomplete episode is added to the buffer and then another chunk of that episode is
    added at a later time, the buffer will automatically concatenate the new fragment to
    the original episode. This way, episodes can be completed via subsequent `add`
    calls.

    Sampling returns batches of size B (number of 'rows'), where each row is a tuple
    of the form

    `(o_t, a_t, sum(r_t+1:t+n), o_t+n)`

    where `o_t` is the observation in `t`, `a_t` the action chosen at observation `o_t`,
    `o_t+n` is the observation `n` timesteps later and `sum(r_t+1:t+n)` is the sum of
    all rewards collected over the time steps between `t+1` and `t+n`. The `n`-step can
    be chosen freely when sampling and defaults to `1`. If `n_step` is a tuple it is
    sampled uniformly across the interval defined by the tuple (for each row in the
    batch).

    Each batch contains - in addition to the data tuples presented above - two further
    columns, namely `n_steps` and `weigths`. The former holds the `n_step` used for each
    row in the batch and the latter the corresponding (importance sampling) weight for
    each row in the batch.

    After sampling priorities can be updated (for the last sampled batch) with
    `self.update_priorities`. This method assigns the new priorities automatically to
    the last sampled timesteps. Note, this implies that sampling timesteps and updating
    their corresponding priorities needs to alternate (e.g. sampling several times and
    then updating the priorities would not work because tjhe buffer caches the last
    sampled timestep indices).

    .. testcode::

        import gymnasium as gym

        from ray.rllib.env.single_agent_episode import SingleAgentEpisode
        from ray.rllib.utils.replay_buffers.prioritized_episode_replay_buffer import (
            PrioritizedEpisodeReplayBuffer
        )

        # Create the environment.
        env = gym.make("CartPole-v1")

        # Set up the loop variables
        terminated = False
        trunctaed = False
        num_timesteps = 10000
        episodes = []

        # Initialize the first episode entries.
        eps = SingleAgentEpisode()
        obs, info = env.reset()
        eps.add_env_reset(obs, info)

        # Sample 10,000 timesteps.
        for i in range(num_timesteps):
            # If terminated we create a new episode.
            if terminated:
                episodes.append(eps.finalize())
                eps = SingleAgentEpisode()
                obs, info = env.reset()
                eps.add_env_reset(obs, info)

            action = env.action_space.sample()
            obs, reward, terminated, truncated, info = env.step(action)
            eps.add_env_step(
                obs,
                action,
                reward,
                info,
                terminated=terminated,
                truncated=truncated
            )

        # Add the last (truncated) episode to the list of episodes.
        if not terminated or truncated:
            episodes.append(eps)

        # Create the buffer.
        buffer = PrioritizedEpisodeReplayBuffer()
        # Add the list of episodes sampled.
        buffer.add(episodes)

        # Pull a sample from the buffer using an `n-step` of 3.
        sample = buffer.sample(num_items=256, gamma=0.95, n_step=3)
    """

    def __init__(
        self,
        capacity: int = 10000,
        *,
        batch_size_B: int = 16,
        batch_length_T: int = 1,
        alpha: float = 1.0,
        **kwargs,
    ):
        """Initializes a `PrioritizedEpisodeReplayBuffer` object

        Args:
            capacity: The total number of timesteps to be storable in this buffer.
                Will start ejecting old episodes once this limit is reached.
            batch_size_B: The number of rows in a SampleBatch returned from `sample()`.
            batch_length_T: The length of each row in a SampleBatch returned from
                `sample()`.
            alpha: The amount of prioritization to be used: `alpha=1.0` means full
                prioritization, `alpha=0.0` means no prioritization.
        """
        super().__init__(
            capacity=capacity, batch_size_B=batch_size_B, batch_length_T=batch_length_T
        )

        # `alpha` should be non-negative.
        assert alpha >= 0
        self._alpha = alpha

        # Initialize segment trees for the priority weights. Note, b/c the trees
        # are binary we need for them a capacity that is an exponential of 2.
        # Double it to enable temporary buffer overflow (we need then free nodes
        # in the trees).
        tree_capacity = int(2 ** np.ceil(np.log2(self.capacity)))

        self._max_priority = 1.0
        self._sum_segment = SumSegmentTree(2 * tree_capacity)
        self._min_segment = MinSegmentTree(2 * tree_capacity)
        # At initialization all nodes are free.
        self._free_nodes = deque(
            list(range(2 * tree_capacity)), maxlen=2 * tree_capacity
        )
        # Keep track of the maximum index used from the trees. This helps
        # to not traverse the complete trees.
        self._max_idx = 0
        # Map from tree indices to sample indices (i.e. `self._indices`).
        self._tree_idx_to_sample_idx = {}

    @override(EpisodeReplayBuffer)
    def add(
        self,
        episodes: Union[List["SingleAgentEpisode"], "SingleAgentEpisode"],
        weight: Optional[float] = None,
    ) -> None:
        """Adds incoming episodes to the replay buffer.

        Note, if the incoming episodes' time steps cause the buffer to overflow,
        older episodes are evicted. Because episodes usually come in chunks and
        not complete, this could lead to edge cases (e.g. with very small capacity
        or very long episode length) where the first part of an episode is evicted
        while the next part just comes in.
        In such cases, we evict the complete episode, including the new chunk,
        unless the episode is the last one in the buffer. In the latter case the
        buffer will be allowed to overflow in a temporary fashion, i.e. during
        the next addition of samples to the buffer an attempt is made to fall below
        capacity again.

        The user is advised to select a large enough buffer with regard to the maximum
        expected episode length.

        Args:
            episodes: A list of `SingleAgentEpisode`s that contain the episode data.
            weight: A starting priority for the time steps in `episodes`. If `None`
                the maximum priority is used, i.e. 1.0 (as suggested in the original
                paper we scale weights to the interval [0.0, 1.0])..
        """

        # TODO (sven, simon): Eventually allow here an array?
        if weight is None:
            weight = self._max_priority

        episodes = force_list(episodes)

        # Add first the timesteps of new episodes to have info about how many
        # episodes should be evicted to stay below capacity.
        new_episode_ids = []
        for eps in episodes:
            new_episode_ids.append(eps.id_)
            self._num_timesteps += len(eps)
            self._num_timesteps_added += len(eps)

        eps_evicted = []
        eps_evicted_ids = []
        eps_evicted_idxs = []
        while (
            self._num_timesteps > self.capacity
            and self._num_remaining_episodes(new_episode_ids, eps_evicted_ids) != 1
        ):
            # Evict episode
            eps_evicted.append(self.episodes.popleft())
            eps_evicted_ids.append(eps_evicted[-1].id_)
            eps_evicted_idxs.append(self.episode_id_to_index.pop(eps_evicted_ids[-1]))
            # If this episode has a new chunk in the new episodes added,
            # we subtract it again.
            # TODO (sven, simon): Should we just treat such an episode chunk
            # as a new episode?
            if eps_evicted_ids[-1] in new_episode_ids:
                len_to_subtract = len(
                    episodes[new_episode_ids.index(eps_evicted_idxs[-1])]
                )
                self._num_timesteps -= len_to_subtract
                self._num_timesteps_added -= len_to_subtract
            # Remove the timesteps of the evicted episode from the counter.
            self._num_timesteps -= len(eps_evicted[-1])
            self._num_episodes_evicted += 1

        # Remove corresponding indices, if episodes were evicted.
        if eps_evicted_idxs:
            new_indices = []
            i = 0
            for idx_triple in self._indices:
                # If the index comes from an evicted episode free the nodes.
                if idx_triple[0] in eps_evicted_idxs:
                    # Here we need the index of a sample in the segment tree.
                    self._free_nodes.appendleft(idx_triple[2])
                    # Also remove the potentially maximum index.
                    self._max_idx -= 1 if self._max_idx == idx_triple[2] else 0
                    self._sum_segment[idx_triple[2]] = 0.0
                    self._min_segment[idx_triple[2]] = float("inf")
                # Otherwise update the index in the index mapping.
                else:
                    new_indices.append(idx_triple)
                    self._tree_idx_to_sample_idx[idx_triple[2]] = i
                    i += 1
            # Assign the new list of indices.
            self._indices = new_indices

        # Now append the indices for the new episodes.
        j = len(self._indices)
        for eps in episodes:
            # If the episode chunk is part of an evicted episode continue.
            if eps.id_ in eps_evicted_ids:
                continue
            # Otherwise, add the episode data to the buffer.
            else:
                eps = copy.deepcopy(eps)
                # If the episode is part of an already existing episode, concatenate.
                if eps.id_ in self.episode_id_to_index:
                    eps_idx = self.episode_id_to_index[eps.id_]
                    existing_eps = self.episodes[eps_idx - self._num_episodes_evicted]
                    old_len = len(existing_eps)
                    self._indices.extend(
                        [
                            (
                                eps_idx,
                                # Note, we add 1 b/c the first timestep is never
                                # sampled.
                                old_len + i + 1,
                                # Get the index in the segment trees.
                                self._get_free_node_and_assign(j + i, weight),
                            )
                            for i in range(len(eps))
                        ]
                    )
                    existing_eps.concat_episode(eps)
                # Otherwise, create a new entry.
                else:
                    self.episodes.append(eps)
                    eps_idx = len(self.episodes) - 1 + self._num_episodes_evicted
                    self.episode_id_to_index[eps.id_] = eps_idx
                    self._indices.extend(
                        [
                            (
                                eps_idx,
                                # Note, we add 1 b/c the first timestep is never
                                # sampled.
                                i + 1,
                                self._get_free_node_and_assign(j + i, weight),
                            )
                            for i in range(len(eps))
                        ]
                    )
                # Increase index.
                j = len(self._indices)

    @override(EpisodeReplayBuffer)
    def sample(
        self,
        num_items: Optional[int] = None,
        *,
        batch_size_B: Optional[int] = None,
        batch_length_T: Optional[int] = None,
        n_step: Optional[Union[int, Tuple]] = None,
        beta: float = 0.0,
        gamma: float = 0.99,
        include_infos: bool = False,
        include_extra_model_outputs: bool = False,
    ) -> SampleBatchType:
        """Samples from a buffer in a prioritized way.

        This sampling method also adds (importance sampling) weights to
        the returned batch. See for prioritized sampling Schaul et al.
        (2016).

        Each sampled item defines a transition of the form:

        `(o_t, a_t, sum(r_(t+1:t+n+1)), o_(t+n), terminated_(t+n), truncated_(t+n))`

        where `o_(t+n)` is drawn by prioritized sampling, i.e. the priority
        of `o_(t+n)` led to the sample and defines the importance weight that
        is returned in the sample batch. `n` is defined by the `n_step` applied.

        If requested, `info`s of a transitions last timestep `t+n` are added to
        the batch.

        Args:
            num_items: Number of items (transitions) to sample from this
                buffer.
            batch_size_B: The number of rows (transitions) to return in the
                batch
            batch_length_T: THe sequence length to sample. At this point in time
                only sequences of length 1 are possible.
            n_step: The n-step to apply. For the default the batch contains in
                `"new_obs"` the observation and in `"obs"` the observation `n`
                time steps before. The reward will be the sum of rewards
                collected in between these two observations and the action will
                be the one executed n steps before such that we always have the
                state-action pair that triggered the rewards.
                If `n_step` is a tuple, it is considered as a range to sample
                from. If `None`, we use `n_step=1`.
            beta: The exponent of the importance sampling weight (see Schaul et
                al. (2016)). A `beta=0.0` does not correct for the bias introduced
                by prioritized replay and `beta=1.0` fully corrects for it.
            gamma: The discount factor to be used when applying n-step calculations.
                The default of `0.99` should be replaced by the `Algorithm`s
                discount factor.
            include_infos: A boolean indicating, if `info`s should be included in
                the batch. This could be of advantage, if the `info` contains
                values from the environment important for loss computation. If
                `True`, the info at the `"new_obs"` in the batch is included.
            include_extra_model_outputs: A boolean indicating, if
                `extra_model_outputs` should be included in the batch. This could be
                of advantage, if the `extra_mdoel_outputs`  contain outputs from the
                model important for loss computation and only able to compute with the
                actual state of model e.g. action log-probabilities, etc.). If `True`,
                the extra model outputs at the `"obs"` in the batch is included (the
                timestep at which the action is computed).

        Returns:
            A sample batch (observations, actions, rewards, new observations,
            terminateds, truncateds, weights) and if requested infos of dimension
            [B, 1].
        """
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

        # Sample the n-step if necessary.
        if isinstance(n_step, tuple):
            # Use random n-step sampling.
            random_n_step = True
        else:
            actual_n_step = n_step or 1
            random_n_step = False

        # Rows to return.
        observations = [[] for _ in range(batch_size_B)]
        next_observations = [[] for _ in range(batch_size_B)]
        actions = [[] for _ in range(batch_size_B)]
        rewards = [[] for _ in range(batch_size_B)]
        is_terminated = [False for _ in range(batch_size_B)]
        is_truncated = [False for _ in range(batch_size_B)]
        weights = [[] for _ in range(batch_size_B)]
        n_steps = [[] for _ in range(batch_size_B)]
        # If `info` should be included, construct also a container for them.
        if include_infos:
            infos = [[] for _ in range(batch_size_B)]
        # If `extra_model_outputs` should be included, construct a container for them.
        if include_extra_model_outputs:
            extra_model_outputs = [[] for _ in range(batch_size_B)]
        # Keep track of the indices that were sampled last for updating the
        # weights later (see `ray.rllib.utils.replay_buffer.utils.
        # update_priorities_in_episode_replay_buffer`).
        self._last_sampled_indices = []

        # Sample proportionally from replay buffer's segments using the weights.
        total_segment_sum = self._sum_segment.sum()
        p_min = self._min_segment.min() / total_segment_sum
        max_weight = (p_min * self.get_num_timesteps()) ** (-beta)
        B = 0
        while B < batch_size_B:
            # First, draw a random sample from Uniform(0, sum over all weights).
            # Note, transitions with higher weight get sampled more often (as
            # more random draws fall into larger intervals).
            random_sum = self.rng.random() * self._sum_segment.sum()
            # Get the highest index in the sum-tree for which the sum is
            # smaller or equal the random sum sample.
            # Note, we sample `o_(t + n_step)` as this is the state that
            # brought the information contained in the TD-error (see Schaul
            # et al. (2018), Algorithm 1).
            idx = self._sum_segment.find_prefixsum_idx(random_sum)
            # Get the theoretical probability mass for drawing this sample.
            p_sample = self._sum_segment[idx] / total_segment_sum
            # Compute the importance sampling weight.
            weight = (p_sample * self.get_num_timesteps()) ** (-beta)
            # Now, get the transition stored at this index.
            index_triple = self._indices[self._tree_idx_to_sample_idx[idx]]

            # Compute the actual episode index (offset by the number of
            # already evicted episodes)
            episode_idx, episode_ts = (
                index_triple[0] - self._num_episodes_evicted,
                index_triple[1],
            )
            episode = self.episodes[episode_idx]

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
            else:
                n_steps[B] = actual_n_step

            # Starting a new chunk.
            # Ensure that each row contains a tuple of the form:
            #   (o_t, a_t, sum(r_(t:t+n_step)), o_(t+n_step))
            # TODO (simon): Implement version for sequence sampling when using RNNs.
            eps_observations = episode.get_observations(
                slice(episode_ts - actual_n_step, episode_ts + 1)
            )
            # Note, the reward that is collected by transitioning from `o_t` to
            # `o_(t+1)` is stored in the next transition in `SingleAgentEpisode`.
            eps_rewards = episode.get_rewards(
                slice(episode_ts - actual_n_step, episode_ts)
            )
            observations[B] = eps_observations[0]
            next_observations[B] = eps_observations[-1]
            # Note, this will be the reward after executing action
            # `a_(episode_ts-n_step+1)`. For `n_step>1` this will be the sum of
            # all rewards that were collected over the last n steps.
            rewards[B] = scipy.signal.lfilter(
                [1], [1, -gamma], eps_rewards[::-1], axis=0
            )[-1]
            # Note, `SingleAgentEpisode` stores the action that followed
            # `o_t` with `o_(t+1)`, therefore, we need the next one.
            actions[B] = episode.get_actions(episode_ts - actual_n_step)
            if include_infos:
                # If infos are included we include the ones from the last timestep
                # as usually the info contains additional values about the last state.
                infos[B] = episode.get_infos(episode_ts)
            if include_extra_model_outputs:
                # If `extra_model_outputs` are included we include the ones from the
                # first timestep as usually the `extra_model_outputs` contain additional
                # values from the forward pass that produced the action at the first
                # timestep.
                # Note, we extract them into single row dictionaries similar to the
                # infos, in a connector we can then extract these into single batch
                # rows.
                extra_model_outputs[B] = {
                    k: episode.get_extra_model_outputs(k, episode_ts - actual_n_step)
                    for k in episode.extra_model_outputs.keys()
                }

            # If the sampled time step is the episode's last time step check, if
            # the episode is terminated or truncated.
            if episode_ts == episode.t:
                is_terminated[B] = episode.is_terminated
                is_truncated[B] = episode.is_truncated

            # TODO (simon): Check, if we have to correct here for sequences
            # later.
            actual_size = 1
            weights[B] = weight / max_weight * actual_size

            # Increment counter.
            B += 1

            # Keep track of sampled indices for updating priorities later.
            self._last_sampled_indices.append(idx)

        self.sampled_timesteps += batch_size_B

        # TODO Return SampleBatch instead of this simpler dict.
        # TODO (simon): Check, if for stateful modules we want to sample
        # here the sequences. If not remove the double list for obs.
        ret = {
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
            ret.update(
                {
                    Columns.INFOS: infos,
                }
            )
        # Include extra model outputs, if necessary.
        if include_extra_model_outputs:
            ret.update(
                # These could be complex, too.
                batch(extra_model_outputs)
            )

        return ret

    # TODO (simon): Adjust docstring.
    def sample_with_keys(
        self,
        num_items: Optional[int] = None,
        *,
        batch_size_B: Optional[int] = None,
        batch_length_T: Optional[int] = None,
        n_step: Optional[Union[int, Tuple]] = None,
        beta: float = 0.0,
        gamma: float = 0.99,
        include_infos: bool = False,
        include_extra_model_outputs: bool = False,
    ) -> SampleBatchType:
        """Samples from a buffer in a prioritized way.

        This sampling method also adds (importance sampling) weights to
        the returned batch. See for prioritized sampling Schaul et al.
        (2016).

        Each sampled item defines a transition of the form:

        `(o_t, a_t, sum(r_(t+1:t+n+1)), o_(t+n), terminated_(t+n), truncated_(t+n))`

        where `o_(t+n)` is drawn by prioritized sampling, i.e. the priority
        of `o_(t+n)` led to the sample and defines the importance weight that
        is returned in the sample batch. `n` is defined by the `n_step` applied.

        If requested, `info`s of a transitions last timestep `t+n` are added to
        the batch.

        Args:
            num_items: Number of items (transitions) to sample from this
                buffer.
            batch_size_B: The number of rows (transitions) to return in the
                batch
            n_step: The n-step to apply. For the default the batch contains in
                `"new_obs"` the observation and in `"obs"` the observation `n`
                time steps before. The reward will be the sum of rewards
                collected in between these two observations and the action will
                be the one executed n steps before such that we always have the
                state-action pair that triggered the rewards.
                If `n_step` is a tuple, it is considered as a range to sample
                from. If `None`, we use `n_step=1`.
            beta: The exponent of the importance sampling weight (see Schaul et
                al. (2016)). A `beta=0.0` does not correct for the bias introduced
                by prioritized replay and `beta=1.0` fully corrects for it.
            gamma: The discount factor to be used when applying n-step caluclations.
                The default of `0.99` should be replaced by the `Algorithm`s
                discount factor.
            include_infos: A boolean indicating, if `info`s should be included in
                the batch. This could be of advantage, if the `info` contains
                values from the environment important for loss computation. If
                `True`, the info at the `"new_obs"` in the batch is included.
            include_extra_model_outputs: A boolean indicating, if
                `extra_model_outputs` should be included in the batch. This could be
                of advantage, if the `extra_mdoel_outputs`  contain outputs from the
                model important for loss computation and only able to compute with the
                actual state of model e.g. action log-probabilities, etc.). If `True`,
                the extra model outputs at the `"obs"` in the batch is included (the
                timestep at which the action is computed).

        Returns:
            A sample batch (observations, actions, rewards, new observations,
            terminateds, truncateds, weights) and if requested infos and extra model
            outputs. Extra model outputs are extracted to single columns in the batch
            and infos are kept as a list of dictionaries. The batch keys are the episode
            ids.
        """
        assert beta >= 0.0

        if num_items is not None:
            assert batch_size_B is None, (
                "Cannot call `sample()` with both `num_items` and `batch_size_B` "
                "provided! Use either one."
            )
            batch_size_B = num_items

        # Use our default values if no sizes/lengths provided.
        batch_size_B = batch_size_B or self.batch_size_B
        batch_length_T = batch_length_T or self.batch_length_T

        # Sample the n-step if necessary.
        if isinstance(n_step, tuple):
            # Use random n-step sampling.
            random_n_step = True
        else:
            actual_n_step = n_step or 1
            random_n_step = False

        # Columns to return.
        observations = {}
        next_observations = {}
        actions = {}
        rewards = {}
        is_terminated = {}
        is_truncated = {}
        weights = {}
        n_steps = {}
        # If `info` should be included, construct also a container for them.
        if include_infos:
            infos = {}
        # If `extra_model_outputs` should be included, construct a container for them.
        if include_extra_model_outputs:
            # Get the keys from an episode in the buffer.
            # TODO (simon, sven): What happens, if different episodes have different
            # extra model outputs or some are missing?
            extra_model_outputs = {
                k: {} for k in self.episodes[0].extra_model_outputs.keys()
            }
        # Keep track of the indices that were sampled last for updating the
        # weights later (see `ray.rllib.utils.replay_buffer.utils.
        # update_priorities_in_episode_replay_buffer`).
        self._last_sampled_indices = []

        # Sample proportionally from replay buffer's segments using the weights.
        total_segment_sum = self._sum_segment.sum()
        p_min = self._min_segment.min() / total_segment_sum
        max_weight = (p_min * self.get_num_timesteps()) ** (-beta)
        B = 0
        while B < batch_size_B:
            # First, draw a random sample from Uniform(0, sum over all weights).
            # Note, transitions with higher weight get sampled more often (as
            # more random draws fall into larger intervals).
            random_sum = self.rng.random() * self._sum_segment.sum(0, self._max_idx + 1)
            # Get the highest index in the sum-tree for which the sum is
            # smaller or equal the random sum sample.
            # Note, we sample `o_(t + n_step)` as this is the state that
            # brought the information contained in the TD-error (see Schaul
            # et al. (2018), Algorithm 1).
            idx = self._sum_segment.find_prefixsum_idx(random_sum)
            # Get the theoretical probability mass for drawing this sample.
            p_sample = self._sum_segment[idx] / total_segment_sum
            # Compute the importance sampling weight.
            weight = (p_sample * self.get_num_timesteps()) ** (-beta)
            # Now, get the transition stored at this index.
            index_triple = self._indices[self._tree_idx_to_sample_idx[idx]]

            # Compute the actual episode index (offset by the number of
            # already evicted episodes)
            episode_idx, episode_ts = (
                index_triple[0] - self._num_episodes_evicted,
                index_triple[1],
            )
            episode = self.episodes[episode_idx]

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

            # Starting a new chunk.
            # Ensure that each row contains a tuple of the form:
            #   (o_t, a_t, sum(r_(t:t+n_step)), o_(t+n_step))
            # TODO (simon): Implement version for sequence sampling when using RNNs.
            eps_observations = episode.get_observations(
                slice(episode_ts - actual_n_step, episode_ts + 1)
            )
            # Note, the reward that is collected by transitioning from `o_t` to
            # `o_(t+1)` is stored in the next transition in `SingleAgentEpisode`.
            eps_rewards = episode.get_rewards(
                slice(episode_ts - actual_n_step, episode_ts)
            )
            if (episode.id_,) not in observations:
                # Add the key to all containers.
                observations[(episode.id_,)] = []
                next_observations[(episode.id_,)] = []
                actions[(episode.id_,)] = []
                rewards[(episode.id_,)] = []
                is_terminated[(episode.id_,)] = []
                is_truncated[(episode.id_,)] = []
                weights[(episode.id_,)] = []
                n_steps[(episode.id_,)] = []
                if include_infos:
                    infos[(episode.id_,)] = []
                if include_extra_model_outputs:
                    # 'extra_model_outputs` has a structure
                    # `{"output_1": {(eps_id0,): [0.4, 2.3], ...}, ...}``
                    for k in extra_model_outputs:
                        extra_model_outputs[k][(episode.id_,)] = []

            # Add the `n_step` used for this item.
            n_steps[(episode.id_,)].append(actual_n_step)

            observations[(episode.id_,)].append(eps_observations[0])
            next_observations[(episode.id_,)].append(eps_observations[-1])
            # Note, this will be the reward after executing action
            # `a_(episode_ts-n_step+1)`. For `n_step>1` this will be the sum of
            # all rewards that were collected over the last n steps.
            rewards[(episode.id_,)].append(
                scipy.signal.lfilter([1], [1, -gamma], eps_rewards[::-1], axis=0)[-1]
            )
            # Note, `SingleAgentEpisode` stores the action that followed
            # `o_t` with `o_(t+1)`, therefore, we need the next one.
            actions[(episode.id_,)].append(
                episode.get_actions(episode_ts - actual_n_step)
            )
            if include_infos:
                # If infos are included we include the ones from the last timestep
                # as usually the info contains additional values about the last state.
                infos[(episode.id_,)].append(episode.get_infos(episode_ts))
            if include_extra_model_outputs:
                # If `extra_model_outputs` are included we include the ones from the
                # first timestep as usually the `extra_model_outputs` contain additional
                # values from the forward pass that produced the action at the first
                # timestep.
                for k in extra_model_outputs:
                    extra_model_outputs[k][(episode.id_,)].append(
                        episode.get_extra_model_outputs(k, episode_ts - actual_n_step)
                    )

            # If the sampled time step is the episode's last time step check, if
            # the episode is terminated or truncated.
            if episode_ts == episode.t:
                is_terminated[(episode.id_,)].append(episode.is_terminated)
                is_truncated[(episode.id_,)].append(episode.is_truncated)
            else:
                is_terminated[(episode.id_,)].append(False)
                is_truncated[(episode.id_,)].append(False)

            # TODO (simon): Check, if we have to correct here for sequences
            # later.
            actual_size = 1
            weights[(episode.id_,)].append(weight / max_weight * actual_size)

            # Increment counter.
            B += 1

            # Keep track of sampled indices for updating priorities later.
            self._last_sampled_indices.append(idx)

        self.sampled_timesteps += batch_size_B

        # TODO Return SampleBatch instead of this simpler dict.
        ret = {
            Columns.OBS: observations,
            Columns.ACTIONS: actions,
            Columns.REWARDS: rewards,
            Columns.NEXT_OBS: next_observations,
            Columns.TERMINATEDS: is_terminated,
            Columns.TRUNCATEDS: is_truncated,
            "weights": weights,
            "n_steps": n_steps,
        }
        # Include infos if necessary.
        if include_infos:
            ret.update(
                {
                    Columns.INFOS: infos,
                }
            )
        # Include extra model outputs, if necessary.
        if include_extra_model_outputs:
            ret.update(extra_model_outputs)

        return ret

    @override(EpisodeReplayBuffer)
    def get_state(self) -> Dict[str, Any]:
        """Gets the state of a `PrioritizedEpisodeReplayBuffer`.

        Returns:
            A state dict that can be stored in a checkpoint.
        """
        # Get super's state.
        state = super().get_state()
        # Add additional attributes.
        state.update(
            {
                "_sum_segment": self._sum_segment.get_state(),
                "_min_segment": self._min_segment.get_state(),
                "_free_nodes": list(self._free_nodes),
                "_max_priority": self._max_priority,
                "_tree_idx_to_sample_idx": list(self._tree_idx_to_sample_idx.items()),
                # TODO (sven, simon): Do we need these?
                "_last_sampled_indices": self._last_sampled_indices,
            }
        )
        return state

    @override(EpisodeReplayBuffer)
    def set_state(self, state) -> None:
        """Sets the state of a `PrioritizedEpisodeReplayBuffer`.

        Args:
            state: A buffer state stored (usually stored in a checkpoint).
        """
        # Set super's state.
        super().set_state()
        # Set additional attributes.
        self._sum_segment.set_state(state["_sum_segment"])
        self._min_segment.set_state(state["_min_segment"])
        self._free_nodes = deque(state["_free_nodes"])
        self._max_priority = state["_max_priority"]
        self._tree_idx_to_sample_idx = dict(state["_tree_idx_to_sample_idx"])
        # TODO (sven, simon): Do we need these?
        self._last_sampled_indices = state["_last_sampled_indices"]

    def update_priorities(self, priorities: NDArray) -> None:
        """Update the priorities of items at corresponding indices.

        Usually, incoming priorities are TD-errors.

        Args:
            priorities: Numpy array containing the new priorities to be used
                in sampling for the items in the last sampled batch.
        """
        assert len(priorities) == len(self._last_sampled_indices)

        for idx, priority in zip(self._last_sampled_indices, priorities):
            # Note, TD-errors come in as absolute values or results from
            # cross-entropy loss calculations.
            # assert priority > 0, f"priority was {priority}"
            priority = max(priority, 1e-12)
            assert 0 <= idx < self._sum_segment.capacity
            # TODO (simon): Create metrics.
            # delta = priority**self._alpha - self._sum_segment[idx]
            # Update the priorities in the segment trees.
            self._sum_segment[idx] = priority**self._alpha
            self._min_segment[idx] = priority**self._alpha
            # Update the maximal priority.
            self._max_priority = max(self._max_priority, priority)

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
        # Return the index.
        return idx

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
