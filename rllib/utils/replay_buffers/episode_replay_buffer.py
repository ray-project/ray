from collections import deque
import copy
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import scipy

from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.annotations import override
from ray.rllib.utils.replay_buffers.base import ReplayBufferInterface
from ray.rllib.utils.typing import SampleBatchType
from ray.rllib.utils import force_list


class EpisodeReplayBuffer(ReplayBufferInterface):
    """Buffer that stores (completed or truncated) episodes by their ID.

    Each "row" (a slot in a deque) in the buffer is occupied by one episode. If an
    incomplete episode is added to the buffer and then another chunk of that episode is
    added at a later time, the buffer will automatically concatenate the new fragment to
    the original episode. This way, episodes can be completed via subsequent `add`
    calls.

    Sampling returns batches of size B (number of "rows"), where each row is a
    trajectory of length T. Each trajectory contains consecutive timesteps from an
    episode, but might not start at the beginning of that episode. Should an episode end
    within such a trajectory, a random next episode (starting from its t0) will be
    concatenated to that "row". Example: `sample(B=2, T=4)` ->

       0 .. 1 .. 2 .. 3  <- T-axis
    0 e5   e6   e7   e8
    1 f2   f3   h0   h2
    ^ B-axis

    .. where e, f, and h are different (randomly picked) episodes, the 0-index (e.g. h0)
    indicates the start of an episode, and `f3` is an episode end (gym environment
    returned terminated=True or truncated=True).

    0-indexed returned timesteps contain the reset observation, a dummy 0.0 reward, as
    well as the first action taken in the episode (action picked after observing
    obs(0)).
    The last index in an episode (e.g. f3 in the example above) contains the final
    observation of the episode, the final reward received, a dummy action
    (repeat the previous action), as well as either terminated=True or truncated=True.
    """

    __slots__ = (
        "capacity",
        "batch_size_B",
        "batch_length_T",
        "episodes",
        "episode_id_to_index",
        "num_episodes_evicted",
        "_indices",
        "_num_timesteps",
        "_num_timesteps_added",
        "sampled_timesteps",
        "rng",
    )

    def __init__(
        self,
        capacity: int = 10000,
        *,
        batch_size_B: int = 16,
        batch_length_T: int = 64,
    ):
        """Initializes an EpisodeReplayBuffer instance.

        Args:
            capacity: The total number of timesteps to be storable in this buffer.
                Will start ejecting old episodes once this limit is reached.
            batch_size_B: The number of rows in a SampleBatch returned from `sample()`.
            batch_length_T: The length of each row in a SampleBatch returned from
                `sample()`.
        """
        self.capacity = capacity
        self.batch_size_B = batch_size_B
        self.batch_length_T = batch_length_T

        # The actual episode buffer. We are using a deque here for faster insertion
        # (left side) and eviction (right side) of data.
        self.episodes = deque()
        # Maps (unique) episode IDs to the index under which to find this episode
        # within our `self.episodes` deque.
        # Note that even after eviction started, the indices in here will NOT be
        # changed. We will therefore need to offset all indices in
        # `self.episode_id_to_index` by the number of episodes that have already been
        # evicted (self._num_episodes_evicted) in order to get the actual index to use
        # on `self.episodes`.
        self.episode_id_to_index = {}
        # The number of episodes that have already been evicted from the buffer
        # due to reaching capacity.
        self._num_episodes_evicted = 0

        # List storing all index tuples: (eps_idx, ts_in_eps_idx), where ...
        # `eps_idx - self._num_episodes_evicted' is the index into self.episodes.
        # `ts_in_eps_idx` is the timestep index within that episode
        #  (0 = 1st timestep, etc..).
        # We sample uniformly from the set of these indices in a `sample()`
        # call.
        self._indices = []

        # The size of the buffer in timesteps.
        self._num_timesteps = 0
        # The number of timesteps added thus far.
        self._num_timesteps_added = 0

        # How many timesteps have been sampled from the buffer in total?
        self.sampled_timesteps = 0

        self.rng = np.random.default_rng(seed=None)

    @override(ReplayBufferInterface)
    def __len__(self) -> int:
        return self.get_num_timesteps()

    @override(ReplayBufferInterface)
    def add(self, episodes: Union[List["SingleAgentEpisode"], "SingleAgentEpisode"]):
        """Converts the incoming SampleBatch into a number of SingleAgentEpisode objects.

        Then adds these episodes to the internal deque.
        """
        episodes = force_list(episodes)

        for eps in episodes:
            # Make sure we don't change what's coming in from the user.
            # TODO (sven): It'd probably be better to make sure in the EnvRunner to not
            #  hold on to episodes (for metrics purposes only) that we are returning
            #  back to the user from `EnvRunner.sample()`. Then we wouldn't have to
            #  do any copying. Instead, either compile the metrics right away on the
            #  EnvRunner OR compile metrics entirely on the Algorithm side (this is
            #  actually preferred).
            eps = copy.deepcopy(eps)

            self._num_timesteps += len(eps)
            self._num_timesteps_added += len(eps)

            # Ongoing episode, concat to existing record.
            if eps.id_ in self.episode_id_to_index:
                eps_idx = self.episode_id_to_index[eps.id_]
                existing_eps = self.episodes[eps_idx - self._num_episodes_evicted]
                old_len = len(existing_eps)
                self._indices.extend([(eps_idx, old_len + i) for i in range(len(eps))])
                existing_eps.concat_episode(eps)
            # New episode. Add to end of our episodes deque.
            else:
                self.episodes.append(eps)
                eps_idx = len(self.episodes) - 1 + self._num_episodes_evicted
                self.episode_id_to_index[eps.id_] = eps_idx
                self._indices.extend([(eps_idx, i) for i in range(len(eps))])

            # Eject old records from front of deque (only if we have more than 1 episode
            # in the buffer).
            while self._num_timesteps > self.capacity and self.get_num_episodes() > 1:
                # Eject oldest episode.
                evicted_eps = self.episodes.popleft()
                evicted_eps_len = len(evicted_eps)
                # Correct our size.
                self._num_timesteps -= evicted_eps_len

                # Erase episode from all our indices:
                # 1) Main episode index.
                evicted_idx = self.episode_id_to_index[evicted_eps.id_]
                del self.episode_id_to_index[evicted_eps.id_]
                # 2) All timestep indices that this episode owned.
                new_indices = []  # New indices that will replace self._indices.
                idx_cursor = 0
                # Loop through all (eps_idx, ts_in_eps_idx)-tuples.
                for i, idx_tuple in enumerate(self._indices):
                    # This tuple is part of the evicted episode -> Add everything
                    # up until here to `new_indices` (excluding this very index, b/c
                    # it's already part of the evicted episode).
                    if idx_cursor is not None and idx_tuple[0] == evicted_idx:
                        new_indices.extend(self._indices[idx_cursor:i])
                        # Set to None to indicate we are in the eviction zone.
                        idx_cursor = None
                    # We are/have been in the eviction zone (i pointing/pointed to the
                    # evicted episode) ..
                    elif idx_cursor is None:
                        # ... but are now not anymore (i is now an index into a
                        # non-evicted episode) -> Set cursor to valid int again.
                        if idx_tuple[0] != evicted_idx:
                            idx_cursor = i
                            # But early-out if evicted episode was only 1 single
                            # timestep long.
                            if evicted_eps_len == 1:
                                break
                        # Early-out: We reached the end of the to-be-evicted episode.
                        # We can stop searching further here (all following tuples
                        # will NOT be in the evicted episode).
                        elif idx_tuple[1] == evicted_eps_len - 1:
                            assert self._indices[i + 1][0] != idx_tuple[0]
                            idx_cursor = i + 1
                            break

                # Jump over (splice-out) the evicted episode if we are still in the
                # eviction zone.
                if idx_cursor is not None:
                    new_indices.extend(self._indices[idx_cursor:])

                # Reset our `self._indices` to the newly compiled list.
                self._indices = new_indices

                # Increase episode evicted counter.
                self._num_episodes_evicted += 1

    @override(ReplayBufferInterface)
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
        sample_episodes: Optional[bool] = False,
        finalize: bool = False,
        **kwargs,
    ) -> Union[SampleBatchType, SingleAgentEpisode]:
        """Samples from a buffer in a randomized way.

        Each sampled item defines a transition of the form:

        `(o_t, a_t, sum(r_(t+1:t+n+1)), o_(t+n), terminated_(t+n), truncated_(t+n))`

        where `o_t` is drawn by randomized sampling.`n` is defined by the `n_step`
        applied.

        If requested, `info`s of a transitions last timestep `t+n` and respective
        extra model outputs (e.g. action log-probabilities) are added to
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
            finalize: If episodes should be finalized.

        Returns:
            Either a batch with transitions in each row or (if `return_episodes=True`)
            a list of 1-step long episodes containing all basic episode data and if
            requested infos and extra model outputs.
        """

        if sample_episodes:
            return self._sample_episodes(
                num_items=num_items,
                batch_size_B=batch_size_B,
                batch_length_T=batch_length_T,
                n_step=n_step,
                beta=beta,
                gamma=gamma,
                include_infos=include_infos,
                include_extra_model_outputs=include_extra_model_outputs,
                finalize=finalize,
            )
        else:
            return self._sample_batch(
                num_items=num_items,
                batch_size_B=batch_size_B,
                batch_length_T=batch_length_T,
            )

    def _sample_batch(
        self,
        num_items: Optional[int] = None,
        *,
        batch_size_B: Optional[int] = None,
        batch_length_T: Optional[int] = None,
    ) -> SampleBatchType:
        """Returns a batch of size B (number of "rows"), where each row has length T.

        Each row contains consecutive timesteps from an episode, but might not start
        at the beginning of that episode. Should an episode end within such a
        row (trajectory), a random next episode (starting from its t0) will be
        concatenated to that row. For more details, see the docstring of the
        EpisodeReplayBuffer class.

        Args:
            num_items: See `batch_size_B`. For compatibility with the
                `ReplayBufferInterface` abstract base class.
            batch_size_B: The number of rows (trajectories) to return in the batch.
            batch_length_T: The length of each row (in timesteps) to return in the
                batch.

        Returns:
            The sampled batch (observations, actions, rewards, terminateds, truncateds)
                of dimensions [B, T, ...].
        """
        if num_items is not None:
            assert batch_size_B is None, (
                "Cannot call `sample()` with both `num_items` and `batch_size_B` "
                "provided! Use either one."
            )
            batch_size_B = num_items

        # Use our default values if no sizes/lengths provided.
        batch_size_B = batch_size_B or self.batch_size_B
        batch_length_T = batch_length_T or self.batch_length_T

        # Rows to return.
        observations = [[] for _ in range(batch_size_B)]
        actions = [[] for _ in range(batch_size_B)]
        rewards = [[] for _ in range(batch_size_B)]
        is_first = [[False] * batch_length_T for _ in range(batch_size_B)]
        is_last = [[False] * batch_length_T for _ in range(batch_size_B)]
        is_terminated = [[False] * batch_length_T for _ in range(batch_size_B)]
        is_truncated = [[False] * batch_length_T for _ in range(batch_size_B)]

        B = 0
        T = 0
        while B < batch_size_B:
            # Pull a new uniform random index tuple: (eps_idx, ts_in_eps_idx).
            index_tuple = self._indices[self.rng.integers(len(self._indices))]

            # Compute the actual episode index (offset by the number of
            # already evicted episodes).
            episode_idx, episode_ts = (
                index_tuple[0] - self._num_episodes_evicted,
                index_tuple[1],
            )
            episode = self.episodes[episode_idx]

            # Starting a new chunk, set is_first to True.
            is_first[B][T] = True

            # Begin of new batch item (row).
            if len(rewards[B]) == 0:
                # And we are at the start of an episode: Set reward to 0.0.
                if episode_ts == 0:
                    rewards[B].append(0.0)
                # We are in the middle of an episode: Set reward to the previous
                # timestep's values.
                else:
                    rewards[B].append(episode.rewards[episode_ts - 1])
            # We are in the middle of a batch item (row). Concat next episode to this
            # row from the next episode's beginning. In other words, we never concat
            # a middle of an episode to another truncated one.
            else:
                episode_ts = 0
                rewards[B].append(0.0)

            observations[B].extend(episode.observations[episode_ts:])
            # Repeat last action to have the same number of actions than observations.
            actions[B].extend(episode.actions[episode_ts:])
            actions[B].append(episode.actions[-1])
            # Number of rewards are also the same as observations b/c we have the
            # initial 0.0 one.
            rewards[B].extend(episode.rewards[episode_ts:])
            assert len(observations[B]) == len(actions[B]) == len(rewards[B])

            T = min(len(observations[B]), batch_length_T)

            # Set is_last=True.
            is_last[B][T - 1] = True
            # If episode is terminated and we have reached the end of it, set
            # is_terminated=True.
            if episode.is_terminated and T == len(observations[B]):
                is_terminated[B][T - 1] = True
            # If episode is truncated and we have reached the end of it, set
            # is_truncated=True.
            elif episode.is_truncated and T == len(observations[B]):
                is_truncated[B][T - 1] = True

            # We are done with this batch row.
            if T == batch_length_T:
                # We may have overfilled this row: Clip trajectory at the end.
                observations[B] = observations[B][:batch_length_T]
                actions[B] = actions[B][:batch_length_T]
                rewards[B] = rewards[B][:batch_length_T]
                # Start filling the next row.
                B += 1
                T = 0

        # Update our sampled counter.
        self.sampled_timesteps += batch_size_B * batch_length_T

        # TODO: Return SampleBatch instead of this simpler dict.
        ret = {
            "obs": np.array(observations),
            "actions": np.array(actions),
            "rewards": np.array(rewards),
            "is_first": np.array(is_first),
            "is_last": np.array(is_last),
            "is_terminated": np.array(is_terminated),
            "is_truncated": np.array(is_truncated),
        }

        return ret

    def _sample_episodes(
        self,
        num_items: Optional[int] = None,
        *,
        batch_size_B: Optional[int] = None,
        batch_length_T: Optional[int] = None,
        n_step: Optional[Union[int, Tuple]] = None,
        gamma: float = 0.99,
        include_infos: bool = False,
        include_extra_model_outputs: bool = False,
        finalize: bool = False,
        **kwargs,
    ) -> List[SingleAgentEpisode]:
        """Samples episodes from a buffer in a randomized way.

        Each sampled item defines a transition of the form:

        `(o_t, a_t, sum(r_(t+1:t+n+1)), o_(t+n), terminated_(t+n), truncated_(t+n))`

        where `o_t` is drawn by randomized sampling.`n` is defined by the `n_step`
        applied.

        If requested, `info`s of a transitions last timestep `t+n` and respective
        extra model outputs (e.g. action log-probabilities) are added to
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
            finalize: If episodes should be finalized.

        Returns:
            A list of 1-step long episodes containing all basic episode data and if
            requested infos and extra model outputs.
        """
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
        actual_n_step = n_step or 1
        random_n_step = isinstance(n_step, tuple)

        # Keep track of the indices that were sampled last for updating the
        # weights later (see `ray.rllib.utils.replay_buffer.utils.
        # update_priorities_in_episode_replay_buffer`).
        self._last_sampled_indices = []

        sampled_episodes = []

        B = 0
        while B < batch_size_B:
            # Pull a new uniform random index tuple: (eps_idx, ts_in_eps_idx).
            index_tuple = self._indices[self.rng.integers(len(self._indices))]

            # Compute the actual episode index (offset by the number of
            # already evicted episodes).
            episode_idx, episode_ts = (
                index_tuple[0] - self._num_episodes_evicted,
                index_tuple[1],
            )
            episode = self.episodes[episode_idx]

            # If we use random n-step sampling, draw the n-step for this item.
            if random_n_step:
                actual_n_step = int(self.rng.integers(n_step[0], n_step[1]))

            # Skip, if we are too far to the end and `episode_ts` + n_step would go
            # beyond the episode's end.
            if episode_ts + actual_n_step > len(episode):
                continue

            # Note, this will be the reward after executing action
            # `a_(episode_ts-n_step+1)`. For `n_step>1` this will be the discounted
            # sum of all discounted rewards that were collected over the last n steps.
            raw_rewards = episode.get_rewards(
                slice(episode_ts, episode_ts + actual_n_step)
            )
            rewards = scipy.signal.lfilter([1], [1, -gamma], raw_rewards[::-1], axis=0)[
                -1
            ]

            # Generate the episode to be returned.
            sampled_episode = SingleAgentEpisode(
                # Ensure that each episode contains a tuple of the form:
                #   (o_t, a_t, sum(r_(t:t+n_step)), o_(t+n_step))
                # Two observations (t and t+n).
                observations=[
                    episode.get_observations(episode_ts),
                    episode.get_observations(episode_ts + actual_n_step),
                ],
                observation_space=episode.observation_space,
                infos=(
                    [
                        episode.get_infos(episode_ts),
                        episode.get_infos(episode_ts + actual_n_step),
                    ]
                    if include_infos
                    else None
                ),
                actions=[episode.get_actions(episode_ts)],
                action_space=episode.action_space,
                rewards=[rewards],
                # If the sampled time step is the episode's last time step check, if
                # the episode is terminated or truncated.
                terminated=(
                    False
                    if episode_ts + actual_n_step < len(episode)
                    else episode.is_terminated
                ),
                truncated=(
                    False
                    if episode_ts + actual_n_step < len(episode)
                    else episode.is_truncated
                ),
                extra_model_outputs={
                    # TODO (simon): Check, if we have to correct here for sequences
                    #  later.
                    "n_step": [actual_n_step],
                    **(
                        {
                            k: [episode.get_extra_model_outputs(k, episode_ts)]
                            for k in episode.extra_model_outputs.keys()
                        }
                        if include_extra_model_outputs
                        else {}
                    ),
                },
                # TODO (sven): Support lookback buffers.
                len_lookback_buffer=0,
                t_started=episode_ts,
            )
            if finalize:
                sampled_episode.finalize()
            sampled_episodes.append(sampled_episode)

            # Increment counter.
            B += 1

        self.sampled_timesteps += batch_size_B

        return sampled_episodes

    def get_num_episodes(self) -> int:
        """Returns number of episodes (completed or truncated) stored in the buffer."""
        return len(self.episodes)

    def get_num_episodes_evicted(self) -> int:
        """Returns number of episodes that have been evicted from the buffer."""
        return self._num_episodes_evicted

    def get_num_timesteps(self) -> int:
        """Returns number of individual timesteps stored in the buffer."""
        return len(self._indices)

    def get_sampled_timesteps(self) -> int:
        """Returns number of timesteps that have been sampled in buffer's lifetime."""
        return self.sampled_timesteps

    def get_added_timesteps(self) -> int:
        """Returns number of timesteps that have been added in buffer's lifetime."""
        return self._num_timesteps_added

    @override(ReplayBufferInterface)
    def get_state(self) -> Dict[str, Any]:
        """Gets a pickable state of the buffer.

        This is used for checkpointing the buffer's state. It is specifically helpful,
        for example, when a trial is paused and resumed later on. The buffer's state
        can be saved to disk and reloaded when the trial is resumed.

        Returns:
            A dict containing all necessary information to restore the buffer's state.
        """
        return {
            "episodes": [eps.get_state() for eps in self.episodes],
            "episode_id_to_index": list(self.episode_id_to_index.items()),
            "_num_episodes_evicted": self._num_episodes_evicted,
            "_indices": self._indices,
            "_num_timesteps": self._num_timesteps,
            "_num_timesteps_added": self._num_timesteps_added,
            "sampled_timesteps": self.sampled_timesteps,
        }

    @override(ReplayBufferInterface)
    def set_state(self, state) -> None:
        """Sets the state of a buffer from a previously stored state.

        See `get_state()` for more information on what is stored in the state. This
        method is used to restore the buffer's state from a previously stored state.
        It is specifically helpful, for example, when a trial is paused and resumed
        later on. The buffer's state can be saved to disk and reloaded when the trial
        is resumed.

        Args:
            state: The state to restore the buffer from.
        """
        self._set_episodes(state)
        self.episode_id_to_index = dict(state["episode_id_to_index"])
        self._num_episodes_evicted = state["_num_episodes_evicted"]
        self._indices = state["_indices"]
        self._num_timesteps = state["_num_timesteps"]
        self._num_timesteps_added = state["_num_timesteps_added"]
        self.sampled_timesteps = state["sampled_timesteps"]

    def _set_episodes(self, state) -> None:
        """Sets the episodes from the state.

        Note, this method is used for class inheritance purposes. It is specifically
        helpful when a subclass of this class wants to override the behavior of how
        episodes are set from the state. By default, it sets `SingleAgentEpuisode`s,
        but subclasses can override this method to set episodes of a different type.
        """
        if not self.episodes:
            self.episodes = deque(
                [
                    SingleAgentEpisode.from_state(eps_data)
                    for eps_data in state["episodes"]
                ]
            )
