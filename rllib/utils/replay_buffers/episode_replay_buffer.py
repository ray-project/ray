from collections import deque
import copy
from typing import Any, Dict, List, Optional, Union
import uuid

import numpy as np

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.replay_buffers.base import ReplayBufferInterface
from ray.rllib.utils.typing import SampleBatchType


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
    def add(self, episodes: Union[List["_Episode"], "_Episode"]):
        """Converts the incoming SampleBatch into a number of _Episode objects.

        Then adds these episodes to the internal deque.
        """
        if isinstance(episodes, _Episode):
            episodes = [episodes]

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

    def get_num_episodes(self) -> int:
        """Returns number of episodes (completed or truncated) stored in the buffer."""
        return len(self.episodes)

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
        self.episodes = deque(
            [_Episode.from_state(eps_data) for eps_data in state["episodes"]]
        )
        self.episode_id_to_index = dict(state["episode_id_to_index"])
        self._num_episodes_evicted = state["_num_episodes_evicted"]
        self._indices = state["_indices"]
        self._num_timesteps = state["_num_timesteps"]
        self._num_timesteps_added = state["_num_timesteps_added"]
        self.sampled_timesteps = state["sampled_timesteps"]


# TODO (sven): Make this EpisodeV3 - replacing EpisodeV2 - to reduce all the
#  information leakage we currently have in EpisodeV2 (policy_map, worker, etc.. are
#  all currently held by EpisodeV2 for no good reason).
class _Episode:
    def __init__(
        self,
        id_: Optional[str] = None,
        *,
        observations=None,
        actions=None,
        rewards=None,
        states=None,
        t: int = 0,
        is_terminated: bool = False,
        is_truncated: bool = False,
        render_images=None,
    ):
        self.id_ = id_ or uuid.uuid4().hex
        # Observations: t0 (initial obs) to T.
        self.observations = [] if observations is None else observations
        # Actions: t1 to T.
        self.actions = [] if actions is None else actions
        # Rewards: t1 to T.
        self.rewards = [] if rewards is None else rewards
        # h-states: t0 (in case this episode is a continuation chunk, we need to know
        # about the initial h) to T.
        self.states = states
        # The global last timestep of the episode and the timesteps when this chunk
        # started.
        self.t = self.t_started = t
        # obs[-1] is the final observation in the episode.
        self.is_terminated = is_terminated
        # obs[-1] is the last obs in a truncated-by-the-env episode (there will no more
        # observations in following chunks for this episode).
        self.is_truncated = is_truncated
        # RGB uint8 images from rendering the env; the images include the corresponding
        # rewards.
        assert render_images is None or observations is not None
        self.render_images = [] if render_images is None else render_images

    def concat_episode(self, episode_chunk: "_Episode"):
        """Adds the given `episode_chunk` to the right side of self."""
        assert episode_chunk.id_ == self.id_
        assert not self.is_done
        # Make sure the timesteps match.
        assert self.t == episode_chunk.t_started

        episode_chunk.validate()

        # Make sure, end matches other episode chunk's beginning.
        assert np.all(episode_chunk.observations[0] == self.observations[-1])
        # Make sure the timesteps match (our last t should be the same as their first).
        assert self.t == episode_chunk.t_started
        # Pop out our end.
        self.observations.pop()

        # Extend ourselves. In case, episode_chunk is already terminated (and numpyfied)
        # we need to convert to lists (as we are ourselves still filling up lists).
        self.observations.extend(list(episode_chunk.observations))
        self.actions.extend(list(episode_chunk.actions))
        self.rewards.extend(list(episode_chunk.rewards))
        self.t = episode_chunk.t
        self.states = episode_chunk.states

        if episode_chunk.is_terminated:
            self.is_terminated = True
        elif episode_chunk.is_truncated:
            self.is_truncated = True
        # Validate.
        self.validate()

    def add_initial_observation(
        self, *, initial_observation, initial_state=None, initial_render_image=None
    ):
        assert not self.is_done
        assert len(self.observations) == 0
        # Assume that this episode is completely empty and has not stepped yet.
        # Leave self.t (and self.t_started) at 0.
        assert self.t == self.t_started == 0

        self.observations.append(initial_observation)
        self.states = initial_state
        if initial_render_image is not None:
            self.render_images.append(initial_render_image)
        self.validate()

    def add_timestep(
        self,
        observation,
        action,
        reward,
        *,
        state=None,
        is_terminated=False,
        is_truncated=False,
        render_image=None,
    ):
        # Cannot add data to an already done episode.
        assert not self.is_done

        self.observations.append(observation)
        self.actions.append(action)
        self.rewards.append(reward)
        self.states = state
        self.t += 1
        if render_image is not None:
            self.render_images.append(render_image)
        self.is_terminated = is_terminated
        self.is_truncated = is_truncated
        self.validate()

    def validate(self):
        # Make sure we always have one more obs stored than rewards (and actions)
        # due to the reset and last-obs logic of an MDP.
        assert len(self.observations) == len(self.rewards) + 1 == len(self.actions) + 1
        assert len(self.rewards) == (self.t - self.t_started)

        # Convert all lists to numpy arrays, if we are terminated.
        if self.is_done:
            self.observations = np.array(self.observations)
            self.actions = np.array(self.actions)
            self.rewards = np.array(self.rewards)
            self.render_images = np.array(self.render_images, dtype=np.uint8)

    @property
    def is_done(self):
        """Whether the episode is actually done (terminated or truncated).

        A done episode cannot be continued via `self.add_timestep()` or being
        concatenated on its right-side with another episode chunk or being
        succeeded via `self.create_successor()`.
        """
        return self.is_terminated or self.is_truncated

    def create_successor(self) -> "_Episode":
        """Returns a successor episode chunk (of len=0) continuing with this one.

        The successor will have the same ID and state as self and its only observation
        will be the last observation in self. Its length will therefore be 0 (no
        steps taken yet).

        This method is useful if you would like to discontinue building an episode
        chunk (b/c you have to return it from somewhere), but would like to have a new
        episode (chunk) instance to continue building the actual env episode at a later
        time.

        Returns:
            The successor Episode chunk of this one with the same ID and state and the
            only observation being the last observation in self.
        """
        assert not self.is_done

        return _Episode(
            # Same ID.
            id_=self.id_,
            # First (and only) observation of successor is this episode's last obs.
            observations=[self.observations[-1]],
            # Same state.
            states=self.states,
            # Continue with self's current timestep.
            t=self.t,
        )

    def to_sample_batch(self):
        return SampleBatch(
            {
                SampleBatch.EPS_ID: np.array([self.id_] * len(self)),
                SampleBatch.OBS: self.observations[:-1],
                SampleBatch.NEXT_OBS: self.observations[1:],
                SampleBatch.ACTIONS: self.actions,
                SampleBatch.REWARDS: self.rewards,
                SampleBatch.TERMINATEDS: np.array(
                    [False] * (len(self) - 1) + [self.is_terminated]
                ),
                SampleBatch.TRUNCATEDS: np.array(
                    [False] * (len(self) - 1) + [self.is_truncated]
                ),
            }
        )

    @staticmethod
    def from_sample_batch(batch):
        return _Episode(
            id_=batch[SampleBatch.EPS_ID][0],
            observations=np.concatenate(
                [batch[SampleBatch.OBS], batch[SampleBatch.NEXT_OBS][None, -1]]
            ),
            actions=batch[SampleBatch.ACTIONS],
            rewards=batch[SampleBatch.REWARDS],
            is_terminated=batch[SampleBatch.TERMINATEDS][-1],
            is_truncated=batch[SampleBatch.TRUNCATEDS][-1],
        )

    def get_return(self):
        return sum(self.rewards)

    def get_state(self):
        return list(
            {
                "id_": self.id_,
                "observations": self.observations,
                "actions": self.actions,
                "rewards": self.rewards,
                "states": self.states,
                "t_started": self.t_started,
                "t": self.t,
                "is_terminated": self.is_terminated,
                "is_truncated": self.is_truncated,
            }.items()
        )

    @staticmethod
    def from_state(state):
        eps = _Episode(id_=state[0][1])
        eps.observations = state[1][1]
        eps.actions = state[2][1]
        eps.rewards = state[3][1]
        eps.states = state[4][1]
        eps.t_started = state[5][1]
        eps.t = state[6][1]
        eps.is_terminated = state[7][1]
        eps.is_truncated = state[8][1]
        return eps

    def __len__(self):
        assert len(self.observations) > 0, (
            "ERROR: Cannot determine length of episode that hasn't started yet! "
            "Call `_Episode.add_initial_observation(initial_observation=...)` first "
            "(after which `len(_Episode)` will be 0)."
        )
        return len(self.observations) - 1
