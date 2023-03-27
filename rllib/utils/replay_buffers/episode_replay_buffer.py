from collections import deque
from typing import Any, Dict, Optional
import uuid

import numpy as np

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.replay_buffers.replay_buffer import ReplayBuffer, StorageUnit
from ray.rllib.utils.typing import SampleBatchType


class EpisodeReplayBuffer(ReplayBuffer):
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
    observation of the episode, the final reward received, a dummy 0-action, as well
    as either terminated=True or truncated=True.
    """
    def __init__(self, capacity: int = 10000):
        super().__init__(capacity=capacity, storage_unit=StorageUnit.TIMESTEPS)

        # The actual episode buffer.
        self.episodes = deque()
        # Maps (unique) episode IDs to the index under which to find this episode
        # within our `self.episodes` deque. Note that after ejection started, the
        # indices will NOT be changed. We will therefore need to offset these indices
        # by the number of episodes that have already been ejected.
        self.episode_id_to_index = {}
        # The number of episodes that have already been ejected from the buffer
        # due to reaching capacity. This is the offset, which we have to subtract
        # from the episode index to get the actual location within `self.episodes`.
        self._num_episodes_ejected = 0

        # List storing all index tuples: [eps_idx, ts_in_eps_idx], where ...
        # `eps_idx - self._num_episodes_ejected' is the index into self.episodes.
        # `ts_in_eps_idx` is the timestep index within that episode
        #  (0 = 1st timestep, etc..).
        # We sample uniformly from the set of these indices in a `sample()`
        # call.
        self._indices = []

        # The size of the buffer in timesteps.
        self.size = 0
        # How many timesteps have been sampled from the buffer in total?
        self.sampled_timesteps = 0

    @override(ReplayBuffer)
    def add(self, batch: SampleBatchType, **kwargs) -> None:
        episode_slices = batch.split_by_episode()
        episodes = [
            _Episode.from_sample_batch(eps_slice)
            for eps_slice in episode_slices
        ]

        for eps in episodes:
            self.size += len(eps)
            # Ongoing episode, concat to existing record.
            if eps.id_ in self.episode_id_to_index:
                buf_idx = self.episode_id_to_index[eps.id_]
                existing_eps = self.episodes[buf_idx - self._num_episodes_ejected]
                old_len = len(existing_eps)
                self._indices.extend([(buf_idx, old_len + i) for i in range(len(eps))])
                existing_eps.concat_episode(eps)
            # New episode. Add to end of our buffer.
            else:
                self.episodes.append(eps)
                buf_idx = len(self.episodes) - 1 + self._num_episodes_ejected
                self.episode_id_to_index[eps.id_] = buf_idx
                self._indices.extend([(buf_idx, i) for i in range(len(eps))])

            # Eject old records from front of deque.
            while self.size > self.capacity:
                # Eject oldest episode.
                ejected_eps = self.episodes.popleft()
                ejected_eps_len = len(ejected_eps)
                # Correct our size.
                self.size -= len(ejected_eps)
                # Erase episode from all our indices.
                # Main episode index.
                ejected_idx = self.episode_id_to_index[ejected_eps.id_]
                del self.episode_id_to_index[ejected_eps.id_]
                # All timestep indices that this episode owned.
                new_indices = []
                idx_cursor = 0
                for i, idx_tuple in enumerate(self._indices):
                    if idx_cursor is not None and idx_tuple[0] == ejected_idx:
                        new_indices.extend(self._indices[idx_cursor:i])
                        idx_cursor = None
                    elif idx_cursor is None:
                        if idx_tuple[0] != ejected_idx:
                            idx_cursor = i
                        # Early-out: We reached the end of the to-be-ejected episode.
                        # We can stop searching further here.
                        elif idx_tuple[1] == ejected_eps_len - 1:
                            assert self._indices[i+1][0] != idx_tuple[0]
                            idx_cursor = i + 1
                            break
                if idx_cursor is not None:
                    new_indices.extend(self._indices[idx_cursor:])
                self._indices = new_indices
                # Increase episode ejected counter.
                self._num_episodes_ejected += 1

    def sample(self, batch_size_B: int = 16, batch_length_T: int = 64):
        # Uniformly sample n samples from self._indices.
        index_tuples_idx = []
        observations = [[] for _ in range(batch_size_B)]
        actions = [[] for _ in range(batch_size_B)]
        rewards = [[] for _ in range(batch_size_B)]
        is_first = [[False] * batch_length_T for _ in range(batch_size_B)]
        is_last = [[False] * batch_length_T for _ in range(batch_size_B)]
        is_terminated = [[False] * batch_length_T for _ in range(batch_size_B)]
        is_truncated = [[False] * batch_length_T for _ in range(batch_size_B)]

        B = 0
        T = 0
        idx_cursor = 0
        while B < batch_size_B:
            # Ran out of uniform samples -> Sample new set.
            if len(index_tuples_idx) <= idx_cursor:
                index_tuples_idx.extend(list(np.random.randint(
                    0, len(self._indices), size=batch_size_B * 10
                )))

            index_tuple = self._indices[index_tuples_idx[idx_cursor]]
            episode_idx, episode_ts = (
                index_tuple[0] - self._num_episodes_ejected, index_tuple[1]
            )
            episode = self.episodes[episode_idx]

            # Starting a new chunk, set continue to False.
            is_first[B][T] = True

            # Begin of new batch item (row).
            if len(rewards[B]) == 0:
                # And we are at the start of an episode: Set reward to 0.0.
                if episode_ts == 0:
                    rewards[B].append(0.0)
                # We are in the middle of an episode: Set reward and h_state to
                # the previous timestep's values.
                else:
                    rewards[B].append(episode.rewards[episode_ts - 1])
            # We are in the middle of a batch item (row). Concat next episode to this
            # row from the episode's beginning. In other words, we never concat
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
            is_last[B][T-1] = True
            # If episode is terminated and we have reached the end of it, set
            # is_terminated=True.
            if episode.is_terminated and T == len(observations[B]):
                is_terminated[B][T-1] = True
            # If episode is truncated and we have reached the end of it, set
            # is_truncated=True.
            elif episode.is_truncated and T == len(observations[B]):
                is_truncated[B][T-1] = True

            # We are done with this batch row.
            if T == batch_length_T:
                # We may have overfilled this row: Clip trajectory at the end.
                observations[B] = observations[B][:batch_length_T]
                actions[B] = actions[B][:batch_length_T]
                rewards[B] = rewards[B][:batch_length_T]
                # Start filling the next row.
                B += 1
                T = 0

            # Use next sampled episode/ts pair to fill the row.
            idx_cursor += 1

        ret = {
            "obs": np.array(observations),
            "actions": np.array(actions),
            "rewards": np.array(rewards),
            "is_first": np.array(is_first),
            "is_last": np.array(is_last),
            "is_terminated": np.array(is_terminated),
            "is_truncated": np.array(is_truncated),
        }

        # Update our sampled counter.
        self.sampled_timesteps += batch_size_B * batch_length_T

        return ret

    def get_num_episodes(self) -> int:
        """Returns the number of episodes (completed or truncated) stored in the buffer.
        """
        return len(self.episodes)

    def get_num_timesteps(self) -> int:
        """Returns the number of individual timesteps stored in the buffer."""
        return len(self._indices)

    def get_sampled_timesteps(self) -> int:
        """Returns the number of timesteps that have been sampled in buffer's lifetime.
        """
        return self.sampled_timesteps

    def get_state(self) -> Dict[str, Any]:
        return np.array(list({
            "episodes": [eps.get_state() for eps in self.episodes],
            "episode_id_to_index": list(self.episode_id_to_index.items()),
            "_num_episodes_ejected": self._num_episodes_ejected,
            "_indices": self._indices,
            "size": self.size,
            "sampled_timesteps": self.sampled_timesteps,
        }.items()))

    def set_state(self, state):
        assert state[0][0] == "episodes"
        self.episodes = deque([
            _Episode.from_state(eps_data) for eps_data in state[0][1]
        ])
        assert state[1][0] == "episode_id_to_index"
        self.episode_id_to_index = dict(state[1][1])
        assert state[2][0] == "_num_episodes_ejected"
        self._num_episodes_ejected = state[2][1]
        assert state[3][0] == "_indices"
        self._indices = state[3][1]
        assert state[4][0] == "size"
        self.size = state[4][1]
        assert state[5][0] == "sampled_timesteps"
        self.sampled_timesteps = state[5][1]


# TODO (sven): Make this EpisodeV3 (replacing EpisodeV2).
class _Episode:
    def __init__(
        self,
        id_: Optional[str] = None,
        *,
        observations=None,
        actions=None,
        rewards=None,
        states=None,
        is_terminated=False,
        is_truncated=False,
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
        assert episode_chunk.id_ == self.id_
        assert not self.is_done()

        episode_chunk.validate()

        # Make sure, end matches other episode chunk's beginning.
        assert np.all(episode_chunk.observations[0] == self.observations[-1])
        # Pop out our end.
        self.observations.pop()

        # Extend ourselves. In case, episode_chunk is already terminated (and numpyfied)
        # we need to convert to lists (as we are ourselves still filling up lists).
        self.observations.extend(list(episode_chunk.observations))
        self.actions.extend(list(episode_chunk.actions))
        self.rewards.extend(list(episode_chunk.rewards))
        self.states = episode_chunk.states

        if episode_chunk.is_terminated:
            self.is_terminated = True
        elif episode_chunk.is_truncated:
            self.is_truncated = True
        # Validate.
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
        assert not self.is_done()

        self.observations.append(observation)
        self.actions.append(action)
        self.rewards.append(reward)
        self.states = state
        if render_image is not None:
            self.render_images.append(render_image)
        self.is_terminated = is_terminated
        self.is_truncated = is_truncated
        self.validate()

    def add_initial_observation(self, *, initial_observation, initial_state=None, initial_render_image=None):
        assert not self.is_done()
        assert len(self.observations) == 0

        self.observations.append(initial_observation)
        self.states = initial_state
        if initial_render_image is not None:
            self.render_images.append(initial_render_image)
        self.validate()

    def validate(self):
        # Make sure we always have one more obs stored than rewards (and actions)
        # due to the reset and last-obs logic of an MDP.
        assert (
            len(self.observations) == len(self.rewards) + 1 == len(self.actions) + 1
        )

        # Convert all lists to numpy arrays, if we are terminated.
        if self.is_done():
            self.observations = np.array(self.observations)
            self.actions = np.array(self.actions)
            self.rewards = np.array(self.rewards)
            self.render_images = np.array(self.render_images, dtype=np.uint8)

    def is_done(self):
        return self.is_terminated or self.is_truncated

    def to_sample_batch(self):
        return SampleBatch({
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
        })

    @staticmethod
    def from_sample_batch(batch):
        return _Episode(
            id_=batch[SampleBatch.EPS_ID][0],
            observations=np.concatenate([
                batch[SampleBatch.OBS], batch[SampleBatch.NEXT_OBS][None, -1]
            ]),
            actions=batch[SampleBatch.ACTIONS],
            rewards=batch[SampleBatch.REWARDS],
            is_terminated=batch[SampleBatch.TERMINATEDS][-1],
            is_truncated=batch[SampleBatch.TRUNCATEDS][-1],
        )

    def get_return(self):
        return sum(self.rewards)

    def get_state(self):
        return list({
            "id_": self.id_,
            "observations": self.observations,
            "actions": self.actions,
            "rewards": self.rewards,
            "states": self.states,
            "is_terminated": self.is_terminated,
            "is_truncated": self.is_truncated,
        }.items())

    @staticmethod
    def from_state(state):
        eps = _Episode(id_=state[0][1])
        eps.observations = state[1][1]
        eps.actions = state[2][1]
        eps.rewards = state[3][1]
        eps.states = state[4][1]
        eps.is_terminated = state[5][1]
        eps.is_truncated = state[6][1]
        return eps

    def __len__(self):
        assert len(self.observations) > 0, (
            "ERROR: Cannot determine length of episode that hasn't started yet! "
            "Call `_Episode.add_initial_obs(initial_observation=...)` first "
            "(after which `len(_Episode)` will be 0)."
        )
        return len(self.observations) - 1
