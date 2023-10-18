import numpy as np
import uuid

from typing import Optional

from ray.rllib.policy.sample_batch import SampleBatch


# TODO (sven): Make this EpisodeV3 - replacing EpisodeV2 - to reduce all the
#  information leakage we currently have in EpisodeV2 (policy_map, worker, etc.. are
#  all currently held by EpisodeV2 for no good reason).
# TODO (simon): Put into rllib/env
class SingleAgentEpisode:
    def __init__(
        self,
        id_: Optional[str] = None,
        *,
        observations=None,
        actions=None,
        rewards=None,
        states=None,
        t_started: int = None,
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
        # Note, the case `t_started > len(observations) - 1` can occur, if a user
        # wants to have an episode that is ongoing but does not want to carry the
        # stale data from the last rollout in it.
        self.t = self.t_started = (
            t_started if t_started is not None else max(len(self.observations) - 1, 0)
        )
        if self.t_started < len(self.observations) - 1:
            self.t = len(self.observations) - 1

        # obs[-1] is the final observation in the episode.
        self.is_terminated = is_terminated
        # obs[-1] is the last obs in a truncated-by-the-env episode (there will no more
        # observations in following chunks for this episode).
        self.is_truncated = is_truncated
        # RGB uint8 images from rendering the env; the images include the corresponding
        # rewards.
        assert render_images is None or observations is not None
        self.render_images = [] if render_images is None else render_images

    def concat_episode(self, episode_chunk: "SingleAgentEpisode"):
        """Adds the given `episode_chunk` to the right side of self."""
        assert episode_chunk.id_ == self.id_
        assert not self.is_done
        # Make sure the timesteps match.
        assert self.t == episode_chunk.t_started

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
        # TODO (sven): Do we have to call validate here? It is our own function
        # that manipulates the object.
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
        # TODO (sven): This is unclear to me. It makes sense
        # to start at a point after the length of experiences
        # provided at initialization, but when we test then here
        # it will imo always error out.
        # Example: we initialize the class by providing 101 observations,
        # 100 actions and rewards.
        # self.t = self.t_started = len(observations) - 1. Then
        # we add a single timestep. self.t += 1 and
        # self.t - self.t_started is 1, but len(rewards) is 100.
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

    def create_successor(self) -> "SingleAgentEpisode":
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

        return SingleAgentEpisode(
            # Same ID.
            id_=self.id_,
            # First (and only) observation of successor is this episode's last obs.
            observations=[self.observations[-1]],
            # Same state.
            states=self.states,
            # Continue with self's current timestep.
            t_started=self.t,
        )

    def to_sample_batch(self):
        return SampleBatch(
            {
                SampleBatch.EPS_ID: np.array([self.id_] * len(self)),
                SampleBatch.OBS: self.observations[:-1],
                SampleBatch.NEXT_OBS: self.observations[1:],
                SampleBatch.ACTIONS: self.actions,
                SampleBatch.REWARDS: self.rewards,
                SampleBatch.T: list(range(self.t_started, self.t)),
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
        is_done = (
            batch[SampleBatch.TERMINATEDS][-1] or batch[SampleBatch.TRUNCATEDS][-1]
        )
        observations = np.concatenate(
            [batch[SampleBatch.OBS], batch[SampleBatch.NEXT_OBS][None, -1]]
        )
        actions = batch[SampleBatch.ACTIONS]
        rewards = batch[SampleBatch.REWARDS]
        return SingleAgentEpisode(
            id_=batch[SampleBatch.EPS_ID][0],
            observations=observations if is_done else observations.tolist(),
            actions=actions if is_done else actions.tolist(),
            rewards=rewards if is_done else rewards.tolist(),
            t_started=batch[SampleBatch.T][0],
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
        eps = SingleAgentEpisode(id_=state[0][1])
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
            "Call `SingleAgentEpisode.add_initial_observation(initial_observation=...)` "
            "first (after which `len(SingleAgentEpisode)` will be 0)."
        )
        return len(self.observations) - 1
