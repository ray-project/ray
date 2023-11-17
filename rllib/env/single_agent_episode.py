import numpy as np
import uuid

from gymnasium.core import ActType, ObsType
from typing import Any, Dict, List, Optional, SupportsFloat

from ray.rllib.policy.sample_batch import SampleBatch


class SingleAgentEpisode:
    def __init__(
        self,
        id_: Optional[str] = None,
        *,
        observations: List[ObsType] = None,
        actions: List[ActType] = None,
        rewards: List[SupportsFloat] = None,
        infos: List[Dict] = None,
        states=None,
        t_started: Optional[int] = None,
        is_terminated: bool = False,
        is_truncated: bool = False,
        render_images: Optional[List[np.ndarray]] = None,
        extra_model_outputs: Optional[Dict[str, Any]] = None,
    ) -> "SingleAgentEpisode":
        """Initializes a `SingleAgentEpisode` instance.

        This constructor can be called with or without sampled data. Note
        that if data is provided the episode will start at timestep
        `t_started = len(observations) - 1` (the initial observation is not
        counted). If the episode should start at `t_started = 0` (e.g.
        because the instance should simply store episode data) this has to
        be provided in the `t_started` parameter of the constructor.

        Args:
            id_: Optional. Unique identifier for this episode. If no id is
                provided the constructor generates a hexadecimal code for the id.
            observations: Optional. A list of observations from a rollout. If
                data is provided it should be complete (i.e. observations, actions,
                rewards, is_terminated, is_truncated, and all necessary
                `extra_model_outputs`). The length of the `observations` defines
                the default starting value. See the parameter `t_started`.
            actions: Optional. A list of actions from a rollout. If data is
                provided it should be complete (i.e. observations, actions,
                rewards, is_terminated, is_truncated, and all necessary
                `extra_model_outputs`).
            rewards: Optional. A list of rewards from a rollout. If data is
                provided it should be complete (i.e. observations, actions,
                rewards, is_terminated, is_truncated, and all necessary
                `extra_model_outputs`).
            infos: Optional. A list of infos from a rollout. If data is
                provided it should be complete (i.e. observations, actions,
                rewards, is_terminated, is_truncated, and all necessary
                `extra_model_outputs`).
            states: Optional. The hidden model states from a rollout. If
                data is provided it should be complete (i.e. observations, actions,
                rewards, is_terminated, is_truncated, and all necessary
                `extra_model_outputs`). States are only avasilable if a stateful
                model (`RLModule`) is used.
            t_started: Optional. The starting timestep of the episode. The default
                is zero. If data is provided, the starting point is from the last
                observation onwards (i.e. `t_started = len(observations) - 1). If
                this parameter is provided the episode starts at the provided value.
            is_terminated: Optional. A boolean indicating, if the episode is already
                terminated. Note, this parameter is only needed, if episode data is
                provided in the constructor. The default is `False`.
            is_truncated: Optional. A boolean indicating, if the episode was
                truncated. Note, this parameter is only needed, if episode data is
                provided in the constructor. The default is `False`.
            render_images: Optional. A list of RGB uint8 images from rendering
                the environment.
            extra_model_outputs: Optional. A list of dictionaries containing specific
                model outputs for the algorithm used (e.g. `vf_preds` and `action_logp`
                for PPO) from a rollout. If data is provided it should be complete
                (i.e. observations, actions, rewards, is_terminated, is_truncated,
                and all necessary `extra_model_outputs`).
        """
        self.id_ = id_ or uuid.uuid4().hex
        # Observations: t0 (initial obs) to T.
        self.observations = [] if observations is None else observations
        # Actions: t1 to T.
        self.actions = [] if actions is None else actions
        # Rewards: t1 to T.
        self.rewards = [] if rewards is None else rewards
        # Infos: t0 (initial info) to T.
        if infos is None:
            self.infos = [{} for _ in range(len(self.observations))]
        else:
            self.infos = infos
        # h-states: t0 (in case this episode is a continuation chunk, we need to know
        # about the initial h) to T.
        self.states = states
        # The global last timestep of the episode and the timesteps when this chunk
        # started.
        # TODO (simon): Check again what are the consequences of this decision for
        # the methods of this class. For example the `validate()` method or
        # `create_successor`. Write a test.
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
        # Extra model outputs, e.g. `action_dist_input` needed in the batch.
        self.extra_model_outputs = (
            {} if extra_model_outputs is None else extra_model_outputs
        )

    def concat_episode(self, episode_chunk: "SingleAgentEpisode"):
        """Adds the given `episode_chunk` to the right side of self.

        Args:
            episode_chunk: Another `SingleAgentEpisode` to be concatenated.

        Returns: A `SingleAegntEpisode` instance containing the concatenated
            from both episodes.
        """
        assert episode_chunk.id_ == self.id_
        assert not self.is_done
        # Make sure the timesteps match.
        assert self.t == episode_chunk.t_started

        episode_chunk.validate()

        # Make sure, end matches other episode chunk's beginning.
        assert np.all(episode_chunk.observations[0] == self.observations[-1])
        # Pop out our last observations and infos (as these are identical
        # to the first obs and infos in the next episode).
        self.observations.pop()
        self.infos.pop()

        # Extend ourselves. In case, episode_chunk is already terminated (and numpyfied)
        # we need to convert to lists (as we are ourselves still filling up lists).
        self.observations.extend(list(episode_chunk.observations))
        self.actions.extend(list(episode_chunk.actions))
        self.rewards.extend(list(episode_chunk.rewards))
        self.infos.extend(list(episode_chunk.infos))
        self.t = episode_chunk.t
        self.states = episode_chunk.states

        if episode_chunk.is_terminated:
            self.is_terminated = True
        elif episode_chunk.is_truncated:
            self.is_truncated = True

        for k, v in episode_chunk.extra_model_outputs.items():
            self.extra_model_outputs[k].extend(list(v))

        # Validate.
        self.validate()

    def add_initial_observation(
        self,
        *,
        initial_observation: ObsType,
        initial_info: Optional[Dict] = None,
        initial_state=None,
        initial_render_image: Optional[np.ndarray] = None,
    ) -> None:
        """Adds the initial data to the episode.

        Args:
            initial_observation: Obligatory. The initial observation.
            initial_info: Optional. The initial info.
            initial_state: Optional. The initial hidden state of a
                model (`RLModule`) if the latter is stateful.
            initial_render_image: Optional. An RGB uint8 image from rendering
                the environment.
        """
        assert not self.is_done
        assert len(self.observations) == 0
        # Assume that this episode is completely empty and has not stepped yet.
        # Leave self.t (and self.t_started) at 0.
        assert self.t == self.t_started == 0

        initial_info = initial_info or {}

        self.observations.append(initial_observation)
        self.states = initial_state
        self.infos.append(initial_info)
        if initial_render_image is not None:
            self.render_images.append(initial_render_image)
        # TODO (sven): Do we have to call validate here? It is our own function
        # that manipulates the object.
        self.validate()

    def add_timestep(
        self,
        observation: ObsType,
        action: ActType,
        reward: SupportsFloat,
        *,
        info: Optional[Dict[str, Any]] = None,
        state=None,
        is_terminated: bool = False,
        is_truncated: bool = False,
        render_image: Optional[np.ndarray] = None,
        extra_model_output: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Adds a timestep to the episode.

        Args:
            observation: The observation received from the
                environment.
            action: The last action used by the agent.
            reward: The last reward received by the agent.
            info: The last info recevied from the environment.
            state: Optional. The last hidden state of the model (`RLModule` ).
                This is only available, if the model is stateful.
            is_terminated: A boolean indicating, if the environment has been
                terminated.
            is_truncated: A boolean indicating, if the environment has been
                truncated.
            render_image: Optional. An RGB uint8 image from rendering
                the environment.
            extra_model_output: The last timestep's specific model outputs
                (e.g. `vf_preds`  for PPO).
        """
        # Cannot add data to an already done episode.
        assert not self.is_done

        self.observations.append(observation)
        self.actions.append(action)
        self.rewards.append(reward)
        info = info or {}
        self.infos.append(info)
        self.states = state
        self.t += 1
        if render_image is not None:
            self.render_images.append(render_image)
        if extra_model_output is not None:
            for k, v in extra_model_output.items():
                if k not in self.extra_model_outputs:
                    self.extra_model_outputs[k] = [v]
                else:
                    self.extra_model_outputs[k].append(v)
        self.is_terminated = is_terminated
        self.is_truncated = is_truncated
        self.validate()

    def validate(self) -> None:
        """Validates the episode.

        This function ensures that the data stored to a `SingleAgentEpisode` is
        in order (e.g. that the correct number of observations, actions, rewards
        are there).
        """
        # Make sure we always have one more obs stored than rewards (and actions)
        # due to the reset and last-obs logic of an MDP.
        assert (
            len(self.observations)
            == len(self.infos)
            == len(self.rewards) + 1
            == len(self.actions) + 1
        )
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

        if len(self.extra_model_outputs) > 0:
            for k, v in self.extra_model_outputs.items():
                assert len(v) == len(self.observations) - 1

        # Convert all lists to numpy arrays, if we are terminated.
        if self.is_done:
            self.convert_lists_to_numpy()

    @property
    def is_done(self) -> bool:
        """Whether the episode is actually done (terminated or truncated).

        A done episode cannot be continued via `self.add_timestep()` or being
        concatenated on its right-side with another episode chunk or being
        succeeded via `self.create_successor()`.
        """
        return self.is_terminated or self.is_truncated

    def convert_lists_to_numpy(self) -> None:
        """Converts list attributes to numpy arrays.

        When an episode is terminated or truncated (`self.is_done`) the data
        will be not anymore touched and instead converted to numpy for later
        use in postprocessing. This function converts all the data stored
        into numpy arrays.
        """

        self.observations = np.array(self.observations)
        self.actions = np.array(self.actions)
        self.rewards = np.array(self.rewards)
        self.infos = np.array(self.infos)
        self.render_images = np.array(self.render_images, dtype=np.uint8)
        for k, v in self.extra_model_outputs.items():
            self.extra_model_outputs[k] = np.array(v)

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
            # First (and only) info of successor is this episode's last info.
            infos=[self.infos[-1]],
            # Same state.
            states=self.states,
            # Continue with self's current timestep.
            t_started=self.t,
        )

    def to_sample_batch(self) -> SampleBatch:
        """Converts a `SingleAgentEpisode` into a `SampleBatch`.

        Note that `RLlib` is relying in training on the `SampleBatch`  class and
        therefore episodes have to be converted to this format before training can
        start.

        Returns:
            An `ray.rLlib.policy.sample_batch.SampleBatch` instance containing this
            episode's data.
        """
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
                # Return the infos after stepping the environment.
                SampleBatch.INFOS: self.infos[1:],
                **self.extra_model_outputs,
            }
        )

    @staticmethod
    def from_sample_batch(batch: SampleBatch) -> "SingleAgentEpisode":
        """Converts a `SampleBatch` instance into a `SingleAegntEpisode`.

        The `ray.rllib.policy.sample_batch.SampleBatch` class is used in `RLlib`
        for training an agent's modules (`RLModule`), converting from or to
        `SampleBatch` can be performed by this function and its counterpart
        `to_sample_batch()`.

        Args:
            batch: A `SampleBatch` instance. It should contain only a single episode.

        Returns:
            An `SingleAegntEpisode` instance containing the data from `batch`.
        """
        is_done = (
            batch[SampleBatch.TERMINATEDS][-1] or batch[SampleBatch.TRUNCATEDS][-1]
        )
        observations = np.concatenate(
            [batch[SampleBatch.OBS], batch[SampleBatch.NEXT_OBS][None, -1]]
        )
        actions = batch[SampleBatch.ACTIONS]
        rewards = batch[SampleBatch.REWARDS]
        # These are the infos after stepping the environment, i.e. without the
        # initial info.
        infos = batch[SampleBatch.INFOS]
        # Concatenate an intiial empty info.
        infos = np.concatenate([np.array([{}]), infos])

        # TODO (simon): This is very ugly, but right now
        #  we can only do it according to the exclusion principle.
        extra_model_output_keys = []
        for k in batch.keys():
            if k not in [
                SampleBatch.EPS_ID,
                SampleBatch.AGENT_INDEX,
                SampleBatch.ENV_ID,
                SampleBatch.AGENT_INDEX,
                SampleBatch.T,
                SampleBatch.SEQ_LENS,
                SampleBatch.OBS,
                SampleBatch.INFOS,
                SampleBatch.NEXT_OBS,
                SampleBatch.ACTIONS,
                SampleBatch.PREV_ACTIONS,
                SampleBatch.REWARDS,
                SampleBatch.PREV_REWARDS,
                SampleBatch.TERMINATEDS,
                SampleBatch.TRUNCATEDS,
                SampleBatch.UNROLL_ID,
                SampleBatch.DONES,
                SampleBatch.CUR_OBS,
            ]:
                extra_model_output_keys.append(k)

        return SingleAgentEpisode(
            id_=batch[SampleBatch.EPS_ID][0],
            observations=observations if is_done else observations.tolist(),
            actions=actions if is_done else actions.tolist(),
            rewards=rewards if is_done else rewards.tolist(),
            t_started=batch[SampleBatch.T][0],
            is_terminated=batch[SampleBatch.TERMINATEDS][-1],
            is_truncated=batch[SampleBatch.TRUNCATEDS][-1],
            infos=infos if is_done else infos.tolist(),
            extra_model_outputs={
                k: (batch[k] if is_done else batch[k].tolist())
                for k in extra_model_output_keys
            },
        )

    def get_return(self) -> float:
        """Calculates an episode's return.

        The return is computed by a simple sum, neglecting the discount factor.
        This is used predominantly for metrics.

        Returns:
            The sum of rewards collected during this episode.
        """
        return sum(self.rewards)

    def get_state(self) -> Dict[str, Any]:
        """Returns the pickable state of an episode.

        The data in the episode is stored into a dictionary. Note that episodes
        can also be generated from states (see `self.from_state()`).

        Returns:
            A dictionary containing all the data from the episode.
        """
        return list(
            {
                "id_": self.id_,
                "observations": self.observations,
                "actions": self.actions,
                "rewards": self.rewards,
                "infos": self.infos,
                "states": self.states,
                "t_started": self.t_started,
                "t": self.t,
                "is_terminated": self.is_terminated,
                "is_truncated": self.is_truncated,
                **self.extra_model_outputs,
            }.items()
        )

    @staticmethod
    def from_state(state: Dict[str, Any]) -> "SingleAgentEpisode":
        """Generates a `SingleAegntEpisode` from a pickable state.

        The data in the state has to be complete. This is always the case when the state
        was created by a `SingleAgentEpisode` itself calling `self.get_state()`.

        Args:
            state: A dictionary containing all episode data.

        Returns:
            A `SingleAgentEpisode` instance holding all the data provided by `state`.
        """
        eps = SingleAgentEpisode(id_=state[0][1])
        eps.observations = state[1][1]
        eps.actions = state[2][1]
        eps.rewards = state[3][1]
        eps.infos = state[4][1]
        eps.states = state[5][1]
        eps.t_started = state[6][1]
        eps.t = state[7][1]
        eps.is_terminated = state[8][1]
        eps.is_truncated = state[9][1]
        eps.extra_model_outputs = {k: v for k, v in state[10:]}
        # Validate the episode to ensure complete data.
        eps.validate()
        return eps

    def __len__(self) -> int:
        """Returning the length of an episode.

        The length of an episode is defined by the length of its data. This is the
        number of timesteps an agent has stepped through an environment so far.
        The length is undefined in case of a just started episode.

        Returns:
            An integer, defining the length of an episode.

        Raises:
            AssertionError: If episode has never been stepped so far.
        """
        assert len(self.observations) > 0, (
            "ERROR: Cannot determine length of episode that hasn't started yet! Call "
            "`SingleAgentEpisode.add_initial_observation(initial_observation=...)` "
            "first (after which `len(SingleAgentEpisode)` will be 0)."
        )
        return len(self.observations) - 1

    def __repr__(self):
        return f"SAEps({self.id_} len={len(self)})"
