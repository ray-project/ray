from collections import defaultdict
import numpy as np
import uuid

from gymnasium.core import ActType, ObsType
import tree  # pip install dm_tree
from typing import Any, Dict, List, Optional, SupportsFloat, Union

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.spaces.space_utils import batch


class SingleAgentEpisode:
    """A class representing RL environment episodes for individual agents.

    SingleAgentEpisode stores observations, info dicts, actions, rewards, and all
    module outputs (e.g. state outs, action logp, etc..) for an individual agent within
    some single-agent or multi-agent environment.
    The two main APIs are the `add_env_reset()` and `add_env_step()` methods, which
    should be called passing the outputs of the respective gym.Env API calls:
    `env.reset()` and `env.step()`.

    A SingleAgentEpisode might also only represent a chunk of an episode, which is
    useful for cases, in which partial (non-complete episode) sampling is performed
    and collected episode data has to be returned before the actual gym.Env episode has
    finished (see `SingleAgentEpisode.cut()`). In order to still maintain visibility
    onto past experiences within such a "cut" episode, SingleAgentEpisode instances
    might have a "lookback buffer" of n timesteps at their beginning (left side), which
    solely exists for the purpose of compiling extra data (e.g. "prev. reward"), but
    is not considered part of the finished/packaged episode (b/c the data in the
    lookback buffer is already part of a previous episode chunk).

    Examples:
        >>> e = SingleAgentEpisode()

    """

    def __init__(
        self,
        id_: Optional[str] = None,
        *,
        observations: List[ObsType] = None,
        actions: List[ActType] = None,
        rewards: List[SupportsFloat] = None,
        infos: List[Dict] = None,
        is_terminated: bool = False,
        is_truncated: bool = False,
        extra_model_outputs: Optional[Dict[str, Any]] = None,
        render_images: Optional[List[np.ndarray]] = None,
        t_started: Optional[int] = None,
        len_lookback_buffer: Optional[int] = 0,
    ) -> "SingleAgentEpisode":
        """Initializes a SingleAgentEpisode instance.

        This constructor can be called with or without already sampled data, part of
        which might be for lookback purposes only and not count as part of
        the actual episode chunk.
        Note that if data is provided already in the constructor here,
        the user can choose to have part of this data be part of the lookback buffer
        by providing the `len_lookback_buffer` arg.
        Also, the starting timestep (not counting a possible lookback buffer)
        can be provided via `t_started`.

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
            is_terminated: Optional. A boolean indicating, if the episode is already
                terminated. Note, this parameter is only needed, if episode data is
                provided in the constructor. The default is `False`.
            is_truncated: Optional. A boolean indicating, if the episode was
                truncated. Note, this parameter is only needed, if episode data is
                provided in the constructor. The default is `False`.
            extra_model_outputs: Optional. A list of dictionaries containing specific
                model outputs for the algorithm used (e.g. `vf_preds` and `action_logp`
                for PPO) from a rollout. If data is provided it should be complete
                (i.e. observations, actions, rewards, is_terminated, is_truncated,
                and all necessary `extra_model_outputs`).
            render_images: Optional. A list of RGB uint8 images from rendering
                the environment.
            t_started: Optional. The starting timestep of the episode. The default
                is zero. If data is provided, the starting point is from the last
                observation onwards (i.e. `t_started = len(observations) - 1). If
                this parameter is provided the episode starts at the provided value.
            len_lookback_buffer: The size of an optional lookback buffer to keep in
                front of this Episode. If >0, will interpret the first
                `len_lookback_buffer` items in each data as NOT part of this actual
                episode chunk, but instead serve as historic data that may be viewed.
                If None, will interpret all provided data in constructor as part of the
                lookback buffer.
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

        # obs[-1] is the final observation in the episode.
        self.is_terminated = is_terminated
        # obs[-1] is the last obs in a truncated-by-the-env episode (there will no more
        # observations in following chunks for this episode).
        self.is_truncated = is_truncated
        # Extra model outputs, e.g. `action_dist_input` needed in the batch.
        self.extra_model_outputs = defaultdict(list, extra_model_outputs or {})
        # RGB uint8 images from rendering the env; the images include the corresponding
        # rewards.
        assert render_images is None or observations is not None
        self.render_images = [] if render_images is None else render_images

        # Lookback buffer length is provided.
        if len_lookback_buffer is not None:
            self._len_lookback_buffer = len_lookback_buffer
        # Lookback buffer length is not provided. Interpret already given data as
        # lookback buffer.
        else:
            self._len_lookback_buffer = len(self.rewards)

        # The global last timestep of the episode and the timesteps when this chunk
        # started (excluding a possible lookback buffer).
        if t_started is not None:
            self.t_started = t_started
        else:
            self.t_started = self._len_lookback_buffer

        self.t = len(self.rewards) - self._len_lookback_buffer + self.t_started

        # Validate the episode data thus far.
        self.validate()

    def concat_episode(self, episode_chunk: "SingleAgentEpisode"):
        """Adds the given `episode_chunk` to the right side of self.

        Args:
            episode_chunk: Another `SingleAgentEpisode` to be concatenated.

        Returns: A `SingleAegntEpisode` instance containing the concatenated
            from both episodes.
        """
        assert episode_chunk.id_ == self.id_
        assert not self.is_done and not self.is_numpy
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
        self.observations.extend(episode_chunk.observations)
        self.actions.extend(episode_chunk.actions)
        self.rewards.extend(episode_chunk.rewards)
        self.infos.extend(episode_chunk.infos)
        self.t = episode_chunk.t

        if episode_chunk.is_terminated:
            self.is_terminated = True
        elif episode_chunk.is_truncated:
            self.is_truncated = True

        for k, v in episode_chunk.extra_model_outputs.items():
            self.extra_model_outputs[k].extend(v)

        # Validate.
        self.validate()

    def add_env_reset(
        self,
        *,
        observation: ObsType,
        info: Optional[Dict] = None,
        render_image: Optional[np.ndarray] = None,
    ) -> None:
        """Adds the initial data (after an `env.reset()`) to the episode.

        This data consists of initial observations and -infos, as well as - optionally -
        a render image.

        Args:
            observation: The initial observation returned by `env.reset()`.
            info: An (optional) info dict returned by `env.reset()`.
            render_image: Optional. An RGB uint8 image from rendering
                the environment.
        """
        assert not self.is_done and not self.is_numpy
        assert len(self.observations) == 0
        # Assume that this episode is completely empty and has not stepped yet.
        # Leave self.t (and self.t_started) at 0.
        assert self.t == self.t_started == 0

        info = info or {}

        self.observations.append(observation)
        self.infos.append(info)
        if render_image is not None:
            self.render_images.append(render_image)

        # Validate our data.
        self.validate()

    def add_env_step(
        self,
        observation: ObsType,
        action: ActType,
        reward: SupportsFloat,
        *,
        info: Optional[Dict[str, Any]] = None,
        is_terminated: bool = False,
        is_truncated: bool = False,
        render_image: Optional[np.ndarray] = None,
        extra_model_output: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Adds the results of an `env.step([action])` call to the episode.

        This data consists of an observation and info dict, an action, a reward,
        terminated/truncated flags, extra model outputs (e.g. action probabilities or
        RNN internal state outputs), and - optionally - a render image.

        Args:
            observation: The observation received from the
                environment.
            action: The last action used by the agent.
            reward: The last reward received by the agent.
            info: The last info recevied from the environment.
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
        assert not self.is_done and not self.is_numpy

        self.observations.append(observation)
        self.actions.append(action)
        self.rewards.append(reward)
        info = info or {}
        self.infos.append(info)
        self.t += 1
        if render_image is not None:
            self.render_images.append(render_image)
        if extra_model_output is not None:
            for k, v in extra_model_output.items():
                self.extra_model_outputs[k].append(v)
        self.is_terminated = is_terminated
        self.is_truncated = is_truncated

        # Validate our data.
        self.validate()

    def validate(self) -> None:
        """Validates the episode's data.

        This function ensures that the data stored to a `SingleAgentEpisode` is
        in order (e.g. that the correct number of observations, actions, rewards
        are there).
        """
        assert len(self.observations) == len(self.infos)
        if len(self.observations) == 0:
            assert (
                len(self.observations)
                == len(self.infos)
                == len(self.rewards)
                == len(self.actions)
            )
        # Make sure we always have one more obs stored than rewards (and actions)
        # due to the reset and last-obs logic of an MDP.
        else:
            assert (
                len(self.observations)
                == len(self.infos)
                == len(self.rewards) + 1
                == len(self.actions) + 1
            )
            for k, v in self.extra_model_outputs.items():
                assert len(v) == len(self.observations) - 1
            # Make sure, length of pre-buffer and len(self) make sense.
            assert self._len_lookback_buffer + len(self) == len(self.rewards)

    @property
    def is_numpy(self) -> bool:
        """True, if the data in this episode is already stored as numpy arrays."""
        if len(self) == 0:
            return False
        # If rewards are still a list, return False.
        # Otherwise, rewards should already be a (1D) numpy array.
        return not isinstance(self.rewards, list)

    @property
    def is_done(self) -> bool:
        """Whether the episode is actually done (terminated or truncated).

        A done episode cannot be continued via `self.add_timestep()` or being
        concatenated on its right-side with another episode chunk or being
        succeeded via `self.create_successor()`.
        """
        return self.is_terminated or self.is_truncated

    def convert_lists_to_numpy(self) -> "SingleAgentEpisode":
        """Converts this Episode's list attributes to numpy arrays.

        In case some data is nested (e.g. we have a dict obs space), each leaf in the
        nested structure is converted into a numpy ndarray and thus the data is
        converted from a list of (nested) structs into a (nested) struct of
        ndarrays of shape (B, ...), where B is always the length of the original list
        of (nested) structs.

        Note that SampleBatch.INFOS are not numpy'ized and will remain a list
        (normally, a list of the original, env-returned dicts). This is due to the
        herterogenous nature of INFOS returned by envs, which would make it unwieldy to
        convert this information to numpy arrays.

        Returns:
             This `SingleAgentEpisode` object with the converted numpy data.
        """

        self.observations = batch(self.observations)
        self.actions = batch(self.actions)
        self.rewards = np.array(self.rewards)
        self.render_images = np.array(self.render_images, dtype=np.uint8)
        for k, v in self.extra_model_outputs.items():
            self.extra_model_outputs[k] = batch(v)

        return self

    def cut(self, overlap: int = 0) -> "SingleAgentEpisode":
        """Returns a successor episode chunk (of len=0) continuing from this Episode.

        The successor will have the same ID as self and its observations
        will be the last observation(s) in this Episode (self). Its length will
        therefore be 0 (no steps taken yet).

        This method is useful if you would like to discontinue building an episode
        chunk (b/c you have to return it from somewhere), but would like to have a new
        episode (chunk) instance to continue building the actual env episode at a later
        time.

        Args:
            overlap: The number of timesteps to take into the new chunk, simply for
                horizon/visibility reasons (but without actually being part of the
                new chunk). For example, if this episode ends in actions 5, 6, 7,
                and 8, and we call `cut(overlap=2)`, the returned chunk will have
                actions 7 and 8 already in it, but still t_started==t==8 (not 7!).
                Episodes being "finalized" into batches for model forward passing or
                used for metrics computations should ignore this extra data at the
                beginning.

        Returns:
            The successor Episode chunk of this one with the same ID and state and the
            only observation being the last observation in self.
        """
        assert not self.is_done

        indices_obs_and_infos = slice(-overlap - 1, None)
        indices_rest = slice(-overlap, None) if overlap > 0 else slice(None, 0)

        return SingleAgentEpisode(
            # Same ID.
            id_=self.id_,
            observations=self.get_observations(indices=indices_obs_and_infos),
            infos=self.get_infos(indices=indices_obs_and_infos),
            actions=self.get_actions(indices=indices_rest),
            rewards=self.get_rewards(indices=indices_rest),
            extra_model_outputs={
                k: self.get_extra_model_outputs(k, indices_rest)
                for k in self.extra_model_outputs.keys()
            },
            # Continue with self's current timestep.
            t_started=self.t,
        )

    def split_at(self, index: int) -> "SingleAgentEpisode":
        """Splits at given index, keeping the 1st half in `self` and returning 2nd half.

        Changes `self` in-place by making it shorter (from 0 to [original length] to
        0 to `split_at`). The second "half" Episode from split_at till the end is
        returned as a new SingleAgentEpisode object.

        Args:
            index: The index at which to perform the split.

        Returns:
            The new SingleAgentEpisode representing the second half of the split. The
            first half of the split will be `self`, which is changed in-place.
        """
        # For now, only allow splitting on finalized, numpy'ized episodes.
        assert self.is_numpy, "Cannot split a non-numpy-converted SingleAgentEpisode!"
        assert (
            len(self) > index - 1
        ), f"Cannot split at {index}! SingleAgentEpisode is only {len(self)} ts long."

        # Keep the same pre-buffer length.
        start_2nd_chunk = index - self._len_lookback_buffer
        second_chunk = SingleAgentEpisode(
            id_=self.id_,
            observations=tree.map_structure(
                lambda s: s[start_2nd_chunk:],
                self.observations,
            ),
            infos=self.infos[start_2nd_chunk:],
            actions=tree.map_structure(
                lambda s: s[start_2nd_chunk:],
                self.actions,
            ),
            rewards=self.rewards[start_2nd_chunk:],
            is_terminated=self.is_terminated,
            is_truncated=self.is_truncated,
            # Provide correct timestep- and pre-buffer information.
            t_started=index,
            len_lookback_buffer=self._len_lookback_buffer,
        )

        # Fix self.
        end_self = index + self._len_lookback_buffer
        self.t = index
        self.observations = tree.map_structure(
            lambda s: s[: end_self + 1],
            self.observations,
        )
        self.infos = self.infos[: end_self + 1]
        self.actions = tree.map_structure(
            lambda s: s[:end_self],
            self.actions,
        )
        self.rewards = self.rewards[:end_self]
        self.extra_model_outputs = tree.map_structure(
            lambda s: s[:end_self],
            self.extra_model_outputs,
        )
        # Set our own terminated/truncated back to False.
        self.is_terminated = False
        self.is_truncated = False

        self.validate()

        return second_chunk

    def get_data_dict(self):
        """Converts a `SingleAgentEpisode` into a data dict mapping str keys to data.

        The keys used are:
        SampleBatch.EPS_ID, T, OBS, INFOS, ACTIONS, REWARDS, TERMINATEDS, TRUNCATEDS,
        and those in `self.extra_model_outputs`.

        Returns:
            A data dict mapping str keys to data records.
        """
        t = list(range(self.t_started, self.t))
        terminateds = [False] * (len(self) - 1) + [self.is_terminated]
        truncateds = [False] * (len(self) - 1) + [self.is_truncated]
        eps_id = [self.id_] * len(self)

        if self.is_numpy:
            t = np.array(t)
            terminateds = np.array(terminateds)
            truncateds = np.array(truncateds)
            eps_id = np.array(eps_id)

        return dict(
            {
                # Trivial 1D data (compiled above).
                SampleBatch.TERMINATEDS: terminateds,
                SampleBatch.TRUNCATEDS: truncateds,
                SampleBatch.T: t,
                SampleBatch.EPS_ID: eps_id,
                # Retrieve obs, infos, actions, rewards using our get_... APIs,
                # which return all relevant timesteps (excluding the lookback
                # buffer!).
                SampleBatch.OBS: self.get_observations(),
                SampleBatch.INFOS: self.get_infos(),
                SampleBatch.ACTIONS: self.get_actions(),
                SampleBatch.REWARDS: self.get_rewards(),
            },
            # All `extra_model_outs`: Same as obs: Use get_... API.
            **{
                k: self.get_extra_model_outputs(k)
                for k in self.extra_model_outputs.keys()
            },
        )

    def get_sample_batch(self) -> SampleBatch:
        """Converts a `SingleAgentEpisode` into a `SampleBatch`.

        Returns:
            A SampleBatch containing all of this episode's data.
        """
        return SampleBatch(self.get_data_dict())

    def get_observations(
        self, indices: Optional[Union[int, List[int], slice]] = None
    ) -> Any:
        if indices is None:
            slice_ = slice(self._len_lookback_buffer, -1)
        elif isinstance(indices, list) and not self.is_numpy:
            return [self.observations[i] for i in indices]
        else:
            slice_ = indices

        if self.is_numpy:
            return tree.map_structure(lambda s: s[slice_], self.observations)
        else:
            return self.observations[slice_]

    def get_infos(self, indices: Optional[Union[int, List[int], slice]] = None) -> Any:
        if indices is None:
            slice_ = slice(self._len_lookback_buffer, -1)
        elif isinstance(indices, int):
            slice_ = slice(indices, indices + 1)
        elif isinstance(indices, list):
            return [self.infos[i] for i in indices]
        else:
            slice_ = indices
        return self.infos[slice_]

    def get_actions(
        self, indices: Optional[Union[int, List[int], slice]] = None
    ) -> Any:
        if indices is None:
            slice_ = slice(self._len_lookback_buffer, None)
        elif isinstance(indices, list) and not self.is_numpy:
            return [self.actions[i] for i in indices]
        else:
            slice_ = indices
        return self.actions[slice_]

    def get_rewards(
        self,
        indices: Optional[Union[int, List[int], slice]] = None,
        fill: Optional[float] = None,
    ) -> Any:
        if indices is None:
            slice_ = slice(self._len_lookback_buffer, None)
        elif isinstance(indices, list) and not self.is_numpy:
            return [self.rewards[i] for i in indices]
        else:
            slice_ = indices
        return self.rewards[slice_]

    def get_extra_model_outputs(
        self, key: str, indices: Optional[Union[int, List[int], slice]] = None
    ) -> Any:
        assert key in self.extra_model_outputs
        data = self.extra_model_outputs[key]
        if indices is None:
            slice_ = slice(self._len_lookback_buffer, None)
        elif isinstance(indices, list) and not self.is_numpy:
            return [data[i] for i in indices]
        else:
            slice_ = indices

        if self.is_numpy:
            return tree.map_structure(lambda s: s[slice_], data)
        else:
            return data[slice_]

    def get_return(self) -> float:
        """Calculates an episode's return.

        The return is computed by a simple sum, neglecting the discount factor.

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
        eps.t_started = state[5][1]
        eps.t = state[6][1]
        eps.is_terminated = state[7][1]
        eps.is_truncated = state[8][1]
        eps.extra_model_outputs = {k: v for k, v in state[9:]}
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
        length = self.t - self.t_started
        #assert length >= 0 and len(self.observations), (
        #    "ERROR: Cannot determine length of episode that hasn't started yet! Call "
        #    "`SingleAgentEpisode.add_env_reset(initial_observation=...)` "
        #    "first (after which `len(SingleAgentEpisode)` will be 0)."
        #)
        return length

    def __repr__(self):
        return f"SAEps({self.id_} len={len(self)})"
