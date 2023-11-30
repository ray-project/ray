import functools
from collections import defaultdict
import numpy as np
import uuid

import gymnasium as gym
from gymnasium.core import ActType, ObsType
from typing import Any, Dict, List, Optional, SupportsFloat, Union

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.env.utils import BufferWithInfiniteLookback


class SingleAgentEpisode:
    """A class representing RL environment episodes for individual agents.

    SingleAgentEpisode stores observations, info dicts, actions, rewards, and all
    module outputs (e.g. state outs, action logp, etc..) for an individual agent within
    some single-agent or multi-agent environment.
    The two main APIs to add data to an ongoing episode are the `add_env_reset()`
    and `add_env_step()` methods, which should be called passing the outputs of the
    respective gym.Env API calls: `env.reset()` and `env.step()`.

    A SingleAgentEpisode might also only represent a chunk of an episode, which is
    useful for cases, in which partial (non-complete episode) sampling is performed
    and collected episode data has to be returned before the actual gym.Env episode has
    finished (see `SingleAgentEpisode.cut()`). In order to still maintain visibility
    onto past experiences within such a "cut" episode, SingleAgentEpisode instances
    can have a "lookback buffer" of n timesteps at their beginning (left side), which
    solely exists for the purpose of compiling extra data (e.g. "prev. reward"), but
    is not considered part of the finished/packaged episode (b/c the data in the
    lookback buffer is already part of a previous episode chunk).

    Powerful getter methods, such as `get_observations()` help collect different types
    of data from the episode at individual time indices or time ranges, including the
    "lookback buffer" range described above. For example, to extract the last 4 rewards
    of an ongoing episode, one can call `self.get_rewards(slice(-4, None))` or
    `self.rewards[-4:]`. This would work, even if the ongoing SingleAgentEpisode is
    a continuation chunk from a much earlier started episode, as long as it has a
    lookback buffer size of sufficient size.

    Examples:

    .. testcode::

        import gymnasium as gym

        from ray.rllib.env.single_agent_episode import SingleAgentEpisode

        # Construct a new episode (without any data in it yet).
        episode = SingleAgentEpisode()
        assert len(episode) == 0

        # Fill the episode with some data (10 timesteps).
        env = gym.make("CartPole-v1")
        obs, infos = env.reset()
        episode.add_env_reset(obs, infos)

        # Even with the initial obs/infos, the episode is still considered len=0.
        assert len(episode) == 0
        for _ in range(10):
            action = env.action_space.sample()
            obs, reward, term, trunc, infos = env.step(action)
            episode.add_env_step(
                observation=obs,
                action=action,
                reward=reward,
                terminated=term,
                truncated=trunc,
                infos=infos,
            )
        assert len(episode) == 10

        # We can now access information from the episode via the getters.

        # Get the last 3 rewards (in a batch of size 3).
        episode.get_rewards(slice(-3, None))  # same as `episode.rewards[-3:]`

        # Get the most recent action (single item, not batched).
        # This works regardless of the action space or whether the episode has
        # been finalized or not (see below).
        episode.get_actions(-1)  # same as episode.actions[-1]

        # Looking back from ts=1, get the previous 4 rewards AND fill with 0.0
        # in case we go over the beginning (ts=0). So we would expect
        # [0.0, 0.0, 0.0, r0] to be returned here, where r0 is the very first received
        # reward in the episode:
        episode.get_rewards(slice(-4, 0), neg_indices_left_of_zero=True, fill=0.0)

        # Note the use of fill=0.0 here (fill everything that's out of range with this
        # value) AND the argument `neg_indices_left_of_zero=True`, which interprets
        # negative indices as being left of ts=0 (e.g. -1 being the timestep before
        # ts=0).

        # Assuming we had a complex action space (nested gym.spaces.Dict) with one or
        # more elements being Discrete or MultiDiscrete spaces:
        # 1) The `fill=...` argument would still work, filling all spaces (Boxes,
        # Discrete) with that provided value.
        # 2) Setting the flag `one_hot_discrete=True` would convert those discrete
        # sub-components automatically into one-hot (or multi-one-hot) tensors.
        # This simplifies the task of having to provide the previous 4 (nested and
        # partially discrete/multi-discrete) actions for each timestep within a training
        # batch, thereby filling timesteps before the episode started with 0.0s and
        # one-hot'ing the discrete/multi-discrete components in these actions:
        episode = SingleAgentEpisode(action_space=gym.spaces.Dict({
            "a": gym.spaces.Discrete(3),
            "b": gym.spaces.MultiDiscrete([2, 3]),
            "c": gym.spaces.Box(-1.0, 1.0, (2,)),
        }))

        # ... fill episode with data ...
        episode.add_env_reset(observation=0)
        # ... from a few steps.
        episode.add_env_step(
            observation=1,
            action={"a":0, "b":np.array([1, 2]), "c":np.array([.5, -.5], np.float32)},
            reward=1.0,
        )

        # In your connector
        prev_4_a = []
        # Note here that len(episode) does NOT include the lookback buffer.
        for ts in range(len(episode)):
            prev_4_a.append(
                episode.get_actions(
                    indices=slice(ts - 4, ts),
                    # Make sure negative indices are interpreted as
                    # "into lookback buffer"
                    neg_indices_left_of_zero=True,
                    # Zero-out everything even further before the lookback buffer.
                    fill=0.0,
                    # Take care of discrete components (get ready as NN input).
                    one_hot_discrete=True,
                )
            )

        # Finally, convert from list of batch items to a struct (same as action space)
        # of batched (numpy) arrays, in which all leafs have B==len(prev_4_a).
        from ray.rllib.utils.spaces.space_utils import batch

        prev_4_actions_col = batch(prev_4_a)
    """

    def __init__(
        self,
        id_: Optional[str] = None,
        *,
        observations: List[ObsType] = None,
        observation_space: Optional[gym.Space] = None,
        actions: List[ActType] = None,
        action_space: Optional[gym.Space] = None,
        rewards: List[SupportsFloat] = None,
        infos: List[Dict] = None,
        terminated: bool = False,
        truncated: bool = False,
        extra_model_outputs: Optional[Dict[str, Any]] = None,
        render_images: Optional[List[np.ndarray]] = None,
        t_started: Optional[int] = None,
        len_lookback_buffer: Optional[int] = 0,
    ) -> "SingleAgentEpisode":
        """Initializes a SingleAgentEpisode instance.

        This constructor can be called with or without already sampled data, part of
        which might then go into the lookback buffer.

        Args:
            id_: Optional. Unique identifier for this episode. If no id is
                provided the constructor generates a hexadecimal code for the id.
            observations: Optional. A list of observations from a rollout. If
                data is provided it should be complete (i.e. observations, actions,
                rewards, terminated, truncated, and all necessary
                `extra_model_outputs`). The length of the `observations` defines
                the default starting value. See the parameter `t_started`.
            actions: Optional. A list of actions from a rollout. If data is
                provided it should be complete (i.e. observations, actions,
                rewards, terminated, truncated, and all necessary
                `extra_model_outputs`).
            rewards: Optional. A list of rewards from a rollout. If data is
                provided it should be complete (i.e. observations, actions,
                rewards, terminated, truncated, and all necessary
                `extra_model_outputs`).
            infos: Optional. A list of infos from a rollout. If data is
                provided it should be complete (i.e. observations, actions,
                rewards, terminated, truncated, and all necessary
                `extra_model_outputs`).
            states: Optional. The hidden model states from a rollout. If
                data is provided it should be complete (i.e. observations, actions,
                rewards, terminated, truncated, and all necessary
                `extra_model_outputs`). States are only avasilable if a stateful
                model (`RLModule`) is used.
            terminated: Optional. A boolean indicating, if the episode is already
                terminated. Note, this parameter is only needed, if episode data is
                provided in the constructor. The default is `False`.
            truncated: Optional. A boolean indicating, if the episode was
                truncated. Note, this parameter is only needed, if episode data is
                provided in the constructor. The default is `False`.
            extra_model_outputs: Optional. A list of dictionaries containing specific
                model outputs for the algorithm used (e.g. `vf_preds` and `action_logp`
                for PPO) from a rollout. If data is provided it should be complete
                (i.e. observations, actions, rewards, terminated, truncated,
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

        # Lookback buffer length is provided.
        if len_lookback_buffer is not None:
            self._len_lookback_buffer = len_lookback_buffer
        # Lookback buffer length is not provided. Interpret already given data as
        # lookback buffer.
        else:
            self._len_lookback_buffer = len(rewards or [])

        infos = infos or [{} for _ in range(len(observations or []))]

        # Observations: t0 (initial obs) to T.
        self.observation_space = observation_space
        self.observations = BufferWithInfiniteLookback(
            data=observations,
            lookback=self._len_lookback_buffer,
            space=observation_space,
        )
        # Actions: t1 to T.
        self.action_space = action_space
        self.actions = BufferWithInfiniteLookback(
            data=actions,
            lookback=self._len_lookback_buffer,
            space=action_space,
        )
        # Rewards: t1 to T.
        self.rewards = BufferWithInfiniteLookback(
            data=rewards,
            lookback=self._len_lookback_buffer,
            space=gym.spaces.Box(float("-inf"), float("inf"), (), np.float32),
        )
        # Infos: t0 (initial info) to T.
        self.infos = BufferWithInfiniteLookback(
            data=infos,
            lookback=self._len_lookback_buffer,
        )

        # obs[-1] is the final observation in the episode.
        self.is_terminated = terminated
        # obs[-1] is the last obs in a truncated-by-the-env episode (there will no more
        # observations in following chunks for this episode).
        self.is_truncated = truncated
        # Extra model outputs, e.g. `action_dist_input` needed in the batch.
        self.extra_model_outputs = defaultdict(
            functools.partial(
                BufferWithInfiniteLookback,
                lookback=self._len_lookback_buffer,
            ),
        )
        for k, v in (extra_model_outputs or {}).items():
            if isinstance(v, BufferWithInfiniteLookback):
                self.extra_model_outputs[k] = v
            else:
                self.extra_model_outputs[k].data = v

        # RGB uint8 images from rendering the env; the images include the corresponding
        # rewards.
        assert render_images is None or observations is not None
        self.render_images = render_images or []

        # The global last timestep of the episode and the timesteps when this chunk
        # started (excluding a possible lookback buffer).
        self.t_started = t_started or 0

        self.t = (
            (len(rewards) if rewards is not None else 0)
            - self._len_lookback_buffer
            + self.t_started
        )

        # Validate the episode data thus far.
        self.validate()

    def concat_episode(self, episode_chunk: "SingleAgentEpisode"):
        """Adds the given `episode_chunk` to the right side of self.

        In order for this to work, both chunks (`self` and `episode_chunk`) must fit
        together. This is checked by the IDs (must be identical), the time step counters
        (`self.t` must be the same as `episode_chunk.t_started`), as well as the
        observations/infos at the concatenation boundaries (`self.observations[-1]`
        must match `episode_chunk.observations[0]`). Also, `self.is_done` must not be
        True, meaning `self.is_terminated` and `self.is_truncated` are both False.

        Args:
            episode_chunk: Another `SingleAgentEpisode` to be concatenated.

        Returns: A `SingleAegntEpisode` instance containing the concatenated
            from both episodes.
        """
        assert episode_chunk.id_ == self.id_
        assert not self.is_done and not self.is_finalized
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
        self.observations.extend(episode_chunk.get_observations())
        self.actions.extend(episode_chunk.get_actions())
        self.rewards.extend(episode_chunk.get_rewards())
        self.infos.extend(episode_chunk.get_infos())
        self.t = episode_chunk.t

        if episode_chunk.is_terminated:
            self.is_terminated = True
        elif episode_chunk.is_truncated:
            self.is_truncated = True

        for model_out_key in episode_chunk.extra_model_outputs.keys():
            self.extra_model_outputs[model_out_key].extend(
                episode_chunk.get_extra_model_outputs(model_out_key)
            )

        # Validate.
        self.validate()

    def add_env_reset(
        self,
        observation: ObsType,
        infos: Optional[Dict] = None,
        *,
        render_image: Optional[np.ndarray] = None,
    ) -> None:
        """Adds the initial data (after an `env.reset()`) to the episode.

        This data consists of initial observations and initial infos, as well as
        - optionally - a render image.

        Args:
            observation: The initial observation returned by `env.reset()`.
            infos: An (optional) info dict returned by `env.reset()`.
            render_image: Optional. An RGB uint8 image from rendering
                the environment right after the reset.
        """
        assert not self.is_done
        assert len(self.observations) == 0
        # Assume that this episode is completely empty and has not stepped yet.
        # Leave self.t (and self.t_started) at 0.
        assert self.t == self.t_started == 0

        infos = infos or {}

        if self.observation_space is not None:
            assert self.observation_space.contains(observation), (
                f"`observation` {observation} does NOT fit SingleAgentEpisode's "
                f"observation_space: {self.observation_space}!"
            )

        self.observations.append(observation)
        self.infos.append(infos)
        if render_image is not None:
            self.render_images.append(render_image)

        # Validate our data.
        self.validate()

    def add_env_step(
        self,
        observation: ObsType,
        action: ActType,
        reward: SupportsFloat,
        infos: Optional[Dict[str, Any]] = None,
        *,
        terminated: bool = False,
        truncated: bool = False,
        render_image: Optional[np.ndarray] = None,
        extra_model_outputs: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Adds results of an `env.step()` call (including the action) to this episode.

        This data consists of an observation and info dict, an action, a reward,
        terminated/truncated flags, extra model outputs (e.g. action probabilities or
        RNN internal state outputs), and - optionally - a render image.

        Args:
            observation: The observation received from the environment after taking
                `action`.
            action: The last action used by the agent during the call to `env.step()`.
            reward: The last reward received by the agent after taking `action`.
            infos: The last info received from the environment after taking `action`.
            terminated: A boolean indicating, if the environment has been
                terminated (after taking `action`).
            truncated: A boolean indicating, if the environment has been
                truncated (after taking `action`).
            render_image: Optional. An RGB uint8 image from rendering
                the environment (after taking `action`).
            extra_model_outputs: The last timestep's specific model outputs.
                These are normally outputs of an RLModule that were computed along with
                `action`, e.g. `action_logp` or `action_dist_inputs`.
        """
        # Cannot add data to an already done episode.
        assert (
            not self.is_done
        ), "The agent is already done: no data can be added to its episode."

        self.observations.append(observation)
        self.actions.append(action)
        self.rewards.append(reward)
        infos = infos or {}
        self.infos.append(infos)
        self.t += 1
        if render_image is not None:
            self.render_images.append(render_image)
        if extra_model_outputs is not None:
            for k, v in extra_model_outputs.items():
                self.extra_model_outputs[k].append(v)
        self.is_terminated = terminated
        self.is_truncated = truncated

        # Validate our data.
        self.validate()
        # Only check spaces every n timesteps.
        if self.t % 50:
            if self.observation_space is not None:
                assert self.observation_space.contains(observation), (
                    f"`observation` {observation} does NOT fit SingleAgentEpisode's "
                    f"observation_space: {self.observation_space}!"
                )
            if self.action_space is not None:
                assert self.action_space.contains(action), (
                    f"`action` {action} does NOT fit SingleAgentEpisode's "
                    f"action_space: {self.action_space}!"
                )

    def validate(self) -> None:
        """Validates the episode's data.

        This function ensures that the data stored to a `SingleAgentEpisode` is
        in order (e.g. that the correct number of observations, actions, rewards
        are there).
        """
        assert len(self.observations) == len(self.infos)
        if len(self.observations) == 0:
            assert len(self.infos) == len(self.rewards) == len(self.actions) == 0
            for k, v in self.extra_model_outputs.items():
                assert len(v) == 0
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
            assert self._len_lookback_buffer + len(self) == len(self.rewards.data)

    @property
    def is_finalized(self) -> bool:
        """True, if the data in this episode is already stored as numpy arrays."""
        # If rewards are still a list, return False.
        # Otherwise, rewards should already be a (1D) numpy array.
        return self.rewards.finalized

    @property
    def is_done(self) -> bool:
        """Whether the episode is actually done (terminated or truncated).

        A done episode cannot be continued via `self.add_timestep()` or being
        concatenated on its right-side with another episode chunk or being
        succeeded via `self.create_successor()`.
        """
        return self.is_terminated or self.is_truncated

    def finalize(self) -> "SingleAgentEpisode":
        """Converts this Episode's list attributes to numpy arrays.

        This means in particular that this episodes' lists of (possibly complex)
        data (e.g. if we have a dict obs space) will be converted to (possibly complex)
        structs, whose leafs are now numpy arrays. Each of these leaf numpy arrays will
        have the same length (batch dimension) as the length of the original lists.

        Note that SampleBatch.INFOS are NEVER numpy'ized and will remain a list
        (normally, a list of the original, env-returned dicts). This is due to the
        herterogenous nature of INFOS returned by envs, which would make it unwieldy to
        convert this information to numpy arrays.

        After calling this method, no further data may be added to this episode via
        the `self.add_env_step()` method.

        Examples:

        .. testcode::

            import numpy as np

            from ray.rllib.env.single_agent_episode import SingleAgentEpisode

            episode = SingleAgentEpisode(
                observations=[0, 1, 2, 3],
                actions=[1, 2, 3],
                rewards=[1, 2, 3],
                # Note: terminated/truncated have nothing to do with an episode
                # being `finalized` or not (via the `self.finalize()` method)!
                terminated=False,
            )
            # Episode has not been finalized (numpy'ized) yet.
            assert not episode.is_finalized
            # We are still operating on lists.
            assert episode.get_observations([1]) == [1]
            assert episode.get_observations(slice(None, 2)) == [0, 1]
            # We can still add data (and even add the terminated=True flag).
            episode.add_env_step(
                observation=4,
                action=4,
                reward=4,
                terminated=True,
            )
            # Still NOT finalized.
            assert not episode.is_finalized

            # Let's finalize the episode.
            episode.finalize()
            assert episode.is_finalized

            # We cannot add data anymore. The following would crash.
            # episode.add_env_step(observation=5, action=5, reward=5)

            # Everything is now numpy arrays (with 0-axis of size
            # B=[len of requested slice]).
            assert isinstance(episode.get_observations([1]), np.ndarray)  # B=1
            assert isinstance(episode.actions[0:2], np.ndarray)  # B=2
            assert isinstance(episode.rewards[1:4], np.ndarray)  # B=3

        Returns:
             This `SingleAgentEpisode` object with the converted numpy data.
        """

        self.observations.finalize()
        self.actions.finalize()
        self.rewards.finalize()
        self.render_images = np.array(self.render_images, dtype=np.uint8)
        for k, v in self.extra_model_outputs.items():
            self.extra_model_outputs[k].finalize()

        return self

    def cut(self, len_lookback_buffer: int = 0) -> "SingleAgentEpisode":
        """Returns a successor episode chunk (of len=0) continuing from this Episode.

        The successor will have the same ID as `self`.
        If no lookback buffer is requested (len_lookback_buffer=0), the successor's
        observations will be the last observation(s) of `self` and its length will
        therefore be 0 (no further steps taken yet). If `len_lookback_buffer` > 0,
        the returned successor will have `len_lookback_buffer` observations (and
        actions, rewards, etc..) taken from the right side (end) of `self`. For example
        if `len_lookback_buffer=2`, the returned successor's lookback buffer actions
        will be identical to `self.actions[-2:]`.

        This method is useful if you would like to discontinue building an episode
        chunk (b/c you have to return it from somewhere), but would like to have a new
        episode instance to continue building the actual gym.Env episode at a later
        time. Vie the `len_lookback_buffer` argument, the continuing chunk (successor)
        will still be able to "look back" into this predecessor episode's data (at
        least to some extend, depending on the value of `len_lookback_buffer`).

        Args:
            len_lookback_buffer: The number of timesteps to take along into the new
                chunk as "lookback buffer". A lookback buffer is additional data on
                the left side of the actual episode data for visibility purposes
                (but without actually being part of the new chunk). For example, if
                `self` ends in actions 5, 6, 7, and 8, and we call
                `self.cut(len_lookback_buffer=2)`, the returned chunk will have
                actions 7 and 8 already in it, but still `t_started`==t==8 (not 7!) and
                a length of 0. If there is not enough data in `self` yet to fulfil
                the `len_lookback_buffer` request, the value of `len_lookback_buffer`
                is automatically adjusted (lowered).

        Returns:
            The successor Episode chunk of this one with the same ID and state and the
            only observation being the last observation in self.
        """
        assert not self.is_done and len_lookback_buffer >= 0

        # Initialize this chunk with the most recent obs and infos (even if lookback is
        # 0). Similar to an initial `env.reset()`.
        indices_obs_and_infos = slice(-len_lookback_buffer - 1, None)
        indices_rest = (
            slice(-len_lookback_buffer, None)
            if len_lookback_buffer > 0
            else slice(None, 0)
        )

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

    def get_observations(
        self,
        indices: Optional[Union[int, List[int], slice]] = None,
        *,
        neg_indices_left_of_zero: bool = False,
        fill: Optional[float] = None,
        one_hot_discrete: bool = False,
    ) -> Any:
        """Returns individual observations or batched ranges thereof from this episode.

        Args:
            indices: A single int is interpreted as an index, from which to return the
                individual observation stored at this index.
                A list of ints is interpreted as a list of indices from which to gather
                individual observations in a batch of size len(indices).
                A slice object is interpreted as a range of observations to be returned.
                Thereby, negative indices by default are interpreted as "before the end"
                unless the `neg_indices_left_of_zero=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
            neg_indices_left_of_zero: If True, negative values in `indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with observations [4, 5, 6,  7, 8, 9],
                where [4, 5, 6] is the lookback buffer range (ts=0 item is 7), will
                respond to `get_observations(-1, neg_indices_left_of_zero=True)`
                with `6` and to
                `get_observations(slice(-2, 1), neg_indices_left_of_zero=True)` with
                `[5, 6,  7]`.
            fill: An optional float value to use for filling up the returned results at
                the boundaries. This filling only happens if the requested index range's
                start/stop boundaries exceed the episode's boundaries (including the
                lookback buffer on the left side). This comes in very handy, if users
                don't want to worry about reaching such boundaries and want to zero-pad.
                For example, an episode with observations [10, 11,  12, 13, 14] and
                lookback buffer size of 2 (meaning observations `10` and `11` are part
                of the lookback buffer) will respond to
                `get_observations(slice(-7, -2), fill=0.0)` with
                `[0.0, 0.0, 10, 11, 12]`.
            one_hot_discrete: If True, will return one-hot vectors (instead of
                int-values) for those sub-components of a (possibly complex) observation
                space that are Discrete or MultiDiscrete.  Note that if `fill=0` and the
                requested `indices` are out of the range of our data, the returned
                one-hot vectors will actually be zero-hot (all slots zero).

        Examples:

        .. testcode::

            from ray.rllib.env.single_agent_episode import SingleAgentEpisode

            episode = SingleAgentEpisode(
                # Discrete(4) observations (ints between 0 and 4 (excl.))
                observation_space=gym.spaces.Discrete(4),
                observations=[0, 1, 2, 3],
                actions=[1, 2, 3], rewards=[1, 2, 3],  # <- not relevant for this demo
            )
            # Plain usage (`indices` arg only).
            episode.get_observations(-1)  # 3
            episode.get_observations(0)  # 0
            episode.get_observations([0, 2])  # [0, 2]
            episode.get_observations([-1, 0])  # [3, 0]
            episode.get_observations(slice(None, 2))  # [0, 1]
            episode.get_observations(slice(-2, None))  # [2, 3]
            # Using `fill=...` (requesting slices beyond the boundaries).
            episode.get_observations(slice(-6, -2), fill=-9)  # [-9, -9, 0, 1]
            episode.get_observations(slice(2, 5), fill=-7)  # [2, 3, -7]
            # Using `one_hot_discrete=True`.
            episode.get_observations(2, one_hot_discrete=True)  # [0 0 1 0]
            episode.get_observations(3, one_hot_discrete=True)  # [0 0 0 1]
            episode.get_observations(
                slice(0, 3),
                one_hot_discrete=True,
            )   # [[1 0 0 0], [0 1 0 0], [0 0 1 0]]
            # Special case: Using `fill=0.0` AND `one_hot_discrete=True`.
            episode.get_observations(
                -1,
                neg_indices_left_of_zero=True,  # -1 means one left of ts=0
                fill=0.0,
                one_hot_discrete=True,
            )  # [0 0 0 0]  <- all 0s one-hot tensor (note difference to [1 0 0 0]!)

        Returns:
            The collected observations.
            As a 0-axis batch, if there are several `indices` or a list of exactly one
            index provided OR `indices` is a slice object.
            As single item (B=0 -> no additional 0-axis) if `indices` is a single int.
        """
        return self.observations.get(
            indices=indices,
            neg_indices_left_of_zero=neg_indices_left_of_zero,
            fill=fill,
            one_hot_discrete=one_hot_discrete,
        )

    def get_infos(
        self,
        indices: Optional[Union[int, List[int], slice]] = None,
        *,
        neg_indices_left_of_zero: bool = False,
        fill: Optional[Any] = None,
    ) -> Any:
        """Returns individual info dicts or batched ranges thereof from this episode.

        Args:
            indices: A single int is interpreted as an index, from which to return the
                individual info dict stored at this index.
                A list of ints is interpreted as a list of indices from which to gather
                individual info dicts in a list of size len(indices).
                A slice object is interpreted as a range of info dicts to be returned.
                Thereby, negative indices by default are interpreted as "before the end"
                unless the `neg_indices_left_of_zero=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
            neg_indices_left_of_zero: If True, negative values in `indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with infos
                [{"l":4}, {"l":5}, {"l":6},  {"a":7}, {"b":8}, {"c":9}], where the
                first 3 items are the lookback buffer (ts=0 item is {"a": 7}), will
                respond to `get_infos(-1, neg_indices_left_of_zero=True)` with
                `{"l":6}` and to
                `get_infos(slice(-2, 1), neg_indices_left_of_zero=True)` with
                `[{"l":5}, {"l":6},  {"a":7}]`.
            fill: An optional value to use for filling up the returned results at
                the boundaries. This filling only happens if the requested index range's
                start/stop boundaries exceed the episode's boundaries (including the
                lookback buffer on the left side). This comes in very handy, if users
                don't want to worry about reaching such boundaries and want to
                auto-fill. For example, an episode with infos
                [{"l":10}, {"l":11},  {"a":12}, {"b":13}, {"c":14}] and lookback buffer
                size of 2 (meaning infos {"l":10}, {"l":11} are part of the lookback
                buffer) will respond to `get_infos(slice(-7, -2), fill={"o": 0.0})`
                with `[{"o":0.0}, {"o":0.0}, {"l":10}, {"l":11}, {"a":12}]`.
                TODO (sven): This would require a space being provided. Maybe we can
                 skip this check for infos, which don't have a space anyways.

        Examples:

        .. testcode::

            from ray.rllib.env.single_agent_episode import SingleAgentEpisode

            episode = SingleAgentEpisode(
                infos=[{"a":0}, {"b":1}, {"c":2}, {"d":3}],
                # The following is needed, but not relevant for this demo.
                observations=[0, 1, 2, 3], actions=[1, 2, 3], rewards=[1, 2, 3],
            )
            # Plain usage (`indices` arg only).
            episode.get_infos(-1)  # {"d":3}
            episode.get_infos(0)  # {"a":0}
            episode.get_infos([0, 2])  # [{"a":0},{"c":2}]
            episode.get_infos([-1, 0])  # [{"d":3},{"a":0}]
            episode.get_infos(slice(None, 2))  # [{"a":0},{"b":1}]
            episode.get_infos(slice(-2, None))  # [{"c":2},{"d":3}]
            # Using `fill=...` (requesting slices beyond the boundaries).
            # TODO (sven): This would require a space being provided. Maybe we can
            #  skip this check for infos, which don't have a space anyways.
            # episode.get_infos(slice(-5, -3), fill={"o":-1})  # [{"o":-1},{"a":0}]
            # episode.get_infos(slice(3, 5), fill={"o":-2})  # [{"d":3},{"o":-2}]

        Returns:
            The collected info dicts.
            As a 0-axis batch, if there are several `indices` or a list of exactly one
            index provided OR `indices` is a slice object.
            As single item (B=0 -> no additional 0-axis) if `indices` is a single int.
        """
        return self.infos.get(
            indices=indices,
            neg_indices_left_of_zero=neg_indices_left_of_zero,
            fill=fill,
        )

    def get_actions(
        self,
        indices: Optional[Union[int, List[int], slice]] = None,
        *,
        neg_indices_left_of_zero: bool = False,
        fill: Optional[float] = None,
        one_hot_discrete: bool = False,
    ) -> Any:
        """Returns individual actions or batched ranges thereof from this episode.

        Args:
            indices: A single int is interpreted as an index, from which to return the
                individual action stored at this index.
                A list of ints is interpreted as a list of indices from which to gather
                individual actions in a batch of size len(indices).
                A slice object is interpreted as a range of actions to be returned.
                Thereby, negative indices by default are interpreted as "before the end"
                unless the `neg_indices_left_of_zero=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
            neg_indices_left_of_zero: If True, negative values in `indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with actions [4, 5, 6,  7, 8, 9], where
                [4, 5, 6] is the lookback buffer range (ts=0 item is 7), will respond
                to `get_actions(-1, neg_indices_left_of_zero=True)` with `6` and
                to `get_actions(slice(-2, 1), neg_indices_left_of_zero=True)` with
                `[5, 6,  7]`.
            fill: An optional float value to use for filling up the returned results at
                the boundaries. This filling only happens if the requested index range's
                start/stop boundaries exceed the episode's boundaries (including the
                lookback buffer on the left side). This comes in very handy, if users
                don't want to worry about reaching such boundaries and want to zero-pad.
                For example, an episode with actions [10, 11,  12, 13, 14] and
                lookback buffer size of 2 (meaning actions `10` and `11` are part
                of the lookback buffer) will respond to
                `get_actions(slice(-7, -2), fill=0.0)` with `[0.0, 0.0, 10, 11, 12]`.
            one_hot_discrete: If True, will return one-hot vectors (instead of
                int-values) for those sub-components of a (possibly complex) action
                space that are Discrete or MultiDiscrete. Note that if `fill=0` and the
                requested `indices` are out of the range of our data, the returned
                one-hot vectors will actually be zero-hot (all slots zero).

        Examples:

        .. testcode::

            import gymnasium as gym
            from ray.rllib.env.single_agent_episode import SingleAgentEpisode

            episode = SingleAgentEpisode(
                # Discrete(4) actions (ints between 0 and 4 (excl.))
                action_space=gym.spaces.Discrete(4),
                actions=[1, 2, 3],
                observations=[0, 1, 2, 3], rewards=[1, 2, 3],  # <- not relevant here
            )
            # Plain usage (`indices` arg only).
            episode.get_actions(-1)  # 3
            episode.get_actions(0)  # 1
            episode.get_actions([0, 2])  # [1, 3]
            episode.get_actions([-1, 0])  # [3, 1]
            episode.get_actions(slice(None, 2))  # [1, 2]
            episode.get_actions(slice(-2, None))  # [2, 3]
            # Using `fill=...` (requesting slices beyond the boundaries).
            episode.get_actions(slice(-5, -2), fill=-9)  # [-9, -9, 1, 2]
            episode.get_actions(slice(1, 5), fill=-7)  # [2, 3, -7, -7]
            # Using `one_hot_discrete=True`.
            episode.get_actions(1, one_hot_discrete=True)  # [0 0 1 0] (action=2)
            episode.get_actions(2, one_hot_discrete=True)  # [0 0 0 1] (action=3)
            episode.get_actions(
                slice(0, 2),
                one_hot_discrete=True,
            )   # [[0 1 0 0], [0 0 0 1]] (actions=1 and 3)
            # Special case: Using `fill=0.0` AND `one_hot_discrete=True`.
            episode.get_actions(
                -1,
                neg_indices_left_of_zero=True,  # -1 means one left of ts=0
                fill=0.0,
                one_hot_discrete=True,
            )  # [0 0 0 0]  <- all 0s one-hot tensor (note difference to [1 0 0 0]!)

        Returns:
            The collected actions.
            As a 0-axis batch, if there are several `indices` or a list of exactly one
            index provided OR `indices` is a slice object.
            As single item (B=0 -> no additional 0-axis) if `indices` is a single int.
        """
        return self.actions.get(
            indices=indices,
            neg_indices_left_of_zero=neg_indices_left_of_zero,
            fill=fill,
            one_hot_discrete=one_hot_discrete,
        )

    def get_rewards(
        self,
        indices: Optional[Union[int, List[int], slice]] = None,
        *,
        neg_indices_left_of_zero: bool = False,
        fill: Optional[Any] = None,
    ) -> Any:
        """Returns individual rewards or batched ranges thereof from this episode.

        Args:
            indices: A single int is interpreted as an index, from which to return the
                individual reward stored at this index.
                A list of ints is interpreted as a list of indices from which to gather
                individual rewards in a batch of size len(indices).
                A slice object is interpreted as a range of rewards to be returned.
                Thereby, negative indices by default are interpreted as "before the end"
                unless the `neg_indices_left_of_zero=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
            neg_indices_left_of_zero: Negative values in `indices` are interpreted as
                 as "before ts=0", meaning going back into the lookback buffer.
                 For example, an episode with rewards [4, 5, 6,  7, 8, 9], where
                 [4, 5, 6] is the lookback buffer range (ts=0 item is 7), will respond
                 to `get_rewards(-1, neg_indices_left_of_zero=True)` with `6` and
                 to `get_rewards(slice(-2, 1), neg_indices_left_of_zero=True)` with
                 `[5, 6,  7]`.
            fill: An optional float value to use for filling up the returned results at
                the boundaries. This filling only happens if the requested index range's
                start/stop boundaries exceed the episode's boundaries (including the
                lookback buffer on the left side). This comes in very handy, if users
                don't want to worry about reaching such boundaries and want to zero-pad.
                For example, an episode with rewards [10, 11,  12, 13, 14] and
                lookback buffer size of 2 (meaning rewards `10` and `11` are part
                of the lookback buffer) will respond to
                `get_rewards(slice(-7, -2), fill=0.0)` with `[0.0, 0.0, 10, 11, 12]`.

        Examples:

        .. testcode::

            from ray.rllib.env.single_agent_episode import SingleAgentEpisode

            episode = SingleAgentEpisode(
                rewards=[1.0, 2.0, 3.0],
                observations=[0, 1, 2, 3], actions=[1, 2, 3],  # <- not relevant here
            )
            # Plain usage (`indices` arg only).
            episode.get_rewards(-1)  # 3.0
            episode.get_rewards(0)  # 1.0
            episode.get_rewards([0, 2])  # [1.0, 3.0]
            episode.get_rewards([-1, 0])  # [3.0, 1.0]
            episode.get_rewards(slice(None, 2))  # [1.0, 2.0]
            episode.get_rewards(slice(-2, None))  # [2.0, 3.0]
            # Using `fill=...` (requesting slices beyond the boundaries).
            episode.get_rewards(slice(-5, -2), fill=0.0)  # [0.0, 0.0, 1.0, 2.0]
            episode.get_rewards(slice(1, 5), fill=0.0)  # [2.0, 3.0, 0.0, 0.0]

        Returns:
            The collected rewards.
            As a 0-axis batch, if there are several `indices` or a list of exactly one
            index provided OR `indices` is a slice object.
            As single item (B=0 -> no additional 0-axis) if `indices` is a single int.
        """
        return self.rewards.get(
            indices=indices,
            neg_indices_left_of_zero=neg_indices_left_of_zero,
            fill=fill,
        )

    def get_extra_model_outputs(
        self,
        key: str,
        indices: Optional[Union[int, List[int], slice]] = None,
        *,
        neg_indices_left_of_zero: bool = False,
        fill: Optional[Any] = None,
    ) -> Any:
        """Returns extra model outputs (under given key) from this episode.

        Args:
            key: The `key` within `self.extra_model_outputs` to extract data for.
            indices: A single int is interpreted as an index, from which to return an
                individual extra model output stored under `key` at index.
                A list of ints is interpreted as a list of indices from which to gather
                individual actions in a batch of size len(indices).
                A slice object is interpreted as a range of extra model outputs to be
                returned. Thereby, negative indices by default are interpreted as
                "before the end" unless the `neg_indices_left_of_zero=True` option is
                used, in which case negative indices are interpreted as "before ts=0",
                meaning going back into the lookback buffer.
            neg_indices_left_of_zero: If True, negative values in `indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with
                extra_model_outputs['a'] = [4, 5, 6,  7, 8, 9], where [4, 5, 6] is the
                lookback buffer range (ts=0 item is 7), will respond to
                `get_extra_model_outputs("a", -1, neg_indices_left_of_zero=True)` with
                `6` and to `get_extra_model_outputs("a", slice(-2, 1),
                neg_indices_left_of_zero=True)` with `[5, 6,  7]`.
            fill: An optional float value to use for filling up the returned results at
                the boundaries. This filling only happens if the requested index range's
                start/stop boundaries exceed the episode's boundaries (including the
                lookback buffer on the left side). This comes in very handy, if users
                don't want to worry about reaching such boundaries and want to zero-pad.
                For example, an episode with
                extra_model_outputs["b"] = [10, 11,  12, 13, 14] and lookback buffer
                size of 2 (meaning `10` and `11` are part of the lookback buffer) will
                respond to
                `get_extra_model_outputs("b", slice(-7, -2), fill=0.0)` with
                `[0.0, 0.0, 10, 11, 12]`.
                TODO (sven): This would require a space being provided. Maybe we can
                 automatically infer the space from existing data?

        Examples:

        .. testcode::

            from ray.rllib.env.single_agent_episode import SingleAgentEpisode

            episode = SingleAgentEpisode(
                extra_model_outputs={"mo": [1, 2, 3]},
                # The following is needed, but not relevant for this demo.
                observations=[0, 1, 2, 3], actions=[1, 2, 3], rewards=[1, 2, 3],
            )

            # Plain usage (`indices` arg only).
            episode.get_extra_model_outputs("mo", -1)  # 3
            episode.get_extra_model_outputs("mo", 1)  # 0
            episode.get_extra_model_outputs("mo", [0, 2])  # [1, 3]
            episode.get_extra_model_outputs("mo", [-1, 0])  # [3, 1]
            episode.get_extra_model_outputs("mo", slice(None, 2))  # [1, 2]
            episode.get_extra_model_outputs("mo", slice(-2, None))  # [2, 3]
            # Using `fill=...` (requesting slices beyond the boundaries).
            # TODO (sven): This would require a space being provided. Maybe we can
            #  automatically infer the space from existing data?
            # episode.get_extra_model_outputs("mo", slice(-5, -2), fill=0)  # [0, 0, 1]
            # episode.get_extra_model_outputs("mo", slice(2, 5), fill=-1)  # [3, -1, -1]

        Returns:
            The collected extra_model_outputs[`key`].
            As a 0-axis batch, if there are several `indices` or a list of exactly one
            index provided OR `indices` is a slice object.
            As single item (B=0 -> no additional 0-axis) if `indices` is a single int.
        """
        value = self.extra_model_outputs[key]
        # The expected case is: `value` is a `BufferWithInfiniteLookback`.
        if isinstance(value, BufferWithInfiniteLookback):
            return value.get(
                indices=indices,
                neg_indices_left_of_zero=neg_indices_left_of_zero,
                fill=fill,
            )
        # It might be that the user has added new key/value pairs in their custom
        # postprocessing/connector logic. The values are then most likely numpy
        # arrays. We convert them automatically to buffers and get the requested
        # indices (with the given options) from there.
        return BufferWithInfiniteLookback(value).get(
            indices, fill=fill, neg_indices_left_of_zero=neg_indices_left_of_zero
        )

    def slice(self, slice_: slice) -> "SingleAgentEpisode":
        """Returns a slice of this episode with the given slice object.

        For example, if `self` contains o0 (the reset observation), o1, o2, o3, and o4
        and the actions a1, a2, a3, and a4 (len of `self` is 4), then a call to
        `self.slice(slice(1, 3))` would return a new SingleAgentEpisode with
        observations o1, o2, and o3, and actions a2 and a3. Note here that there is
        always one observation more in an episode than there are actions (and rewards
        and extra model outputs) due to the initial observation received after an env
        reset.

        Note that in any case, the lookback buffer will remain (if possible) at the same
        size as it has been previously set to (`self._len_lookback_buffer`) and the
        given slice object will NOT have to provide for this extra offset at the
        beginning.

        Args:
            slice_: The slice object to use for slicing. This should exclude the
                lookback buffer, which will be prepended automatically to the returned
                slice.

        Returns:
            The new SingleAgentEpisode representing the requested slice.
        """
        # Figure out, whether slicing stops at the very end of this episode to know
        # whether `self.is_terminated/is_truncated` should be kept as-is.
        keep_done = slice_.stop is None or slice_.stop == len(self)
        start = slice_.start or 0
        t_started = self.t_started + start + (0 if start >= 0 else len(self))

        neg_indices_left_of_zero = (slice_.start or 0) >= 0
        slice_ = slice(
            # Make sure that the lookback buffer is part of the new slice as well.
            (slice_.start or 0) - self._len_lookback_buffer,
            slice_.stop,
            slice_.step,
        )
        slice_obs_infos = slice(
            slice_.start,
            # Obs and infos need one more step at the end.
            ((slice_.stop if slice_.stop != -1 else (len(self) - 1)) or len(self)) + 1,
            slice_.step,
        )
        return SingleAgentEpisode(
            id_=self.id_,
            observations=self.get_observations(
                slice_obs_infos,
                neg_indices_left_of_zero=neg_indices_left_of_zero,
            ),
            infos=self.get_infos(
                slice_obs_infos,
                neg_indices_left_of_zero=neg_indices_left_of_zero,
            ),
            actions=self.get_actions(
                slice_,
                neg_indices_left_of_zero=neg_indices_left_of_zero,
            ),
            rewards=self.get_rewards(
                slice_,
                neg_indices_left_of_zero=neg_indices_left_of_zero,
            ),
            extra_model_outputs={
                k: self.get_extra_model_outputs(
                    k,
                    slice_,
                    neg_indices_left_of_zero=neg_indices_left_of_zero,
                )
                for k in self.extra_model_outputs
            },
            terminated=(self.is_terminated if keep_done else False),
            truncated=(self.is_truncated if keep_done else False),
            # Provide correct timestep- and pre-buffer information.
            t_started=t_started,
            len_lookback_buffer=self._len_lookback_buffer,
        )

    def get_data_dict(self):
        """Converts a SingleAgentEpisode into a data dict mapping str keys to data.

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

        if self.is_finalized:
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
                SampleBatch.OBS: self.get_observations(slice(None, -1)),
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
        """Converts this `SingleAgentEpisode` into a `SampleBatch`.

        Returns:
            A SampleBatch containing all of this episode's data.
        """
        return SampleBatch(self.get_data_dict())

    def get_return(self) -> float:
        """Calculates an episode's return, excluding the lookback buffer's rewards.

        The return is computed by a simple sum, neglecting the discount factor.

        Returns:
            The sum of rewards collected during this episode, excluding possible data
            inside the lookback buffer.
        """
        return sum(self.get_rewards())

    def __len__(self) -> int:
        """Returning the length of an episode.

        The length of an episode is defined by the length of its data, excluding
        the lookback buffer data. The length is the number of timesteps an agent has
        stepped through an environment thus far.

        The length is 0 in case of an episode whose env has NOT been reset yet, but
        also 0 right after the `env.reset()` data has been added via
        `self.add_env_reset()`. Only after the first call to `env.step()` (and
        `self.add_env_step()`, the length will be 1.

        Returns:
            An integer, defining the length of an episode.
        """
        return self.t - self.t_started

    def __repr__(self):
        return f"SAEps({self.id_} len={len(self)})"

    def __getitem__(self, item: slice) -> "SingleAgentEpisode":
        """Enable squared bracket indexing- and slicing syntax, e.g. episode[-4:]."""
        if isinstance(item, slice):
            return self.slice(slice_=item)
        else:
            raise NotImplementedError(
                f"SingleAgentEpisode does not support getting item '{item}'! "
                "Only slice objects allowed with the syntax: `episode[a:b]`."
            )
