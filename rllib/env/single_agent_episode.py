from collections import defaultdict
import copy
import functools
import numpy as np
import time
import uuid

import gymnasium as gym
from gymnasium.core import ActType, ObsType
from typing import Any, Dict, List, Optional, SupportsFloat, Union

from ray.rllib.core.columns import Columns
from ray.rllib.env.utils.infinite_lookback_buffer import InfiniteLookbackBuffer
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.serialization import gym_space_from_dict, gym_space_to_dict
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.typing import AgentID, ModuleID
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
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
        import numpy as np

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
        for _ in range(5):
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
        assert len(episode) == 5

        # We can now access information from the episode via the getter APIs.

        # Get the last 3 rewards (in a batch of size 3).
        episode.get_rewards(slice(-3, None))  # same as `episode.rewards[-3:]`

        # Get the most recent action (single item, not batched).
        # This works regardless of the action space or whether the episode has
        # been numpy'ized or not (see below).
        episode.get_actions(-1)  # same as episode.actions[-1]

        # Looking back from ts=1, get the previous 4 rewards AND fill with 0.0
        # in case we go over the beginning (ts=0). So we would expect
        # [0.0, 0.0, 0.0, r0] to be returned here, where r0 is the very first received
        # reward in the episode:
        episode.get_rewards(slice(-4, 0), neg_index_as_lookback=True, fill=0.0)

        # Note the use of fill=0.0 here (fill everything that's out of range with this
        # value) AND the argument `neg_index_as_lookback=True`, which interprets
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
                    neg_index_as_lookback=True,
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

    __slots__ = (
        "actions",
        "agent_id",
        "extra_model_outputs",
        "id_",
        "infos",
        "is_terminated",
        "is_truncated",
        "module_id",
        "multi_agent_episode_id",
        "observations",
        "rewards",
        "t",
        "t_started",
        "_action_space",
        "_last_added_observation",
        "_last_added_infos",
        "_last_step_time",
        "_observation_space",
        "_start_time",
        "_custom_data",
    )

    def __init__(
        self,
        id_: Optional[str] = None,
        *,
        observations: Optional[Union[List[ObsType], InfiniteLookbackBuffer]] = None,
        observation_space: Optional[gym.Space] = None,
        infos: Optional[Union[List[Dict], InfiniteLookbackBuffer]] = None,
        actions: Optional[Union[List[ActType], InfiniteLookbackBuffer]] = None,
        action_space: Optional[gym.Space] = None,
        rewards: Optional[Union[List[SupportsFloat], InfiniteLookbackBuffer]] = None,
        terminated: bool = False,
        truncated: bool = False,
        extra_model_outputs: Optional[Dict[str, Any]] = None,
        t_started: Optional[int] = None,
        len_lookback_buffer: Union[int, str] = "auto",
        agent_id: Optional[AgentID] = None,
        module_id: Optional[ModuleID] = None,
        multi_agent_episode_id: Optional[int] = None,
    ):
        """Initializes a SingleAgentEpisode instance.

        This constructor can be called with or without already sampled data, part of
        which might then go into the lookback buffer.

        Args:
            id_: Unique identifier for this episode. If no ID is provided the
                constructor generates a unique hexadecimal code for the id.
            observations: Either a list of individual observations from a sampling or
                an already instantiated `InfiniteLookbackBuffer` object (possibly
                with observation data in it). If a list, will construct the buffer
                automatically (given the data and the `len_lookback_buffer` argument).
            observation_space: An optional gym.Space, which all individual observations
                should abide to. If not None and this SingleAgentEpisode is numpy'ized
                (via the `self.to_numpy()` method), and data is appended or set, the new
                data will be checked for correctness.
            infos: Either a list of individual info dicts from a sampling or
                an already instantiated `InfiniteLookbackBuffer` object (possibly
                with info dicts in it). If a list, will construct the buffer
                automatically (given the data and the `len_lookback_buffer` argument).
            actions: Either a list of individual info dicts from a sampling or
                an already instantiated `InfiniteLookbackBuffer` object (possibly
                with info dict] data in it). If a list, will construct the buffer
                automatically (given the data and the `len_lookback_buffer` argument).
            action_space: An optional gym.Space, which all individual actions
                should abide to. If not None and this SingleAgentEpisode is numpy'ized
                (via the `self.to_numpy()` method), and data is appended or set, the new
                data will be checked for correctness.
            rewards: Either a list of individual rewards from a sampling or
                an already instantiated `InfiniteLookbackBuffer` object (possibly
                with reward data in it). If a list, will construct the buffer
                automatically (given the data and the `len_lookback_buffer` argument).
            extra_model_outputs: A dict mapping string keys to either lists of
                individual extra model output tensors (e.g. `action_logp` or
                `state_outs`) from a sampling or to already instantiated
                `InfiniteLookbackBuffer` object (possibly with extra model output data
                in it). If mapping is to lists, will construct the buffers automatically
                (given the data and the `len_lookback_buffer` argument).
            terminated: A boolean indicating, if the episode is already terminated.
            truncated: A boolean indicating, if the episode has been truncated.
            t_started: Optional. The starting timestep of the episode. The default
                is zero. If data is provided, the starting point is from the last
                observation onwards (i.e. `t_started = len(observations) - 1`). If
                this parameter is provided the episode starts at the provided value.
            len_lookback_buffer: The size of the (optional) lookback buffers to keep in
                front of this Episode for each type of data (observations, actions,
                etc..). If larger 0, will interpret the first `len_lookback_buffer`
                items in each type of data as NOT part of this actual
                episode chunk, but instead serve as "historical" record that may be
                viewed and used to derive new data from. For example, it might be
                necessary to have a lookback buffer of four if you would like to do
                observation frame stacking and your episode has been cut and you are now
                operating on a new chunk (continuing from the cut one). Then, for the
                first 3 items, you would have to be able to look back into the old
                chunk's data.
                If `len_lookback_buffer` is "auto" (default), will interpret all
                provided data in the constructor as part of the lookback buffers.
            agent_id: An optional AgentID indicating which agent this episode belongs
                to. This information is stored under `self.agent_id` and only serves
                reference purposes.
            module_id: An optional ModuleID indicating which RLModule this episode
                belongs to. Normally, this information is obtained by querying an
                `agent_to_module_mapping_fn` with a given agent ID. This information
                is stored under `self.module_id` and only serves reference purposes.
            multi_agent_episode_id: An optional EpisodeID of the encapsulating
                `MultiAgentEpisode` that this `SingleAgentEpisode` belongs to.
        """
        self.id_ = id_ or uuid.uuid4().hex

        self.agent_id = agent_id
        self.module_id = module_id
        self.multi_agent_episode_id = multi_agent_episode_id

        # Lookback buffer length is not provided. Interpret already given data as
        # lookback buffer lengths for all data types.
        len_rewards = len(rewards) if rewards is not None else 0
        if len_lookback_buffer == "auto" or len_lookback_buffer > len_rewards:
            len_lookback_buffer = len_rewards

        infos = infos or [{} for _ in range(len(observations or []))]

        # Observations: t0 (initial obs) to T.
        self._observation_space = None
        if isinstance(observations, InfiniteLookbackBuffer):
            self.observations = observations
        else:
            self.observations = InfiniteLookbackBuffer(
                data=observations,
                lookback=len_lookback_buffer,
            )
        self.observation_space = observation_space
        # Infos: t0 (initial info) to T.
        if isinstance(infos, InfiniteLookbackBuffer):
            self.infos = infos
        else:
            self.infos = InfiniteLookbackBuffer(
                data=infos,
                lookback=len_lookback_buffer,
            )
        # Actions: t1 to T.
        self._action_space = None
        if isinstance(actions, InfiniteLookbackBuffer):
            self.actions = actions
        else:
            self.actions = InfiniteLookbackBuffer(
                data=actions,
                lookback=len_lookback_buffer,
            )
        self.action_space = action_space
        # Rewards: t1 to T.
        if isinstance(rewards, InfiniteLookbackBuffer):
            self.rewards = rewards
        else:
            self.rewards = InfiniteLookbackBuffer(
                data=rewards,
                lookback=len_lookback_buffer,
                space=gym.spaces.Box(float("-inf"), float("inf"), (), np.float32),
            )

        # obs[-1] is the final observation in the episode.
        self.is_terminated = terminated
        # obs[-1] is the last obs in a truncated-by-the-env episode (there will no more
        # observations in following chunks for this episode).
        self.is_truncated = truncated

        # Extra model outputs, e.g. `action_dist_input` needed in the batch.
        self.extra_model_outputs = {}
        for k, v in (extra_model_outputs or {}).items():
            if isinstance(v, InfiniteLookbackBuffer):
                self.extra_model_outputs[k] = v
            else:
                # We cannot use the defaultdict's own constructor here as this would
                # auto-set the lookback buffer to 0 (there is no data passed to that
                # constructor). Then, when we manually have to set the data property,
                # the lookback buffer would still be (incorrectly) 0.
                self.extra_model_outputs[k] = InfiniteLookbackBuffer(
                    data=v, lookback=len_lookback_buffer
                )

        # The (global) timestep when this episode (possibly an episode chunk) started,
        # excluding a possible lookback buffer.
        self.t_started = t_started or 0
        # The current (global) timestep in the episode (possibly an episode chunk).
        self.t = len(self.rewards) + self.t_started

        # Cache for custom data. May be used to store custom metrics from within a
        # callback for the ongoing episode (e.g. render images).
        self._custom_data = {}

        # Keep timer stats on deltas between steps.
        self._start_time = None
        self._last_step_time = None

        self._last_added_observation = None
        self._last_added_infos = None

        # Validate the episode data thus far.
        self.validate()

    def add_env_reset(
        self,
        observation: ObsType,
        infos: Optional[Dict] = None,
    ) -> None:
        """Adds the initial data (after an `env.reset()`) to the episode.

        This data consists of initial observations and initial infos.

        Args:
            observation: The initial observation returned by `env.reset()`.
            infos: An (optional) info dict returned by `env.reset()`.
        """
        assert not self.is_reset
        assert not self.is_done
        assert len(self.observations) == 0
        # Assume that this episode is completely empty and has not stepped yet.
        # Leave self.t (and self.t_started) at 0.
        assert self.t == self.t_started == 0

        infos = infos or {}

        self.observations.append(observation)
        self.infos.append(infos)

        self._last_added_observation = observation
        self._last_added_infos = infos

        # Validate our data.
        self.validate()

        # Start the timer for this episode.
        self._start_time = time.perf_counter()

    def add_env_step(
        self,
        observation: ObsType,
        action: ActType,
        reward: SupportsFloat,
        infos: Optional[Dict[str, Any]] = None,
        *,
        terminated: bool = False,
        truncated: bool = False,
        extra_model_outputs: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Adds results of an `env.step()` call (including the action) to this episode.

        This data consists of an observation and info dict, an action, a reward,
        terminated/truncated flags, and extra model outputs (e.g. action probabilities
        or RNN internal state outputs).

        Args:
            observation: The next observation received from the environment after(!)
                taking `action`.
            action: The last action used by the agent during the call to `env.step()`.
            reward: The last reward received by the agent after taking `action`.
            infos: The last info received from the environment after taking `action`.
            terminated: A boolean indicating, if the environment has been
                terminated (after taking `action`).
            truncated: A boolean indicating, if the environment has been
                truncated (after taking `action`).
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
        if extra_model_outputs is not None:
            for k, v in extra_model_outputs.items():
                if k not in self.extra_model_outputs:
                    self.extra_model_outputs[k] = InfiniteLookbackBuffer([v])
                else:
                    self.extra_model_outputs[k].append(v)
        self.is_terminated = terminated
        self.is_truncated = truncated

        self._last_added_observation = observation
        self._last_added_infos = infos

        # Only check spaces if numpy'ized AND every n timesteps.
        if self.is_numpy and self.t % 100:
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

        # Validate our data.
        self.validate()

        # Step time stats.
        self._last_step_time = time.perf_counter()
        if self._start_time is None:
            self._start_time = self._last_step_time

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
                assert len(v) == 0, (k, v, v.data, len(v))
        # Make sure we always have one more obs stored than rewards (and actions)
        # due to the reset/last-obs logic of an MDP.
        else:
            assert (
                len(self.observations)
                == len(self.infos)
                == len(self.rewards) + 1
                == len(self.actions) + 1
            ), (
                len(self.observations),
                len(self.infos),
                len(self.rewards),
                len(self.actions),
            )
            for k, v in self.extra_model_outputs.items():
                assert len(v) == len(self.observations) - 1

    @property
    def custom_data(self):
        return self._custom_data

    @property
    def is_reset(self) -> bool:
        """Returns True if `self.add_env_reset()` has already been called."""
        return len(self.observations) > 0

    @property
    def is_numpy(self) -> bool:
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

    def to_numpy(self) -> "SingleAgentEpisode":
        """Converts this Episode's list attributes to numpy arrays.

        This means in particular that this episodes' lists of (possibly complex)
        data (e.g. if we have a dict obs space) will be converted to (possibly complex)
        structs, whose leafs are now numpy arrays. Each of these leaf numpy arrays will
        have the same length (batch dimension) as the length of the original lists.

        Note that the data under the Columns.INFOS are NEVER numpy'ized and will remain
        a list (normally, a list of the original, env-returned dicts). This is due to
        the herterogenous nature of INFOS returned by envs, which would make it unwieldy
        to convert this information to numpy arrays.

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
                # being numpy'ized or not (via the `self.to_numpy()` method)!
                terminated=False,
                len_lookback_buffer=0,  # no lookback; all data is actually "in" episode
            )
            # Episode has not been numpy'ized yet.
            assert not episode.is_numpy
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
            # Still NOT numpy'ized.
            assert not episode.is_numpy

            # Numpy'ized the episode.
            episode.to_numpy()
            assert episode.is_numpy

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
        if len(self) > 0:
            self.actions.finalize()
            self.rewards.finalize()
            for k, v in self.extra_model_outputs.items():
                self.extra_model_outputs[k].finalize()

        return self

    def concat_episode(self, other: "SingleAgentEpisode") -> None:
        """Adds the given `other` SingleAgentEpisode to the right side of `self`.

        In order for this to work, both chunks (`self` and `other`) must fit
        together. This is checked by the IDs (must be identical), the time step counters
        (`self.env_t` must be the same as `episode_chunk.env_t_started`), as well as the
        observations/infos at the concatenation boundaries. Also, `self.is_done` must
        not be True, meaning `self.is_terminated` and `self.is_truncated` are both
        False.

        Args:
            other: The other `SingleAgentEpisode` to be concatenated to this one.

        Returns:
            A `SingleAgentEpisode` instance containing the concatenated data
            from both episodes (`self` and `other`).
        """
        assert other.id_ == self.id_
        # NOTE (sven): This is what we agreed on. As the replay buffers must be
        # able to concatenate.
        assert not self.is_done
        # Make sure the timesteps match.
        assert self.t == other.t_started
        # Validate `other`.
        other.validate()

        # Make sure, end matches other episode chunk's beginning.
        assert np.all(other.observations[0] == self.observations[-1])
        # Pop out our last observations and infos (as these are identical
        # to the first obs and infos in the next episode).
        self.observations.pop()
        self.infos.pop()

        # Extend ourselves. In case, episode_chunk is already terminated and numpy'ized
        # we need to convert to lists (as we are ourselves still filling up lists).
        self.observations.extend(other.get_observations())
        self.actions.extend(other.get_actions())
        self.rewards.extend(other.get_rewards())
        self.infos.extend(other.get_infos())
        self.t = other.t

        if other.is_terminated:
            self.is_terminated = True
        elif other.is_truncated:
            self.is_truncated = True

        for key in other.extra_model_outputs.keys():
            assert key in self.extra_model_outputs
            self.extra_model_outputs[key].extend(other.get_extra_model_outputs(key))

        # Merge with `other`'s custom_data, but give `other` priority b/c we assume
        # that as a follow-up chunk of `self` other has a more complete version of
        # `custom_data`.
        self.custom_data.update(other.custom_data)

        # Validate.
        self.validate()

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

        sa_episode = SingleAgentEpisode(
            # Same ID.
            id_=self.id_,
            observations=self.get_observations(indices=indices_obs_and_infos),
            observation_space=self.observation_space,
            infos=self.get_infos(indices=indices_obs_and_infos),
            actions=self.get_actions(indices=indices_rest),
            action_space=self.action_space,
            rewards=self.get_rewards(indices=indices_rest),
            extra_model_outputs={
                k: self.get_extra_model_outputs(k, indices_rest)
                for k in self.extra_model_outputs.keys()
            },
            # Continue with self's current timestep.
            t_started=self.t,
            # Use the length of the provided data as lookback buffer.
            len_lookback_buffer="auto",
        )
        # Deepcopy all custom data in `self` to be continued in the cut episode.
        sa_episode._custom_data = copy.deepcopy(self.custom_data)

        return sa_episode

    # TODO (sven): Distinguish between:
    #  - global index: This is the absolute, global timestep whose values always
    #    start from 0 (at the env reset). So doing get_observations(0, global_ts=True)
    #    should always return the exact 1st observation (reset obs), no matter what. In
    #    case we are in an episode chunk and `fill` or a sufficient lookback buffer is
    #    provided, this should yield a result. Otherwise, error.
    #  - global index=False -> indices are relative to the chunk start. If a chunk has
    #    t_started=6 and we ask for index=0, then return observation at timestep 6
    #    (t_started).
    def get_observations(
        self,
        indices: Optional[Union[int, List[int], slice]] = None,
        *,
        neg_index_as_lookback: bool = False,
        fill: Optional[Any] = None,
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
                unless the `neg_index_as_lookback=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
                If None, will return all observations (from ts=0 to the end).
            neg_index_as_lookback: If True, negative values in `indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with observations [4, 5, 6,  7, 8, 9],
                where [4, 5, 6] is the lookback buffer range (ts=0 item is 7), will
                respond to `get_observations(-1, neg_index_as_lookback=True)`
                with `6` and to
                `get_observations(slice(-2, 1), neg_index_as_lookback=True)` with
                `[5, 6,  7]`.
            fill: An optional value to use for filling up the returned results at
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

            import gymnasium as gym

            from ray.rllib.env.single_agent_episode import SingleAgentEpisode
            from ray.rllib.utils.test_utils import check

            episode = SingleAgentEpisode(
                # Discrete(4) observations (ints between 0 and 4 (excl.))
                observation_space=gym.spaces.Discrete(4),
                observations=[0, 1, 2, 3],
                actions=[1, 2, 3], rewards=[1, 2, 3],  # <- not relevant for this demo
                len_lookback_buffer=0,  # no lookback; all data is actually "in" episode
            )
            # Plain usage (`indices` arg only).
            check(episode.get_observations(-1), 3)
            check(episode.get_observations(0), 0)
            check(episode.get_observations([0, 2]), [0, 2])
            check(episode.get_observations([-1, 0]), [3, 0])
            check(episode.get_observations(slice(None, 2)), [0, 1])
            check(episode.get_observations(slice(-2, None)), [2, 3])
            # Using `fill=...` (requesting slices beyond the boundaries).
            check(episode.get_observations(slice(-6, -2), fill=-9), [-9, -9, 0, 1])
            check(episode.get_observations(slice(2, 5), fill=-7), [2, 3, -7])
            # Using `one_hot_discrete=True`.
            check(episode.get_observations(2, one_hot_discrete=True), [0, 0, 1, 0])
            check(episode.get_observations(3, one_hot_discrete=True), [0, 0, 0, 1])
            check(episode.get_observations(
                slice(0, 3),
                one_hot_discrete=True,
            ), [[1, 0, 0, 0], [0, 1, 0, 0], [0, 0, 1, 0]])
            # Special case: Using `fill=0.0` AND `one_hot_discrete=True`.
            check(episode.get_observations(
                -1,
                neg_index_as_lookback=True,  # -1 means one left of ts=0
                fill=0.0,
                one_hot_discrete=True,
            ), [0, 0, 0, 0])  # <- all 0s one-hot tensor (note difference to [1 0 0 0]!)

        Returns:
            The collected observations.
            As a 0-axis batch, if there are several `indices` or a list of exactly one
            index provided OR `indices` is a slice object.
            As single item (B=0 -> no additional 0-axis) if `indices` is a single int.
        """
        return self.observations.get(
            indices=indices,
            neg_index_as_lookback=neg_index_as_lookback,
            fill=fill,
            one_hot_discrete=one_hot_discrete,
        )

    def get_infos(
        self,
        indices: Optional[Union[int, List[int], slice]] = None,
        *,
        neg_index_as_lookback: bool = False,
        fill: Optional[Any] = None,
    ) -> Any:
        """Returns individual info dicts or list (ranges) thereof from this episode.

        Args:
            indices: A single int is interpreted as an index, from which to return the
                individual info dict stored at this index.
                A list of ints is interpreted as a list of indices from which to gather
                individual info dicts in a list of size len(indices).
                A slice object is interpreted as a range of info dicts to be returned.
                Thereby, negative indices by default are interpreted as "before the end"
                unless the `neg_index_as_lookback=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
                If None, will return all infos (from ts=0 to the end).
            neg_index_as_lookback: If True, negative values in `indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with infos
                [{"l":4}, {"l":5}, {"l":6},  {"a":7}, {"b":8}, {"c":9}], where the
                first 3 items are the lookback buffer (ts=0 item is {"a": 7}), will
                respond to `get_infos(-1, neg_index_as_lookback=True)` with
                `{"l":6}` and to
                `get_infos(slice(-2, 1), neg_index_as_lookback=True)` with
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

        Examples:

        .. testcode::

            from ray.rllib.env.single_agent_episode import SingleAgentEpisode

            episode = SingleAgentEpisode(
                infos=[{"a":0}, {"b":1}, {"c":2}, {"d":3}],
                # The following is needed, but not relevant for this demo.
                observations=[0, 1, 2, 3], actions=[1, 2, 3], rewards=[1, 2, 3],
                len_lookback_buffer=0,  # no lookback; all data is actually "in" episode
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
            neg_index_as_lookback=neg_index_as_lookback,
            fill=fill,
        )

    def get_actions(
        self,
        indices: Optional[Union[int, List[int], slice]] = None,
        *,
        neg_index_as_lookback: bool = False,
        fill: Optional[Any] = None,
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
                unless the `neg_index_as_lookback=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
                If None, will return all actions (from ts=0 to the end).
            neg_index_as_lookback: If True, negative values in `indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with actions [4, 5, 6,  7, 8, 9], where
                [4, 5, 6] is the lookback buffer range (ts=0 item is 7), will respond
                to `get_actions(-1, neg_index_as_lookback=True)` with `6` and
                to `get_actions(slice(-2, 1), neg_index_as_lookback=True)` with
                `[5, 6,  7]`.
            fill: An optional value to use for filling up the returned results at
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
                len_lookback_buffer=0,  # no lookback; all data is actually "in" episode
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
                neg_index_as_lookback=True,  # -1 means one left of ts=0
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
            neg_index_as_lookback=neg_index_as_lookback,
            fill=fill,
            one_hot_discrete=one_hot_discrete,
        )

    def get_rewards(
        self,
        indices: Optional[Union[int, List[int], slice]] = None,
        *,
        neg_index_as_lookback: bool = False,
        fill: Optional[float] = None,
    ) -> Any:
        """Returns individual rewards or batched ranges thereof from this episode.

        Args:
            indices: A single int is interpreted as an index, from which to return the
                individual reward stored at this index.
                A list of ints is interpreted as a list of indices from which to gather
                individual rewards in a batch of size len(indices).
                A slice object is interpreted as a range of rewards to be returned.
                Thereby, negative indices by default are interpreted as "before the end"
                unless the `neg_index_as_lookback=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
                If None, will return all rewards (from ts=0 to the end).
            neg_index_as_lookback: Negative values in `indices` are interpreted as
                 as "before ts=0", meaning going back into the lookback buffer.
                 For example, an episode with rewards [4, 5, 6,  7, 8, 9], where
                 [4, 5, 6] is the lookback buffer range (ts=0 item is 7), will respond
                 to `get_rewards(-1, neg_index_as_lookback=True)` with `6` and
                 to `get_rewards(slice(-2, 1), neg_index_as_lookback=True)` with
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
                len_lookback_buffer=0,  # no lookback; all data is actually "in" episode
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
            neg_index_as_lookback=neg_index_as_lookback,
            fill=fill,
        )

    def get_extra_model_outputs(
        self,
        key: str,
        indices: Optional[Union[int, List[int], slice]] = None,
        *,
        neg_index_as_lookback: bool = False,
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
                "before the end" unless the `neg_index_as_lookback=True` option is
                used, in which case negative indices are interpreted as "before ts=0",
                meaning going back into the lookback buffer.
                If None, will return all extra model outputs (from ts=0 to the end).
            neg_index_as_lookback: If True, negative values in `indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with
                extra_model_outputs['a'] = [4, 5, 6,  7, 8, 9], where [4, 5, 6] is the
                lookback buffer range (ts=0 item is 7), will respond to
                `get_extra_model_outputs("a", -1, neg_index_as_lookback=True)` with
                `6` and to `get_extra_model_outputs("a", slice(-2, 1),
                neg_index_as_lookback=True)` with `[5, 6,  7]`.
            fill: An optional value to use for filling up the returned results at
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
                len_lookback_buffer=0,  # no lookback; all data is actually "in" episode
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
        # The expected case is: `value` is a `InfiniteLookbackBuffer`.
        if isinstance(value, InfiniteLookbackBuffer):
            return value.get(
                indices=indices,
                neg_index_as_lookback=neg_index_as_lookback,
                fill=fill,
            )
        # TODO (sven): This does not seem to be solid yet. Users should NOT be able
        #  to just write directly into our buffers. Instead, use:
        #  `self.set_extra_model_outputs(key, new_data, at_indices=...)` and if key
        #  is not known, add a new buffer to the `extra_model_outputs` dict.
        assert False
        # It might be that the user has added new key/value pairs in their custom
        # postprocessing/connector logic. The values are then most likely numpy
        # arrays. We convert them automatically to buffers and get the requested
        # indices (with the given options) from there.
        return InfiniteLookbackBuffer(value).get(
            indices, fill=fill, neg_index_as_lookback=neg_index_as_lookback
        )

    def set_observations(
        self,
        *,
        new_data,
        at_indices: Optional[Union[int, List[int], slice]] = None,
        neg_index_as_lookback: bool = False,
    ) -> None:
        """Overwrites all or some of this Episode's observations with the provided data.

        Note that an episode's observation data cannot be written to directly as it is
        managed by a `InfiniteLookbackBuffer` object. Normally, individual, current
        observations are added to the episode either by calling `self.add_env_step` or
        more directly (and manually) via `self.observations.append|extend()`.
        However, for certain postprocessing steps, the entirety (or a slice) of an
        episode's observations might have to be rewritten, which is when
        `self.set_observations()` should be used.

        Args:
            new_data: The new observation data to overwrite existing data with.
                This may be a list of individual observation(s) in case this episode
                is still not numpy'ized yet. In case this episode has already been
                numpy'ized, this should be (possibly complex) struct matching the
                observation space and with a batch size of its leafs exactly the size
                of the to-be-overwritten slice or segment (provided by `at_indices`).
            at_indices: A single int is interpreted as one index, which to overwrite
                with `new_data` (which is expected to be a single observation).
                A list of ints is interpreted as a list of indices, all of which to
                overwrite with `new_data` (which is expected to be of the same size
                as `len(at_indices)`).
                A slice object is interpreted as a range of indices to be overwritten
                with `new_data` (which is expected to be of the same size as the
                provided slice).
                Thereby, negative indices by default are interpreted as "before the end"
                unless the `neg_index_as_lookback=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
            neg_index_as_lookback: If True, negative values in `at_indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with
                observations = [4, 5, 6,  7, 8, 9], where [4, 5, 6] is the
                lookback buffer range (ts=0 item is 7), will handle a call to
                `set_observations(individual_observation, -1,
                neg_index_as_lookback=True)` by overwriting the value of 6 in our
                observations buffer with the provided "individual_observation".

        Raises:
            IndexError: If the provided `at_indices` do not match the size of
                `new_data`.
        """
        self.observations.set(
            new_data=new_data,
            at_indices=at_indices,
            neg_index_as_lookback=neg_index_as_lookback,
        )

    def set_actions(
        self,
        *,
        new_data,
        at_indices: Optional[Union[int, List[int], slice]] = None,
        neg_index_as_lookback: bool = False,
    ) -> None:
        """Overwrites all or some of this Episode's actions with the provided data.

        Note that an episode's action data cannot be written to directly as it is
        managed by a `InfiniteLookbackBuffer` object. Normally, individual, current
        actions are added to the episode either by calling `self.add_env_step` or
        more directly (and manually) via `self.actions.append|extend()`.
        However, for certain postprocessing steps, the entirety (or a slice) of an
        episode's actions might have to be rewritten, which is when
        `self.set_actions()` should be used.

        Args:
            new_data: The new action data to overwrite existing data with.
                This may be a list of individual action(s) in case this episode
                is still not numpy'ized yet. In case this episode has already been
                numpy'ized, this should be (possibly complex) struct matching the
                action space and with a batch size of its leafs exactly the size
                of the to-be-overwritten slice or segment (provided by `at_indices`).
            at_indices: A single int is interpreted as one index, which to overwrite
                with `new_data` (which is expected to be a single action).
                A list of ints is interpreted as a list of indices, all of which to
                overwrite with `new_data` (which is expected to be of the same size
                as `len(at_indices)`).
                A slice object is interpreted as a range of indices to be overwritten
                with `new_data` (which is expected to be of the same size as the
                provided slice).
                Thereby, negative indices by default are interpreted as "before the end"
                unless the `neg_index_as_lookback=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
            neg_index_as_lookback: If True, negative values in `at_indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with
                actions = [4, 5, 6,  7, 8, 9], where [4, 5, 6] is the
                lookback buffer range (ts=0 item is 7), will handle a call to
                `set_actions(individual_action, -1,
                neg_index_as_lookback=True)` by overwriting the value of 6 in our
                actions buffer with the provided "individual_action".

        Raises:
            IndexError: If the provided `at_indices` do not match the size of
                `new_data`.
        """
        self.actions.set(
            new_data=new_data,
            at_indices=at_indices,
            neg_index_as_lookback=neg_index_as_lookback,
        )

    def set_rewards(
        self,
        *,
        new_data,
        at_indices: Optional[Union[int, List[int], slice]] = None,
        neg_index_as_lookback: bool = False,
    ) -> None:
        """Overwrites all or some of this Episode's rewards with the provided data.

        Note that an episode's reward data cannot be written to directly as it is
        managed by a `InfiniteLookbackBuffer` object. Normally, individual, current
        rewards are added to the episode either by calling `self.add_env_step` or
        more directly (and manually) via `self.rewards.append|extend()`.
        However, for certain postprocessing steps, the entirety (or a slice) of an
        episode's rewards might have to be rewritten, which is when
        `self.set_rewards()` should be used.

        Args:
            new_data: The new reward data to overwrite existing data with.
                This may be a list of individual reward(s) in case this episode
                is still not numpy'ized yet. In case this episode has already been
                numpy'ized, this should be a np.ndarray with a length exactly
                the size of the to-be-overwritten slice or segment (provided by
                `at_indices`).
            at_indices: A single int is interpreted as one index, which to overwrite
                with `new_data` (which is expected to be a single reward).
                A list of ints is interpreted as a list of indices, all of which to
                overwrite with `new_data` (which is expected to be of the same size
                as `len(at_indices)`).
                A slice object is interpreted as a range of indices to be overwritten
                with `new_data` (which is expected to be of the same size as the
                provided slice).
                Thereby, negative indices by default are interpreted as "before the end"
                unless the `neg_index_as_lookback=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
            neg_index_as_lookback: If True, negative values in `at_indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with
                rewards = [4, 5, 6,  7, 8, 9], where [4, 5, 6] is the
                lookback buffer range (ts=0 item is 7), will handle a call to
                `set_rewards(individual_reward, -1,
                neg_index_as_lookback=True)` by overwriting the value of 6 in our
                rewards buffer with the provided "individual_reward".

        Raises:
            IndexError: If the provided `at_indices` do not match the size of
                `new_data`.
        """
        self.rewards.set(
            new_data=new_data,
            at_indices=at_indices,
            neg_index_as_lookback=neg_index_as_lookback,
        )

    def set_extra_model_outputs(
        self,
        *,
        key,
        new_data,
        at_indices: Optional[Union[int, List[int], slice]] = None,
        neg_index_as_lookback: bool = False,
    ) -> None:
        """Overwrites all or some of this Episode's extra model outputs with `new_data`.

        Note that an episode's `extra_model_outputs` data cannot be written to directly
        as it is managed by a `InfiniteLookbackBuffer` object. Normally, individual,
        current `extra_model_output` values are added to the episode either by calling
        `self.add_env_step` or more directly (and manually) via
        `self.extra_model_outputs[key].append|extend()`. However, for certain
        postprocessing steps, the entirety (or a slice) of an episode's
        `extra_model_outputs` might have to be rewritten or a new key (a new type of
        `extra_model_outputs`) must be inserted, which is when
        `self.set_extra_model_outputs()` should be used.

        Args:
            key: The `key` within `self.extra_model_outputs` to override data on or
                to insert as a new key into `self.extra_model_outputs`.
            new_data: The new data to overwrite existing data with.
                This may be a list of individual reward(s) in case this episode
                is still not numpy'ized yet. In case this episode has already been
                numpy'ized, this should be a np.ndarray with a length exactly
                the size of the to-be-overwritten slice or segment (provided by
                `at_indices`).
            at_indices: A single int is interpreted as one index, which to overwrite
                with `new_data` (which is expected to be a single reward).
                A list of ints is interpreted as a list of indices, all of which to
                overwrite with `new_data` (which is expected to be of the same size
                as `len(at_indices)`).
                A slice object is interpreted as a range of indices to be overwritten
                with `new_data` (which is expected to be of the same size as the
                provided slice).
                Thereby, negative indices by default are interpreted as "before the end"
                unless the `neg_index_as_lookback=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
            neg_index_as_lookback: If True, negative values in `at_indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with
                rewards = [4, 5, 6,  7, 8, 9], where [4, 5, 6] is the
                lookback buffer range (ts=0 item is 7), will handle a call to
                `set_rewards(individual_reward, -1,
                neg_index_as_lookback=True)` by overwriting the value of 6 in our
                rewards buffer with the provided "individual_reward".

        Raises:
            IndexError: If the provided `at_indices` do not match the size of
                `new_data`.
        """
        # Record already exists -> Set existing record's data to new values.
        assert key in self.extra_model_outputs
        self.extra_model_outputs[key].set(
            new_data=new_data,
            at_indices=at_indices,
            neg_index_as_lookback=neg_index_as_lookback,
        )

    def slice(
        self,
        slice_: slice,
        *,
        len_lookback_buffer: Optional[int] = None,
    ) -> "SingleAgentEpisode":
        """Returns a slice of this episode with the given slice object.

        For example, if `self` contains o0 (the reset observation), o1, o2, o3, and o4
        and the actions a1, a2, a3, and a4 (len of `self` is 4), then a call to
        `self.slice(slice(1, 3))` would return a new SingleAgentEpisode with
        observations o1, o2, and o3, and actions a2 and a3. Note here that there is
        always one observation more in an episode than there are actions (and rewards
        and extra model outputs) due to the initial observation received after an env
        reset.

        .. testcode::

            from ray.rllib.env.single_agent_episode import SingleAgentEpisode
            from ray.rllib.utils.test_utils import check

            # Generate a simple multi-agent episode.
            observations = [0, 1, 2, 3, 4, 5]
            actions = [1, 2, 3, 4, 5]
            rewards = [0.1, 0.2, 0.3, 0.4, 0.5]
            episode = SingleAgentEpisode(
                observations=observations,
                actions=actions,
                rewards=rewards,
                len_lookback_buffer=0,  # all given data is part of the episode
            )
            slice_1 = episode[:1]
            check(slice_1.observations, [0, 1])
            check(slice_1.actions, [1])
            check(slice_1.rewards, [0.1])

            slice_2 = episode[-2:]
            check(slice_2.observations, [3, 4, 5])
            check(slice_2.actions, [4, 5])
            check(slice_2.rewards, [0.4, 0.5])

        Args:
            slice_: The slice object to use for slicing. This should exclude the
                lookback buffer, which will be prepended automatically to the returned
                slice.
            len_lookback_buffer: If not None, forces the returned slice to try to have
                this number of timesteps in its lookback buffer (if available). If None
                (default), tries to make the returned slice's lookback as large as the
                current lookback buffer of this episode (`self`).

        Returns:
            The new SingleAgentEpisode representing the requested slice.
        """
        # Translate `slice_` into one that only contains 0-or-positive ints and will
        # NOT contain any None.
        start = slice_.start
        stop = slice_.stop

        # Start is None -> 0.
        if start is None:
            start = 0
        # Start is negative -> Interpret index as counting "from end".
        elif start < 0:
            start = len(self) + start

        # Stop is None -> Set stop to our len (one ts past last valid index).
        if stop is None:
            stop = len(self)
        # Stop is negative -> Interpret index as counting "from end".
        elif stop < 0:
            stop = len(self) + stop

        step = slice_.step if slice_.step is not None else 1

        # Figure out, whether slicing stops at the very end of this episode to know
        # whether `self.is_terminated/is_truncated` should be kept as-is.
        keep_done = stop == len(self)
        # Provide correct timestep- and pre-buffer information.
        t_started = self.t_started + start

        _lb = (
            len_lookback_buffer
            if len_lookback_buffer is not None
            else self.observations.lookback
        )
        if (
            start >= 0
            and start - _lb < 0
            and self.observations.lookback < (_lb - start)
        ):
            _lb = self.observations.lookback + start
        observations = InfiniteLookbackBuffer(
            data=self.get_observations(
                slice(start - _lb, stop + 1, step),
                neg_index_as_lookback=True,
            ),
            lookback=_lb,
            space=self.observation_space,
        )

        _lb = (
            len_lookback_buffer
            if len_lookback_buffer is not None
            else self.infos.lookback
        )
        if start >= 0 and start - _lb < 0 and self.infos.lookback < (_lb - start):
            _lb = self.infos.lookback + start
        infos = InfiniteLookbackBuffer(
            data=self.get_infos(
                slice(start - _lb, stop + 1, step),
                neg_index_as_lookback=True,
            ),
            lookback=_lb,
        )

        _lb = (
            len_lookback_buffer
            if len_lookback_buffer is not None
            else self.actions.lookback
        )
        if start >= 0 and start - _lb < 0 and self.actions.lookback < (_lb - start):
            _lb = self.actions.lookback + start
        actions = InfiniteLookbackBuffer(
            data=self.get_actions(
                slice(start - _lb, stop, step),
                neg_index_as_lookback=True,
            ),
            lookback=_lb,
            space=self.action_space,
        )

        _lb = (
            len_lookback_buffer
            if len_lookback_buffer is not None
            else self.rewards.lookback
        )
        if start >= 0 and start - _lb < 0 and self.rewards.lookback < (_lb - start):
            _lb = self.rewards.lookback + start
        rewards = InfiniteLookbackBuffer(
            data=self.get_rewards(
                slice(start - _lb, stop, step),
                neg_index_as_lookback=True,
            ),
            lookback=_lb,
        )

        extra_model_outputs = {}
        for k, v in self.extra_model_outputs.items():
            _lb = len_lookback_buffer if len_lookback_buffer is not None else v.lookback
            if start >= 0 and start - _lb < 0 and v.lookback < (_lb - start):
                _lb = v.lookback + start
            extra_model_outputs[k] = InfiniteLookbackBuffer(
                data=self.get_extra_model_outputs(
                    key=k,
                    indices=slice(start - _lb, stop, step),
                    neg_index_as_lookback=True,
                ),
                lookback=_lb,
            )

        return SingleAgentEpisode(
            id_=self.id_,
            # In the following, offset `start`s automatically by lookbacks.
            observations=observations,
            observation_space=self.observation_space,
            infos=infos,
            actions=actions,
            action_space=self.action_space,
            rewards=rewards,
            extra_model_outputs=extra_model_outputs,
            terminated=(self.is_terminated if keep_done else False),
            truncated=(self.is_truncated if keep_done else False),
            t_started=t_started,
        )

    def get_data_dict(self):
        """Converts a SingleAgentEpisode into a data dict mapping str keys to data.

        The keys used are:
        Columns.EPS_ID, T, OBS, INFOS, ACTIONS, REWARDS, TERMINATEDS, TRUNCATEDS,
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
                Columns.TERMINATEDS: terminateds,
                Columns.TRUNCATEDS: truncateds,
                Columns.T: t,
                Columns.EPS_ID: eps_id,
                # Retrieve obs, infos, actions, rewards using our get_... APIs,
                # which return all relevant timesteps (excluding the lookback
                # buffer!). Slice off last obs and infos to have the same number
                # of them as we have actions and rewards.
                Columns.OBS: self.get_observations(slice(None, -1)),
                Columns.INFOS: self.get_infos(slice(None, -1)),
                Columns.ACTIONS: self.get_actions(),
                Columns.REWARDS: self.get_rewards(),
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
        Note that if `self` is a continuation chunk (resulting from a call to
        `self.cut()`), the previous chunk's rewards are NOT counted and thus NOT
        part of the returned reward sum.

        Returns:
            The sum of rewards collected during this episode, excluding possible data
            inside the lookback buffer and excluding possible data in a predecessor
            chunk.
        """
        return sum(self.get_rewards())

    def get_duration_s(self) -> float:
        """Returns the duration of this Episode (chunk) in seconds."""
        if self._last_step_time is None:
            return 0.0
        return self._last_step_time - self._start_time

    def env_steps(self) -> int:
        """Returns the number of environment steps.

        Note, this episode instance could be a chunk of an actual episode.

        Returns:
            An integer that counts the number of environment steps this episode instance
            has seen.
        """
        return len(self)

    def agent_steps(self) -> int:
        """Returns the number of agent steps.

        Note, these are identical to the environment steps for a single-agent episode.

        Returns:
            An integer counting the number of agent steps executed during the time this
            episode instance records.
        """
        return self.env_steps()

    def get_state(self) -> Dict[str, Any]:
        """Returns the pickable state of an episode.

        The data in the episode is stored into a dictionary. Note that episodes
        can also be generated from states (see `SingleAgentEpisode.from_state()`).

        Returns:
            A dict containing all the data from the episode.
        """
        infos = self.infos.get_state()
        infos["data"] = np.array([info if info else None for info in infos["data"]])
        return {
            "id_": self.id_,
            "agent_id": self.agent_id,
            "module_id": self.module_id,
            "multi_agent_episode_id": self.multi_agent_episode_id,
            # Note, all data is stored in `InfiniteLookbackBuffer`s.
            "observations": self.observations.get_state(),
            "actions": self.actions.get_state(),
            "rewards": self.rewards.get_state(),
            "infos": self.infos.get_state(),
            "extra_model_outputs": {
                k: v.get_state() if v else v
                for k, v in self.extra_model_outputs.items()
            }
            if len(self.extra_model_outputs) > 0
            else None,
            "is_terminated": self.is_terminated,
            "is_truncated": self.is_truncated,
            "t_started": self.t_started,
            "t": self.t,
            "_observation_space": gym_space_to_dict(self._observation_space)
            if self._observation_space
            else None,
            "_action_space": gym_space_to_dict(self._action_space)
            if self._action_space
            else None,
            "_start_time": self._start_time,
            "_last_step_time": self._last_step_time,
            "custom_data": self.custom_data,
        }

    @staticmethod
    def from_state(state: Dict[str, Any]) -> "SingleAgentEpisode":
        """Creates a new `SingleAgentEpisode` instance from a state dict.

        Args:
            state: The state dict, as returned by `self.get_state()`.

        Returns:
            A new `SingleAgentEpisode` instance with the data from the state dict.
        """
        # Create an empy episode instance.
        episode = SingleAgentEpisode(id_=state["id_"])
        # Load all the data from the state dict into the episode.
        episode.agent_id = state["agent_id"]
        episode.module_id = state["module_id"]
        episode.multi_agent_episode_id = state["multi_agent_episode_id"]
        # Convert data back to `InfiniteLookbackBuffer`s.
        episode.observations = InfiniteLookbackBuffer.from_state(state["observations"])
        episode.actions = InfiniteLookbackBuffer.from_state(state["actions"])
        episode.rewards = InfiniteLookbackBuffer.from_state(state["rewards"])
        episode.infos = InfiniteLookbackBuffer.from_state(state["infos"])
        episode.extra_model_outputs = (
            defaultdict(
                functools.partial(
                    InfiniteLookbackBuffer, lookback=episode.observations.lookback
                ),
                {
                    k: InfiniteLookbackBuffer.from_state(v)
                    for k, v in state["extra_model_outputs"].items()
                },
            )
            if state["extra_model_outputs"]
            else defaultdict(
                functools.partial(
                    InfiniteLookbackBuffer, lookback=episode.observations.lookback
                ),
            )
        )
        episode.is_terminated = state["is_terminated"]
        episode.is_truncated = state["is_truncated"]
        episode.t_started = state["t_started"]
        episode.t = state["t"]
        # We need to convert the spaces to dictionaries for serialization.
        episode._observation_space = (
            gym_space_from_dict(state["_observation_space"])
            if state["_observation_space"]
            else None
        )
        episode._action_space = (
            gym_space_from_dict(state["_action_space"])
            if state["_action_space"]
            else None
        )
        episode._start_time = state["_start_time"]
        episode._last_step_time = state["_last_step_time"]
        episode._custom_data = state.get("custom_data", {})
        # Validate the episode.
        episode.validate()

        return episode

    @property
    def observation_space(self):
        return self._observation_space

    @observation_space.setter
    def observation_space(self, value):
        self._observation_space = self.observations.space = value

    @property
    def action_space(self):
        return self._action_space

    @action_space.setter
    def action_space(self, value):
        self._action_space = self.actions.space = value

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
        return (
            f"SAEps(len={len(self)} done={self.is_done} "
            f"R={self.get_return()} id_={self.id_})"
        )

    def __getitem__(self, item: slice) -> "SingleAgentEpisode":
        """Enable squared bracket indexing- and slicing syntax, e.g. episode[-4:]."""
        if isinstance(item, slice):
            return self.slice(slice_=item)
        else:
            raise NotImplementedError(
                f"SingleAgentEpisode does not support getting item '{item}'! "
                "Only slice objects allowed with the syntax: `episode[a:b]`."
            )

    @Deprecated(new="SingleAgentEpisode.custom_data[some-key] = ...", error=True)
    def add_temporary_timestep_data(self):
        pass

    @Deprecated(new="SingleAgentEpisode.custom_data[some-key]", error=True)
    def get_temporary_timestep_data(self):
        pass
