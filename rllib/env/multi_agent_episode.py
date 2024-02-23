from collections import defaultdict
import copy
from typing import (
    Any,
    Callable,
    Collection,
    DefaultDict,
    Dict,
    List,
    Optional,
    Set,
    Union,
)
import uuid

import gymnasium as gym
import numpy as np

from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.env.utils.infinite_lookback_buffer import InfiniteLookbackBuffer
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils import force_list
from ray.rllib.utils.error import MultiAgentEnvError
from ray.rllib.utils.spaces.space_utils import batch
from ray.rllib.utils.typing import AgentID, ModuleID, MultiAgentDict


# TODO (simon): Include cases in which the number of agents in an
# episode are shrinking or growing during the episode itself.
class MultiAgentEpisode:
    """Stores multi-agent episode data.

    The central attribute of the class is the timestep mapping
    `self.env_t_to_agent_t` that maps AgentIDs to their specific environment steps to
    the agent's own scale/timesteps.

    Each AgentID in the `MultiAgentEpisode` has its own `SingleAgentEpisode` object
    in which this agent's data is stored. Together with the env_t_to_agent_t mapping,
    we can extract information either on any individual agent's time scale or from
    the (global) multi-agent environment time scale.

    Extraction of data from a MultiAgentEpisode happens via the getter APIs, e.g.
    `get_observations()`, which work analogous to the ones implemented in the
    `SingleAgentEpisode` class.

    Note that recorded `terminateds`/`truncateds` come as simple
    `MultiAgentDict`s mapping AgentID to bools and thus have no assignment to a
    certain timestep (analogous to a SingleAgentEpisode's single `terminated/truncated`
    boolean flag). Instead we assign it to the last observation recorded.
    Theoretically, there could occur edge cases in some environments
    where an agent receives partial rewards and then terminates without
    a last observation. In these cases, we duplicate the last observation.

    Also, if no initial observation has been received yet for an agent, but
    some  rewards for this same agent already occurred, we delete the agent's data
    up to here, b/c there is nothing to learn from these "premature" rewards.
    """

    SKIP_ENV_TS_TAG = "S"

    def __init__(
        self,
        id_: Optional[str] = None,
        *,
        observations: Optional[List[MultiAgentDict]] = None,
        observation_space: Optional[gym.Space] = None,
        infos: Optional[List[MultiAgentDict]] = None,
        actions: Optional[List[MultiAgentDict]] = None,
        action_space: Optional[gym.Space] = None,
        rewards: Optional[List[MultiAgentDict]] = None,
        terminateds: Union[MultiAgentDict, bool] = False,
        truncateds: Union[MultiAgentDict, bool] = False,
        render_images: Optional[List[np.ndarray]] = None,
        extra_model_outputs: Optional[List[MultiAgentDict]] = None,
        env_t_started: Optional[int] = None,
        agent_t_started: Optional[Dict[AgentID, int]] = None,
        len_lookback_buffer: Union[int, str] = "auto",
        agent_episode_ids: Optional[Dict[AgentID, str]] = None,
        agent_module_ids: Optional[Dict[AgentID, ModuleID]] = None,
        agent_to_module_mapping_fn: Optional[
            Callable[[AgentID, "MultiAgentEpisode"], ModuleID]
        ] = None,
    ):
        """Initializes a `MultiAgentEpisode`.

        Args:
            id_: Optional. Either a string to identify an episode or None.
                If None, a hexadecimal id is created. In case of providing
                a string, make sure that it is unique, as episodes get
                concatenated via this string.
            agent_module_ids: An optional dict mapping AgentIDs to their respective
                ModuleIDs (these mapping are always valid for an entire episode and
                thus won't change during the course of this episode). If a mapping from
                agent to module has already been provided via this dict, the (optional)
                `agent_to_module_mapping_fn` will NOT be used again to map the same
                agent (agents do not change their assigned module in the course of
                one episode).
            agent_to_module_mapping_fn: A callable taking an AgentID and a
                MultiAgentEpisode as args and returning a ModuleID. Used to map agents
                that have not been mapped yet (because they just entered this episode)
                to a ModuleID. The resulting ModuleID is only stored inside the agent's
                SingleAgentEpisode object.
            agent_episode_ids: An optional dict mapping AgentIDs
                to their corresponding `SingleAgentEpisode`. If None, each
                `SingleAgentEpisode` in `MultiAgentEpisode.agent_episodes`
                will generate a hexadecimal code. If a dictionary is provided
                make sure that IDs are unique as agents' `SingleAgentEpisode`s
                get concatenated or recreated by it.
            observations: A list of dictionaries mapping agent IDs to observations.
                Can be None. If provided, should match all other episode data
                (actions, rewards, etc.) in terms of list lengths and agent IDs.
            observation_space: An optional gym.spaces.Dict mapping agent IDs to
                individual agents' spaces, which all (individual agents') observations
                should abide to. If not None and this MultiAgentEpisode is finalized
                (via the `self.finalize()` method), and data is appended or set, the new
                data will be checked for correctness.
            infos: A list of dictionaries mapping agent IDs to info dicts.
                Can be None. If provided, should match all other episode data
                (observations, rewards, etc.) in terms of list lengths and agent IDs.
            actions: A list of dictionaries mapping agent IDs to actions.
                Can be None. If provided, should match all other episode data
                (observations, rewards, etc.) in terms of list lengths and agent IDs.
            action_space: An optional gym.spaces.Dict mapping agent IDs to
                individual agents' spaces, which all (individual agents') actions
                should abide to. If not None and this MultiAgentEpisode is finalized
                (via the `self.finalize()` method), and data is appended or set, the new
                data will be checked for correctness.
            rewards: A list of dictionaries mapping agent IDs to rewards.
                Can be None. If provided, should match all other episode data
                (actions, rewards, etc.) in terms of list lengths and agent IDs.
            terminateds: A boolean defining if an environment has
                terminated OR a MultiAgentDict mapping individual agent ids
                to boolean flags indicating whether individual agents have terminated.
                A special __all__ key in these dicts indicates, whether the episode
                is terminated for all agents.
                The default is `False`, i.e. the episode has not been terminated.
            truncateds: A boolean defining if the environment has been
                truncated OR a MultiAgentDict mapping individual agent ids
                to boolean flags indicating whether individual agents have been
                truncated. A special __all__ key in these dicts indicates, whether the
                episode is truncated for all agents.
                The default is `False`, i.e. the episode has not been truncated.
            render_images: A list of RGB uint8 images from rendering
                the multi-agent environment.
            extra_model_outputs: A list of dictionaries mapping agent IDs to their
                corresponding extra model outputs. Each of these "outputs" is a dict
                mapping keys (str) to model output values, for example for
                `key=STATE_OUT`, the values would be the internal state outputs for
                that agent.
            env_t_started: The env timestep (int) that defines the starting point
                of the episode. This is only larger zero, if an already ongoing episode
                chunk is being created, for example by slicing an ongoing episode or
                by calling the `cut()` method on an ongoing episode.
            agent_t_started: A dict mapping AgentIDs to the respective agent's (local)
                timestep at which its SingleAgentEpisode chunk started.
            len_lookback_buffer: The size of the lookback buffers to keep in
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
        """
        self.id_: str = id_ or uuid.uuid4().hex
        if agent_to_module_mapping_fn is None:
            from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

            agent_to_module_mapping_fn = (
                AlgorithmConfig.DEFAULT_AGENT_TO_MODULE_MAPPING_FN
            )
        self.agent_to_module_mapping_fn = agent_to_module_mapping_fn

        # Lookback buffer length is not provided. Interpret all provided data as
        # lookback buffer.
        if len_lookback_buffer == "auto":
            len_lookback_buffer = len(rewards or [])

        self.observation_space = observation_space or {}
        self.action_space = action_space or {}

        terminateds = terminateds or {}
        truncateds = truncateds or {}

        # The global last timestep of the episode and the timesteps when this chunk
        # started (excluding a possible lookback buffer).
        self.env_t_started = env_t_started or 0
        self.env_t = (
            (len(rewards) if rewards is not None else 0)
            - len_lookback_buffer
            + self.env_t_started
        )
        self.agent_t_started = defaultdict(int, agent_t_started or {})

        # Keeps track of the correspondence between agent steps and environment steps.
        # Under each AgentID as key is a InfiniteLookbackBuffer with the following
        # data in it:
        # The indices of the items in the data represent environment timesteps,
        # starting from index=0 for the `env.reset()` and with each `env.step()` call
        # increase by 1.
        # The values behind these (env timestep) indices represent the agent timesteps
        # happening at these env timesteps and the special value of
        # `self.SKIP_ENV_TS_TAG` means that the agent did NOT step at the given env
        # timestep.
        # Thus, agents that are part of the reset obs, will start their mapping data
        # with a [0 ...], all other agents will start their mapping data with:
        # [self.SKIP_ENV_TS_TAG, ...].
        self.env_t_to_agent_t: DefaultDict[
            AgentID, InfiniteLookbackBuffer
        ] = defaultdict(InfiniteLookbackBuffer)

        # In the `MultiAgentEpisode` we need these buffers to keep track of actions,
        # that happen when an agent got observations and acted, but did not receive
        # a next observation, yet. In this case we buffer the action, add the rewards,
        # and record `is_terminated/is_truncated` until the next observation is
        # received.
        self._agent_buffered_actions = {}
        self._agent_buffered_extra_model_outputs = defaultdict(dict)
        self._agent_buffered_rewards = {}

        # If this is an ongoing episode than the last `__all__` should be `False`
        self.is_terminated: bool = (
            terminateds
            if isinstance(terminateds, bool)
            else terminateds.get("__all__", False)
        )

        # If this is an ongoing episode than the last `__all__` should be `False`
        self.is_truncated: bool = (
            truncateds
            if isinstance(truncateds, bool)
            else truncateds.get("__all__", False)
        )

        # The individual agent SingleAgentEpisode objects.
        self.agent_episodes: Dict[AgentID, SingleAgentEpisode] = {}
        self._init_single_agent_episodes(
            agent_module_ids=agent_module_ids,
            agent_episode_ids=agent_episode_ids,
            observations=observations,
            infos=infos,
            actions=actions,
            rewards=rewards,
            terminateds=terminateds,
            truncateds=truncateds,
            extra_model_outputs=extra_model_outputs,
            len_lookback_buffer=len_lookback_buffer,
        )

        # RGB uint8 images from rendering the env; the images include the corresponding
        # rewards.
        assert render_images is None or observations is not None
        self.render_images: Union[List[np.ndarray], List[object]] = (
            [] if render_images is None else render_images
        )

        # Validate ourselves.
        self.validate()

    def add_env_reset(
        self,
        *,
        observations: MultiAgentDict,
        infos: Optional[MultiAgentDict] = None,
        render_image: Optional[np.ndarray] = None,
    ) -> None:
        """Stores initial observation.

        Args:
            observations: A dictionary mapping agent IDs to initial observations.
                Note that some agents may not have an initial observation.
            infos: A dictionary mapping agent IDs to initial info dicts.
                Note that some agents may not have an initial info dict. If not None,
                the agent IDs in `infos` must be a subset of those in `observations`
                meaning it would not be allowed to have an agent with an info dict,
                but not with an observation.
            render_image: A (global) RGB uint8 image from rendering the environment
                (for all agents).
        """
        assert not self.is_done
        # Assume that this episode is completely empty and has not stepped yet.
        # Leave self.env_t (and self.env_t_started) at 0.
        assert self.env_t == self.env_t_started == 0
        infos = infos or {}

        # Note that we store the render images into the `MultiAgentEpisode`
        # instead into each `SingleAgentEpisode`.
        if render_image is not None:
            self.render_images.append(render_image)

        # Note, all agents will have an initial observation, some may have an initial
        # info dict as well.
        for agent_id, agent_obs in observations.items():
            # Update env_t_to_agent_t mapping (all agents that are part of the reset
            # obs have their first mapping 0 (env_t) -> 0 (agent_t)).
            self.env_t_to_agent_t[agent_id].append(0)
            # Create SingleAgentEpisode, if necessary.
            if agent_id not in self.agent_episodes:
                self.agent_episodes[agent_id] = SingleAgentEpisode(
                    agent_id=agent_id,
                    module_id=self.agent_to_module_mapping_fn(agent_id, self),
                    observation_space=self.observation_space.get(agent_id),
                    action_space=self.action_space.get(agent_id),
                )
            # Add initial observations (and infos) to the agent's episode.
            self.agent_episodes[agent_id].add_env_reset(
                observation=agent_obs,
                infos=infos.get(agent_id),
            )

    def add_env_step(
        self,
        observations: MultiAgentDict,
        actions: MultiAgentDict,
        rewards: MultiAgentDict,
        infos: Optional[MultiAgentDict] = None,
        *,
        terminateds: Optional[MultiAgentDict] = None,
        truncateds: Optional[MultiAgentDict] = None,
        render_image: Optional[np.ndarray] = None,
        extra_model_outputs: Optional[MultiAgentDict] = None,
    ) -> None:
        """Adds a timestep to the episode.

        Args:
            observations: A dictionary mapping agent IDs to their corresponding
                next observations. Note that some agents may not have stepped at this
                timestep.
            actions: Mandatory. A dictionary mapping agent IDs to their
                corresponding actions. Note that some agents may not have stepped at
                this timestep.
            rewards: Mandatory. A dictionary mapping agent IDs to their
                corresponding observations. Note that some agents may not have stepped
                at this timestep.
            infos: A dictionary mapping agent IDs to their
                corresponding info. Note that some agents may not have stepped at this
                timestep.
            terminateds: A dictionary mapping agent IDs to their `terminated` flags,
                indicating, whether the environment has been terminated for them.
                A special `__all__` key indicates that the episode is terminated for
                all agent IDs.
            terminateds: A dictionary mapping agent IDs to their `truncated` flags,
                indicating, whether the environment has been truncated for them.
                A special `__all__` key indicates that the episode is `truncated` for
                all agent IDs.
            render_image: An RGB uint8 image from rendering the environment.
            extra_model_outputs: A dictionary mapping agent IDs to their
                corresponding specific model outputs (also in a dictionary; e.g.
                `vf_preds` for PPO).
        """
        # Cannot add data to an already done episode.
        if self.is_done:
            raise MultiAgentEnvError(
                "Cannot call `add_env_step` on a MultiAgentEpisode that is already "
                "done!"
            )

        infos = infos or {}
        terminateds = terminateds or {}
        truncateds = truncateds or {}
        extra_model_outputs = extra_model_outputs or {}

        # Increase (global) env step by one.
        self.env_t += 1

        # TODO (sven, simon): Will there still be an `__all__` that is
        #  terminated or truncated?
        # TODO (simon): Maybe allow user to not provide this and then `__all__` is
        #  False?
        self.is_terminated = terminateds.get("__all__", False)
        self.is_truncated = truncateds.get("__all__", False)

        # Note that we store the render images into the `MultiAgentEpisode`
        # instead of storing them into each `SingleAgentEpisode`.
        if render_image is not None:
            self.render_images.append(render_image)

        # For all agents that are not stepping in this env step, but that are not done
        # yet -> Add a skip tag to their env- to agent-step mappings.
        stepped_agent_ids = set(observations.keys())
        for agent_id, env_t_to_agent_t in self.env_t_to_agent_t.items():
            if agent_id not in stepped_agent_ids:
                env_t_to_agent_t.append(self.SKIP_ENV_TS_TAG)

        # Loop through all agent IDs that we received data for in this step:
        # Those found in observations, actions, and rewards.
        agent_ids_with_data = (
            set(observations.keys())
            | set(actions.keys())
            | set(rewards.keys())
            | set(terminateds.keys())
            | set(truncateds.keys())
            | set(
                self.agent_episodes.keys()
                if terminateds.get("__all__") or truncateds.get("__all__")
                else set()
            )
        ) - {"__all__"}
        for agent_id in agent_ids_with_data:
            if agent_id not in self.agent_episodes:
                self.agent_episodes[agent_id] = SingleAgentEpisode(
                    agent_id=agent_id,
                    module_id=self.agent_to_module_mapping_fn(agent_id, self),
                    observation_space=self.observation_space.get(agent_id),
                    action_space=self.action_space.get(agent_id),
                )
            sa_episode: SingleAgentEpisode = self.agent_episodes[agent_id]

            # Collect value to be passed (at end of for-loop) into `add_env_step()`
            # call.
            _observation = observations.get(agent_id)
            _action = actions.get(agent_id)
            _reward = rewards.get(agent_id)
            _infos = infos.get(agent_id)
            _terminated = terminateds.get(agent_id, False) or self.is_terminated
            _truncated = truncateds.get(agent_id, False) or self.is_truncated
            _extra_model_outputs = extra_model_outputs.get(agent_id)

            # The value to place into the env- to agent-step map for this agent ID.
            # _agent_step = self.SKIP_ENV_TS_TAG

            # Agents, whose SingleAgentEpisode had already been done before this
            # step should NOT have received any data in this step.
            if sa_episode.is_done and any(
                v is not None
                for v in [_observation, _action, _reward, _infos, _extra_model_outputs]
            ):
                raise MultiAgentEnvError(
                    f"Agent {agent_id} already had its `SingleAgentEpisode.is_done` "
                    f"set to True, but still received data in a following step! "
                    f"obs={_observation} act={_action} rew={_reward} info={_infos} "
                    f"extra_model_outputs={_extra_model_outputs}."
                )
            _reward = _reward or 0.0

            # CASE 1: A complete agent step is available (in one env step).
            # -------------------------------------------------------------
            # We have an observation and an action for this agent ->
            # Add the agent step to the single agent episode.
            # ... action -> next obs + reward ...
            if _observation is not None and _action is not None:
                if agent_id not in rewards:
                    raise MultiAgentEnvError(
                        f"Agent {agent_id} acted (and received next obs), but did NOT "
                        f"receive any reward from the env!"
                    )

            # CASE 2: Step gets completed with a buffered action OR first observation.
            # ------------------------------------------------------------------------
            # We have an observation, but no action ->
            # a) Action (and extra model outputs) must be buffered already. Also use
            # collected buffered rewards.
            # b) The observation is the first observation for this agent ID.
            elif _observation is not None and _action is None:
                _action = self._agent_buffered_actions.pop(agent_id, None)

                # We have a buffered action (the agent had acted after the previous
                # observation, but the env had not responded - until now - with another
                # observation).
                # ...[buffered action] ... ... -> next obs + (reward)? ...
                if _action is not None:
                    # Get the extra model output if available.
                    _extra_model_outputs = self._agent_buffered_extra_model_outputs.pop(
                        agent_id, None
                    )
                    _reward = self._agent_buffered_rewards.pop(agent_id, 0.0) + _reward
                    # _agent_step = len(sa_episode)
                # First observation for this agent, we have no buffered action.
                # ... [done]? ... -> [1st obs for agent ID]
                else:
                    # The agent is already done -> The agent thus has never stepped once
                    # and we do not have to create a SingleAgentEpisode for it.
                    if _terminated or _truncated:
                        self._del_buffers(agent_id)
                        continue
                    # This must be the agent's initial observation.
                    else:
                        # Prepend n skip tags to this agent's mapping + the initial [0].
                        self.env_t_to_agent_t[agent_id].extend(
                            [self.SKIP_ENV_TS_TAG] * self.env_t + [0]
                        )
                        # Make `add_env_reset` call and continue with next agent.
                        sa_episode.add_env_reset(observation=_observation, infos=_infos)
                        continue

            # CASE 3: Step is started (by an action), but not completed (no next obs).
            # ------------------------------------------------------------------------
            # We have no observation, but we have an action to be buffered (and used
            # when we do receive the next obs for this agent in the future).
            elif agent_id not in observations and agent_id in actions:
                # Agent got truncated -> Error b/c we would need a last (truncation)
                # observation for this (otherwise, e.g. bootstrapping would not work).
                # [previous obs] [action] (to be buffered) ... ... [truncated]
                if _truncated:
                    raise MultiAgentEnvError(
                        f"Agent {agent_id} acted and then got truncated, but did NOT "
                        "receive a last (truncation) observation, required for e.g. "
                        "value function bootstrapping!"
                    )
                # Agent got terminated.
                # [previous obs] [action] (to be buffered) ... ... [terminated]
                elif _terminated:
                    # If the agent was terminated and no observation is provided,
                    # duplicate the previous one (this is a technical "fix" to properly
                    # complete the single agent episode; this last observation is never
                    # used for learning anyway).
                    _observation = sa_episode.get_observations(-1)
                    _infos = sa_episode.get_infos(-1)
                # Agent is still alive.
                # [previous obs] [action] (to be buffered) ...
                else:
                    # Buffer action, reward, and extra_model_outputs.
                    assert agent_id not in self._agent_buffered_actions
                    self._agent_buffered_actions[agent_id] = _action
                    self._agent_buffered_rewards[agent_id] = _reward
                    self._agent_buffered_extra_model_outputs[
                        agent_id
                    ] = _extra_model_outputs

            # CASE 4: Step has started in the past and is still ongoing (no observation,
            # no action).
            # --------------------------------------------------------------------------
            # Record reward and terminated/truncated flags.
            else:
                _action = self._agent_buffered_actions.get(agent_id)

                # Agent is done.
                if _terminated or _truncated:
                    # If the agent has NOT stepped, we treat it as not being
                    # part of this episode.
                    # ... ... [other agents doing stuff] ... ... [agent done]
                    if _action is None:
                        self._del_buffers(agent_id)
                        continue

                    # Agent got truncated -> Error b/c we would need a last (truncation)
                    # observation for this (otherwise, e.g. bootstrapping would not
                    # work).
                    if _truncated:
                        raise MultiAgentEnvError(
                            f"Agent {agent_id} acted and then got truncated, but did "
                            "NOT receive a last (truncation) observation, required "
                            "for e.g. value function bootstrapping!"
                        )

                    # [obs] ... ... [buffered action] ... ... [done]
                    # If the agent was terminated and no observation is provided,
                    # duplicate the previous one (this is a technical "fix" to properly
                    # complete the single agent episode; this last observation is never
                    # used for learning anyway).
                    _observation = sa_episode.get_observations(-1)
                    _infos = sa_episode.get_infos(-1)
                    # `_action` is already `get` above. We don't need to pop out from
                    # the buffer as it gets wiped out anyway below b/c the agent is
                    # done.
                    _extra_model_outputs = self._agent_buffered_extra_model_outputs.pop(
                        agent_id, None
                    )
                    _reward = self._agent_buffered_rewards.pop(agent_id, 0.0) + _reward
                # The agent is still alive, just add current reward to buffer.
                else:
                    self._agent_buffered_rewards[agent_id] = (
                        self._agent_buffered_rewards.get(agent_id, 0.0) + _reward
                    )

            # If agent is stepping, add timestep to `SingleAgentEpisode`.
            if _observation is not None:
                sa_episode.add_env_step(
                    observation=_observation,
                    action=_action,
                    reward=_reward,
                    infos=_infos,
                    terminated=_terminated,
                    truncated=_truncated,
                    extra_model_outputs=_extra_model_outputs,
                )
                # Update the env- to agent-step mapping.
                self.env_t_to_agent_t[agent_id].append(
                    len(sa_episode) + sa_episode.observations.lookback
                )

            # Agent is also done. -> Erase all buffered values for this agent
            # (they should be empty at this point anyways).
            if _terminated or _truncated:
                self._del_buffers(agent_id)

    def validate(self) -> None:
        """Validates the episode's data.

        This function ensures that the data stored to a `MultiAgentEpisode` is
        in order (e.g. that the correct number of observations, actions, rewards
        are there).
        """
        for eps in self.agent_episodes.values():
            eps.validate()

        # TODO (sven): Validate MultiAgentEpisode specifics, like the timestep mappings,
        #  action/reward buffers, etc..

    @property
    def is_finalized(self) -> bool:
        """True, if the data in this episode is already stored as numpy arrays."""
        is_finalized = next(iter(self.agent_episodes.values())).is_finalized
        # Make sure that all single agent's episodes' `finalized` flags are the same.
        if not all(
            eps.is_finalized is is_finalized for eps in self.agent_episodes.values()
        ):
            raise RuntimeError(
                f"Only some SingleAgentEpisode objects in {self} are finalized (others "
                f"are not)!"
            )
        return is_finalized

    @property
    def is_done(self):
        """Whether the episode is actually done (terminated or truncated).

        A done episode cannot be continued via `self.add_env_step()` or being
        concatenated on its right-side with another episode chunk or being
        succeeded via `self.cut()`.

        Note that in a multi-agent environment this does not necessarily
        correspond to single agents having terminated or being truncated.

        `self.is_terminated` should be `True`, if all agents are terminated and
        `self.is_truncated` should be `True`, if all agents are truncated. If
        only one or more (but not all!) agents are `terminated/truncated the
        `MultiAgentEpisode.is_terminated/is_truncated` should be `False`. This
        information about single agent's terminated/truncated states can always
        be retrieved from the `SingleAgentEpisode`s inside the 'MultiAgentEpisode`
        one.

        If all agents are either terminated or truncated, but in a mixed fashion,
        i.e. some are terminated and others are truncated: This is currently
        undefined and could potentially be a problem (if a user really implemented
        such a multi-agent env that behaves this way).

        Returns:
            Boolean defining if an episode has either terminated or truncated.
        """
        return self.is_terminated or self.is_truncated

    def finalize(
        self,
        drop_zero_len_single_agent_episodes: bool = False,
    ) -> "MultiAgentEpisode":
        """Converts this Episode's list attributes to numpy arrays.

        This means in particular that this episodes' lists (per single agent) of
        (possibly complex) data (e.g. an agent having a dict obs space) will be
        converted to (possibly complex) structs, whose leafs are now numpy arrays.
        Each of these leaf numpy arrays will have the same length (batch dimension)
        as the length of the original lists.

        Note that SampleBatch.INFOS are NEVER numpy'ized and will remain a list
        (normally, a list of the original, env-returned dicts). This is due to the
        herterogenous nature of INFOS returned by envs, which would make it unwieldy to
        convert this information to numpy arrays.

        After calling this method, no further data may be added to this episode via
        the `self.add_env_step()` method.

        Examples:

        .. testcode::

            import numpy as np

            from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
            from ray.rllib.env.tests.test_multi_agent_episode import (
                TestMultiAgentEpisode
            )

            # Create some multi-agent episode data.
            (
                observations,
                actions,
                rewards,
                terminateds,
                truncateds,
                infos,
            ) = TestMultiAgentEpisode._mock_multi_agent_records()
            # Define the agent ids.
            agent_ids = ["agent_1", "agent_2", "agent_3", "agent_4", "agent_5"]

            episode = MultiAgentEpisode(
                observations=observations,
                infos=infos,
                actions=actions,
                rewards=rewards,
                # Note: terminated/truncated have nothing to do with an episode
                # being `finalized` or not (via the `self.finalize()` method)!
                terminateds=terminateds,
                truncateds=truncateds,
                len_lookback_buffer=0,  # no lookback; all data is actually "in" episode
            )

            # Episode has not been finalized (numpy'ized) yet.
            assert not episode.is_finalized
            # We are still operating on lists.
            assert (
                episode.get_observations(
                    indices=[1],
                    agent_ids="agent_1",
                ) == {"agent_1": [1]}
            )

            # Let's finalize the episode.
            episode.finalize()
            assert episode.is_finalized

            # Everything is now numpy arrays (with 0-axis of size
            # B=[len of requested slice]).
            assert (
                isinstance(episode.get_observations(
                    indices=[1],
                    agent_ids="agent_1",
                )["agent_1"], np.ndarray)
            )

        Args:
            drop_zero_len_single_agent_episodes: If True, will remove from this
                episode all underlying SingleAgentEpisodes that have a len of 0
                (meaning that these SingleAgentEpisodes only have a reset obs as
                their data thus far, making them useless for learning anything from
                them).

        Returns:
             This `MultiAgentEpisode` object with the converted numpy data.
        """

        for agent_id, agent_eps in self.agent_episodes.copy().items():
            if len(agent_eps) == 0 and drop_zero_len_single_agent_episodes:
                del self.agent_episodes[agent_id]
            else:
                agent_eps.finalize()

        return self

    def cut(self, len_lookback_buffer: int = 0) -> "MultiAgentEpisode":
        """Returns a successor episode chunk (of len=0) continuing from this Episode.

        The successor will have the same ID as `self`.
        If no lookback buffer is requested (len_lookback_buffer=0), the successor's
        observations will be the last observation(s) of `self` and its length will
        therefore be 0 (no further steps taken yet). If `len_lookback_buffer` > 0,
        the returned successor will have `len_lookback_buffer` observations (and
        actions, rewards, etc..) taken from the right side (end) of `self`. For example
        if `len_lookback_buffer=2`, the returned successor's lookback buffer actions
        will be identical to teh results of `self.get_actions([-2, -1])`.

        This method is useful if you would like to discontinue building an episode
        chunk (b/c you have to return it from somewhere), but would like to have a new
        episode instance to continue building the actual gym.Env episode at a later
        time. Vie the `len_lookback_buffer` argument, the continuing chunk (successor)
        will still be able to "look back" into this predecessor episode's data (at
        least to some extend, depending on the value of `len_lookback_buffer`).

        Args:
            len_lookback_buffer: The number of environment timesteps to take along into
                the new chunk as "lookback buffer". A lookback buffer is additional data
                on the left side of the actual episode data for visibility purposes
                (but without actually being part of the new chunk). For example, if
                `self` ends in actions: agent_1=5,6,7 and agent_2=6,7, and we call
                `self.cut(len_lookback_buffer=2)`, the returned chunk will have
                actions 6 and 7 for both agents already in it, but still
                `t_started`==t==8 (not 7!) and a length of 0. If there is not enough
                data in `self` yet to fulfil the `len_lookback_buffer` request, the
                value of `len_lookback_buffer` is automatically adjusted (lowered).

        Returns:
            The successor Episode chunk of this one with the same ID and state and the
            only observation being the last observation in self.
        """
        assert len_lookback_buffer >= 0
        if self.is_done:
            raise RuntimeError(
                "Can't call `MultiAgentEpisode.cut()` when the episode is already done!"
            )

        # If there is data (e.g. actions) in the agents' buffers, we might have to
        # re-adjust the lookback len further into the past to make sure that these
        # agents have at least one observation to look back to.
        for agent_id, agent_actions in self._agent_buffered_actions.items():
            assert self.env_t_to_agent_t[agent_id].get(-1) == self.SKIP_ENV_TS_TAG
            for i in range(1, self.env_t_to_agent_t[agent_id].lookback + 1):
                if (
                    self.env_t_to_agent_t[agent_id].get(
                        -i, neg_indices_left_of_zero=True
                    )
                    != self.SKIP_ENV_TS_TAG
                ):
                    len_lookback_buffer = max(len_lookback_buffer, i)

        # Initialize this episode chunk with the most recent observations
        # and infos (even if lookback is zero). Similar to an initial `env.reset()`
        indices_obs_and_infos = slice(-len_lookback_buffer - 1, None)
        indices_rest = (
            slice(-len_lookback_buffer, None)
            if len_lookback_buffer > 0
            else slice(None, 0)  # -> empty slice
        )

        successor = MultiAgentEpisode(
            # Same ID.
            id_=self.id_,
            # Same agent IDs.
            # Same single agents' episode IDs.
            agent_episode_ids=self.agent_episode_ids,
            agent_module_ids={
                aid: self.agent_episodes[aid].module_id for aid in self.agent_ids
            },
            agent_to_module_mapping_fn=self.agent_to_module_mapping_fn,
            observations=self.get_observations(
                indices=indices_obs_and_infos, return_list=True
            ),
            observation_space=self.observation_space,
            infos=self.get_infos(indices=indices_obs_and_infos, return_list=True),
            actions=self.get_actions(indices=indices_rest, return_list=True),
            action_space=self.action_space,
            rewards=self.get_rewards(indices=indices_rest, return_list=True),
            # List of MADicts, mapping agent IDs to their respective extra model output
            # dicts.
            extra_model_outputs=self.get_extra_model_outputs(
                key=None,  # all keys
                indices=indices_rest,
                return_list=True,
            ),
            terminateds=self.get_terminateds(),
            truncateds=self.get_truncateds(),
            # Continue with `self`'s current timestep.
            env_t_started=self.env_t,
            len_lookback_buffer="auto",
        )

        # Copy over the current buffer values.
        successor._agent_buffered_actions = copy.deepcopy(self._agent_buffered_actions)
        successor._agent_buffered_rewards = self._agent_buffered_rewards.copy()
        successor._agent_buffered_extra_model_outputs = copy.deepcopy(
            self._agent_buffered_extra_model_outputs
        )

        return successor

    @property
    def agent_ids(self) -> Set[AgentID]:
        """Returns the agent ids."""
        return set(self.agent_episodes.keys())

    @property
    def agent_episode_ids(self) -> MultiAgentDict:
        """Returns ids from each agent's `SingleAgentEpisode`."""

        return {
            agent_id: agent_eps.id_
            for agent_id, agent_eps in self.agent_episodes.items()
        }

    def get_observations(
        self,
        indices: Optional[Union[int, List[int], slice]] = None,
        agent_ids: Optional[Union[Collection[AgentID], AgentID]] = None,
        *,
        env_steps: bool = True,
        # global_indices: bool = False,
        neg_indices_left_of_zero: bool = False,
        fill: Optional[Any] = None,
        one_hot_discrete: bool = False,
        return_list: bool = False,
    ) -> Union[MultiAgentDict, List[MultiAgentDict]]:
        """Returns agents' observations or batched ranges thereof from this episode.

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
                If None, will return all observations (from ts=0 to the end).
            agent_ids: An optional collection of AgentIDs or a single AgentID to get
                observations for. If None, will return observations for all agents in
                this episode.
            env_steps: Whether `indices` should be interpreted as environment time steps
                (True) or per-agent timesteps (False).
            neg_indices_left_of_zero: If True, negative values in `indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with agent A's observations
                [4, 5, 6,  7, 8, 9], where [4, 5, 6] is the lookback buffer range
                (ts=0 item is 7), will respond to `get_observations(-1, agent_ids=[A],
                neg_indices_left_of_zero=True)` with {A: `6`} and to
                `get_observations(slice(-2, 1), agent_ids=[A],
                neg_indices_left_of_zero=True)` with {A: `[5, 6,  7]`}.
            fill: An optional value to use for filling up the returned results at
                the boundaries. This filling only happens if the requested index range's
                start/stop boundaries exceed the episode's boundaries (including the
                lookback buffer on the left side). This comes in very handy, if users
                don't want to worry about reaching such boundaries and want to zero-pad.
                For example, an episode with agent A' observations [10, 11,  12, 13, 14]
                and lookback buffer size of 2 (meaning observations `10` and `11` are
                part of the lookback buffer) will respond to
                `get_observations(slice(-7, -2), agent_ids=[A], fill=0.0)` with
                `{A: [0.0, 0.0, 10, 11, 12]}`.
            one_hot_discrete: If True, will return one-hot vectors (instead of
                int-values) for those sub-components of a (possibly complex) observation
                space that are Discrete or MultiDiscrete.  Note that if `fill=0` and the
                requested `indices` are out of the range of our data, the returned
                one-hot vectors will actually be zero-hot (all slots zero).
            return_list: Whether to return a list of multi-agent dicts (instead of
                a single multi-agent dict of lists/structs). False by default. This
                option can only be used when `env_steps` is True due to the fact the
                such a list can only be interpreted as one env step per list item
                (would not work with agent steps).

        Returns:
            A dictionary mapping agent IDs to observations (at the given
            `indices`). If `env_steps` is True, only agents that have stepped
            (were ready) at the given env step `indices` are returned (i.e. not all
            agent IDs are necessarily in the keys).
            If `return_list` is True, returns a list of MultiAgentDicts (mapping agent
            IDs to observations) instead.
        """
        return self._get(
            what="observations",
            indices=indices,
            agent_ids=agent_ids,
            env_steps=env_steps,
            neg_indices_left_of_zero=neg_indices_left_of_zero,
            fill=fill,
            one_hot_discrete=one_hot_discrete,
            return_list=return_list,
        )

    def get_infos(
        self,
        indices: Optional[Union[int, List[int], slice]] = None,
        agent_ids: Optional[Union[Collection[AgentID], AgentID]] = None,
        *,
        env_steps: bool = True,
        neg_indices_left_of_zero: bool = False,
        fill: Optional[Any] = None,
        return_list: bool = False,
    ) -> Union[MultiAgentDict, List[MultiAgentDict]]:
        """Returns agents' info dicts or list (ranges) thereof from this episode.

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
                If None, will return all infos (from ts=0 to the end).
            agent_ids: An optional collection of AgentIDs or a single AgentID to get
                info dicts for. If None, will return info dicts for all agents in
                this episode.
            env_steps: Whether `indices` should be interpreted as environment time steps
                (True) or per-agent timesteps (False).
            neg_indices_left_of_zero: If True, negative values in `indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with agent A's info dicts
                [{"l":4}, {"l":5}, {"l":6},  {"a":7}, {"b":8}, {"c":9}], where the
                first 3 items are the lookback buffer (ts=0 item is {"a": 7}), will
                respond to `get_infos(-1, agent_ids=A, neg_indices_left_of_zero=True)`
                with `{A: {"l":6}}` and to
                `get_infos(slice(-2, 1), agent_ids=A, neg_indices_left_of_zero=True)`
                with `{A: [{"l":5}, {"l":6},  {"a":7}]}`.
            fill: An optional value to use for filling up the returned results at
                the boundaries. This filling only happens if the requested index range's
                start/stop boundaries exceed the episode's boundaries (including the
                lookback buffer on the left side). This comes in very handy, if users
                don't want to worry about reaching such boundaries and want to
                auto-fill. For example, an episode with agent A's infos being
                [{"l":10}, {"l":11},  {"a":12}, {"b":13}, {"c":14}] and lookback buffer
                size of 2 (meaning infos {"l":10}, {"l":11} are part of the lookback
                buffer) will respond to `get_infos(slice(-7, -2), agent_ids=A,
                fill={"o": 0.0})` with
                `{A: [{"o":0.0}, {"o":0.0}, {"l":10}, {"l":11}, {"a":12}]}`.
            return_list: Whether to return a list of multi-agent dicts (instead of
                a single multi-agent dict of lists/structs). False by default. This
                option can only be used when `env_steps` is True due to the fact the
                such a list can only be interpreted as one env step per list item
                (would not work with agent steps).

        Returns:
            A dictionary mapping agent IDs to observations (at the given
            `indices`). If `env_steps` is True, only agents that have stepped
            (were ready) at the given env step `indices` are returned (i.e. not all
            agent IDs are necessarily in the keys).
            If `return_list` is True, returns a list of MultiAgentDicts (mapping agent
            IDs to infos) instead.
        """
        return self._get(
            what="infos",
            indices=indices,
            agent_ids=agent_ids,
            env_steps=env_steps,
            neg_indices_left_of_zero=neg_indices_left_of_zero,
            fill=fill,
            return_list=return_list,
        )

    def get_actions(
        self,
        indices: Optional[Union[int, List[int], slice]] = None,
        agent_ids: Optional[Union[Collection[AgentID], AgentID]] = None,
        *,
        env_steps: bool = True,
        neg_indices_left_of_zero: bool = False,
        fill: Optional[Any] = None,
        one_hot_discrete: bool = False,
        return_list: bool = False,
    ) -> Union[MultiAgentDict, List[MultiAgentDict]]:
        """Returns agents' actions or batched ranges thereof from this episode.

        Args:
            indices: A single int is interpreted as an index, from which to return the
                individual actions stored at this index.
                A list of ints is interpreted as a list of indices from which to gather
                individual actions in a batch of size len(indices).
                A slice object is interpreted as a range of actions to be returned.
                Thereby, negative indices by default are interpreted as "before the end"
                unless the `neg_indices_left_of_zero=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
                If None, will return all actions (from ts=0 to the end).
            agent_ids: An optional collection of AgentIDs or a single AgentID to get
                actions for. If None, will return actions for all agents in
                this episode.
            env_steps: Whether `indices` should be interpreted as environment time steps
                (True) or per-agent timesteps (False).
            neg_indices_left_of_zero: If True, negative values in `indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with agent A's actions
                [4, 5, 6,  7, 8, 9], where [4, 5, 6] is the lookback buffer range
                (ts=0 item is 7), will respond to `get_actions(-1, agent_ids=[A],
                neg_indices_left_of_zero=True)` with {A: `6`} and to
                `get_actions(slice(-2, 1), agent_ids=[A],
                neg_indices_left_of_zero=True)` with {A: `[5, 6,  7]`}.
            fill: An optional value to use for filling up the returned results at
                the boundaries. This filling only happens if the requested index range's
                start/stop boundaries exceed the episode's boundaries (including the
                lookback buffer on the left side). This comes in very handy, if users
                don't want to worry about reaching such boundaries and want to zero-pad.
                For example, an episode with agent A' actions [10, 11,  12, 13, 14]
                and lookback buffer size of 2 (meaning actions `10` and `11` are
                part of the lookback buffer) will respond to
                `get_actions(slice(-7, -2), agent_ids=[A], fill=0.0)` with
                `{A: [0.0, 0.0, 10, 11, 12]}`.
            one_hot_discrete: If True, will return one-hot vectors (instead of
                int-values) for those sub-components of a (possibly complex) observation
                space that are Discrete or MultiDiscrete.  Note that if `fill=0` and the
                requested `indices` are out of the range of our data, the returned
                one-hot vectors will actually be zero-hot (all slots zero).
            return_list: Whether to return a list of multi-agent dicts (instead of
                a single multi-agent dict of lists/structs). False by default. This
                option can only be used when `env_steps` is True due to the fact the
                such a list can only be interpreted as one env step per list item
                (would not work with agent steps).

        Returns:
            A dictionary mapping agent IDs to actions (at the given
            `indices`). If `env_steps` is True, only agents that have stepped
            (were ready) at the given env step `indices` are returned (i.e. not all
            agent IDs are necessarily in the keys).
            If `return_list` is True, returns a list of MultiAgentDicts (mapping agent
            IDs to actions) instead.
        """
        return self._get(
            what="actions",
            indices=indices,
            agent_ids=agent_ids,
            env_steps=env_steps,
            neg_indices_left_of_zero=neg_indices_left_of_zero,
            fill=fill,
            one_hot_discrete=one_hot_discrete,
            return_list=return_list,
        )

    def get_rewards(
        self,
        indices: Optional[Union[int, List[int], slice]] = None,
        agent_ids: Optional[Union[Collection[AgentID], AgentID]] = None,
        *,
        env_steps: bool = True,
        neg_indices_left_of_zero: bool = False,
        fill: Optional[float] = None,
        return_list: bool = False,
    ) -> Union[MultiAgentDict, List[MultiAgentDict]]:
        """Returns agents' rewards or batched ranges thereof from this episode.

        Args:
            indices: A single int is interpreted as an index, from which to return the
                individual rewards stored at this index.
                A list of ints is interpreted as a list of indices from which to gather
                individual rewards in a batch of size len(indices).
                A slice object is interpreted as a range of rewards to be returned.
                Thereby, negative indices by default are interpreted as "before the end"
                unless the `neg_indices_left_of_zero=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
                If None, will return all rewards (from ts=0 to the end).
            agent_ids: An optional collection of AgentIDs or a single AgentID to get
                rewards for. If None, will return rewards for all agents in
                this episode.
            env_steps: Whether `indices` should be interpreted as environment time steps
                (True) or per-agent timesteps (False).
            neg_indices_left_of_zero: If True, negative values in `indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with agent A's rewards
                [4, 5, 6,  7, 8, 9], where [4, 5, 6] is the lookback buffer range
                (ts=0 item is 7), will respond to `get_rewards(-1, agent_ids=[A],
                neg_indices_left_of_zero=True)` with {A: `6`} and to
                `get_rewards(slice(-2, 1), agent_ids=[A],
                neg_indices_left_of_zero=True)` with {A: `[5, 6,  7]`}.
            fill: An optional float value to use for filling up the returned results at
                the boundaries. This filling only happens if the requested index range's
                start/stop boundaries exceed the episode's boundaries (including the
                lookback buffer on the left side). This comes in very handy, if users
                don't want to worry about reaching such boundaries and want to zero-pad.
                For example, an episode with agent A' rewards [10, 11,  12, 13, 14]
                and lookback buffer size of 2 (meaning rewards `10` and `11` are
                part of the lookback buffer) will respond to
                `get_rewards(slice(-7, -2), agent_ids=[A], fill=0.0)` with
                `{A: [0.0, 0.0, 10, 11, 12]}`.
            return_list: Whether to return a list of multi-agent dicts (instead of
                a single multi-agent dict of lists/structs). False by default. This
                option can only be used when `env_steps` is True due to the fact the
                such a list can only be interpreted as one env step per list item
                (would not work with agent steps).

        Returns:
            A dictionary mapping agent IDs to rewards (at the given
            `indices`). If `env_steps` is True, only agents that have stepped
            (were ready) at the given env step `indices` are returned (i.e. not all
            agent IDs are necessarily in the keys).
            If `return_list` is True, returns a list of MultiAgentDicts (mapping agent
            IDs to rewards) instead.
        """
        return self._get(
            what="rewards",
            indices=indices,
            agent_ids=agent_ids,
            env_steps=env_steps,
            neg_indices_left_of_zero=neg_indices_left_of_zero,
            fill=fill,
            return_list=return_list,
        )

    def get_extra_model_outputs(
        self,
        key: Optional[str] = None,
        indices: Optional[Union[int, List[int], slice]] = None,
        agent_ids: Optional[Union[Collection[AgentID], AgentID]] = None,
        *,
        env_steps: bool = True,
        neg_indices_left_of_zero: bool = False,
        fill: Optional[Any] = None,
        return_list: bool = False,
    ) -> Union[MultiAgentDict, List[MultiAgentDict]]:
        """Returns agents' actions or batched ranges thereof from this episode.

        Args:
            key: The `key` within each agents' extra_model_outputs dict to extract
                data for. If None, return data of all extra model output keys.
            indices: A single int is interpreted as an index, from which to return the
                individual extra model outputs stored at this index.
                A list of ints is interpreted as a list of indices from which to gather
                individual extra model outputs in a batch of size len(indices).
                A slice object is interpreted as a range of extra model outputs to be
                returned.
                Thereby, negative indices by default are interpreted as "before the end"
                unless the `neg_indices_left_of_zero=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
                If None, will return all extra model outputs (from ts=0 to the end).
            agent_ids: An optional collection of AgentIDs or a single AgentID to get
                extra model outputs for. If None, will return extra model outputs for
                all agents in this episode.
            env_steps: Whether `indices` should be interpreted as environment time steps
                (True) or per-agent timesteps (False).
            neg_indices_left_of_zero: If True, negative values in `indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with agent A's actions
                [4, 5, 6,  7, 8, 9], where [4, 5, 6] is the lookback buffer range
                (ts=0 item is 7), will respond to `get_actions(-1, agent_ids=[A],
                neg_indices_left_of_zero=True)` with {A: `6`} and to
                `get_actions(slice(-2, 1), agent_ids=[A],
                neg_indices_left_of_zero=True)` with {A: `[5, 6,  7]`}.
            fill: An optional value to use for filling up the returned results at
                the boundaries. This filling only happens if the requested index range's
                start/stop boundaries exceed the episode's boundaries (including the
                lookback buffer on the left side). This comes in very handy, if users
                don't want to worry about reaching such boundaries and want to zero-pad.
                For example, an episode with agent A' actions [10, 11,  12, 13, 14]
                and lookback buffer size of 2 (meaning actions `10` and `11` are
                part of the lookback buffer) will respond to
                `get_actions(slice(-7, -2), agent_ids=[A], fill=0.0)` with
                `{A: [0.0, 0.0, 10, 11, 12]}`.
            one_hot_discrete: If True, will return one-hot vectors (instead of
                int-values) for those sub-components of a (possibly complex) observation
                space that are Discrete or MultiDiscrete.  Note that if `fill=0` and the
                requested `indices` are out of the range of our data, the returned
                one-hot vectors will actually be zero-hot (all slots zero).
            return_list: Whether to return a list of multi-agent dicts (instead of
                a single multi-agent dict of lists/structs). False by default. This
                option can only be used when `env_steps` is True due to the fact the
                such a list can only be interpreted as one env step per list item
                (would not work with agent steps).

        Returns:
            A dictionary mapping agent IDs to actions (at the given
            `indices`). If `env_steps` is True, only agents that have stepped
            (were ready) at the given env step `indices` are returned (i.e. not all
            agent IDs are necessarily in the keys).
            If `return_list` is True, returns a list of MultiAgentDicts (mapping agent
            IDs to extra_model_outputs) instead.
        """
        return self._get(
            what="extra_model_outputs",
            extra_model_outputs_key=key,
            indices=indices,
            agent_ids=agent_ids,
            env_steps=env_steps,
            neg_indices_left_of_zero=neg_indices_left_of_zero,
            fill=fill,
            return_list=return_list,
        )

    def get_terminateds(self) -> MultiAgentDict:
        """Gets the terminateds at given indices."""
        terminateds = {
            agent_id: self.agent_episodes[agent_id].is_terminated
            for agent_id in self.agent_ids
        }
        terminateds.update({"__all__": self.is_terminated})
        return terminateds

    def get_truncateds(self) -> MultiAgentDict:
        truncateds = {
            agent_id: self.agent_episodes[agent_id].is_truncated
            for agent_id in self.agent_ids
        }
        truncateds.update({"__all__": self.is_terminated})
        return truncateds

    def __len__(self):
        """Returns the length of an `MultiAgentEpisode`.

        Note that the length of an episode is defined by the difference
        between its actual timestep and the starting point.

        Returns: An integer defining the length of the episode or an
            error if the episode has not yet started.
        """
        assert (
            sum(len(agent_map) for agent_map in self.env_t_to_agent_t.values()) > 0
        ), (
            "ERROR: Cannot determine length of episode that hasn't started, yet!"
            "Call `MultiAgentEpisode.add_env_reset(observations=)` "
            "first (after which `len(MultiAgentEpisode)` will be 0)."
        )
        return self.env_t - self.env_t_started

    def get_state(self) -> Dict[str, Any]:
        """Returns the state of a multi-agent episode.

        Note that from an episode's state the episode itself can
        be recreated.

        Returns: A dicitonary containing pickable data fro a
            `MultiAgentEpisode`.
        """
        # TODO (simon): Add the buffers.
        return list(
            {
                "id_": self.id_,
                "agent_ids": self.agent_ids,
                "env_t_to_agent_t": self.env_t_to_agent_t,
                "global_actions_t": self.global_actions_t,
                "partial_rewards_t": self.partial_rewards_t,
                "partial_rewards": self.partial_rewards,
                "agent_episodes": list(
                    {
                        agent_id: agent_eps.get_state()
                        for agent_id, agent_eps in self.agent_episodes.items()
                    }.items()
                ),
                "env_t_started": self.env_t_started,
                "env_t": self.env_t,
                "ts_carriage_return": self.ts_carriage_return,
                "is_terminated": self.is_terminated,
                "is_truncated": self.is_truncated,
            }.items()
        )

    @staticmethod
    def from_state(state) -> None:
        """Creates a multi-agent episode from a state dictionary.

        See `MultiAgentEpisode.get_state()` for creating a state for
        a `MultiAgentEpisode` pickable state. For recreating a
        `MultiAgentEpisode` from a state, this state has to be complete,
        i.e. all data must have been stored in the state.
        """
        # TODO (simon): Add the buffers.
        episode = MultiAgentEpisode(id=state[0][1])
        episode._agent_ids = state[1][1]
        episode.env_t_to_agent_t = state[2][1]
        episode.global_actions_t = state[3][1]
        episode.partial_rewards_t = state[4][1]
        episode.partial_rewards = state[5][1]
        episode.agent_episodes = {
            agent_id: SingleAgentEpisode.from_state(agent_state)
            for agent_id, agent_state in state[6][1]
        }
        episode.env_t_started = state[7][1]
        episode.env_t = state[8][1]
        episode.ts_carriage_return = state[9][1]
        episode.is_terminated = state[10][1]
        episode.is_trcunated = state[11][1]
        return episode

    def get_sample_batch(self) -> MultiAgentBatch:
        """Converts this `MultiAgentEpisode` into a `MultiAgentBatch`.

        Each `SingleAgentEpisode` instances in `MultiAgentEpisode.agent_epiosdes`
        will be converted into a `SampleBatch` and the environment timestep will be
        passed as the returned MultiAgentBatch's `env_steps`.

        Returns:
            A MultiAgentBatch containing all of this episode's data.
        """
        # TODO (simon): Check, if timesteps should be converted into global
        # timesteps instead of agent steps.
        # Note, only agents that have stepped are included into the batch.
        return MultiAgentBatch(
            policy_batches={
                agent_id: agent_eps.get_sample_batch()
                for agent_id, agent_eps in self.agent_episodes.items()
                if agent_eps.t - agent_eps.t_started > 0
            },
            env_steps=self.env_t - self.env_t_started,
        )

    def get_return(
        self,
        consider_buffer: bool = False,
    ) -> float:
        """Returns all-agent return.

        Args:
            consider_buffer: Whether we should also consider
                buffered rewards wehn calculating the overall return. Agents might
                have received partial rewards, i.e. rewards without an
                observation. These are stored to the buffer for each agent and added up
                until the next observation is received by that agent.

        Returns:
            The sum of all single-agents' returns (maybe including the buffered
            rewards per agent).
        """
        env_return = sum(
            agent_eps.get_return() for agent_eps in self.agent_episodes.values()
        )
        if consider_buffer:
            for buffered_r in self._agent_buffered_rewards.values():
                env_return += buffered_r

        return env_return

    def get_agents_to_act(self) -> Set[AgentID]:
        """Returns a set of agent IDs required to send an action to `env.step()` next.

        Those are generally the agents that received an observation in the most recent
        `env.step()` call.

        Returns:
            A set of AgentIDs that are suposed to send actions to the next `env.step()`
            call.
        """
        return {
            aid
            for aid in self.get_observations(-1).keys()
            if not self.agent_episodes[aid].is_done
        }

    def get_agents_that_stepped(self) -> Set[AgentID]:
        """Returns a set of agent IDs of those agents that just finished stepping.

        These are all the agents that have an observation logged at the last env
        timestep, which may include agents, whose single agent episode just terminated
        or truncated.

        Returns:
            A set of AgentIDs of those agents that just finished stepping (that have a
            most recent observation on the env timestep scale), regardless of whether
            their single agent episodes are done or not.
        """
        return set(self.get_observations(-1).keys())

    def env_steps(self) -> int:
        """Returns the number of environment steps.

        Note, this episode instance could be a chunk of an actual episode.

        Returns:
            An integer that counts the number of environment steps this episode instance
            has seen.
        """
        return len(self)

    def agent_steps(self) -> int:
        """Number of agent steps.

        Note, there are >= 1 agent steps per environment step.

        Returns:
            An integer counting the number of agent steps executed during the time this
            episode instance records.
        """
        return sum(len(eps) for eps in self.agent_episodes)

    # TODO (sven, simon): This function can only deal with data if it does not contain
    #  terminated or truncated agents (i.e. you have to provide ONLY alive agents in the
    #  agent_ids in the constructor - the episode does not deduce the agents).
    def _init_single_agent_episodes(
        self,
        *,
        agent_module_ids: Optional[Dict[AgentID, ModuleID]] = None,
        agent_episode_ids: Optional[Dict[AgentID, str]] = None,
        observations: Optional[List[MultiAgentDict]] = None,
        actions: Optional[List[MultiAgentDict]] = None,
        rewards: Optional[List[MultiAgentDict]] = None,
        infos: Optional[List[MultiAgentDict]] = None,
        terminateds: Union[MultiAgentDict, bool] = False,
        truncateds: Union[MultiAgentDict, bool] = False,
        extra_model_outputs: Optional[List[MultiAgentDict]] = None,
        len_lookback_buffer: int,
    ):
        if observations is None:
            return
        if actions is None:
            actions = []
            assert not rewards
            rewards = []
            assert not extra_model_outputs
            extra_model_outputs = []

        # Infos and extra_model_outputs are allowed to be None -> Fill them with
        # proper dummy values, if so.
        if infos is None:
            infos = [{} for _ in range(len(observations))]
        if extra_model_outputs is None:
            extra_model_outputs = [{} for _ in range(len(actions))]

        observations_per_agent = defaultdict(list)
        infos_per_agent = defaultdict(list)
        actions_per_agent = defaultdict(list)
        rewards_per_agent = defaultdict(list)
        extra_model_outputs_per_agent = defaultdict(list)
        done_per_agent = defaultdict(bool)
        len_lookback_buffer_per_agent = defaultdict(int)

        all_agent_ids = set(
            agent_episode_ids.keys() if agent_episode_ids is not None else []
        )
        agent_module_ids = agent_module_ids or {}

        # Step through all observations and interpret these as the (global) env steps.
        env_t = self.env_t - len_lookback_buffer
        for data_idx, (obs, inf) in enumerate(zip(observations, infos)):
            # If we do have actions/extra outs/rewards for this timestep, use the data.
            # It may be that these lists have the same length as the observations list,
            # in which case the data will be buffered (agent did step/send an action,
            # but the step has not been concluded yet by the env).
            act = actions[data_idx] if len(actions) > data_idx else {}
            extra_outs = (
                extra_model_outputs[data_idx]
                if len(extra_model_outputs) > data_idx
                else {}
            )
            rew = rewards[data_idx] if len(rewards) > data_idx else {}

            for agent_id, agent_obs in obs.items():
                all_agent_ids.add(agent_id)

                observations_per_agent[agent_id].append(agent_obs)
                infos_per_agent[agent_id].append(inf.get(agent_id, {}))

                # Pull out buffered action (if not first obs for this agent) and
                # complete step for agent.
                if len(observations_per_agent[agent_id]) > 1:
                    actions_per_agent[agent_id].append(
                        self._agent_buffered_actions.pop(agent_id)
                    )
                    extra_model_outputs_per_agent[agent_id].append(
                        self._agent_buffered_extra_model_outputs.pop(agent_id)
                    )
                    rewards_per_agent[agent_id].append(
                        self._agent_buffered_rewards.pop(agent_id)
                    )
                # First obs for this agent. Make sure the agent's mapping is
                # appropriately prepended with self.SKIP_ENV_TS_TAG tags.
                else:
                    self.env_t_to_agent_t[agent_id].extend(
                        [self.SKIP_ENV_TS_TAG] * data_idx
                    )
                    len_lookback_buffer_per_agent[agent_id] += data_idx

                # Agent is still continuing (has an action for the next step).
                if agent_id in act:
                    # Always push actions/extra outputs into buffer, then remove them
                    # from there, once the next observation comes in. Same for rewards.
                    self._agent_buffered_actions[agent_id] = act[agent_id]
                    self._agent_buffered_extra_model_outputs[agent_id] = extra_outs.get(
                        agent_id, {}
                    )
                    self._agent_buffered_rewards[
                        agent_id
                    ] = self._agent_buffered_rewards.get(agent_id, 0.0) + rew.get(
                        agent_id, 0.0
                    )
                # Agent is done (has no action for the next step).
                elif terminateds.get(agent_id) or truncateds.get(agent_id):
                    done_per_agent[agent_id] = True
                # This is the last obs (no further action/reward data).
                else:
                    assert data_idx == len(observations) - 1

                # Update env_t_to_agent_t mapping.
                self.env_t_to_agent_t[agent_id].append(
                    len(observations_per_agent[agent_id]) - 1
                )

            # Those agents that did NOT step get None added to their mapping.
            for agent_id in all_agent_ids:
                if agent_id not in obs and agent_id not in done_per_agent:
                    self.env_t_to_agent_t[agent_id].append(self.SKIP_ENV_TS_TAG)

            # Update per-agent lookback buffer and t_started counters.
            for agent_id in all_agent_ids:
                if env_t < self.env_t_started:
                    if agent_id not in done_per_agent:
                        len_lookback_buffer_per_agent[agent_id] += 1

            # Increase env timestep by one.
            env_t += 1

        # - Validate per-agent data.
        # - Fix lookback buffers of env_t_to_agent_t mappings.
        for agent_id, buf in self.env_t_to_agent_t.items():
            # Skip agent if it doesn't seem to have any data.
            if agent_id not in observations_per_agent:
                continue
            assert (
                len(observations_per_agent[agent_id])
                == len(infos_per_agent[agent_id])
                == len(actions_per_agent[agent_id]) + 1
                == len(extra_model_outputs_per_agent[agent_id]) + 1
                == len(rewards_per_agent[agent_id]) + 1
            )
            buf.lookback = len_lookback_buffer_per_agent[agent_id]

        # Now create the individual episodes from the collected per-agent data.
        for agent_id, agent_obs in observations_per_agent.items():
            lookback = sum(
                s != self.SKIP_ENV_TS_TAG
                for s in self.env_t_to_agent_t[agent_id].get(
                    slice(-len_lookback_buffer_per_agent[agent_id], 0),
                    neg_indices_left_of_zero=True,
                )
            )
            # Try to figure out the module ID for this agent.
            # If not provided explicitly, try the mapping function (if provided).
            module_id = agent_module_ids.get(
                agent_id, self.agent_to_module_mapping_fn(agent_id, self)
            )
            sa_episode = SingleAgentEpisode(
                id_=(
                    agent_episode_ids.get(agent_id)
                    if agent_episode_ids is not None
                    else None
                ),
                agent_id=agent_id,
                module_id=module_id,
                observations=agent_obs,
                observation_space=self.observation_space.get(agent_id),
                infos=infos_per_agent[agent_id],
                actions=actions_per_agent[agent_id],
                action_space=self.action_space.get(agent_id),
                rewards=rewards_per_agent[agent_id],
                extra_model_outputs={
                    k: [i[k] for i in extra_model_outputs_per_agent[agent_id]]
                    for k in extra_model_outputs_per_agent[agent_id][0].keys()
                }
                if extra_model_outputs_per_agent[agent_id]
                else None,
                terminated=terminateds.get(agent_id, False),
                truncated=truncateds.get(agent_id, False),
                t_started=self.agent_t_started[agent_id],
                len_lookback_buffer=lookback,
            )
            self.agent_episodes[agent_id] = sa_episode

    def _get(
        self,
        *,
        what,
        indices,
        agent_ids=None,
        env_steps=True,
        neg_indices_left_of_zero=False,
        fill=None,
        one_hot_discrete=False,
        return_list=False,
        extra_model_outputs_key=None,
    ):
        agent_ids = set(force_list(agent_ids)) or self.agent_ids

        kwargs = dict(
            what=what,
            indices=indices,
            agent_ids=agent_ids,
            neg_indices_left_of_zero=neg_indices_left_of_zero,
            fill=fill,
            # Rewards and infos do not support one_hot_discrete option.
            one_hot_discrete=dict(
                {} if not one_hot_discrete else {"one_hot_discrete": one_hot_discrete}
            ),
            extra_model_outputs_key=extra_model_outputs_key,
        )

        # User specified agent timesteps (indices) -> Simply delegate everything
        # to the individual agents' SingleAgentEpisodes.
        if env_steps is False:
            if return_list:
                raise ValueError(
                    f"`MultiAgentEpisode.get_{what}()` can't be called with both "
                    "`env_steps=False` and `return_list=True`!"
                )
            return self._get_data_by_agent_steps(**kwargs)
        # User specified env timesteps (indices) -> We need to translate them for each
        # agent into agent-timesteps.
        # Return a list of individual per-env-timestep multi-agent dicts.
        elif return_list:
            return self._get_data_by_env_steps_as_list(**kwargs)
        # Return a single multi-agent dict with lists/arrays as leafs.
        else:
            return self._get_data_by_env_steps(**kwargs)

    def _get_data_by_agent_steps(
        self,
        *,
        what,
        indices,
        agent_ids,
        neg_indices_left_of_zero,
        fill,
        one_hot_discrete,
        extra_model_outputs_key,
    ):
        ret = {}
        for agent_id, sa_episode in self.agent_episodes.items():
            if agent_id not in agent_ids:
                continue
            inf_lookback_buffer = getattr(sa_episode, what)
            buffer_val = self._get_buffer_value(what, agent_id)
            if extra_model_outputs_key is not None:
                inf_lookback_buffer = inf_lookback_buffer[extra_model_outputs_key]
                buffer_val = buffer_val[extra_model_outputs_key]
            agent_value = inf_lookback_buffer.get(
                indices=indices,
                neg_indices_left_of_zero=neg_indices_left_of_zero,
                fill=fill,
                _add_last_ts_value=buffer_val,
                **one_hot_discrete,
            )
            if agent_value is None or agent_value == []:
                continue
            ret[agent_id] = agent_value
        return ret

    def _get_data_by_env_steps_as_list(
        self,
        *,
        what,
        indices,
        agent_ids,
        neg_indices_left_of_zero,
        fill,
        one_hot_discrete,
        extra_model_outputs_key,
    ):
        # Collect indices for each agent first, so we can construct the list in
        # the next step.
        agent_indices = {}
        agent_id = None
        for agent_id, sa_episode in self.agent_episodes.items():
            if agent_id not in agent_ids:
                continue
            agent_indices[agent_id] = self.env_t_to_agent_t[agent_id].get(
                indices,
                neg_indices_left_of_zero=neg_indices_left_of_zero,
                fill=self.SKIP_ENV_TS_TAG,
                # For those records where there is no "hanging" last timestep (all
                # other than obs and infos), we have to ignore the last entry in
                # the env_t_to_agent_t mappings.
                _ignore_last_ts=what not in ["observations", "infos"],
            )
        if not agent_indices:
            return []
        ret = []
        for i in range(len(next(iter(agent_indices.values())))):
            ret2 = {}
            for agent_id, idxes in agent_indices.items():
                agent_value = self._get_single_agent_data_by_index(
                    what=what,
                    agent_id=agent_id,
                    index=idxes[i],
                    fill=fill,
                    one_hot_discrete=one_hot_discrete,
                    buffer_val=self._get_buffer_value(what, agent_id),
                    extra_model_outputs_key=extra_model_outputs_key,
                )
                if agent_value is not None:
                    ret2[agent_id] = agent_value
            ret.append(ret2)
        return ret

    def _get_data_by_env_steps(
        self,
        *,
        what,
        indices,
        agent_ids,
        neg_indices_left_of_zero,
        fill,
        one_hot_discrete,
        extra_model_outputs_key,
    ):
        ret = {}
        for agent_id, sa_episode in self.agent_episodes.items():
            if agent_id not in agent_ids:
                continue
            buffer_val = self._get_buffer_value(what, agent_id)
            agent_indices = self.env_t_to_agent_t[agent_id].get(
                indices,
                neg_indices_left_of_zero=neg_indices_left_of_zero,
                fill=self.SKIP_ENV_TS_TAG if fill is not None else None,
                # For those records where there is no "hanging" last timestep (all
                # other than obs and infos), we have to ignore the last entry in
                # the env_t_to_agent_t mappings.
                _ignore_last_ts=what not in ["observations", "infos"],
            )
            if isinstance(agent_indices, list):
                agent_values = self._get_single_agent_data_by_env_step_indices(
                    what=what,
                    agent_id=agent_id,
                    indices=agent_indices,
                    fill=fill,
                    one_hot_discrete=one_hot_discrete,
                    buffer_val=buffer_val,
                    extra_model_outputs_key=extra_model_outputs_key,
                )
                if len(agent_values) == 0:
                    continue
            else:
                agent_values = self._get_single_agent_data_by_index(
                    what=what,
                    agent_id=agent_id,
                    index=agent_indices,
                    fill=fill,
                    one_hot_discrete=one_hot_discrete,
                    buffer_val=buffer_val,
                    extra_model_outputs_key=extra_model_outputs_key,
                )
            if agent_values is None:
                continue
            ret[agent_id] = agent_values
        return ret

    def _get_single_agent_data_by_index(
        self,
        *,
        what,
        agent_id,
        index,
        fill,
        one_hot_discrete,
        buffer_val,
        extra_model_outputs_key,
    ):
        sa_episode = self.agent_episodes[agent_id]

        if index == self.SKIP_ENV_TS_TAG:
            # We don't want to fill -> Skip this agent.
            if fill is None:
                return
            # Provide filled value for this agent.
            return getattr(sa_episode, f"get_{what}")(
                indices=1000000000000,
                neg_indices_left_of_zero=False,
                fill=fill,
                **dict(
                    {}
                    if extra_model_outputs_key is None
                    else {"key": extra_model_outputs_key}
                ),
                **one_hot_discrete,
            )
        # No skip timestep -> Provide value at given index for this agent.
        else:
            inf_lookback_buffer = getattr(sa_episode, what)

            if what == "extra_model_outputs":
                # Special case: extra_model_outputs and key=None (return all keys as
                # a dict). Note that `inf_lookback_buffer` is NOT an infinite lookback
                # buffer, but a dict mapping keys to individual infinite lookback
                # buffers.
                if extra_model_outputs_key is None:
                    return {
                        key: sub_buffer.get(
                            indices=index - sub_buffer.lookback,
                            neg_indices_left_of_zero=True,
                            fill=fill,
                            _add_last_ts_value=buffer_val,
                            **one_hot_discrete,
                        )
                        for key, sub_buffer in inf_lookback_buffer.items()
                    }

                # `what=extra_model_outputs` and `key` is given -> `inf_lookback_buffer`
                # is not a buffer, but a dict -> extract actual buffer from this dict
                # using `key`.
                inf_lookback_buffer = inf_lookback_buffer[extra_model_outputs_key]

            # Extract data directly from the infinite lookback buffer object.
            return inf_lookback_buffer.get(
                indices=index - inf_lookback_buffer.lookback,
                neg_indices_left_of_zero=True,
                fill=fill,
                _add_last_ts_value=buffer_val,
                **one_hot_discrete,
            )

    def _get_single_agent_data_by_env_step_indices(
        self,
        *,
        what,
        agent_id,
        indices,
        fill,
        one_hot_discrete,
        buffer_val,
        extra_model_outputs_key,
    ):
        sa_episode = self.agent_episodes[agent_id]

        inf_lookback_buffer = getattr(sa_episode, what)
        if extra_model_outputs_key is not None:
            inf_lookback_buffer = inf_lookback_buffer[extra_model_outputs_key]

        # If there are self.SKIP_ENV_TS_TAG items in `agent_indices` and user
        # wants to fill these (together with outside-episode-bounds indices) ->
        # Provide these skipped timesteps as filled values.
        if self.SKIP_ENV_TS_TAG in indices and fill is not None:
            single_fill_value = inf_lookback_buffer.get(
                indices=1000000000000,
                neg_indices_left_of_zero=False,
                fill=fill,
                **one_hot_discrete,
            )
            ret = []
            for i in indices:
                if i == self.SKIP_ENV_TS_TAG:
                    ret.append(single_fill_value)
                else:
                    ret.append(
                        inf_lookback_buffer.get(
                            indices=i - getattr(sa_episode, what).lookback,
                            neg_indices_left_of_zero=True,
                            fill=fill,
                            _add_last_ts_value=buffer_val,
                            **one_hot_discrete,
                        )
                    )
            if self.is_finalized:
                ret = batch(ret)
        else:
            # Filter these indices out up front.
            indices = [
                i - inf_lookback_buffer.lookback
                for i in indices
                if i != self.SKIP_ENV_TS_TAG
            ]
            ret = inf_lookback_buffer.get(
                indices=indices,
                neg_indices_left_of_zero=True,
                fill=fill,
                _add_last_ts_value=buffer_val,
                **one_hot_discrete,
            )
        return ret

    def _get_buffer_value(self, what, agent_id):
        if what == "actions":
            return self._agent_buffered_actions.get(agent_id)
        elif what == "extra_model_outputs":
            return self._agent_buffered_extra_model_outputs.get(agent_id)
        elif what == "rewards":
            return self._agent_buffered_rewards.get(agent_id)

    def _del_buffers(self, agent_id):
        self._agent_buffered_actions.pop(agent_id, None)
        self._agent_buffered_extra_model_outputs.pop(agent_id, None)
        self._agent_buffered_rewards.pop(agent_id, None)
