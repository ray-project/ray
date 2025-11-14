import copy
import time
import uuid
from collections import defaultdict
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

import gymnasium as gym

from ray._common.deprecation import Deprecated
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.env.utils.infinite_lookback_buffer import InfiniteLookbackBuffer
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils import force_list
from ray.rllib.utils.error import MultiAgentEnvError
from ray.rllib.utils.spaces.space_utils import batch
from ray.rllib.utils.typing import AgentID, ModuleID, MultiAgentDict
from ray.util.annotations import PublicAPI


# TODO (simon): Include cases in which the number of agents in an
#  episode are shrinking or growing during the episode itself.
@PublicAPI(stability="alpha")
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

    __slots__ = (
        "id_",
        "agent_to_module_mapping_fn",
        "_agent_to_module_mapping",
        "observation_space",
        "action_space",
        "env_t_started",
        "env_t",
        "agent_t_started",
        "env_t_to_agent_t",
        "_hanging_actions_end",
        "_hanging_extra_model_outputs_end",
        "_hanging_rewards_end",
        "_hanging_rewards_begin",
        "is_terminated",
        "is_truncated",
        "agent_episodes",
        "_last_step_time",
        "_len_lookback_buffers",
        "_start_time",
        "_custom_data",
    )

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
            observations: A list of dictionaries mapping agent IDs to observations.
                Can be None. If provided, should match all other episode data
                (actions, rewards, etc.) in terms of list lengths and agent IDs.
            observation_space: An optional gym.spaces.Dict mapping agent IDs to
                individual agents' spaces, which all (individual agents') observations
                should abide to. If not None and this MultiAgentEpisode is numpy'ized
                (via the `self.to_numpy()` method), and data is appended or set, the new
                data will be checked for correctness.
            infos: A list of dictionaries mapping agent IDs to info dicts.
                Can be None. If provided, should match all other episode data
                (observations, rewards, etc.) in terms of list lengths and agent IDs.
            actions: A list of dictionaries mapping agent IDs to actions.
                Can be None. If provided, should match all other episode data
                (observations, rewards, etc.) in terms of list lengths and agent IDs.
            action_space: An optional gym.spaces.Dict mapping agent IDs to
                individual agents' spaces, which all (individual agents') actions
                should abide to. If not None and this MultiAgentEpisode is numpy'ized
                (via the `self.to_numpy()` method), and data is appended or set, the new
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
            agent_episode_ids: An optional dict mapping AgentIDs
                to their corresponding `SingleAgentEpisode`. If None, each
                `SingleAgentEpisode` in `MultiAgentEpisode.agent_episodes`
                will generate a hexadecimal code. If a dictionary is provided,
                make sure that IDs are unique, because the agents' `SingleAgentEpisode`
                instances are concatenated or recreated by it.
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
        """
        self.id_: str = id_ or uuid.uuid4().hex
        if agent_to_module_mapping_fn is None:
            from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

            agent_to_module_mapping_fn = (
                AlgorithmConfig.DEFAULT_AGENT_TO_MODULE_MAPPING_FN
            )
        self.agent_to_module_mapping_fn = agent_to_module_mapping_fn
        # In case a user - e.g. via callbacks - already forces a mapping to happen
        # via the `module_for()` API even before the agent has entered the episode
        # (and has its SingleAgentEpisode created), we store all aldeary done mappings
        # in this dict here.
        self._agent_to_module_mapping: Dict[AgentID, ModuleID] = agent_module_ids or {}

        # Lookback buffer length is not provided. Interpret all provided data as
        # lookback buffer.
        if len_lookback_buffer == "auto":
            len_lookback_buffer = len(rewards or [])
        self._len_lookback_buffers = len_lookback_buffer

        self.observation_space = observation_space or {}
        self.action_space = action_space or {}

        terminateds = terminateds or {}
        truncateds = truncateds or {}

        # The global last timestep of the episode and the timesteps when this chunk
        # started (excluding a possible lookback buffer).
        self.env_t_started = env_t_started or 0
        self.env_t = (
            (len(rewards) if rewards is not None else 0)
            - self._len_lookback_buffers
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

        # Create caches for hanging actions/rewards/extra_model_outputs.
        # When an agent gets an observation (and then sends an action), but does not
        # receive immediately a next observation, we store the "hanging" action (and
        # related rewards and extra model outputs) in the caches postfixed w/ `_end`
        # until the next observation is received.
        self._hanging_actions_end = {}
        self._hanging_extra_model_outputs_end = defaultdict(dict)
        self._hanging_rewards_end = defaultdict(float)

        # In case of a `cut()` or `slice()`, we also need to store the hanging actions,
        # rewards, and extra model outputs that were already "hanging" in preceeding
        # episode slice.
        self._hanging_rewards_begin = defaultdict(float)

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
        )

        # Cache for custom data. May be used to store custom metrics from within a
        # callback for the ongoing episode (e.g. render images).
        self._custom_data = {}

        # Keep timer stats on deltas between steps.
        self._start_time = None
        self._last_step_time = None

        # Validate ourselves.
        self.validate()

    def add_env_reset(
        self,
        *,
        observations: MultiAgentDict,
        infos: Optional[MultiAgentDict] = None,
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
        """
        assert not self.is_done
        # Assume that this episode is completely empty and has not stepped yet.
        # Leave self.env_t (and self.env_t_started) at 0.
        assert self.env_t == self.env_t_started == 0
        infos = infos or {}

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
                    module_id=self.module_for(agent_id),
                    multi_agent_episode_id=self.id_,
                    observation_space=self.observation_space.get(agent_id),
                    action_space=self.action_space.get(agent_id),
                )
            # Add initial observations (and infos) to the agent's episode.
            self.agent_episodes[agent_id].add_env_reset(
                observation=agent_obs,
                infos=infos.get(agent_id),
            )

        # Validate our data.
        self.validate()

        # Start the timer for this episode.
        self._start_time = time.perf_counter()

    def add_env_step(
        self,
        observations: MultiAgentDict,
        actions: MultiAgentDict,
        rewards: MultiAgentDict,
        infos: Optional[MultiAgentDict] = None,
        *,
        terminateds: Optional[MultiAgentDict] = None,
        truncateds: Optional[MultiAgentDict] = None,
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

        # Find out, whether this episode is terminated/truncated (for all agents).
        # Case 1: all agents are terminated or all are truncated.
        self.is_terminated = terminateds.get("__all__", False)
        self.is_truncated = truncateds.get("__all__", False)
        # Find all agents that were done at prior timesteps and add the agents that are
        # done at the present timestep.
        agents_done = set(
            [aid for aid, sa_eps in self.agent_episodes.items() if sa_eps.is_done]
            + [aid for aid in terminateds if terminateds[aid]]
            + [aid for aid in truncateds if truncateds[aid]]
        )
        # Case 2: Some agents are truncated and the others are terminated -> Declare
        # this episode as terminated.
        if all(aid in set(agents_done) for aid in self.agent_ids):
            self.is_terminated = True

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
                sa_episode = SingleAgentEpisode(
                    agent_id=agent_id,
                    module_id=self.module_for(agent_id),
                    multi_agent_episode_id=self.id_,
                    observation_space=self.observation_space.get(agent_id),
                    action_space=self.action_space.get(agent_id),
                )
            else:
                sa_episode = self.agent_episodes[agent_id]

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

            # CASE 2: Step gets completed with a hanging action OR first observation.
            # ------------------------------------------------------------------------
            # We have an observation, but no action ->
            # a) Action (and extra model outputs) must be hanging already. Also use
            # collected hanging rewards and extra_model_outputs.
            # b) The observation is the first observation for this agent ID.
            elif _observation is not None and _action is None:
                _action = self._hanging_actions_end.pop(agent_id, None)

                # We have a hanging action (the agent had acted after the previous
                # observation, but the env had not responded - until now - with another
                # observation).
                # ...[hanging action] ... ... -> next obs + (reward)? ...
                if _action is not None:
                    # Get the extra model output if available.
                    _extra_model_outputs = self._hanging_extra_model_outputs_end.pop(
                        agent_id, None
                    )
                    _reward = self._hanging_rewards_end.pop(agent_id, 0.0) + _reward
                # First observation for this agent, we have no hanging action.
                # ... [done]? ... -> [1st obs for agent ID]
                else:
                    # The agent is already done -> The agent thus has never stepped once
                    # and we do not have to create a SingleAgentEpisode for it.
                    if _terminated or _truncated:
                        self._del_hanging(agent_id)
                        continue
                    # This must be the agent's initial observation.
                    else:
                        # Prepend n skip tags to this agent's mapping + the initial [0].
                        assert agent_id not in self.env_t_to_agent_t
                        self.env_t_to_agent_t[agent_id].extend(
                            [self.SKIP_ENV_TS_TAG] * self.env_t + [0]
                        )
                        self.env_t_to_agent_t[
                            agent_id
                        ].lookback = self._len_lookback_buffers
                        # Make `add_env_reset` call and continue with next agent.
                        sa_episode.add_env_reset(observation=_observation, infos=_infos)
                        # Add possible reward to begin cache.
                        self._hanging_rewards_begin[agent_id] += _reward
                        # Now that the SAEps is valid, add it to our dict.
                        self.agent_episodes[agent_id] = sa_episode
                        continue

            # CASE 3: Step is started (by an action), but not completed (no next obs).
            # ------------------------------------------------------------------------
            # We have no observation, but we have a hanging action (used when we receive
            # the next obs for this agent in the future).
            elif agent_id not in observations and agent_id in actions:
                # Agent got truncated -> Error b/c we would need a last (truncation)
                # observation for this (otherwise, e.g. bootstrapping would not work).
                # [previous obs] [action] (hanging) ... ... [truncated]
                if _truncated:
                    raise MultiAgentEnvError(
                        f"Agent {agent_id} acted and then got truncated, but did NOT "
                        "receive a last (truncation) observation, required for e.g. "
                        "value function bootstrapping!"
                    )
                # Agent got terminated.
                # [previous obs] [action] (hanging) ... ... [terminated]
                elif _terminated:
                    # If the agent was terminated and no observation is provided,
                    # duplicate the previous one (this is a technical "fix" to properly
                    # complete the single agent episode; this last observation is never
                    # used for learning anyway).
                    _observation = sa_episode._last_added_observation
                    _infos = sa_episode._last_added_infos
                # Agent is still alive.
                # [previous obs] [action] (hanging) ...
                else:
                    # Hanging action, reward, and extra_model_outputs.
                    assert agent_id not in self._hanging_actions_end
                    self._hanging_actions_end[agent_id] = _action
                    self._hanging_rewards_end[agent_id] = _reward
                    self._hanging_extra_model_outputs_end[
                        agent_id
                    ] = _extra_model_outputs

            # CASE 4: Step has started in the past and is still ongoing (no observation,
            # no action).
            # --------------------------------------------------------------------------
            # Record reward and terminated/truncated flags.
            else:
                _action = self._hanging_actions_end.get(agent_id)

                # Agent is done.
                if _terminated or _truncated:
                    # If the agent has NOT stepped, we treat it as not being
                    # part of this episode.
                    # ... ... [other agents doing stuff] ... ... [agent done]
                    if _action is None:
                        self._del_hanging(agent_id)
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

                    # [obs] ... ... [hanging action] ... ... [done]
                    # If the agent was terminated and no observation is provided,
                    # duplicate the previous one (this is a technical "fix" to properly
                    # complete the single agent episode; this last observation is never
                    # used for learning anyway).
                    _observation = sa_episode._last_added_observation
                    _infos = sa_episode._last_added_infos
                    # `_action` is already `get` above. We don't need to pop out from
                    # the cache as it gets wiped out anyway below b/c the agent is
                    # done.
                    _extra_model_outputs = self._hanging_extra_model_outputs_end.pop(
                        agent_id, None
                    )
                    _reward = self._hanging_rewards_end.pop(agent_id, 0.0) + _reward
                # The agent is still alive, just add current reward to cache.
                else:
                    # But has never stepped in this episode -> add to begin cache.
                    if agent_id not in self.agent_episodes:
                        self._hanging_rewards_begin[agent_id] += _reward
                    # Otherwise, add to end cache.
                    else:
                        self._hanging_rewards_end[agent_id] += _reward

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

            # Agent is also done. -> Erase all hanging values for this agent
            # (they should be empty at this point anyways).
            if _terminated or _truncated:
                self._del_hanging(agent_id)

        # Validate our data.
        self.validate()

        # Step time stats.
        self._last_step_time = time.perf_counter()
        if self._start_time is None:
            self._start_time = self._last_step_time

    def validate(self) -> None:
        """Validates the episode's data.

        This function ensures that the data stored to a `MultiAgentEpisode` is
        in order (e.g. that the correct number of observations, actions, rewards
        are there).
        """
        for eps in self.agent_episodes.values():
            eps.validate()

        # TODO (sven): Validate MultiAgentEpisode specifics, like the timestep mappings,
        #  action/reward caches, etc..

    @property
    def custom_data(self):
        return self._custom_data

    @property
    def is_reset(self) -> bool:
        """Returns True if `self.add_env_reset()` has already been called."""
        return any(
            len(sa_episode.observations) > 0
            for sa_episode in self.agent_episodes.values()
        )

    @property
    def is_numpy(self) -> bool:
        """True, if the data in this episode is already stored as numpy arrays."""
        is_numpy = next(iter(self.agent_episodes.values())).is_numpy
        # Make sure that all single agent's episodes' `is_numpy` flags are the same.
        if not all(eps.is_numpy is is_numpy for eps in self.agent_episodes.values()):
            raise RuntimeError(
                f"Only some SingleAgentEpisode objects in {self} are converted to "
                f"numpy, others are not!"
            )
        return is_numpy

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

    def to_numpy(self) -> "MultiAgentEpisode":
        """Converts this Episode's list attributes to numpy arrays.

        This means in particular that this episodes' lists (per single agent) of
        (possibly complex) data (e.g. an agent having a dict obs space) will be
        converted to (possibly complex) structs, whose leafs are now numpy arrays.
        Each of these leaf numpy arrays will have the same length (batch dimension)
        as the length of the original lists.

        Note that Columns.INFOS are NEVER numpy'ized and will remain a list
        (normally, a list of the original, env-returned dicts). This is due to the
        heterogeneous nature of INFOS returned by envs, which would make it unwieldy to
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
                # being converted `to_numpy` or not (via the `self.to_numpy()` method)!
                terminateds=terminateds,
                truncateds=truncateds,
                len_lookback_buffer=0,  # no lookback; all data is actually "in" episode
            )

            # Episode has not been numpy'ized yet.
            assert not episode.is_numpy
            # We are still operating on lists.
            assert (
                episode.get_observations(
                    indices=[1],
                    agent_ids="agent_1",
                ) == {"agent_1": [1]}
            )

            # Numpy'ized the episode.
            episode.to_numpy()
            assert episode.is_numpy

            # Everything is now numpy arrays (with 0-axis of size
            # B=[len of requested slice]).
            assert (
                isinstance(episode.get_observations(
                    indices=[1],
                    agent_ids="agent_1",
                )["agent_1"], np.ndarray)
            )

        Returns:
             This `MultiAgentEpisode` object with the converted numpy data.
        """

        for agent_id, agent_eps in self.agent_episodes.copy().items():
            agent_eps.to_numpy()

        return self

    def concat_episode(self, other: "MultiAgentEpisode") -> None:
        """Adds the given `other` MultiAgentEpisode to the right side of `self`.

        In order for this to work, both chunks (`self` and `other`) must fit
        together. This is checked by the IDs (must be identical), the time step counters
        (`self.env_t` must be the same as `episode_chunk.env_t_started`), as well as the
        observations/infos of the individual agents at the concatenation boundaries.
        Also, `self.is_done` must not be True, meaning `self.is_terminated` and
        `self.is_truncated` are both False.

        Args:
            other: The other `MultiAgentEpisode` to be concatenated to this one.

        Returns:
            A `MultiAgentEpisode` instance containing the concatenated data
            from both episodes (`self` and `other`).
        """
        # Make sure the IDs match.
        assert other.id_ == self.id_
        # NOTE (sven): This is what we agreed on. As the replay buffers must be
        # able to concatenate.
        assert not self.is_done
        # Make sure the timesteps match.
        assert self.env_t == other.env_t_started
        # Validate `other`.
        other.validate()

        # Concatenate the individual SingleAgentEpisodes from both chunks.
        all_agent_ids = set(self.agent_ids) | set(other.agent_ids)
        for agent_id in all_agent_ids:
            sa_episode = self.agent_episodes.get(agent_id)

            # If agent is only in the new episode chunk -> Store all the data of `other`
            # wrt agent in `self`.
            if sa_episode is None:
                self.agent_episodes[agent_id] = other.agent_episodes[agent_id]
                self.env_t_to_agent_t[agent_id] = other.env_t_to_agent_t[agent_id]
                self.agent_t_started[agent_id] = other.agent_t_started[agent_id]
                self._copy_hanging(agent_id, other)

            # If the agent was done in `self`, ignore and continue. There should not be
            # any data of that agent in `other`.
            elif sa_episode.is_done:
                continue

            # If the agent has data in both chunks, concatenate on the single-agent
            # level, thereby making sure the hanging values (begin and end) match.
            elif agent_id in other.agent_episodes:
                # If `other` has hanging (end) values -> Add these to `self`'s agent
                # SingleAgentEpisode (as a new timestep) and only then concatenate.
                # Otherwise, the concatentaion would fail b/c of missing data.
                if agent_id in self._hanging_actions_end:
                    assert agent_id in self._hanging_extra_model_outputs_end
                    sa_episode.add_env_step(
                        observation=other.agent_episodes[agent_id].get_observations(0),
                        infos=other.agent_episodes[agent_id].get_infos(0),
                        action=self._hanging_actions_end[agent_id],
                        reward=(
                            self._hanging_rewards_end[agent_id]
                            + other._hanging_rewards_begin[agent_id]
                        ),
                        extra_model_outputs=(
                            self._hanging_extra_model_outputs_end[agent_id]
                        ),
                    )
                sa_episode.concat_episode(other.agent_episodes[agent_id])
                # Override `self`'s hanging (end) values with `other`'s hanging (end).
                if agent_id in other._hanging_actions_end:
                    self._hanging_actions_end[agent_id] = copy.deepcopy(
                        other._hanging_actions_end[agent_id]
                    )
                    self._hanging_rewards_end[agent_id] = other._hanging_rewards_end[
                        agent_id
                    ]
                    self._hanging_extra_model_outputs_end[agent_id] = copy.deepcopy(
                        other._hanging_extra_model_outputs_end[agent_id]
                    )

                # Concatenate the env- to agent-timestep mappings.
                j = self.env_t
                for i, val in enumerate(other.env_t_to_agent_t[agent_id][1:]):
                    if val == self.SKIP_ENV_TS_TAG:
                        self.env_t_to_agent_t[agent_id].append(self.SKIP_ENV_TS_TAG)
                    else:
                        self.env_t_to_agent_t[agent_id].append(i + 1 + j)

            # Otherwise, the agent is only in `self` and not done. All data is stored
            # already -> skip
            # else: pass

        # Update all timestep counters.
        self.env_t = other.env_t
        # Check, if the episode is terminated or truncated.
        if other.is_terminated:
            self.is_terminated = True
        elif other.is_truncated:
            self.is_truncated = True

        # Merge with `other`'s custom_data, but give `other` priority b/c we assume
        # that as a follow-up chunk of `self` other has a more complete version of
        # `custom_data`.
        self.custom_data.update(other.custom_data)

        # Validate.
        self.validate()

    def cut(self, len_lookback_buffer: int = 0) -> "MultiAgentEpisode":
        """Returns a successor episode chunk (of len=0) continuing from this Episode.

        The successor will have the same ID as `self`.
        If no lookback buffer is requested (len_lookback_buffer=0), the successor's
        observations will be the last observation(s) of `self` and its length will
        therefore be 0 (no further steps taken yet). If `len_lookback_buffer` > 0,
        the returned successor will have `len_lookback_buffer` observations (and
        actions, rewards, etc..) taken from the right side (end) of `self`. For example
        if `len_lookback_buffer=2`, the returned successor's lookback buffer actions
        will be identical to the results of `self.get_actions([-2, -1])`.

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

        # If there is hanging data (e.g. actions) in the agents' caches, we might have
        # to re-adjust the lookback len further into the past to make sure that these
        # agents have at least one observation to look back to. Otherwise, the timestep
        # that got cut into will be "lost" for learning from it.
        orig_len_lb = len_lookback_buffer
        for agent_id, agent_actions in self._hanging_actions_end.items():
            assert self.env_t_to_agent_t[agent_id].get(-1) == self.SKIP_ENV_TS_TAG
            for i in range(orig_len_lb, len(self.env_t_to_agent_t[agent_id].data) + 1):
                if self.env_t_to_agent_t[agent_id].get(-i) != self.SKIP_ENV_TS_TAG:
                    len_lookback_buffer = max(len_lookback_buffer, i - 1)
                    break

        # Initialize this episode chunk with the most recent observations
        # and infos (even if lookback is zero). Similar to an initial `env.reset()`
        indices_obs_and_infos = slice(-len_lookback_buffer - 1, None)
        indices_rest = (
            slice(-len_lookback_buffer, None)
            if len_lookback_buffer > 0
            else slice(None, 0)  # -> empty slice
        )

        observations = self.get_observations(
            indices=indices_obs_and_infos, return_list=True
        )
        infos = self.get_infos(indices=indices_obs_and_infos, return_list=True)
        actions = self.get_actions(indices=indices_rest, return_list=True)
        rewards = self.get_rewards(indices=indices_rest, return_list=True)
        extra_model_outputs = self.get_extra_model_outputs(
            key=None,  # all keys
            indices=indices_rest,
            return_list=True,
        )

        successor = MultiAgentEpisode(
            # Same ID.
            id_=self.id_,
            observations=observations,
            observation_space=self.observation_space,
            infos=infos,
            actions=actions,
            action_space=self.action_space,
            rewards=rewards,
            # List of MADicts, mapping agent IDs to their respective extra model output
            # dicts.
            extra_model_outputs=extra_model_outputs,
            terminateds=self.get_terminateds(),
            truncateds=self.get_truncateds(),
            # Continue with `self`'s current timesteps.
            env_t_started=self.env_t,
            agent_t_started={
                aid: self.agent_episodes[aid].t
                for aid in self.agent_ids
                if not self.agent_episodes[aid].is_done
            },
            # Same AgentIDs and SingleAgentEpisode IDs.
            agent_episode_ids=self.agent_episode_ids,
            agent_module_ids={
                aid: self.agent_episodes[aid].module_id for aid in self.agent_ids
            },
            agent_to_module_mapping_fn=self.agent_to_module_mapping_fn,
            # All data we provided to the c'tor goes into the lookback buffer.
            len_lookback_buffer="auto",
        )

        # Copy over the hanging (end) values into the hanging (begin) caches of the
        # successor.
        successor._hanging_rewards_begin = self._hanging_rewards_end.copy()

        # Deepcopy all custom data in `self` to be continued in the cut episode.
        successor._custom_data = copy.deepcopy(self.custom_data)

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

    def module_for(self, agent_id: AgentID) -> Optional[ModuleID]:
        """Returns the ModuleID for a given AgentID.

        Forces the agent-to-module mapping to be performed (via
        `self.agent_to_module_mapping_fn`), if this has not been done yet.
        Note that all such mappings are stored in the `self._agent_to_module_mapping`
        property.

        Args:
            agent_id: The AgentID to get a mapped ModuleID for.

        Returns:
            The ModuleID mapped to from the given `agent_id`.
        """
        if agent_id not in self._agent_to_module_mapping:
            module_id = self._agent_to_module_mapping[
                agent_id
            ] = self.agent_to_module_mapping_fn(agent_id, self)
            return module_id
        else:
            return self._agent_to_module_mapping[agent_id]

    def get_observations(
        self,
        indices: Optional[Union[int, List[int], slice]] = None,
        agent_ids: Optional[Union[Collection[AgentID], AgentID]] = None,
        *,
        env_steps: bool = True,
        # global_indices: bool = False,
        neg_index_as_lookback: bool = False,
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
                unless the `neg_index_as_lookback=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
                If None, will return all observations (from ts=0 to the end).
            agent_ids: An optional collection of AgentIDs or a single AgentID to get
                observations for. If None, will return observations for all agents in
                this episode.
            env_steps: Whether `indices` should be interpreted as environment time steps
                (True) or per-agent timesteps (False).
            neg_index_as_lookback: If True, negative values in `indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with agent A's observations
                [4, 5, 6,  7, 8, 9], where [4, 5, 6] is the lookback buffer range
                (ts=0 item is 7), will respond to `get_observations(-1, agent_ids=[A],
                neg_index_as_lookback=True)` with {A: `6`} and to
                `get_observations(slice(-2, 1), agent_ids=[A],
                neg_index_as_lookback=True)` with {A: `[5, 6,  7]`}.
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
            neg_index_as_lookback=neg_index_as_lookback,
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
        neg_index_as_lookback: bool = False,
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
                unless the `neg_index_as_lookback=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
                If None, will return all infos (from ts=0 to the end).
            agent_ids: An optional collection of AgentIDs or a single AgentID to get
                info dicts for. If None, will return info dicts for all agents in
                this episode.
            env_steps: Whether `indices` should be interpreted as environment time steps
                (True) or per-agent timesteps (False).
            neg_index_as_lookback: If True, negative values in `indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with agent A's info dicts
                [{"l":4}, {"l":5}, {"l":6},  {"a":7}, {"b":8}, {"c":9}], where the
                first 3 items are the lookback buffer (ts=0 item is {"a": 7}), will
                respond to `get_infos(-1, agent_ids=A, neg_index_as_lookback=True)`
                with `{A: {"l":6}}` and to
                `get_infos(slice(-2, 1), agent_ids=A, neg_index_as_lookback=True)`
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
            neg_index_as_lookback=neg_index_as_lookback,
            fill=fill,
            return_list=return_list,
        )

    def get_actions(
        self,
        indices: Optional[Union[int, List[int], slice]] = None,
        agent_ids: Optional[Union[Collection[AgentID], AgentID]] = None,
        *,
        env_steps: bool = True,
        neg_index_as_lookback: bool = False,
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
                unless the `neg_index_as_lookback=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
                If None, will return all actions (from ts=0 to the end).
            agent_ids: An optional collection of AgentIDs or a single AgentID to get
                actions for. If None, will return actions for all agents in
                this episode.
            env_steps: Whether `indices` should be interpreted as environment time steps
                (True) or per-agent timesteps (False).
            neg_index_as_lookback: If True, negative values in `indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with agent A's actions
                [4, 5, 6,  7, 8, 9], where [4, 5, 6] is the lookback buffer range
                (ts=0 item is 7), will respond to `get_actions(-1, agent_ids=[A],
                neg_index_as_lookback=True)` with {A: `6`} and to
                `get_actions(slice(-2, 1), agent_ids=[A],
                neg_index_as_lookback=True)` with {A: `[5, 6,  7]`}.
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
            neg_index_as_lookback=neg_index_as_lookback,
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
        neg_index_as_lookback: bool = False,
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
                unless the `neg_index_as_lookback=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
                If None, will return all rewards (from ts=0 to the end).
            agent_ids: An optional collection of AgentIDs or a single AgentID to get
                rewards for. If None, will return rewards for all agents in
                this episode.
            env_steps: Whether `indices` should be interpreted as environment time steps
                (True) or per-agent timesteps (False).
            neg_index_as_lookback: If True, negative values in `indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with agent A's rewards
                [4, 5, 6,  7, 8, 9], where [4, 5, 6] is the lookback buffer range
                (ts=0 item is 7), will respond to `get_rewards(-1, agent_ids=[A],
                neg_index_as_lookback=True)` with {A: `6`} and to
                `get_rewards(slice(-2, 1), agent_ids=[A],
                neg_index_as_lookback=True)` with {A: `[5, 6,  7]`}.
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
            neg_index_as_lookback=neg_index_as_lookback,
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
        neg_index_as_lookback: bool = False,
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
                unless the `neg_index_as_lookback=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
                If None, will return all extra model outputs (from ts=0 to the end).
            agent_ids: An optional collection of AgentIDs or a single AgentID to get
                extra model outputs for. If None, will return extra model outputs for
                all agents in this episode.
            env_steps: Whether `indices` should be interpreted as environment time steps
                (True) or per-agent timesteps (False).
            neg_index_as_lookback: If True, negative values in `indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, an episode with agent A's actions
                [4, 5, 6,  7, 8, 9], where [4, 5, 6] is the lookback buffer range
                (ts=0 item is 7), will respond to `get_actions(-1, agent_ids=[A],
                neg_index_as_lookback=True)` with {A: `6`} and to
                `get_actions(slice(-2, 1), agent_ids=[A],
                neg_index_as_lookback=True)` with {A: `[5, 6,  7]`}.
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
            neg_index_as_lookback=neg_index_as_lookback,
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

    def slice(
        self,
        slice_: slice,
        *,
        len_lookback_buffer: Optional[int] = None,
    ) -> "MultiAgentEpisode":
        """Returns a slice of this episode with the given slice object.

        Works analogous to
        :py:meth:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode.slice`

        However, the important differences are:
        - `slice_` is provided in (global) env steps, not agent steps.
        - In case `slice_` ends - for a certain agent - in an env step, where that
        particular agent does not have an observation, the previous observation will
        be included, but the next action and sum of rewards until this point will
        be stored in the agent's hanging values caches for the returned
        MultiAgentEpisode slice.

        .. testcode::

            from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
            from ray.rllib.utils.test_utils import check

            # Generate a simple multi-agent episode.
            observations = [
                {"a0": 0, "a1": 0},  # 0
                {         "a1": 1},  # 1
                {         "a1": 2},  # 2
                {"a0": 3, "a1": 3},  # 3
                {"a0": 4},           # 4
            ]
            # Actions are the same as observations (except for last obs, which doesn't
            # have an action).
            actions = observations[:-1]
            # Make up a reward for each action.
            rewards = [
                {aid: r / 10 + 0.1 for aid, r in o.items()}
                for o in observations
            ]
            episode = MultiAgentEpisode(
                observations=observations,
                actions=actions,
                rewards=rewards,
                len_lookback_buffer=0,
            )

            # Slice the episode and check results.
            slice = episode[1:3]
            a0 = slice.agent_episodes["a0"]
            a1 = slice.agent_episodes["a1"]
            check((a0.observations, a1.observations), ([3], [1, 2, 3]))
            check((a0.actions, a1.actions), ([], [1, 2]))
            check((a0.rewards, a1.rewards), ([], [0.2, 0.3]))
            check((a0.is_done, a1.is_done), (False, False))

            # If a slice ends in a "gap" for an agent, expect actions and rewards to be
            # cached for this agent.
            slice = episode[:2]
            a0 = slice.agent_episodes["a0"]
            check(a0.observations, [0])
            check(a0.actions, [])
            check(a0.rewards, [])
            check(slice._hanging_actions_end["a0"], 0)
            check(slice._hanging_rewards_end["a0"], 0.1)

        Args:
            slice_: The slice object to use for slicing. This should exclude the
                lookback buffer, which will be prepended automatically to the returned
                slice.
            len_lookback_buffer: If not None, forces the returned slice to try to have
                this number of timesteps in its lookback buffer (if available). If None
                (default), tries to make the returned slice's lookback as large as the
                current lookback buffer of this episode (`self`).

        Returns:
            The new MultiAgentEpisode representing the requested slice.
        """
        if slice_.step not in [1, None]:
            raise NotImplementedError(
                "Slicing MultiAgentEnv with a step other than 1 (you used"
                f" {slice_.step}) is not supported!"
            )

        # Translate `slice_` into one that only contains 0-or-positive ints and will
        # NOT contain any None.
        start = slice_.start
        stop = slice_.stop

        # Start is None -> 0.
        if start is None:
            start = 0
        # Start is negative -> Interpret index as counting "from end".
        elif start < 0:
            start = max(len(self) + start, 0)
        # Start is larger than len(self) -> Clip to len(self).
        elif start > len(self):
            start = len(self)

        # Stop is None -> Set stop to our len (one ts past last valid index).
        if stop is None:
            stop = len(self)
        # Stop is negative -> Interpret index as counting "from end".
        elif stop < 0:
            stop = max(len(self) + stop, 0)
        # Stop is larger than len(self) -> Clip to len(self).
        elif stop > len(self):
            stop = len(self)

        ref_lookback = None
        try:
            for aid, sa_episode in self.agent_episodes.items():
                if ref_lookback is None:
                    ref_lookback = sa_episode.observations.lookback
                assert sa_episode.observations.lookback == ref_lookback
                assert sa_episode.actions.lookback == ref_lookback
                assert sa_episode.rewards.lookback == ref_lookback
                assert all(
                    ilb.lookback == ref_lookback
                    for ilb in sa_episode.extra_model_outputs.values()
                )
        except AssertionError:
            raise ValueError(
                "Can only slice a MultiAgentEpisode if all lookback buffers in this "
                "episode have the exact same size!"
            )

        # Determine terminateds/truncateds and when (in agent timesteps) the
        # single-agent episode slices start.
        terminateds = {}
        truncateds = {}
        agent_t_started = {}
        for aid, sa_episode in self.agent_episodes.items():
            mapping = self.env_t_to_agent_t[aid]
            # If the (agent) timestep directly at the slice stop boundary is equal to
            # the length of the single-agent episode of this agent -> Use the
            # single-agent episode's terminated/truncated flags.
            # If `stop` is already beyond this agent's single-agent episode, then we
            # don't have to keep track of this: The MultiAgentEpisode initializer will
            # automatically determine that this agent must be done (b/c it has no action
            # following its final observation).
            if (
                stop < len(mapping)
                and mapping[stop] != self.SKIP_ENV_TS_TAG
                and len(sa_episode) == mapping[stop]
            ):
                terminateds[aid] = sa_episode.is_terminated
                truncateds[aid] = sa_episode.is_truncated
            # Determine this agent's t_started.
            if start < len(mapping):
                for i in range(start, len(mapping)):
                    if mapping[i] != self.SKIP_ENV_TS_TAG:
                        agent_t_started[aid] = sa_episode.t_started + mapping[i]
                        break
        terminateds["__all__"] = all(
            terminateds.get(aid) for aid in self.agent_episodes
        )
        truncateds["__all__"] = all(truncateds.get(aid) for aid in self.agent_episodes)

        # Determine all other slice contents.
        _lb = len_lookback_buffer if len_lookback_buffer is not None else ref_lookback
        if start - _lb < 0 and ref_lookback < (_lb - start):
            _lb = ref_lookback + start
        observations = self.get_observations(
            slice(start - _lb, stop + 1),
            neg_index_as_lookback=True,
            return_list=True,
        )
        actions = self.get_actions(
            slice(start - _lb, stop),
            neg_index_as_lookback=True,
            return_list=True,
        )
        rewards = self.get_rewards(
            slice(start - _lb, stop),
            neg_index_as_lookback=True,
            return_list=True,
        )
        extra_model_outputs = self.get_extra_model_outputs(
            indices=slice(start - _lb, stop),
            neg_index_as_lookback=True,
            return_list=True,
        )

        # Create the actual slice to be returned.
        ma_episode = MultiAgentEpisode(
            id_=self.id_,
            # In the following, offset `start`s automatically by lookbacks.
            observations=observations,
            observation_space=self.observation_space,
            actions=actions,
            action_space=self.action_space,
            rewards=rewards,
            extra_model_outputs=extra_model_outputs,
            terminateds=terminateds,
            truncateds=truncateds,
            len_lookback_buffer=_lb,
            env_t_started=self.env_t_started + start,
            agent_episode_ids={
                aid: eid.id_ for aid, eid in self.agent_episodes.items()
            },
            agent_t_started=agent_t_started,
            agent_module_ids=self._agent_to_module_mapping,
            agent_to_module_mapping_fn=self.agent_to_module_mapping_fn,
        )

        # Numpy'ize slice if `self` is also finalized.
        if self.is_numpy:
            ma_episode.to_numpy()

        return ma_episode

    def __len__(self):
        """Returns the length of an `MultiAgentEpisode`.

        Note that the length of an episode is defined by the difference
        between its actual timestep and the starting point.

        Returns: An integer defining the length of the episode or an
            error if the episode has not yet started.
        """
        return self.env_t - self.env_t_started

    def __repr__(self):
        sa_eps_returns = {
            aid: sa_eps.get_return() for aid, sa_eps in self.agent_episodes.items()
        }
        return (
            f"MAEps(len={len(self)} done={self.is_done} "
            f"Rs={sa_eps_returns} id_={self.id_})"
        )

    def print(self) -> None:
        """Prints this MultiAgentEpisode as a table of observations for the agents."""

        # Find the maximum timestep across all agents to determine the grid width.
        max_ts = max(ts.len_incl_lookback() for ts in self.env_t_to_agent_t.values())
        lookback = next(iter(self.env_t_to_agent_t.values())).lookback
        longest_agent = max(len(aid) for aid in self.agent_ids)
        # Construct the header.
        header = (
            "ts"
            + (" " * longest_agent)
            + "   ".join(str(i) for i in range(-lookback, max_ts - lookback))
            + "\n"
        )
        # Construct each agent's row.
        rows = []
        for agent, inf_buffer in self.env_t_to_agent_t.items():
            row = f"{agent}  " + (" " * (longest_agent - len(agent)))
            for t in inf_buffer.data:
                # Two spaces for alignment.
                if t == "S":
                    row += "    "
                # Mark the step with an x.
                else:
                    row += " x  "
            # Remove trailing space for alignment.
            rows.append(row.rstrip())

        # Join all components into a final string
        print(header + "\n".join(rows))

    def get_state(self) -> Dict[str, Any]:
        """Returns the state of a multi-agent episode.

        Note that from an episode's state the episode itself can
        be recreated.

        Returns: A dicitonary containing pickable data for a
            `MultiAgentEpisode`.
        """
        return {
            "id_": self.id_,
            "agent_to_module_mapping_fn": self.agent_to_module_mapping_fn,
            "_agent_to_module_mapping": self._agent_to_module_mapping,
            "observation_space": self.observation_space,
            "action_space": self.action_space,
            "env_t_started": self.env_t_started,
            "env_t": self.env_t,
            "agent_t_started": self.agent_t_started,
            # TODO (simon): Check, if we can store the `InfiniteLookbackBuffer`
            "env_t_to_agent_t": self.env_t_to_agent_t,
            "_hanging_actions_end": self._hanging_actions_end,
            "_hanging_extra_model_outputs_end": self._hanging_extra_model_outputs_end,
            "_hanging_rewards_end": self._hanging_rewards_end,
            "_hanging_rewards_begin": self._hanging_rewards_begin,
            "is_terminated": self.is_terminated,
            "is_truncated": self.is_truncated,
            "agent_episodes": list(
                {
                    agent_id: agent_eps.get_state()
                    for agent_id, agent_eps in self.agent_episodes.items()
                }.items()
            ),
            "_start_time": self._start_time,
            "_last_step_time": self._last_step_time,
            "custom_data": self.custom_data,
        }

    @staticmethod
    def from_state(state: Dict[str, Any]) -> "MultiAgentEpisode":
        """Creates a multi-agent episode from a state dictionary.

        See `MultiAgentEpisode.get_state()` for creating a state for
        a `MultiAgentEpisode` pickable state. For recreating a
        `MultiAgentEpisode` from a state, this state has to be complete,
        i.e. all data must have been stored in the state.

        Args:
            state: A dict containing all data required to recreate a MultiAgentEpisode`.
                See `MultiAgentEpisode.get_state()`.

        Returns:
            A `MultiAgentEpisode` instance created from the state data.
        """
        # Create an empty `MultiAgentEpisode` instance.
        episode = MultiAgentEpisode(id_=state["id_"])
        # Fill the instance with the state data.
        episode.agent_to_module_mapping_fn = state["agent_to_module_mapping_fn"]
        episode._agent_to_module_mapping = state["_agent_to_module_mapping"]
        episode.observation_space = state["observation_space"]
        episode.action_space = state["action_space"]
        episode.env_t_started = state["env_t_started"]
        episode.env_t = state["env_t"]
        episode.agent_t_started = state["agent_t_started"]
        episode.env_t_to_agent_t = state["env_t_to_agent_t"]
        episode._hanging_actions_end = state["_hanging_actions_end"]
        episode._hanging_extra_model_outputs_end = state[
            "_hanging_extra_model_outputs_end"
        ]
        episode._hanging_rewards_end = state["_hanging_rewards_end"]
        episode._hanging_rewards_begin = state["_hanging_rewards_begin"]
        episode.is_terminated = state["is_terminated"]
        episode.is_truncated = state["is_truncated"]
        episode.agent_episodes = {
            agent_id: SingleAgentEpisode.from_state(agent_state)
            for agent_id, agent_state in state["agent_episodes"]
        }
        episode._start_time = state["_start_time"]
        episode._last_step_time = state["_last_step_time"]
        episode._custom_data = state.get("custom_data", {})

        # Validate the episode.
        episode.validate()

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
        include_hanging_rewards: bool = False,
    ) -> float:
        """Returns all-agent return.

        Args:
            include_hanging_rewards: Whether we should also consider
                hanging rewards wehn calculating the overall return. Agents might
                have received partial rewards, i.e. rewards without an
                observation. These are stored in the "hanging" caches (begin and end)
                for each agent and added up until the next observation is received by
                that agent.

        Returns:
            The sum of all single-agents' returns (maybe including the hanging
            rewards per agent).
        """
        env_return = sum(
            agent_eps.get_return() for agent_eps in self.agent_episodes.values()
        )
        if include_hanging_rewards:
            for hanging_r in self._hanging_rewards_begin.values():
                env_return += hanging_r
            for hanging_r in self._hanging_rewards_end.values():
                env_return += hanging_r

        return env_return

    def get_agents_to_act(self) -> Set[AgentID]:
        """Returns a set of agent IDs required to send an action to `env.step()` next.

        Those are generally the agents that received an observation in the most recent
        `env.step()` call.

        Returns:
            A set of AgentIDs that are supposed to send actions to the next `env.step()`
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

    def get_duration_s(self) -> float:
        """Returns the duration of this Episode (chunk) in seconds."""
        if self._last_step_time is None:
            return 0.0
        return self._last_step_time - self._start_time

    def set_observations(
        self,
        *,
        new_data: MultiAgentDict,
        at_indices: Optional[Union[int, List[int], slice]] = None,
        neg_index_as_lookback: bool = False,
    ) -> None:
        """Overwrites all or some single-agent Episode's observations with the provided data.

        This is a helper method to batch `SingleAgentEpisode.set_observations`.
        For more detail, see `SingleAgentEpisode.set_observations`.

        Args:
            new_data: A dict mapping agent IDs to new observation data.
                Each value in the dict is the new observation data to overwrite existing data with.
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
        for agent_id, new_agent_data in new_data.items():
            if agent_id not in self.agent_episodes:
                raise KeyError(f"AgentID '{agent_id}' not found in this episode.")
            self.agent_episodes[agent_id].set_observations(
                new_data=new_agent_data,
                at_indices=at_indices,
                neg_index_as_lookback=neg_index_as_lookback,
            )

    def set_actions(
        self,
        *,
        new_data: MultiAgentDict,
        at_indices: Optional[Union[int, List[int], slice]] = None,
        neg_index_as_lookback: bool = False,
    ) -> None:
        """Overwrites all or some of this Episode's actions with the provided data.

        This is a helper method to batch `SingleAgentEpisode.set_actions`.
        For more detail, see `SingleAgentEpisode.set_actions`.

        Args:
            new_data: A dict mapping agent IDs to new action data.
                Each value in the dict is the new action data to overwrite existing data with.
                This may be a list of individual action(s) in case this episode
                is still not numpy'ized yet. In case this episode has already been
                numpy'ized, this should be (possibly complex) struct matching the
                action space and with a batch size of its leafs exactly the size
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
                actions = [4, 5, 6,  7, 8, 9], where [4, 5, 6] is the
                lookback buffer range (ts=0 item is 7), will handle a call to
                `set_actions(individual_action, -1,
                neg_index_as_lookback=True)` by overwriting the value of 6 in our
                actions buffer with the provided "individual_action".

        Raises:
            IndexError: If the provided `at_indices` do not match the size of
                `new_data`.
        """
        for agent_id, new_agent_data in new_data.items():
            if agent_id not in self.agent_episodes:
                raise KeyError(f"AgentID '{agent_id}' not found in this episode.")
            self.agent_episodes[agent_id].set_actions(
                new_data=new_agent_data,
                at_indices=at_indices,
                neg_index_as_lookback=neg_index_as_lookback,
            )

    def set_rewards(
        self,
        *,
        new_data: MultiAgentDict,
        at_indices: Optional[Union[int, List[int], slice]] = None,
        neg_index_as_lookback: bool = False,
    ) -> None:
        """Overwrites all or some of this Episode's rewards with the provided data.

        This is a helper method to batch `SingleAgentEpisode.set_rewards`.
        For more detail, see `SingleAgentEpisode.set_rewards`.

        Args:
            new_data: A dict mapping agent IDs to new reward data.
                Each value in the dict is the new reward data to overwrite existing data with.
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
        for agent_id, new_agent_data in new_data.items():
            if agent_id not in self.agent_episodes:
                raise KeyError(f"AgentID '{agent_id}' not found in this episode.")
            self.agent_episodes[agent_id].set_rewards(
                new_data=new_agent_data,
                at_indices=at_indices,
                neg_index_as_lookback=neg_index_as_lookback,
            )

    def set_extra_model_outputs(
        self,
        *,
        key,
        new_data: MultiAgentDict,
        at_indices: Optional[Union[int, List[int], slice]] = None,
        neg_index_as_lookback: bool = False,
    ) -> None:
        """Overwrites all or some of this Episode's extra model outputs with `new_data`.

        This is a helper method to batch `SingleAgentEpisode.set_extra_model_outputs`.
        For more detail, see `SingleAgentEpisode.set_extra_model_outputs`.

        Args:
            key: The `key` within `self.extra_model_outputs` to override data on or
                to insert as a new key into `self.extra_model_outputs`.
            new_data: A dict mapping agent IDs to new extra model outputs data.
                Each value in the dict is the new extra model outputs data to overwrite existing data with.
                This may be a list of individual reward(s) in case this episode
                is still not numpy'ized yet. In case this episode has already been
                numpy'ized, this should be a np.ndarray with a length exactly
                the size of the to-be-overwritten slice or segment (provided by
                `at_indices`).
            at_indices: A single int is interpreted as one index, which to overwrite
                with `new_data` (which is expected to be a single extra model output).
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
                extra_model_outputs[key][agent_id] = [4, 5, 6, 7, 8, 9], where [4, 5, 6] is the
                lookback buffer range (ts=0 item is 7), will handle a call to
                `set_extra_model_outputs(key, individual_output, -1,
                neg_index_as_lookback=True)` by overwriting the value of 6 in our
                extra_model_outputs[key][agent_id] buffer with the provided "individual_output".

        Raises:
            IndexError: If the provided `at_indices` do not match the size of
                `new_data`.
        """
        for agent_id, new_agent_data in new_data.items():
            if agent_id not in self.agent_episodes:
                raise KeyError(f"AgentID '{agent_id}' not found in this episode.")
            self.agent_episodes[agent_id].set_extra_model_outputs(
                key=key,
                new_data=new_agent_data,
                at_indices=at_indices,
                neg_index_as_lookback=neg_index_as_lookback,
            )

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
        return sum(len(eps) for eps in self.agent_episodes.values())

    def __getitem__(self, item: slice) -> "MultiAgentEpisode":
        """Enable squared bracket indexing- and slicing syntax, e.g. episode[-4:]."""
        if isinstance(item, slice):
            return self.slice(slice_=item)
        else:
            raise NotImplementedError(
                f"MultiAgentEpisode does not support getting item '{item}'! "
                "Only slice objects allowed with the syntax: `episode[a:b]`."
            )

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
    ):
        if observations is None:
            return
        if actions is None:
            assert not rewards
            assert not extra_model_outputs
            actions = []
            rewards = []
            extra_model_outputs = []

        # Infos and `extra_model_outputs` are allowed to be None -> Fill them with
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
        len_lookback_buffer_per_agent = defaultdict(lambda: self._len_lookback_buffers)

        all_agent_ids = set(
            agent_episode_ids.keys() if agent_episode_ids is not None else []
        )
        agent_module_ids = agent_module_ids or {}

        # Step through all observations and interpret these as the (global) env steps.
        for data_idx, (obs, inf) in enumerate(zip(observations, infos)):
            # If we do have actions/extra outs/rewards for this timestep, use the data.
            # It may be that these lists have the same length as the observations list,
            # in which case the data will be cached (agent did step/send an action,
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

                # Pull out hanging action (if not first obs for this agent) and
                # complete step for agent.
                if len(observations_per_agent[agent_id]) > 1:
                    actions_per_agent[agent_id].append(
                        self._hanging_actions_end.pop(agent_id)
                    )
                    extra_model_outputs_per_agent[agent_id].append(
                        self._hanging_extra_model_outputs_end.pop(agent_id)
                    )
                    rewards_per_agent[agent_id].append(
                        self._hanging_rewards_end.pop(agent_id)
                    )
                # First obs for this agent. Make sure the agent's mapping is
                # appropriately prepended with self.SKIP_ENV_TS_TAG tags.
                else:
                    if agent_id not in self.env_t_to_agent_t:
                        self.env_t_to_agent_t[agent_id].extend(
                            [self.SKIP_ENV_TS_TAG] * data_idx
                        )
                        len_lookback_buffer_per_agent[agent_id] -= data_idx

                # Agent is still continuing (has an action for the next step).
                if agent_id in act:
                    # Always push actions/extra outputs into cache, then remove them
                    # from there, once the next observation comes in. Same for rewards.
                    self._hanging_actions_end[agent_id] = act[agent_id]
                    self._hanging_extra_model_outputs_end[agent_id] = extra_outs.get(
                        agent_id, {}
                    )
                    self._hanging_rewards_end[agent_id] += rew.get(agent_id, 0.0)
                # Agent is done (has no action for the next step).
                elif terminateds.get(agent_id) or truncateds.get(agent_id):
                    done_per_agent[agent_id] = True
                # There is more (global) action/reward data. This agent must therefore
                # be done. Automatically add it to `done_per_agent` and `terminateds`.
                elif data_idx < len(observations) - 1:
                    done_per_agent[agent_id] = terminateds[agent_id] = True

                # Update env_t_to_agent_t mapping.
                self.env_t_to_agent_t[agent_id].append(
                    len(observations_per_agent[agent_id]) - 1
                )

            # Those agents that did NOT step:
            # - Get self.SKIP_ENV_TS_TAG added to their env_t_to_agent_t mapping.
            # - Get their reward (if any) added up.
            for agent_id in all_agent_ids:
                if agent_id not in obs and agent_id not in done_per_agent:
                    self.env_t_to_agent_t[agent_id].append(self.SKIP_ENV_TS_TAG)
                    # If we are still in the global lookback buffer segment, deduct 1
                    # from this agents' lookback buffer, b/c we don't want the agent
                    # to use this (missing) obs/data in its single-agent lookback.
                    if (
                        len(self.env_t_to_agent_t[agent_id])
                        - self._len_lookback_buffers
                        <= 0
                    ):
                        len_lookback_buffer_per_agent[agent_id] -= 1
                    self._hanging_rewards_end[agent_id] += rew.get(agent_id, 0.0)

        # - Validate per-agent data.
        # - Fix lookback buffers of env_t_to_agent_t mappings.
        for agent_id in list(self.env_t_to_agent_t.keys()):
            # Skip agent if it doesn't seem to have any data.
            if agent_id not in observations_per_agent:
                del self.env_t_to_agent_t[agent_id]
                continue
            assert (
                len(observations_per_agent[agent_id])
                == len(infos_per_agent[agent_id])
                == len(actions_per_agent[agent_id]) + 1
                == len(extra_model_outputs_per_agent[agent_id]) + 1
                == len(rewards_per_agent[agent_id]) + 1
            )
            self.env_t_to_agent_t[agent_id].lookback = self._len_lookback_buffers

        # Now create the individual episodes from the collected per-agent data.
        for agent_id, agent_obs in observations_per_agent.items():
            # If agent only has a single obs AND is already done, remove all its traces
            # from this MultiAgentEpisode.
            if len(agent_obs) == 1 and done_per_agent.get(agent_id):
                self._del_agent(agent_id)
                continue

            # Try to figure out the module ID for this agent.
            # If not provided explicitly by the user that initializes this episode
            # object, try our mapping function.
            module_id = agent_module_ids.get(
                agent_id, self.agent_to_module_mapping_fn(agent_id, self)
            )
            # Create this agent's SingleAgentEpisode.
            sa_episode = SingleAgentEpisode(
                id_=(
                    agent_episode_ids.get(agent_id)
                    if agent_episode_ids is not None
                    else None
                ),
                agent_id=agent_id,
                module_id=module_id,
                multi_agent_episode_id=self.id_,
                observations=agent_obs,
                observation_space=self.observation_space.get(agent_id),
                infos=infos_per_agent[agent_id],
                actions=actions_per_agent[agent_id],
                action_space=self.action_space.get(agent_id),
                rewards=rewards_per_agent[agent_id],
                extra_model_outputs=(
                    {
                        k: [i[k] for i in extra_model_outputs_per_agent[agent_id]]
                        for k in extra_model_outputs_per_agent[agent_id][0].keys()
                    }
                    if extra_model_outputs_per_agent[agent_id]
                    else None
                ),
                terminated=terminateds.get(agent_id, False),
                truncated=truncateds.get(agent_id, False),
                t_started=self.agent_t_started[agent_id],
                len_lookback_buffer=max(len_lookback_buffer_per_agent[agent_id], 0),
            )
            # .. and store it.
            self.agent_episodes[agent_id] = sa_episode

    def _get(
        self,
        *,
        what,
        indices,
        agent_ids=None,
        env_steps=True,
        neg_index_as_lookback=False,
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
            neg_index_as_lookback=neg_index_as_lookback,
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
        neg_index_as_lookback,
        fill,
        one_hot_discrete,
        extra_model_outputs_key,
    ):
        # Return requested data by agent-steps.
        ret = {}
        # For each agent, we retrieve the data through passing the given indices into
        # the SingleAgentEpisode of that agent.
        for agent_id, sa_episode in self.agent_episodes.items():
            if agent_id not in agent_ids:
                continue
            inf_lookback_buffer = getattr(sa_episode, what)
            hanging_val = self._get_hanging_value(what, agent_id)
            # User wants a specific `extra_model_outputs` key.
            if extra_model_outputs_key is not None:
                inf_lookback_buffer = inf_lookback_buffer[extra_model_outputs_key]
                hanging_val = hanging_val[extra_model_outputs_key]
            agent_value = inf_lookback_buffer.get(
                indices=indices,
                neg_index_as_lookback=neg_index_as_lookback,
                fill=fill,
                _add_last_ts_value=hanging_val,
                **one_hot_discrete,
            )
            if agent_value is None or agent_value == []:
                continue
            ret[agent_id] = agent_value
        return ret

    def _get_data_by_env_steps_as_list(
        self,
        *,
        what: str,
        indices: Union[int, slice, List[int]],
        agent_ids: Collection[AgentID],
        neg_index_as_lookback: bool,
        fill: Any,
        one_hot_discrete,
        extra_model_outputs_key: str,
    ) -> List[MultiAgentDict]:
        # Collect indices for each agent first, so we can construct the list in
        # the next step.
        agent_indices = {}
        for agent_id in self.agent_episodes.keys():
            if agent_id not in agent_ids:
                continue
            agent_indices[agent_id] = self.env_t_to_agent_t[agent_id].get(
                indices,
                neg_index_as_lookback=neg_index_as_lookback,
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
                hanging_val = self._get_hanging_value(what, agent_id)
                (
                    inf_lookback_buffer,
                    indices_to_use,
                ) = self._get_inf_lookback_buffer_or_dict(
                    agent_id,
                    what,
                    extra_model_outputs_key,
                    hanging_val,
                    filter_for_skip_indices=idxes[i],
                )
                if (
                    what == "extra_model_outputs"
                    and not inf_lookback_buffer
                    and not hanging_val
                ):
                    continue
                agent_value = self._get_single_agent_data_by_index(
                    what=what,
                    inf_lookback_buffer=inf_lookback_buffer,
                    agent_id=agent_id,
                    index_incl_lookback=indices_to_use,
                    fill=fill,
                    one_hot_discrete=one_hot_discrete,
                    extra_model_outputs_key=extra_model_outputs_key,
                    hanging_val=hanging_val,
                )
                if agent_value is not None:
                    ret2[agent_id] = agent_value
            ret.append(ret2)
        return ret

    def _get_data_by_env_steps(
        self,
        *,
        what: str,
        indices: Union[int, slice, List[int]],
        agent_ids: Collection[AgentID],
        neg_index_as_lookback: bool,
        fill: Any,
        one_hot_discrete: bool,
        extra_model_outputs_key: str,
    ) -> MultiAgentDict:
        ignore_last_ts = what not in ["observations", "infos"]
        ret = {}
        for agent_id, sa_episode in self.agent_episodes.items():
            if agent_id not in agent_ids:
                continue
            hanging_val = self._get_hanging_value(what, agent_id)
            agent_indices = self.env_t_to_agent_t[agent_id].get(
                indices,
                neg_index_as_lookback=neg_index_as_lookback,
                fill=self.SKIP_ENV_TS_TAG if fill is not None else None,
                # For those records where there is no "hanging" last timestep (all
                # other than obs and infos), we have to ignore the last entry in
                # the env_t_to_agent_t mappings.
                _ignore_last_ts=ignore_last_ts,
            )
            inf_lookback_buffer, agent_indices = self._get_inf_lookback_buffer_or_dict(
                agent_id,
                what,
                extra_model_outputs_key,
                hanging_val,
                filter_for_skip_indices=agent_indices,
            )
            if isinstance(agent_indices, list):
                agent_values = self._get_single_agent_data_by_env_step_indices(
                    what=what,
                    agent_id=agent_id,
                    indices_incl_lookback=agent_indices,
                    fill=fill,
                    one_hot_discrete=one_hot_discrete,
                    hanging_val=hanging_val,
                    extra_model_outputs_key=extra_model_outputs_key,
                )
                if len(agent_values) > 0:
                    ret[agent_id] = agent_values
            else:
                agent_values = self._get_single_agent_data_by_index(
                    what=what,
                    inf_lookback_buffer=inf_lookback_buffer,
                    agent_id=agent_id,
                    index_incl_lookback=agent_indices,
                    fill=fill,
                    one_hot_discrete=one_hot_discrete,
                    extra_model_outputs_key=extra_model_outputs_key,
                    hanging_val=hanging_val,
                )
                if agent_values is not None:
                    ret[agent_id] = agent_values
        return ret

    def _get_single_agent_data_by_index(
        self,
        *,
        what: str,
        inf_lookback_buffer: InfiniteLookbackBuffer,
        agent_id: AgentID,
        index_incl_lookback: Union[int, str],
        fill: Any,
        one_hot_discrete: dict,
        extra_model_outputs_key: str,
        hanging_val: Any,
    ) -> Any:
        sa_episode = self.agent_episodes[agent_id]

        if index_incl_lookback == self.SKIP_ENV_TS_TAG:
            # We don't want to fill -> Skip this agent.
            if fill is None:
                return
            # Provide filled value for this agent.
            return getattr(sa_episode, f"get_{what}")(
                indices=1000000000000,
                neg_index_as_lookback=False,
                fill=fill,
                **dict(
                    {}
                    if extra_model_outputs_key is None
                    else {"key": extra_model_outputs_key}
                ),
                **one_hot_discrete,
            )

        # No skip timestep -> Provide value at given index for this agent.

        # Special case: extra_model_outputs and key=None (return all keys as
        # a dict). Note that `inf_lookback_buffer` is NOT an infinite lookback
        # buffer, but a dict mapping keys to individual infinite lookback
        # buffers.
        elif what == "extra_model_outputs" and extra_model_outputs_key is None:
            assert hanging_val is None or isinstance(hanging_val, dict)
            ret = {}
            if inf_lookback_buffer:
                for key, sub_buffer in inf_lookback_buffer.items():
                    ret[key] = sub_buffer.get(
                        indices=index_incl_lookback - sub_buffer.lookback,
                        neg_index_as_lookback=True,
                        fill=fill,
                        _add_last_ts_value=(
                            None if hanging_val is None else hanging_val[key]
                        ),
                        **one_hot_discrete,
                    )
            else:
                for key in hanging_val.keys():
                    ret[key] = InfiniteLookbackBuffer().get(
                        indices=index_incl_lookback,
                        neg_index_as_lookback=True,
                        fill=fill,
                        _add_last_ts_value=hanging_val[key],
                        **one_hot_discrete,
                    )
            return ret

        # Extract data directly from the infinite lookback buffer object.
        else:
            return inf_lookback_buffer.get(
                indices=index_incl_lookback - inf_lookback_buffer.lookback,
                neg_index_as_lookback=True,
                fill=fill,
                _add_last_ts_value=hanging_val,
                **one_hot_discrete,
            )

    def _get_single_agent_data_by_env_step_indices(
        self,
        *,
        what: str,
        agent_id: AgentID,
        indices_incl_lookback: Union[int, str],
        fill: Optional[Any] = None,
        one_hot_discrete: bool = False,
        extra_model_outputs_key: Optional[str] = None,
        hanging_val: Optional[Any] = None,
    ) -> Any:
        """Returns single data item from the episode based on given (env step) indices.

        The returned data item will have a batch size that matches the env timesteps
        defined via `indices_incl_lookback`.

        Args:
            what: A (str) descriptor of what data to collect. Must be one of
                "observations", "infos", "actions", "rewards", or "extra_model_outputs".
            indices_incl_lookback: A list of ints specifying, which indices
                to pull from the InfiniteLookbackBuffer defined by `agent_id` and `what`
                (and maybe `extra_model_outputs_key`). Note that these indices
                disregard the special logic of the lookback buffer. Meaning if one
                index in `indices_incl_lookback` is 0, then the first value in the
                lookback buffer should be returned, not the first value after the
                lookback buffer (which would be normal behavior for pulling items from
                an `InfiniteLookbackBuffer` object).
            agent_id: The individual agent ID to pull data for. Used to lookup the
                `SingleAgentEpisode` object for this agent in `self`.
            fill: An optional float value to use for filling up the returned results at
                the boundaries. This filling only happens if the requested index range's
                start/stop boundaries exceed the buffer's boundaries (including the
                lookback buffer on the left side). This comes in very handy, if users
                don't want to worry about reaching such boundaries and want to zero-pad.
                For example, a buffer with data [10, 11,  12, 13, 14] and lookback
                buffer size of 2 (meaning `10` and `11` are part of the lookback buffer)
                will respond to `indices_incl_lookback=[-1, -2, 0]` and `fill=0.0`
                with `[0.0, 0.0, 10]`.
            one_hot_discrete: If True, will return one-hot vectors (instead of
                int-values) for those sub-components of a (possibly complex) space
                that are Discrete or MultiDiscrete. Note that if `fill=0` and the
                requested `indices_incl_lookback` are out of the range of our data, the
                returned one-hot vectors will actually be zero-hot (all slots zero).
            extra_model_outputs_key: Only if what is "extra_model_outputs", this
                specifies the sub-key (str) inside the extra_model_outputs dict, e.g.
                STATE_OUT or ACTION_DIST_INPUTS.
            hanging_val: In case we are pulling actions, rewards, or extra_model_outputs
                data, there might be information "hanging" (cached). For example,
                if an agent receives an observation o0 and then immediately sends an
                action a0 back, but then does NOT immediately reveive a next
                observation, a0 is now cached (not fully logged yet with this
                episode). The currently cached value must be provided here to be able
                to return it in case the index is -1 (most recent timestep).

        Returns:
            A data item corresponding to the provided args.
        """
        sa_episode = self.agent_episodes[agent_id]

        inf_lookback_buffer = getattr(sa_episode, what)
        if extra_model_outputs_key is not None:
            inf_lookback_buffer = inf_lookback_buffer[extra_model_outputs_key]

        # If there are self.SKIP_ENV_TS_TAG items in `indices_incl_lookback` and user
        # wants to fill these (together with outside-episode-bounds indices) ->
        # Provide these skipped timesteps as filled values.
        if self.SKIP_ENV_TS_TAG in indices_incl_lookback and fill is not None:
            single_fill_value = inf_lookback_buffer.get(
                indices=1000000000000,
                neg_index_as_lookback=False,
                fill=fill,
                **one_hot_discrete,
            )
            ret = []
            for i in indices_incl_lookback:
                if i == self.SKIP_ENV_TS_TAG:
                    ret.append(single_fill_value)
                else:
                    ret.append(
                        inf_lookback_buffer.get(
                            indices=i - getattr(sa_episode, what).lookback,
                            neg_index_as_lookback=True,
                            fill=fill,
                            _add_last_ts_value=hanging_val,
                            **one_hot_discrete,
                        )
                    )
            if self.is_numpy:
                ret = batch(ret)
        else:
            # Filter these indices out up front.
            indices = [
                i - inf_lookback_buffer.lookback
                for i in indices_incl_lookback
                if i != self.SKIP_ENV_TS_TAG
            ]
            ret = inf_lookback_buffer.get(
                indices=indices,
                neg_index_as_lookback=True,
                fill=fill,
                _add_last_ts_value=hanging_val,
                **one_hot_discrete,
            )
        return ret

    def _get_hanging_value(self, what: str, agent_id: AgentID) -> Any:
        """Returns the hanging action/reward/extra_model_outputs for given agent."""
        if what == "actions":
            return self._hanging_actions_end.get(agent_id)
        elif what == "extra_model_outputs":
            return self._hanging_extra_model_outputs_end.get(agent_id)
        elif what == "rewards":
            return self._hanging_rewards_end.get(agent_id)

    def _copy_hanging(self, agent_id: AgentID, other: "MultiAgentEpisode") -> None:
        """Copies hanging action, reward, extra_model_outputs from `other` to `self."""
        if agent_id in other._hanging_rewards_begin:
            self._hanging_rewards_begin[agent_id] = other._hanging_rewards_begin[
                agent_id
            ]
        if agent_id in other._hanging_rewards_end:
            self._hanging_actions_end[agent_id] = copy.deepcopy(
                other._hanging_actions_end[agent_id]
            )
            self._hanging_rewards_end[agent_id] = other._hanging_rewards_end[agent_id]
            self._hanging_extra_model_outputs_end[agent_id] = copy.deepcopy(
                other._hanging_extra_model_outputs_end[agent_id]
            )

    def _del_hanging(self, agent_id: AgentID) -> None:
        """Deletes all hanging action, reward, extra_model_outputs of given agent."""
        self._hanging_rewards_begin.pop(agent_id, None)

        self._hanging_actions_end.pop(agent_id, None)
        self._hanging_extra_model_outputs_end.pop(agent_id, None)
        self._hanging_rewards_end.pop(agent_id, None)

    def _del_agent(self, agent_id: AgentID) -> None:
        """Deletes all data of given agent from this episode."""
        self._del_hanging(agent_id)
        self.agent_episodes.pop(agent_id, None)
        self.agent_ids.discard(agent_id)
        self.env_t_to_agent_t.pop(agent_id, None)
        self._agent_to_module_mapping.pop(agent_id, None)
        self.agent_t_started.pop(agent_id, None)

    def _get_inf_lookback_buffer_or_dict(
        self,
        agent_id: AgentID,
        what: str,
        extra_model_outputs_key: Optional[str] = None,
        hanging_val: Optional[Any] = None,
        filter_for_skip_indices=None,
    ):
        """Returns a single InfiniteLookbackBuffer or a dict of such.

        In case `what` is "extra_model_outputs" AND `extra_model_outputs_key` is None,
        a dict is returned. In all other cases, a single InfiniteLookbackBuffer is
        returned.
        """
        inf_lookback_buffer_or_dict = inf_lookback_buffer = getattr(
            self.agent_episodes[agent_id], what
        )
        if what == "extra_model_outputs":
            if extra_model_outputs_key is not None:
                inf_lookback_buffer = inf_lookback_buffer_or_dict[
                    extra_model_outputs_key
                ]
            elif inf_lookback_buffer_or_dict:
                inf_lookback_buffer = next(iter(inf_lookback_buffer_or_dict.values()))
            elif filter_for_skip_indices is not None:
                return inf_lookback_buffer_or_dict, filter_for_skip_indices
            else:
                return inf_lookback_buffer_or_dict

        if filter_for_skip_indices is not None:
            inf_lookback_buffer_len = (
                len(inf_lookback_buffer)
                + inf_lookback_buffer.lookback
                + (hanging_val is not None)
            )
            ignore_last_ts = what not in ["observations", "infos"]
            if isinstance(filter_for_skip_indices, list):
                filter_for_skip_indices = [
                    "S" if ignore_last_ts and i == inf_lookback_buffer_len else i
                    for i in filter_for_skip_indices
                ]
            elif ignore_last_ts and filter_for_skip_indices == inf_lookback_buffer_len:
                filter_for_skip_indices = "S"
            return inf_lookback_buffer_or_dict, filter_for_skip_indices
        else:
            return inf_lookback_buffer_or_dict

    @Deprecated(new="MultiAgentEpisode.custom_data[some-key] = ...", error=True)
    def add_temporary_timestep_data(self):
        pass

    @Deprecated(new="MultiAgentEpisode.custom_data[some-key]", error=True)
    def get_temporary_timestep_data(self):
        pass
