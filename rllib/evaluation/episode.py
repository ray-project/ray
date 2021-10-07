from collections import defaultdict
import numpy as np
import random
from typing import List, Dict, Callable, Any, TYPE_CHECKING

from ray.rllib.env.base_env import _DUMMY_AGENT_ID
from ray.rllib.policy.policy_map import PolicyMap
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.spaces.space_utils import flatten_to_single_ndarray
from ray.rllib.utils.typing import SampleBatchType, AgentID, PolicyID, \
    EnvActionType, EnvID, EnvInfoDict, EnvObsType
from ray.util import log_once

if TYPE_CHECKING:
    from ray.rllib.evaluation.rollout_worker import RolloutWorker
    from ray.rllib.evaluation.sample_batch_builder import \
        MultiAgentSampleBatchBuilder


@DeveloperAPI
class MultiAgentEpisode:
    """Tracks the current state of a (possibly multi-agent) episode.

    Attributes:
        new_batch_builder (func): Create a new MultiAgentSampleBatchBuilder.
        add_extra_batch (func): Return a built MultiAgentBatch to the sampler.
        batch_builder (obj): Batch builder for the current episode.
        total_reward (float): Summed reward across all agents in this episode.
        length (int): Length of this episode.
        episode_id (int): Unique id identifying this trajectory.
        agent_rewards (dict): Summed rewards broken down by agent.
        custom_metrics (dict): Dict where the you can add custom metrics.
        user_data (dict): Dict that you can use for temporary storage. E.g.
            in between two custom callbacks referring to the same episode.
        hist_data (dict): Dict mapping str keys to List[float] for storage of
            per-timestep float data throughout the episode.

    Use case 1: Model-based rollouts in multi-agent:
        A custom compute_actions() function in a policy can inspect the
        current episode state and perform a number of rollouts based on the
        policies and state of other agents in the environment.

    Use case 2: Returning extra rollouts data.
        The model rollouts can be returned back to the sampler by calling:

        >>> batch = episode.new_batch_builder()
        >>> for each transition:
               batch.add_values(...)  # see sampler for usage
        >>> episode.extra_batches.add(batch.build_and_reset())
    """

    def __init__(
            self,
            policies: PolicyMap,
            policy_mapping_fn: Callable[
                [AgentID, "MultiAgentEpisode", "RolloutWorker"], PolicyID],
            batch_builder_factory: Callable[[],
                                            "MultiAgentSampleBatchBuilder"],
            extra_batch_callback: Callable[[SampleBatchType], None],
            env_id: EnvID,
            *,
            worker: "RolloutWorker" = None,
    ):
        self.new_batch_builder: Callable[
            [], "MultiAgentSampleBatchBuilder"] = batch_builder_factory
        self.add_extra_batch: Callable[[SampleBatchType],
                                       None] = extra_batch_callback
        self.batch_builder: "MultiAgentSampleBatchBuilder" = \
            batch_builder_factory()
        self.total_reward: float = 0.0
        self.length: int = 0
        self.episode_id: int = random.randrange(2e9)
        self.env_id = env_id
        self.worker = worker
        self.agent_rewards: Dict[AgentID, float] = defaultdict(float)
        self.custom_metrics: Dict[str, float] = {}
        self.user_data: Dict[str, Any] = {}
        self.hist_data: Dict[str, List[float]] = {}
        self.media: Dict[str, Any] = {}
        self.policy_map: PolicyMap = policies
        self._policies = self.policy_map  # backward compatibility
        self.policy_mapping_fn: Callable[[
            AgentID, "MultiAgentEpisode", "RolloutWorker"
        ], PolicyID] = policy_mapping_fn
        self._next_agent_index: int = 0
        self._agent_to_index: Dict[AgentID, int] = {}
        self._agent_to_policy: Dict[AgentID, PolicyID] = {}
        self._agent_to_rnn_state: Dict[AgentID, List[Any]] = {}
        self._agent_to_last_obs: Dict[AgentID, EnvObsType] = {}
        self._agent_to_last_raw_obs: Dict[AgentID, EnvObsType] = {}
        self._agent_to_last_done: Dict[AgentID, bool] = {}
        self._agent_to_last_info: Dict[AgentID, EnvInfoDict] = {}
        self._agent_to_last_action: Dict[AgentID, EnvActionType] = {}
        self._agent_to_last_pi_info: Dict[AgentID, dict] = {}
        self._agent_to_prev_action: Dict[AgentID, EnvActionType] = {}
        self._agent_reward_history: Dict[AgentID, List[int]] = defaultdict(
            list)

    # TODO: Deprecated.
    @property
    def _policy_mapping_fn(self):
        deprecation_warning(
            old="MultiAgentEpisode._policy_mapping_fn",
            new="MultiAgentEpisode.policy_mapping_fn",
            error=False,
        )
        return self.policy_mapping_fn

    @DeveloperAPI
    def soft_reset(self) -> None:
        """Clears rewards and metrics, but retains RNN and other state.

        This is used to carry state across multiple logical episodes in the
        same env (i.e., if `soft_horizon` is set).
        """
        self.length = 0
        self.episode_id = random.randrange(2e9)
        self.total_reward = 0.0
        self.agent_rewards = defaultdict(float)
        self._agent_reward_history = defaultdict(list)

    @DeveloperAPI
    def policy_for(self, agent_id: AgentID = _DUMMY_AGENT_ID) -> PolicyID:
        """Returns and stores the policy ID for the specified agent.

        If the agent is new, the policy mapping fn will be called to bind the
        agent to a policy for the duration of the entire episode (even if the
        policy_mapping_fn is changed in the meantime).

        Args:
            agent_id (AgentID): The agent ID to lookup the policy ID for.

        Returns:
            PolicyID: The policy ID for the specified agent.
        """

        if agent_id not in self._agent_to_policy:
            # Try new API: pass in agent_id and episode as named args.
            # New signature should be: (agent_id, episode, worker, **kwargs)
            try:
                policy_id = self._agent_to_policy[agent_id] = \
                    self.policy_mapping_fn(agent_id, self, worker=self.worker)
            except TypeError as e:
                if "positional argument" in e.args[0] or \
                        "unexpected keyword argument" in e.args[0]:
                    if log_once("policy_mapping_new_signature"):
                        deprecation_warning(
                            old="policy_mapping_fn(agent_id)",
                            new="policy_mapping_fn(agent_id, episode, "
                            "worker, **kwargs)")
                    policy_id = self._agent_to_policy[agent_id] = \
                        self.policy_mapping_fn(agent_id)
                else:
                    raise e
        else:
            policy_id = self._agent_to_policy[agent_id]
        if policy_id not in self.policy_map:
            raise KeyError("policy_mapping_fn returned invalid policy id "
                           f"'{policy_id}'!")
        return policy_id

    @DeveloperAPI
    def last_observation_for(
            self, agent_id: AgentID = _DUMMY_AGENT_ID) -> EnvObsType:
        """Returns the last observation for the specified agent."""

        return self._agent_to_last_obs.get(agent_id)

    @DeveloperAPI
    def last_raw_obs_for(self,
                         agent_id: AgentID = _DUMMY_AGENT_ID) -> EnvObsType:
        """Returns the last un-preprocessed obs for the specified agent."""

        return self._agent_to_last_raw_obs.get(agent_id)

    @DeveloperAPI
    def last_info_for(self,
                      agent_id: AgentID = _DUMMY_AGENT_ID) -> EnvInfoDict:
        """Returns the last info for the specified agent."""

        return self._agent_to_last_info.get(agent_id)

    @DeveloperAPI
    def last_action_for(self,
                        agent_id: AgentID = _DUMMY_AGENT_ID) -> EnvActionType:
        """Returns the last action for the specified agent, or zeros."""

        if agent_id in self._agent_to_last_action:
            return flatten_to_single_ndarray(
                self._agent_to_last_action[agent_id])
        else:
            policy_id = self.policy_for(agent_id)
            policy = self.policy_map[policy_id]
            flat = flatten_to_single_ndarray(policy.action_space.sample())
            if hasattr(policy.action_space, "dtype"):
                return np.zeros_like(flat, dtype=policy.action_space.dtype)
            return np.zeros_like(flat)

    @DeveloperAPI
    def prev_action_for(self,
                        agent_id: AgentID = _DUMMY_AGENT_ID) -> EnvActionType:
        """Returns the previous action for the specified agent."""

        if agent_id in self._agent_to_prev_action:
            return flatten_to_single_ndarray(
                self._agent_to_prev_action[agent_id])
        else:
            # We're at t=0, so return all zeros.
            return np.zeros_like(self.last_action_for(agent_id))

    @DeveloperAPI
    def prev_reward_for(self, agent_id: AgentID = _DUMMY_AGENT_ID) -> float:
        """Returns the previous reward for the specified agent."""

        history = self._agent_reward_history[agent_id]
        if len(history) >= 2:
            return history[-2]
        else:
            # We're at t=0, so there is no previous reward, just return zero.
            return 0.0

    @DeveloperAPI
    def rnn_state_for(self, agent_id: AgentID = _DUMMY_AGENT_ID) -> List[Any]:
        """Returns the last RNN state for the specified agent."""

        if agent_id not in self._agent_to_rnn_state:
            policy_id = self.policy_for(agent_id)
            policy = self.policy_map[policy_id]
            self._agent_to_rnn_state[agent_id] = policy.get_initial_state()
        return self._agent_to_rnn_state[agent_id]

    @DeveloperAPI
    def last_done_for(self, agent_id: AgentID = _DUMMY_AGENT_ID) -> bool:
        """Returns the last done flag received for the specified agent."""
        if agent_id not in self._agent_to_last_done:
            self._agent_to_last_done[agent_id] = False
        return self._agent_to_last_done[agent_id]

    @DeveloperAPI
    def last_pi_info_for(self, agent_id: AgentID = _DUMMY_AGENT_ID) -> dict:
        """Returns the last info object for the specified agent."""

        return self._agent_to_last_pi_info[agent_id]

    @DeveloperAPI
    def get_agents(self) -> List[AgentID]:
        """Returns list of agent IDs that have appeared in this episode.

        Returns:
            List[AgentID]: The list of all agents that have appeared so
                far in this episode.
        """
        return list(self._agent_to_index.keys())

    def _add_agent_rewards(self, reward_dict: Dict[AgentID, float]) -> None:
        for agent_id, reward in reward_dict.items():
            if reward is not None:
                self.agent_rewards[agent_id,
                                   self.policy_for(agent_id)] += reward
                self.total_reward += reward
                self._agent_reward_history[agent_id].append(reward)

    def _set_rnn_state(self, agent_id, rnn_state):
        self._agent_to_rnn_state[agent_id] = rnn_state

    def _set_last_observation(self, agent_id, obs):
        self._agent_to_last_obs[agent_id] = obs

    def _set_last_raw_obs(self, agent_id, obs):
        self._agent_to_last_raw_obs[agent_id] = obs

    def _set_last_done(self, agent_id, done):
        self._agent_to_last_done[agent_id] = done

    def _set_last_info(self, agent_id, info):
        self._agent_to_last_info[agent_id] = info

    def _set_last_action(self, agent_id, action):
        if agent_id in self._agent_to_last_action:
            self._agent_to_prev_action[agent_id] = \
                self._agent_to_last_action[agent_id]
        self._agent_to_last_action[agent_id] = action

    def _set_last_pi_info(self, agent_id, pi_info):
        self._agent_to_last_pi_info[agent_id] = pi_info

    def _agent_index(self, agent_id):
        if agent_id not in self._agent_to_index:
            self._agent_to_index[agent_id] = self._next_agent_index
            self._next_agent_index += 1
        return self._agent_to_index[agent_id]
