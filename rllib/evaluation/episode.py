from collections import defaultdict
import numpy as np
import random
from typing import List, Dict, Callable, Any, TYPE_CHECKING

from ray.rllib.env.base_env import _DUMMY_AGENT_ID
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.spaces.space_utils import flatten_to_single_ndarray
from ray.rllib.utils.typing import SampleBatchType, AgentID, PolicyID, \
    EnvActionType, EnvID, EnvInfoDict, EnvObsType

if TYPE_CHECKING:
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

    def __init__(self, policies: Dict[PolicyID, Policy],
                 policy_mapping_fn: Callable[[AgentID], PolicyID],
                 batch_builder_factory: Callable[
                     [], "MultiAgentSampleBatchBuilder"],
                 extra_batch_callback: Callable[[SampleBatchType], None],
                 env_id: EnvID):
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
        self.agent_rewards: Dict[AgentID, float] = defaultdict(float)
        self.custom_metrics: Dict[str, float] = {}
        self.user_data: Dict[str, Any] = {}
        self.hist_data: Dict[str, List[float]] = {}
        self.media: Dict[str, Any] = {}
        self._policies: Dict[PolicyID, Policy] = policies
        self._policy_mapping_fn: Callable[[AgentID], PolicyID] = \
            policy_mapping_fn
        self._next_agent_index: int = 0
        self._agent_to_index: Dict[AgentID, int] = {}
        self._agent_to_policy: Dict[AgentID, PolicyID] = {}
        self._agent_to_rnn_state: Dict[AgentID, List[Any]] = {}
        self._agent_to_last_obs: Dict[AgentID, EnvObsType] = {}
        self._agent_to_last_raw_obs: Dict[AgentID, EnvObsType] = {}
        self._agent_to_last_info: Dict[AgentID, EnvInfoDict] = {}
        self._agent_to_last_action: Dict[AgentID, EnvActionType] = {}
        self._agent_to_last_pi_info: Dict[AgentID, dict] = {}
        self._agent_to_prev_action: Dict[AgentID, EnvActionType] = {}
        self._agent_reward_history: Dict[AgentID, List[int]] = defaultdict(
            list)

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
        agent to a policy for the duration of the episode.

        Args:
            agent_id (AgentID): The agent ID to lookup the policy ID for.

        Returns:
            PolicyID: The policy ID for the specified agent.
        """

        if agent_id not in self._agent_to_policy:
            self._agent_to_policy[agent_id] = self._policy_mapping_fn(agent_id)
        return self._agent_to_policy[agent_id]

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
            policy = self._policies[self.policy_for(agent_id)]
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
            policy = self._policies[self.policy_for(agent_id)]
            self._agent_to_rnn_state[agent_id] = policy.get_initial_state()
        return self._agent_to_rnn_state[agent_id]

    @DeveloperAPI
    def last_pi_info_for(self, agent_id: AgentID = _DUMMY_AGENT_ID) -> dict:
        """Returns the last info object for the specified agent."""

        return self._agent_to_last_pi_info[agent_id]

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
