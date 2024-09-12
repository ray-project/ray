import random
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import tree  # pip install dm_tree

from ray.rllib.env.base_env import _DUMMY_AGENT_ID
from ray.rllib.policy.policy_map import PolicyMap
from ray.rllib.utils.annotations import OldAPIStack
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.spaces.space_utils import flatten_to_single_ndarray
from ray.rllib.utils.typing import (
    AgentID,
    EnvActionType,
    EnvID,
    EnvInfoDict,
    EnvObsType,
    PolicyID,
    SampleBatchType,
)
from ray.util import log_once

if TYPE_CHECKING:
    from ray.rllib.evaluation.rollout_worker import RolloutWorker
    from ray.rllib.evaluation.sample_batch_builder import MultiAgentSampleBatchBuilder


@OldAPIStack
class Episode:
    """Tracks the current state of a (possibly multi-agent) episode.

    Attributes:
        new_batch_builder: Create a new MultiAgentSampleBatchBuilder.
        add_extra_batch: Return a built MultiAgentBatch to the sampler.
        batch_builder: Batch builder for the current episode.
        total_reward: Summed reward across all agents in this episode.
        length: Length of this episode.
        episode_id: Unique id identifying this trajectory.
        agent_rewards: Summed rewards broken down by agent.
        custom_metrics: Dict where the you can add custom metrics.
        user_data: Dict that you can use for temporary storage. E.g.
            in between two custom callbacks referring to the same episode.
        hist_data: Dict mapping str keys to List[float] for storage of
            per-timestep float data throughout the episode.

    Use case 1: Model-based rollouts in multi-agent:
        A custom compute_actions() function in a policy can inspect the
        current episode state and perform a number of rollouts based on the
        policies and state of other agents in the environment.

    Use case 2: Returning extra rollouts data.
        The model rollouts can be returned back to the sampler by calling:

        .. testcode::
            :skipif: True

            batch = episode.new_batch_builder()
            for each transition:
                batch.add_values(...)  # see sampler for usage
            episode.extra_batches.add(batch.build_and_reset())
    """

    def __init__(
        self,
        policies: PolicyMap,
        policy_mapping_fn: Callable[[AgentID, "Episode", "RolloutWorker"], PolicyID],
        batch_builder_factory: Callable[[], "MultiAgentSampleBatchBuilder"],
        extra_batch_callback: Callable[[SampleBatchType], None],
        env_id: EnvID,
        *,
        worker: Optional["RolloutWorker"] = None,
    ):
        """Initializes an Episode instance.

        Args:
            policies: The PolicyMap object (mapping PolicyIDs to Policy
                objects) to use for determining, which policy is used for
                which agent.
            policy_mapping_fn: The mapping function mapping AgentIDs to
                PolicyIDs.
            batch_builder_factory:
            extra_batch_callback:
            env_id: The environment's ID in which this episode runs.
            worker: The RolloutWorker instance, in which this episode runs.
        """
        self.new_batch_builder: Callable[
            [], "MultiAgentSampleBatchBuilder"
        ] = batch_builder_factory
        self.add_extra_batch: Callable[[SampleBatchType], None] = extra_batch_callback
        self.batch_builder: "MultiAgentSampleBatchBuilder" = batch_builder_factory()
        self.total_reward: float = 0.0
        self.length: int = 0
        self.started = False
        self.episode_id: int = random.randrange(int(1e18))
        self.env_id = env_id
        self.worker = worker
        self.agent_rewards: Dict[Tuple[AgentID, PolicyID], float] = defaultdict(float)
        self.custom_metrics: Dict[str, float] = {}
        self.user_data: Dict[str, Any] = {}
        self.hist_data: Dict[str, List[float]] = {}
        self.media: Dict[str, Any] = {}
        self.policy_map: PolicyMap = policies
        self._policies = self.policy_map  # backward compatibility
        self.policy_mapping_fn: Callable[
            [AgentID, "Episode", "RolloutWorker"], PolicyID
        ] = policy_mapping_fn
        self.is_faulty = False
        self._next_agent_index: int = 0
        self._agent_to_index: Dict[AgentID, int] = {}
        self._agent_to_policy: Dict[AgentID, PolicyID] = {}
        self._agent_to_rnn_state: Dict[AgentID, List[Any]] = {}
        self._agent_to_last_obs: Dict[AgentID, EnvObsType] = {}
        self._agent_to_last_raw_obs: Dict[AgentID, EnvObsType] = {}
        self._agent_to_last_terminated: Dict[AgentID, bool] = {}
        self._agent_to_last_truncated: Dict[AgentID, bool] = {}
        self._agent_to_last_info: Dict[AgentID, EnvInfoDict] = {}
        self._agent_to_last_action: Dict[AgentID, EnvActionType] = {}
        self._agent_to_last_extra_action_outs: Dict[AgentID, dict] = {}
        self._agent_to_prev_action: Dict[AgentID, EnvActionType] = {}
        self._agent_reward_history: Dict[AgentID, List[int]] = defaultdict(list)

    def policy_for(self, agent_id: AgentID = _DUMMY_AGENT_ID) -> PolicyID:
        """Returns and stores the policy ID for the specified agent.

        If the agent is new, the policy mapping fn will be called to bind the
        agent to a policy for the duration of the entire episode (even if the
        policy_mapping_fn is changed in the meantime!).

        Args:
            agent_id: The agent ID to lookup the policy ID for.

        Returns:
            The policy ID for the specified agent.
        """

        # Perform a new policy_mapping_fn lookup and bind AgentID for the
        # duration of this episode to the returned PolicyID.
        if agent_id not in self._agent_to_policy:
            # Try new API: pass in agent_id and episode as named args.
            # New signature should be: (agent_id, episode, worker, **kwargs)
            try:
                policy_id = self._agent_to_policy[agent_id] = self.policy_mapping_fn(
                    agent_id, self, worker=self.worker
                )
            except TypeError as e:
                if (
                    "positional argument" in e.args[0]
                    or "unexpected keyword argument" in e.args[0]
                ):
                    if log_once("policy_mapping_new_signature"):
                        deprecation_warning(
                            old="policy_mapping_fn(agent_id)",
                            new="policy_mapping_fn(agent_id, episode, "
                            "worker, **kwargs)",
                        )
                    policy_id = self._agent_to_policy[
                        agent_id
                    ] = self.policy_mapping_fn(agent_id)
                else:
                    raise e
        # Use already determined PolicyID.
        else:
            policy_id = self._agent_to_policy[agent_id]

        # PolicyID not found in policy map -> Error.
        if policy_id not in self.policy_map:
            raise KeyError(
                "policy_mapping_fn returned invalid policy id " f"'{policy_id}'!"
            )
        return policy_id

    def last_observation_for(
        self, agent_id: AgentID = _DUMMY_AGENT_ID
    ) -> Optional[EnvObsType]:
        """Returns the last observation for the specified AgentID.

        Args:
            agent_id: The agent's ID to get the last observation for.

        Returns:
            Last observation the specified AgentID has seen. None in case
            the agent has never made any observations in the episode.
        """

        return self._agent_to_last_obs.get(agent_id)

    def last_raw_obs_for(
        self, agent_id: AgentID = _DUMMY_AGENT_ID
    ) -> Optional[EnvObsType]:
        """Returns the last un-preprocessed obs for the specified AgentID.

        Args:
            agent_id: The agent's ID to get the last un-preprocessed
                observation for.

        Returns:
            Last un-preprocessed observation the specified AgentID has seen.
            None in case the agent has never made any observations in the
            episode.
        """
        return self._agent_to_last_raw_obs.get(agent_id)

    def last_info_for(
        self, agent_id: AgentID = _DUMMY_AGENT_ID
    ) -> Optional[EnvInfoDict]:
        """Returns the last info for the specified AgentID.

        Args:
            agent_id: The agent's ID to get the last info for.

        Returns:
            Last info dict the specified AgentID has seen.
            None in case the agent has never made any observations in the
            episode.
        """
        return self._agent_to_last_info.get(agent_id)

    def last_action_for(self, agent_id: AgentID = _DUMMY_AGENT_ID) -> EnvActionType:
        """Returns the last action for the specified AgentID, or zeros.

        The "last" action is the most recent one taken by the agent.

        Args:
            agent_id: The agent's ID to get the last action for.

        Returns:
            Last action the specified AgentID has executed.
            Zeros in case the agent has never performed any actions in the
            episode.
        """
        policy_id = self.policy_for(agent_id)
        policy = self.policy_map[policy_id]

        # Agent has already taken at least one action in the episode.
        if agent_id in self._agent_to_last_action:
            if policy.config.get("_disable_action_flattening"):
                return self._agent_to_last_action[agent_id]
            else:
                return flatten_to_single_ndarray(self._agent_to_last_action[agent_id])
        # Agent has not acted yet, return all zeros.
        else:
            if policy.config.get("_disable_action_flattening"):
                return tree.map_structure(
                    lambda s: np.zeros_like(s.sample(), s.dtype)
                    if hasattr(s, "dtype")
                    else np.zeros_like(s.sample()),
                    policy.action_space_struct,
                )
            else:
                flat = flatten_to_single_ndarray(policy.action_space.sample())
                if hasattr(policy.action_space, "dtype"):
                    return np.zeros_like(flat, dtype=policy.action_space.dtype)
                return np.zeros_like(flat)

    def prev_action_for(self, agent_id: AgentID = _DUMMY_AGENT_ID) -> EnvActionType:
        """Returns the previous action for the specified agent, or zeros.

        The "previous" action is the one taken one timestep before the
        most recent action taken by the agent.

        Args:
            agent_id: The agent's ID to get the previous action for.

        Returns:
            Previous action the specified AgentID has executed.
            Zero in case the agent has never performed any actions (or only
            one) in the episode.
        """
        policy_id = self.policy_for(agent_id)
        policy = self.policy_map[policy_id]

        # We are at t > 1 -> There has been a previous action by this agent.
        if agent_id in self._agent_to_prev_action:
            if policy.config.get("_disable_action_flattening"):
                return self._agent_to_prev_action[agent_id]
            else:
                return flatten_to_single_ndarray(self._agent_to_prev_action[agent_id])
        # We're at t <= 1, so return all zeros.
        else:
            if policy.config.get("_disable_action_flattening"):
                return tree.map_structure(
                    lambda a: np.zeros_like(a, a.dtype)
                    if hasattr(a, "dtype")  # noqa
                    else np.zeros_like(a),  # noqa
                    self.last_action_for(agent_id),
                )
            else:
                return np.zeros_like(self.last_action_for(agent_id))

    def last_reward_for(self, agent_id: AgentID = _DUMMY_AGENT_ID) -> float:
        """Returns the last reward for the specified agent, or zero.

        The "last" reward is the one received most recently by the agent.

        Args:
            agent_id: The agent's ID to get the last reward for.

        Returns:
            Last reward for the the specified AgentID.
            Zero in case the agent has never performed any actions
            (and thus received rewards) in the episode.
        """

        history = self._agent_reward_history[agent_id]
        # We are at t > 0 -> Return previously received reward.
        if len(history) >= 1:
            return history[-1]
        # We're at t=0, so there is no previous reward, just return zero.
        else:
            return 0.0

    def prev_reward_for(self, agent_id: AgentID = _DUMMY_AGENT_ID) -> float:
        """Returns the previous reward for the specified agent, or zero.

        The "previous" reward is the one received one timestep before the
        most recently received reward of the agent.

        Args:
            agent_id: The agent's ID to get the previous reward for.

        Returns:
            Previous reward for the the specified AgentID.
            Zero in case the agent has never performed any actions (or only
            one) in the episode.
        """

        history = self._agent_reward_history[agent_id]
        # We are at t > 1 -> Return reward prior to most recent (last) one.
        if len(history) >= 2:
            return history[-2]
        # We're at t <= 1, so there is no previous reward, just return zero.
        else:
            return 0.0

    def rnn_state_for(self, agent_id: AgentID = _DUMMY_AGENT_ID) -> List[Any]:
        """Returns the last RNN state for the specified agent.

        Args:
            agent_id: The agent's ID to get the most recent RNN state for.

        Returns:
            Most recent RNN state of the the specified AgentID.
        """

        if agent_id not in self._agent_to_rnn_state:
            policy_id = self.policy_for(agent_id)
            policy = self.policy_map[policy_id]
            self._agent_to_rnn_state[agent_id] = policy.get_initial_state()
        return self._agent_to_rnn_state[agent_id]

    def last_terminated_for(self, agent_id: AgentID = _DUMMY_AGENT_ID) -> bool:
        """Returns the last `terminated` flag for the specified AgentID.

        Args:
            agent_id: The agent's ID to get the last `terminated` flag for.

        Returns:
            Last terminated flag for the specified AgentID.
        """
        if agent_id not in self._agent_to_last_terminated:
            self._agent_to_last_terminated[agent_id] = False
        return self._agent_to_last_terminated[agent_id]

    def last_truncated_for(self, agent_id: AgentID = _DUMMY_AGENT_ID) -> bool:
        """Returns the last `truncated` flag for the specified AgentID.

        Args:
            agent_id: The agent's ID to get the last `truncated` flag for.

        Returns:
            Last truncated flag for the specified AgentID.
        """
        if agent_id not in self._agent_to_last_truncated:
            self._agent_to_last_truncated[agent_id] = False
        return self._agent_to_last_truncated[agent_id]

    def last_extra_action_outs_for(
        self,
        agent_id: AgentID = _DUMMY_AGENT_ID,
    ) -> dict:
        """Returns the last extra-action outputs for the specified agent.

        This data is returned by a call to
        `Policy.compute_actions_from_input_dict` as the 3rd return value
        (1st return value = action; 2nd return value = RNN state outs).

        Args:
            agent_id: The agent's ID to get the last extra-action outs for.

        Returns:
            The last extra-action outs for the specified AgentID.
        """
        return self._agent_to_last_extra_action_outs[agent_id]

    def get_agents(self) -> List[AgentID]:
        """Returns list of agent IDs that have appeared in this episode.

        Returns:
            The list of all agent IDs that have appeared so far in this
            episode.
        """
        return list(self._agent_to_index.keys())

    def _add_agent_rewards(self, reward_dict: Dict[AgentID, float]) -> None:
        for agent_id, reward in reward_dict.items():
            if reward is not None:
                self.agent_rewards[agent_id, self.policy_for(agent_id)] += reward
                self.total_reward += reward
                self._agent_reward_history[agent_id].append(reward)

    def _set_rnn_state(self, agent_id, rnn_state):
        self._agent_to_rnn_state[agent_id] = rnn_state

    def _set_last_observation(self, agent_id, obs):
        self._agent_to_last_obs[agent_id] = obs

    def _set_last_raw_obs(self, agent_id, obs):
        self._agent_to_last_raw_obs[agent_id] = obs

    def _set_last_terminated(self, agent_id, terminated):
        self._agent_to_last_terminated[agent_id] = terminated

    def _set_last_truncated(self, agent_id, truncated):
        self._agent_to_last_truncated[agent_id] = truncated

    def _set_last_info(self, agent_id, info):
        self._agent_to_last_info[agent_id] = info

    def _set_last_action(self, agent_id, action):
        if agent_id in self._agent_to_last_action:
            self._agent_to_prev_action[agent_id] = self._agent_to_last_action[agent_id]
        self._agent_to_last_action[agent_id] = action

    def _set_last_extra_action_outs(self, agent_id, pi_info):
        self._agent_to_last_extra_action_outs[agent_id] = pi_info

    def _agent_index(self, agent_id):
        if agent_id not in self._agent_to_index:
            self._agent_to_index[agent_id] = self._next_agent_index
            self._next_agent_index += 1
        return self._agent_to_index[agent_id]

    @property
    def _policy_mapping_fn(self):
        deprecation_warning(
            old="Episode._policy_mapping_fn",
            new="Episode.policy_mapping_fn",
            error=True,
        )
        return self.policy_mapping_fn
