import random
from collections import defaultdict
import numpy as np
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple

from ray.rllib.env.base_env import _DUMMY_AGENT_ID
from ray.rllib.evaluation.collectors.simple_list_collector import (
    _PolicyCollector,
    _PolicyCollectorGroup,
)
from ray.rllib.evaluation.collectors.agent_collector import AgentCollector
from ray.rllib.policy.policy_map import PolicyMap
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import OldAPIStack
from ray.rllib.utils.typing import AgentID, EnvID, EnvInfoDict, PolicyID, TensorType

if TYPE_CHECKING:
    from ray.rllib.callbacks.callbacks import RLlibCallback
    from ray.rllib.evaluation.rollout_worker import RolloutWorker


@OldAPIStack
class EpisodeV2:
    """Tracks the current state of a (possibly multi-agent) episode."""

    def __init__(
        self,
        env_id: EnvID,
        policies: PolicyMap,
        policy_mapping_fn: Callable[[AgentID, "EpisodeV2", "RolloutWorker"], PolicyID],
        *,
        worker: Optional["RolloutWorker"] = None,
        callbacks: Optional["RLlibCallback"] = None,
    ):
        """Initializes an Episode instance.

        Args:
            env_id: The environment's ID in which this episode runs.
            policies: The PolicyMap object (mapping PolicyIDs to Policy
                objects) to use for determining, which policy is used for
                which agent.
            policy_mapping_fn: The mapping function mapping AgentIDs to
                PolicyIDs.
            worker: The RolloutWorker instance, in which this episode runs.
        """
        # Unique id identifying this trajectory.
        self.episode_id: int = random.randrange(int(1e18))
        # ID of the environment this episode is tracking.
        self.env_id = env_id
        # Summed reward across all agents in this episode.
        self.total_reward: float = 0.0
        # Active (uncollected) # of env steps taken by this episode.
        # Start from -1. After add_init_obs(), we will be at 0 step.
        self.active_env_steps: int = -1
        # Total # of env steps taken by this episode.
        # Start from -1, After add_init_obs(), we will be at 0 step.
        self.total_env_steps: int = -1
        # Active (uncollected) agent steps.
        self.active_agent_steps: int = 0
        # Total # of steps take by all agents in this env.
        self.total_agent_steps: int = 0
        # Dict for user to add custom metrics.
        # TODO (sven): We should probably unify custom_metrics, user_data,
        #  and hist_data into a single data container for user to track per-step.
        # metrics and states.
        self.custom_metrics: Dict[str, float] = {}
        # Temporary storage. E.g. storing data in between two custom
        # callbacks referring to the same episode.
        self.user_data: Dict[str, Any] = {}
        # Dict mapping str keys to List[float] for storage of
        # per-timestep float data throughout the episode.
        self.hist_data: Dict[str, List[float]] = {}
        self.media: Dict[str, Any] = {}

        self.worker = worker
        self.callbacks = callbacks

        self.policy_map: PolicyMap = policies
        self.policy_mapping_fn: Callable[
            [AgentID, "EpisodeV2", "RolloutWorker"], PolicyID
        ] = policy_mapping_fn
        # Per-agent data collectors.
        self._agent_to_policy: Dict[AgentID, PolicyID] = {}
        self._agent_collectors: Dict[AgentID, AgentCollector] = {}

        self._next_agent_index: int = 0
        self._agent_to_index: Dict[AgentID, int] = {}

        # Summed rewards broken down by agent.
        self.agent_rewards: Dict[Tuple[AgentID, PolicyID], float] = defaultdict(float)
        self._agent_reward_history: Dict[AgentID, List[int]] = defaultdict(list)

        self._has_init_obs: Dict[AgentID, bool] = {}
        self._last_terminateds: Dict[AgentID, bool] = {}
        self._last_truncateds: Dict[AgentID, bool] = {}
        # Keep last info dict around, in case an environment tries to signal
        # us something.
        self._last_infos: Dict[AgentID, Dict] = {}

    def policy_for(
        self, agent_id: AgentID = _DUMMY_AGENT_ID, refresh: bool = False
    ) -> PolicyID:
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
        if agent_id not in self._agent_to_policy or refresh:
            policy_id = self._agent_to_policy[agent_id] = self.policy_mapping_fn(
                agent_id,  # agent_id
                self,  # episode
                worker=self.worker,
            )
        # Use already determined PolicyID.
        else:
            policy_id = self._agent_to_policy[agent_id]

        # PolicyID not found in policy map -> Error.
        if policy_id not in self.policy_map:
            raise KeyError(
                "policy_mapping_fn returned invalid policy id " f"'{policy_id}'!"
            )
        return policy_id

    def get_agents(self) -> List[AgentID]:
        """Returns list of agent IDs that have appeared in this episode.

        Returns:
            The list of all agent IDs that have appeared so far in this
            episode.
        """
        return list(self._agent_to_index.keys())

    def agent_index(self, agent_id: AgentID) -> int:
        """Get the index of an agent among its environment.

        A new index will be created if an agent is seen for the first time.

        Args:
            agent_id: ID of an agent.

        Returns:
            The index of this agent.
        """
        if agent_id not in self._agent_to_index:
            self._agent_to_index[agent_id] = self._next_agent_index
            self._next_agent_index += 1
        return self._agent_to_index[agent_id]

    def step(self) -> None:
        """Advance the episode forward by one step."""
        self.active_env_steps += 1
        self.total_env_steps += 1

    def add_init_obs(
        self,
        *,
        agent_id: AgentID,
        init_obs: TensorType,
        init_infos: Dict[str, TensorType],
        t: int = -1,
    ) -> None:
        """Add initial env obs at the start of a new episode

        Args:
            agent_id: Agent ID.
            init_obs: Initial observations.
            init_infos: Initial infos dicts.
            t: timestamp.
        """
        policy = self.policy_map[self.policy_for(agent_id)]

        # Add initial obs to Trajectory.
        assert agent_id not in self._agent_collectors

        self._agent_collectors[agent_id] = AgentCollector(
            policy.view_requirements,
            max_seq_len=policy.config["model"]["max_seq_len"],
            disable_action_flattening=policy.config.get(
                "_disable_action_flattening", False
            ),
            is_policy_recurrent=policy.is_recurrent(),
            intial_states=policy.get_initial_state(),
            _enable_new_api_stack=False,
        )
        self._agent_collectors[agent_id].add_init_obs(
            episode_id=self.episode_id,
            agent_index=self.agent_index(agent_id),
            env_id=self.env_id,
            init_obs=init_obs,
            init_infos=init_infos,
            t=t,
        )

        self._has_init_obs[agent_id] = True

    def add_action_reward_done_next_obs(
        self,
        agent_id: AgentID,
        values: Dict[str, TensorType],
    ) -> None:
        """Add action, reward, info, and next_obs as a new step.

        Args:
            agent_id: Agent ID.
            values: Dict of action, reward, info, and next_obs.
        """
        # Make sure, agent already has some (at least init) data.
        assert agent_id in self._agent_collectors

        self.active_agent_steps += 1
        self.total_agent_steps += 1

        # Include the current agent id for multi-agent algorithms.
        if agent_id != _DUMMY_AGENT_ID:
            values["agent_id"] = agent_id

        # Add action/reward/next-obs (and other data) to Trajectory.
        self._agent_collectors[agent_id].add_action_reward_next_obs(values)

        # Keep track of agent reward history.
        reward = values[SampleBatch.REWARDS]
        self.total_reward += reward
        self.agent_rewards[(agent_id, self.policy_for(agent_id))] += reward
        self._agent_reward_history[agent_id].append(reward)

        # Keep track of last terminated info for agent.
        if SampleBatch.TERMINATEDS in values:
            self._last_terminateds[agent_id] = values[SampleBatch.TERMINATEDS]
        # Keep track of last truncated info for agent.
        if SampleBatch.TRUNCATEDS in values:
            self._last_truncateds[agent_id] = values[SampleBatch.TRUNCATEDS]

        # Keep track of last info dict if available.
        if SampleBatch.INFOS in values:
            self.set_last_info(agent_id, values[SampleBatch.INFOS])

    def postprocess_episode(
        self,
        batch_builder: _PolicyCollectorGroup,
        is_done: bool = False,
        check_dones: bool = False,
    ) -> None:
        """Build and return currently collected training samples by policies.

        Clear agent collector states if this episode is done.

        Args:
            batch_builder: _PolicyCollectorGroup for saving the collected per-agent
                sample batches.
            is_done: If this episode is done (terminated or truncated).
            check_dones: Whether to make sure per-agent trajectories are actually done.
        """
        # TODO: (sven) Once we implement multi-agent communication channels,
        #  we have to resolve the restriction of only sending other agent
        #  batches from the same policy to the postprocess methods.
        # Build SampleBatches for the given episode.
        pre_batches = {}
        for agent_id, collector in self._agent_collectors.items():
            # Build only if there is data and agent is part of given episode.
            if collector.agent_steps == 0:
                continue
            pid = self.policy_for(agent_id)
            policy = self.policy_map[pid]
            pre_batch = collector.build_for_training(policy.view_requirements)
            pre_batches[agent_id] = (pid, policy, pre_batch)

        for agent_id, (pid, policy, pre_batch) in pre_batches.items():
            # Entire episode is said to be done.
            # Error if no DONE at end of this agent's trajectory.
            if is_done and check_dones and not pre_batch.is_terminated_or_truncated():
                raise ValueError(
                    "Episode {} terminated for all agents, but we still "
                    "don't have a last observation for agent {} (policy "
                    "{}). ".format(self.episode_id, agent_id, self.policy_for(agent_id))
                    + "Please ensure that you include the last observations "
                    "of all live agents when setting done[__all__] to "
                    "True."
                )

            # Skip a trajectory's postprocessing (and thus using it for training),
            # if its agent's info exists and contains the training_enabled=False
            # setting (used by our PolicyClients).
            if not self._last_infos.get(agent_id, {}).get("training_enabled", True):
                continue

            if (
                not pre_batch.is_single_trajectory()
                or len(np.unique(pre_batch[SampleBatch.EPS_ID])) > 1
            ):
                raise ValueError(
                    "Batches sent to postprocessing must only contain steps "
                    "from a single trajectory.",
                    pre_batch,
                )

            if len(pre_batches) > 1:
                other_batches = pre_batches.copy()
                del other_batches[agent_id]
            else:
                other_batches = {}

            # Call the Policy's Exploration's postprocess method.
            post_batch = pre_batch
            if getattr(policy, "exploration", None) is not None:
                policy.exploration.postprocess_trajectory(
                    policy, post_batch, policy.get_session()
                )
            post_batch.set_get_interceptor(None)
            post_batch = policy.postprocess_trajectory(post_batch, other_batches, self)

            from ray.rllib.evaluation.rollout_worker import get_global_worker

            self.callbacks.on_postprocess_trajectory(
                worker=get_global_worker(),
                episode=self,
                agent_id=agent_id,
                policy_id=pid,
                policies=self.policy_map,
                postprocessed_batch=post_batch,
                original_batches=pre_batches,
            )

            # Append post_batch for return.
            if pid not in batch_builder.policy_collectors:
                batch_builder.policy_collectors[pid] = _PolicyCollector(policy)
            batch_builder.policy_collectors[pid].add_postprocessed_batch_for_training(
                post_batch, policy.view_requirements
            )

        batch_builder.agent_steps += self.active_agent_steps
        batch_builder.env_steps += self.active_env_steps

        # AgentCollector cleared.
        self.active_agent_steps = 0
        self.active_env_steps = 0

    def has_init_obs(self, agent_id: AgentID = None) -> bool:
        """Returns whether this episode has initial obs for an agent.

        If agent_id is None, return whether we have received any initial obs,
        in other words, whether this episode is completely fresh.
        """
        if agent_id is not None:
            return agent_id in self._has_init_obs and self._has_init_obs[agent_id]
        else:
            return any(list(self._has_init_obs.values()))

    def is_done(self, agent_id: AgentID) -> bool:
        return self.is_terminated(agent_id) or self.is_truncated(agent_id)

    def is_terminated(self, agent_id: AgentID) -> bool:
        return self._last_terminateds.get(agent_id, False)

    def is_truncated(self, agent_id: AgentID) -> bool:
        return self._last_truncateds.get(agent_id, False)

    def set_last_info(self, agent_id: AgentID, info: Dict):
        self._last_infos[agent_id] = info

    def last_info_for(
        self, agent_id: AgentID = _DUMMY_AGENT_ID
    ) -> Optional[EnvInfoDict]:
        return self._last_infos.get(agent_id)

    @property
    def length(self):
        return self.total_env_steps
