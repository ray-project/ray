import logging
import numpy as np
from typing import Dict, Optional, Union

from ray.rllib.agents.callbacks import DefaultCallbacks
from ray.rllib.env.base_env import _DUMMY_AGENT_ID
from ray.rllib.evaluation.episode import MultiAgentEpisode
from ray.rllib.evaluation.policy_trajectories import PolicyTrajectories
from ray.rllib.evaluation.trajectory import Trajectory
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.debug import summarize
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.types import AgentID, EnvID, PolicyID, TensorType
from ray.util.debug import log_once

logger = logging.getLogger(__name__)


class _FastMultiAgentSampleBatchBuilder:
    """Builds SampleBatches for each policy (and agent) in a multi-agent env.

    Note: This is an experimental class only used when
    `config._use_trajectory_view_api` = True.
    Once `_use_trajectory_view_api` becomes the default in configs:
    This class will deprecate the `SampleBatchBuilder` class.

    Input data is collected in `Trajectory` objects (per-agent), which
    efficiently preallocate memory over n timesteps and re-use the same memory
    even for succeeding agents and episodes.
    Once an agent is done (episode terminated for this agent or the Sampler hit
    the `rollout_fragment_len` limit), the agent's Trajectory is appended
    (as a SampleBatch object) to a corresponding `PolicyTrajectories` object,
    which holds all data for one Policy (note that M agents map to N Policies,
    where M >= N).
    """

    def __init__(self, policy_map: Dict[PolicyID, Policy],
                 clip_rewards: Union[bool, float],
                 callbacks: DefaultCallbacks,
                 buffer_size: Optional[Union[float, int]]):
        """Initializes a _FastMultiAgentSampleBatchBuilder object.

        Args:
            policy_map (Dict[PolicyID,Policy]): Maps policy ids to policy
                instances.
            clip_rewards (Union[bool,float]): Whether to clip rewards before
                postprocessing (at +/-1.0) or the actual value to +/- clip.
            callbacks (DefaultCallbacks): RLlib callbacks.
            buffer_size (Optional[Union[int,float]]): The max number of
                timesteps to fit into one buffer column.
        """

        self.policy_map = policy_map
        self.clip_rewards = clip_rewards
        self.callbacks = callbacks
        if buffer_size == float("inf") or buffer_size is None:
            buffer_size = 1000
        self.buffer_size = int(buffer_size)

        # Build the Policies' SampleBatchBuilders.
        self.policy_trajectories = {
            k: PolicyTrajectories(buffer_size=self.buffer_size)
            for k in policy_map.keys()
        }
        # Whenever we observe a new agent, add a new SampleBatchBuilder for
        # this agent.
        self.single_agent_trajectories = {}
        # Internal agent-to-policy map.
        self.agent_to_policy = {}
        # Number of "inference" steps taken in the environment.
        # Regardless of the number of agents involved in each of these steps.
        self.count = 0

    def total(self) -> int:
        """Returns total number of steps taken in the env (sum of all agents).

        Returns:
            int: The number of steps taken in total in the environment over all
                agents.
        """

        return sum(a.timestep for a in self.single_agent_trajectories.values())

    def has_pending_agent_data(self) -> bool:
        """Returns whether there is pending unprocessed data.

        Returns:
            bool: True if there is at least one per-agent builder (with data
                in it).
        """

        return self.total() > 0

    def add_init_obs(self,
                     env_id: EnvID,
                     agent_id: AgentID,
                     policy_id: PolicyID,
                     obs: TensorType) -> None:
        """Add the given dictionary (row) of values to this batch.

        Arguments:
            env_id (EnvID): Unique id for the episode we are adding values for.
            agent_id (AgentID): Unique id for the agent we are adding
                values for.
            policy_id (PolicyID): Unique id for policy controlling the agent.
            obs (TensorType): Initial observation (after env.reset()).
        """
        # Make sure our mappings are up to date.
        if agent_id not in self.agent_to_policy:
            self.agent_to_policy[agent_id] = policy_id
        else:
            assert self.agent_to_policy[agent_id] == policy_id

        # We don't have a Trajcetory for this agent ID yet, create a new one.
        if agent_id not in self.single_agent_trajectories:
            self.single_agent_trajectories[agent_id] = Trajectory(
                buffer_size=self.buffer_size)
        # Add initial obs to Trajectory.
        self.single_agent_trajectories[agent_id].add_init_obs(
            env_id, agent_id, policy_id, obs)

    def add_action_reward_next_obs(self,
                                   env_id: EnvID,
                                   agent_id: AgentID,
                                   policy_id: PolicyID,
                                   **values: Dict[str, TensorType]) -> None:
        """Add the given dictionary (row) of values to this batch.

        Args:
            env_id (EnvID): Unique id for the episode we are adding values for.
            agent_id (AgentID): Unique id for the agent we are adding
                values for.
            policy_id (PolicyID): Unique id for policy controlling the agent.
            values (Dict[str,TensorType]): Row of values to add for this agent.
        """
        assert agent_id in self.single_agent_trajectories

        # Make sure our mappings are up to date.
        if agent_id not in self.agent_to_policy:
            self.agent_to_policy[agent_id] = policy_id
        else:
            assert self.agent_to_policy[agent_id] == policy_id

        # Include the current agent id for multi-agent algorithms.
        if agent_id != _DUMMY_AGENT_ID:
            values["agent_id"] = agent_id

        # Add action/reward/next-obs (and other data) to Trajectory.
        self.single_agent_trajectories[agent_id].add_action_reward_next_obs(
            env_id, agent_id, policy_id, values)

    def postprocess_batch_so_far(
        self,
        episode: Optional[MultiAgentEpisode] = None) -> None:
        """Apply policy postprocessors to any unprocessed rows.

        This pushes the postprocessed per-agent data into the
        per-policy PolicyTrajectories and clears per-agent state so far
        (only leaving any currently ongoing trajectories still available
        for (backward) view-generation).

        Args:
            episode (Optional[MultiAgentEpisode]): The Episode object that
                holds this _FastMultiAgentBatchBuilder object.
        """

        # Materialize the per-agent batches so far.
        pre_batches = {}
        for agent_id, trajectory in self.single_agent_trajectories.items():
            # Only if this trajectory has any data.
            if trajectory.timestep > 0:
                pre_batches[agent_id] = (
                    self.policy_map[self.agent_to_policy[agent_id]],
                    trajectory.get_sample_batch_and_reset())

        # Apply postprocessor.
        post_batches = {}
        if self.clip_rewards is True:
            for _, (_, pre_batch) in pre_batches.items():
                pre_batch["rewards"] = np.sign(pre_batch["rewards"])
        elif self.clip_rewards:
            for _, (_, pre_batch) in pre_batches.items():
                pre_batch["rewards"] = np.clip(
                    pre_batch["rewards"],
                    a_min=-self.clip_rewards,
                    a_max=self.clip_rewards)
        for agent_id, (_, pre_batch) in pre_batches.items():
            if any(pre_batch["dones"][:-1]) or len(set(
                    pre_batch["eps_id"])) > 1:
                raise ValueError(
                    "Batches sent to postprocessing must only contain steps "
                    "from a single episode!", pre_batch)

            other_batches = None
            if len(pre_batches) > 1:
                other_batches = pre_batches.copy()
                del other_batches[agent_id]

            policy = self.policy_map[self.agent_to_policy[agent_id]]
            post_batches[agent_id] = policy.postprocess_trajectory(
                pre_batch, other_batches, episode)
            post_batches[agent_id].last_obs = pre_batch.last_obs
            # Call the Policy's Exploration's postprocess method.
            if getattr(policy, "exploration", None) is not None:
                policy.exploration.postprocess_trajectory(
                    policy, post_batches[agent_id],
                    getattr(policy, "_sess", None))

        if log_once("after_post"):
            logger.info(
                "Trajectory fragment after postprocess_trajectory():"
                "\n\n{}\n".format(summarize(post_batches)))

        # Append into policy batches and reset
        from ray.rllib.evaluation.rollout_worker import get_global_worker
        for agent_id, post_batch in sorted(post_batches.items()):
            self.callbacks.on_postprocess_trajectory(
                worker=get_global_worker(),
                episode=episode,
                agent_id=agent_id,
                policy_id=self.agent_to_policy[agent_id],
                policies=self.policy_map,
                postprocessed_batch=post_batch,
                original_batches=pre_batches)
            self.policy_trajectories[self.agent_to_policy[
                agent_id]].add_sample_batch(post_batch)

    def check_missing_dones(self) -> None:
        for agent_id, trajectory in self.single_agent_trajectories.items():
            if not trajectory.buffers["dones"][trajectory.cursor - 1]:
                raise ValueError(
                    "The environment terminated for all agents, but we still "
                    "don't have a last observation for "
                    "agent {} (policy {}). ".format(
                        agent_id, self.agent_to_policy[agent_id]) +
                    "Please ensure that you include the last observations "
                    "of all live agents when setting '__all__' done to True. "
                    "Alternatively, set no_done_at_end=True to allow this.")

    def get_multi_agent_batch_and_reset(
        self,
        episode: Optional[MultiAgentEpisode] = None) -> MultiAgentBatch:
        """Returns the accumulated sample batches for each policy.

        Any unprocessed rows will be first postprocessed with a policy
        postprocessor. The internal state of this builder will be reset.

        Args:
            episode (Optional[MultiAgentEpisode]): The Episode object that
                holds this MultiAgentBatchBuilder object or None.

        Returns:
            MultiAgentBatch: Returns the accumulated sample batches for each
                policy.
        """

        self.postprocess_batch_so_far(episode)
        policy_batches = {}
        for policy_id, trajectories in self.policy_trajectories.items():
            if trajectories.cursor > 0:
                policy_batches[
                    policy_id] = trajectories.get_sample_batch_and_reset()
        old_count = self.count
        self.count = 0
        return MultiAgentBatch.wrap_as_needed(policy_batches, old_count)

    def build_and_reset(self, episode=None):
        deprecation_warning(
            "get_multi_agent_batch_and_reset", "build_and_reset", error=False)
        return self.get_multi_agent_batch_and_reset(episode)
