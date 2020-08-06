import logging
from typing import Dict, Optional

from ray.rllib.env.base_env import _DUMMY_AGENT_ID
from ray.rllib.evaluation.episode import MultiAgentEpisode
from ray.rllib.evaluation.per_policy_sample_collector import \
    PerPolicySampleCollector
from ray.rllib.evaluation.sample_collector import _SampleCollector
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils import force_list
from ray.rllib.utils.annotations import override
from ray.rllib.utils.debug import summarize
from ray.rllib.utils.types import AgentID, EnvID, EpisodeID, PolicyID, \
    TensorType
from ray.util.debug import log_once

logger = logging.getLogger(__name__)


class _FastMultiAgentSampleBatchBuilder(_SampleCollector):
    """Builds SampleBatches for each policy (and agent) in a multi-agent env.

    Note: This is an experimental class only used when
    `config._use_trajectory_view_api` = True.
    Once `_use_trajectory_view_api` becomes the default in configs:
    This class will deprecate the `SampleBatchBuilder` class.

    Input data is collected in central per-policy buffers, which
    efficiently pre-allocate memory (over n timesteps) and re-use the same
    memory even for succeeding agents and episodes.
    Input_dicts for action computations, SampleBatches for postprocessing, and
    train_batch dicts are - if possible - created from the central per-policy
    buffers via views to avoid copying of data).
    """

    def __init__(self,
                 policy_map: Dict[PolicyID, Policy],
                 callbacks: "DefaultCallbacks",
                 num_agents: int = 1000,
                 num_timesteps=None,
                 time_major: Optional[bool] = False):
        """Initializes a _FastMultiAgentSampleBatchBuilder object.

        Args:
            policy_map (Dict[PolicyID,Policy]): Maps policy ids to policy
                instances.
            callbacks (DefaultCallbacks): RLlib callbacks.
            num_agents (int): The max number of agent slots to pre-allocate
                in the buffer.
            num_timesteps (int): The max number of timesteps to pre-allocate
                in the buffer.
        """

        self.policy_map = policy_map
        self.callbacks = callbacks
        if num_agents == float("inf") or num_agents is None:
            num_agents = 1000
        self.num_agents = int(num_agents)

        # Collect SampleBatches per-policy in PolicyTrajectories objects.
        self.rollout_sample_collectors = {}
        for pid, policy in policy_map.items():
            # Figure out max-shifts (before and after).
            view_reqs = policy.get_view_requirements()
            max_shift_before = 1
            max_shift_after = 0
            for vr in view_reqs.values():
                shift = force_list(vr.shift)
                if max_shift_before > shift[0]:
                    max_shift_before = shift[0]
                if max_shift_after < shift[-1]:
                    max_shift_after = shift[-1]
            # Figure out num_timesteps and num_agents.
            kwargs = {"time_major": time_major}
            if policy.is_recurrent():
                kwargs["num_timesteps"] = \
                    policy.model.model_config["max_seq_len"]
                kwargs["time_major"] = True
            elif num_timesteps is not None:
                kwargs["num_timesteps"] = num_timesteps

            self.rollout_sample_collectors[pid] = PerPolicySampleCollector(
                num_agents=self.num_agents,
                shift_before=-max_shift_before, shift_after=max_shift_after,
                policy_id=pid,
                **kwargs)

        # Internal agent-to-policy map.
        self.agent_to_policy = {}
        # Number of "inference" steps taken in the environment.
        # Regardless of the number of agents involved in each of these steps.
        self.count = 0

    @override(_SampleCollector)
    def total_env_steps(self) -> int:
        """Returns total number of steps taken in the env (sum of all agents).

        Returns:
            int: The number of steps taken in total in the environment over all
                agents.
        """
        return sum(a.timesteps_since_last_reset
                   for a in self.rollout_sample_collectors.values())

    @override(_SampleCollector)
    def has_non_postprocessed_data(self) -> bool:
        """Returns whether there is pending unprocessed data.

        Returns:
            bool: True if there is at least one per-agent builder (with data
                in it).
        """

        return self.total() > 0

    @override(_SampleCollector)
    def add_init_obs(
            self,
            episode_id: EpisodeID,
            agent_id: AgentID,
            env_id: EnvID,
            policy_id: PolicyID,
            obs: TensorType) -> None:
        """Add the given dictionary (row) of values to this batch.

        Args:
            episode_id (EpisodeID): Unique id for the episode we are adding
                values for.
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

        # Add initial obs to Trajectory.
        self.rollout_sample_collectors[policy_id].add_init_obs(
            episode_id, agent_id, env_id, chunk_num=0, init_obs=obs)

    @override(_SampleCollector)
    def add_action_reward_next_obs(
            self,
            episode_id: EpisodeID,
            agent_id: AgentID,
            env_id: EnvID,
            policy_id: PolicyID,
            agent_done: bool,
            values: Dict[str, TensorType]) -> None:
        """Add the given dictionary (row) of values to this batch.

        Args:
            env_id (EpisodeID): Unique id for the episode we are adding values
                for.
            agent_id (AgentID): Unique id for the agent we are adding
                values for.
            policy_id (PolicyID): Unique id for policy controlling the agent.
            values (Dict[str,TensorType]): Row of values to add for this agent.
        """
        assert policy_id in self.rollout_sample_collectors

        # Make sure our mappings are up to date.
        if agent_id not in self.agent_to_policy:
            self.agent_to_policy[agent_id] = policy_id
        else:
            assert self.agent_to_policy[agent_id] == policy_id

        # Include the current agent id for multi-agent algorithms.
        if agent_id != _DUMMY_AGENT_ID:
            values["agent_id"] = agent_id

        # Add action/reward/next-obs (and other data) to Trajectory.
        self.rollout_sample_collectors[policy_id].add_action_reward_next_obs(
            episode_id, agent_id, env_id, agent_done, values)

    @override(_SampleCollector)
    def postprocess_trajectories_so_far(
            self, episode: Optional[MultiAgentEpisode] = None) -> None:
        """Apply policy postprocessing to any unprocessed data in an episode.

        TODO: docstring

        Args:
            episode (Optional[MultiAgentEpisode]): The Episode object for which
                to post-process data.
        """

        # Loop through each per-policy collector and create a view (for each
        # agent as SampleBatch) from its buffers for post-processing
        all_agent_batches = {}
        for pid, rc in self.rollout_sample_collectors.items():
            policy = self.policy_map[pid]
            model = policy.model
            agent_batches = rc.get_postprocessing_sample_batches(
                model, episode)

            for agent_key, batch in agent_batches.items():
                other_batches = None
                if len(agent_batches) > 1:
                    other_batches = agent_batches.copy()
                    del other_batches[agent_key]

                agent_batches[agent_key] = policy.postprocess_trajectory(
                    batch, other_batches, episode)
                # Call the Policy's Exploration's postprocess method.
                if getattr(policy, "exploration", None) is not None:
                    agent_batches[
                        agent_key] = policy.exploration.postprocess_trajectory(
                            policy, agent_batches[agent_key],
                            getattr(policy, "_sess", None))

                # Add new columns' data to buffers.
                for col in agent_batches[agent_key].new_columns:
                    data = agent_batches[agent_key].data[col]
                    rc._build_buffers({col: data[0]})
                    timesteps = data.shape[0]
                    rc.buffers[col][rc.shift_before:rc.shift_before +
                                    timesteps, rc.agent_key_to_slot[
                                        agent_key]] = data

            all_agent_batches.update(agent_batches)

        if log_once("after_post"):
            logger.info("Trajectory fragment after postprocess_trajectory():"
                        "\n\n{}\n".format(summarize(all_agent_batches)))

        # Append into policy batches and reset
        from ray.rllib.evaluation.rollout_worker import get_global_worker
        for agent_key, batch in sorted(all_agent_batches.items()):
            self.callbacks.on_postprocess_trajectory(
                worker=get_global_worker(),
                episode=episode,
                agent_id=agent_key[0],
                policy_id=self.agent_to_policy[agent_key[0]],
                policies=self.policy_map,
                postprocessed_batch=batch,
                original_batches=None)  # TODO: (sven) do we really need this?

    @override(_SampleCollector)
    def check_missing_dones(self, episode_id: EpisodeID) -> None:
        for pid, rc in self.rollout_sample_collectors.items():
            for agent_key, slot in rc.agent_key_to_slot.items():
                # Only check for given episode.
                # Only check for last chunk (all previous ones are
                # non-terminal).
                if (agent_key[1] == episode_id
                        and rc.agent_key_to_chunk_num[agent_key[:2]] ==
                        agent_key[2]):
                    t = rc.agent_key_to_timestep[agent_key] - 1
                    b = rc.agent_key_to_slot[agent_key]
                    if not rc.buffers["dones"][t][b]:
                        raise ValueError(
                            "Episode {} terminated for all agents, but we "
                            "still don't have a last observation for "
                            "agent {} (policy {}). ".format(agent_key[0], pid)
                            + "Please ensure that you include the last "
                            "observations of all live agents when setting "
                            "'__all__' done to True. Alternatively, set "
                            "no_done_at_end=True to allow this.")

    @override(_SampleCollector)
    def get_multi_agent_batch_and_reset(self):
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

        self.postprocess_trajectories_so_far()
        policy_batches = {}
        for pid, rc in self.rollout_sample_collectors.items():
            policy = self.policy_map[pid]
            model = policy.model
            policy_batches[pid] = rc.get_train_sample_batch_and_reset(model)

        ma_batch = MultiAgentBatch.wrap_as_needed(policy_batches, self.count)
        # Reset our across-all-agents env step count.
        self.count = 0
        return ma_batch
