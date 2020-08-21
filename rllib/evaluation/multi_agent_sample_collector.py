import logging
from typing import Dict, Optional, TYPE_CHECKING

from ray.rllib.env.base_env import _DUMMY_AGENT_ID
from ray.rllib.evaluation.episode import MultiAgentEpisode
from ray.rllib.evaluation.per_policy_sample_collector import \
    _PerPolicySampleCollector
from ray.rllib.evaluation.sample_collector import _SampleCollector
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils import force_list
from ray.rllib.utils.annotations import override
from ray.rllib.utils.debug import summarize
from ray.rllib.utils.typing import AgentID, EnvID, EpisodeID, PolicyID, \
    TensorType
from ray.util.debug import log_once

if TYPE_CHECKING:
    from ray.rllib.agents.callbacks import DefaultCallbacks

logger = logging.getLogger(__name__)


class _MultiAgentSampleCollector(_SampleCollector):
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

    def __init__(
            self,
            policy_map: Dict[PolicyID, Policy],
            callbacks: "DefaultCallbacks",
            # TODO: (sven) make `num_agents` flexibly grow in size.
            num_agents: int = 100,
            num_timesteps=None,
            time_major: Optional[bool] = False):
        """Initializes a _MultiAgentSampleCollector object.

        Args:
            policy_map (Dict[PolicyID,Policy]): Maps policy ids to policy
                instances.
            callbacks (DefaultCallbacks): RLlib callbacks (configured in the
                Trainer config dict). Used for trajectory postprocessing event.
            num_agents (int): The max number of agent slots to pre-allocate
                in the buffer.
            num_timesteps (int): The max number of timesteps to pre-allocate
                in the buffer.
            time_major (Optional[bool]): Whether to preallocate buffers and
                collect samples in time-major fashion (TxBx...).
        """

        self.policy_map = policy_map
        self.callbacks = callbacks
        if num_agents == float("inf") or num_agents is None:
            num_agents = 1000
        self.num_agents = int(num_agents)

        # Collect SampleBatches per-policy in _PerPolicySampleCollectors.
        self.policy_sample_collectors = {}
        for pid, policy in policy_map.items():
            # Figure out max-shifts (before and after).
            view_reqs = policy.training_view_requirements
            max_shift_before = 0
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
                    policy.config["model"]["max_seq_len"]
                kwargs["time_major"] = True
            elif num_timesteps is not None:
                kwargs["num_timesteps"] = num_timesteps

            self.policy_sample_collectors[pid] = _PerPolicySampleCollector(
                num_agents=self.num_agents,
                shift_before=-max_shift_before,
                shift_after=max_shift_after,
                **kwargs)

        # Internal agent-to-policy map.
        self.agent_to_policy = {}
        # Number of "inference" steps taken in the environment.
        # Regardless of the number of agents involved in each of these steps.
        self.count = 0

    @override(_SampleCollector)
    def add_init_obs(self, episode_id: EpisodeID, agent_id: AgentID,
                     env_id: EnvID, policy_id: PolicyID,
                     obs: TensorType) -> None:
        # Make sure our mappings are up to date.
        if agent_id not in self.agent_to_policy:
            self.agent_to_policy[agent_id] = policy_id
        else:
            assert self.agent_to_policy[agent_id] == policy_id

        # Add initial obs to Trajectory.
        self.policy_sample_collectors[policy_id].add_init_obs(
            episode_id, agent_id, env_id, chunk_num=0, init_obs=obs)

    @override(_SampleCollector)
    def add_action_reward_next_obs(self, episode_id: EpisodeID,
                                   agent_id: AgentID, env_id: EnvID,
                                   policy_id: PolicyID, agent_done: bool,
                                   values: Dict[str, TensorType]) -> None:
        assert policy_id in self.policy_sample_collectors

        # Make sure our mappings are up to date.
        if agent_id not in self.agent_to_policy:
            self.agent_to_policy[agent_id] = policy_id
        else:
            assert self.agent_to_policy[agent_id] == policy_id

        # Include the current agent id for multi-agent algorithms.
        if agent_id != _DUMMY_AGENT_ID:
            values["agent_id"] = agent_id

        # Add action/reward/next-obs (and other data) to Trajectory.
        self.policy_sample_collectors[policy_id].add_action_reward_next_obs(
            episode_id, agent_id, env_id, agent_done, values)

    @override(_SampleCollector)
    def total_env_steps(self) -> int:
        return sum(a.timesteps_since_last_reset
                   for a in self.policy_sample_collectors.values())

    def total(self):
        # TODO: (sven) deprecate; use `self.total_env_steps`, instead.
        #  Sampler is currently still using `total()`.
        return self.total_env_steps()

    @override(_SampleCollector)
    def get_inference_input_dict(self, policy_id: PolicyID) -> \
            Dict[str, TensorType]:
        policy = self.policy_map[policy_id]
        view_reqs = policy.model.inference_view_requirements
        return self.policy_sample_collectors[
            policy_id].get_inference_input_dict(view_reqs)

    @override(_SampleCollector)
    def has_non_postprocessed_data(self) -> bool:
        return self.total_env_steps() > 0

    @override(_SampleCollector)
    def postprocess_trajectories_so_far(
            self, episode: Optional[MultiAgentEpisode] = None) -> None:
        # Loop through each per-policy collector and create a view (for each
        # agent as SampleBatch) from its buffers for post-processing
        all_agent_batches = {}
        for pid, rc in self.policy_sample_collectors.items():
            policy = self.policy_map[pid]
            view_reqs = policy.training_view_requirements
            agent_batches = rc.get_postprocessing_sample_batches(
                episode, view_reqs)

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
        for pid, rc in self.policy_sample_collectors.items():
            for agent_key in rc.agent_key_to_slot.keys():
                # Only check for given episode and only for last chunk
                # (all previous chunks for that agent in the episode are
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
        self.postprocess_trajectories_so_far()
        policy_batches = {}
        for pid, rc in self.policy_sample_collectors.items():
            policy = self.policy_map[pid]
            view_reqs = policy.training_view_requirements
            policy_batches[pid] = rc.get_train_sample_batch_and_reset(
                view_reqs)

        ma_batch = MultiAgentBatch.wrap_as_needed(policy_batches, self.count)
        # Reset our across-all-agents env step count.
        self.count = 0
        return ma_batch
