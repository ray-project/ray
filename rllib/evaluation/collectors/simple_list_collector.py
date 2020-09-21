import collections
import logging
import numpy as np
from typing import List, Any, Dict, Optional, TYPE_CHECKING

from ray.rllib.env.base_env import _DUMMY_AGENT_ID
from ray.rllib.evaluation.collectors.sample_collector import _SampleCollector
from ray.rllib.evaluation.episode import MultiAgentEpisode
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.utils.debug import summarize
from ray.rllib.utils.typing import PolicyID, AgentID
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.util.debug import log_once

_, tf, _ = try_import_tf()
torch, _ = try_import_torch()

if TYPE_CHECKING:
    from ray.rllib.agents.callbacks import DefaultCallbacks

logger = logging.getLogger(__name__)


def to_float_array(v: List[Any]) -> np.ndarray:
    arr = np.array(v)
    if arr.dtype == np.float64:
        return arr.astype(np.float32)  # save some memory
    return arr


class _SingleTrajectoryBuilder:

    _next_unroll_id = 0  # disambiguates unrolls within a single episode

    def __init__(self, shift_before: int = 0, shift_after: int = 0):
        self.buffers: Dict[str, List] = {}
        self.shift_before = max(shift_before, 1)
        self.shift_after = shift_after
        self.count = 0

    def add_init_obs(self, #episode_id: EpisodeID, agent_id: AgentID,
                     #env_id: EnvID, chunk_num: int,
                     init_obs: TensorType) -> None:
        if SampleBatch.OBS not in self.buffers:
            self._build_buffers(
                single_row={
                    SampleBatch.OBS: init_obs,
                    #SampleBatch.EPS_ID: episode_id,
                    #SampleBatch.AGENT_INDEX: agent_id,
                    #"env_id": env_id,
                })
        self.buffers[SampleBatch.OBS].append(init_obs)

    def add_action_reward_next_obs(
            self, #episode_id: EpisodeID, agent_id: AgentID, env_id: EnvID,
            #agent_done: bool,
            values: Dict[str, TensorType]) -> None:

        for k, v in values.items():
            if k not in self.buffers:
                self._build_buffers(single_row=values)
            self.buffers[k].append(v)
        self.count += 1

        #if not agent_done:
        #    self._add_to_next_inference_call(agent_key, env_id, agent_slot, ts)

    def add_batch(self, batch: SampleBatch) -> None:
        for k, column in batch.items():
            if k not in self.buffers:
                self._build_buffers(single_row=column[0])
            self.buffers[k].extend(column)
        self.count += batch.count

    def build_and_reset(self) -> SampleBatch:
        # TODO: measure performance gains when using a UsageTrackingDict
        #  instead of a SampleBatch for postprocessing (this would eliminate
        #  copies (for creating this SampleBatch) of many unused columns for
        #  no reason (not used by postprocessor)).
        batch = SampleBatch(
            {k: to_float_array(v)
             for k, v in self.buffers.items()})
        if SampleBatch.UNROLL_ID not in batch.data:
            batch.data[SampleBatch.UNROLL_ID] = np.repeat(
                _SingleTrajectoryBuilder._next_unroll_id, batch.count)
            _SingleTrajectoryBuilder._next_unroll_id += 1
        self.buffers.clear()
        self.count = 0
        return batch

    def _build_buffers(self, single_row: Dict[str, TensorType]) -> None:
        #time_size = self.num_timesteps + self.shift_before + self.shift_after
        for col, data in single_row.items():
            if col in self.buffers:
                continue
            shift = 1 if col == SampleBatch.OBS else 0
            #base_shape = (time_size, self.num_agents) if self.time_major else \
            #    (self.num_agents, time_size)
            # Python primitive -> np.array.
            if isinstance(data, (int, float, bool)):
                #t_ = type(data)
                #dtype = np.float32 if t_ == float else \
                #    np.int32 if type(data) == int else np.bool_
                self.buffers[col] = [0 for _ in range(shift)] # np.zeros(shape=base_shape, dtype=dtype)
            # np.ndarray, torch.Tensor, or tf.Tensor.
            else:
                shape = data.shape
                dtype = data.dtype
                if torch and isinstance(data, torch.Tensor):
                    self.buffers[col] = [torch.zeros(
                        *shape, dtype=dtype, device=data.device) for _ in range(shift)]
                elif tf and isinstance(data, tf.Tensor):
                    self.buffers[col] = [tf.zeros(shape=shape, dtype=dtype) for _ in range(shift)]
                else:
                    self.buffers[col] = [np.zeros(shape=shape, dtype=dtype) for _ in range(shift)]


class _SimpleListCollector(_SampleCollector):
    """Util to build SampleBatches for each policy in a multi-agent env.

    Input data is per-agent, while output data is per-policy. There is an M:N
    mapping between agents and policies. We retain one local batch builder
    per agent. When an agent is done, then its local batch is appended into the
    corresponding policy batch for the agent's policy.
    """

    def __init__(self, policy_map: Dict[PolicyID, Policy],
                 clip_rewards: Union[bool, float],
                 callbacks: "DefaultCallbacks"):
        """Initialize a MultiAgentSampleBatchBuilder.

        Args:
            policy_map (Dict[str, Policy]): Maps policy ids to policy instances.
            clip_rewards (Union[bool, float]): Whether to clip rewards before
                postprocessing (at +/-1.0) or the actual value to +/- clip.
            callbacks (DefaultCallbacks): RLlib callbacks.
        """

        self.policy_map = policy_map
        self.clip_rewards = clip_rewards
        # Build the Policies' SampleBatchBuilders.
        self.policy_collectors = {
            pid: _SingleTrajectoryBuilder()
            for pid in policy_map.keys()
        }
        # Whenever we observe a new agent, add a new SampleBatchBuilder for
        # this agent.
        self.agent_builders = {}
        # Internal agent-to-policy map.
        self.agent_to_policy = {}
        self.callbacks = callbacks
        # Number of "inference" steps taken in the environment.
        # Regardless of the number of agents involved in each of these steps.
        self.count = 0

        # Agents to collect data from for the next forward pass (per policy).
        self.forward_pass_indices = {pid: [] for pid in policy_map.keys()}
        self.forward_pass_size = {pid: 0 for pid in policy_map.keys()}
        # Maps index from the forward pass batch to (agent_id, episode_id,
        # env_id) tuple.
        self.forward_pass_index_to_agent = {}
        self.agent_to_forward_pass_index = {}

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
        self.policy_collectors[policy_id].add_init_obs(init_obs=obs)

        self._add_to_next_inference_call(agent_id)

    @override(_SampleCollector)
    def add_action_reward_next_obs(self, episode_id: EpisodeID,
                                   agent_id: AgentID, env_id: EnvID,
                                   policy_id: PolicyID, agent_done: bool,
                                   values: Dict[str, TensorType]) -> None:
        assert policy_id in self.policy_collectors

        # Make sure our mappings are up to date.
        if agent_id not in self.agent_to_policy:
            self.agent_to_policy[agent_id] = policy_id
        else:
            assert self.agent_to_policy[agent_id] == policy_id

        # Include the current agent id for multi-agent algorithms.
        if agent_id != _DUMMY_AGENT_ID:
            values["agent_id"] = agent_id

        # Add action/reward/next-obs (and other data) to Trajectory.
        self.policy_collectors[policy_id].add_action_reward_next_obs(values)

    @override(_SampleCollector)
    def total_env_steps(self) -> int:
        return sum(a.count for a in self.agent_builders.values())

    @override(_SampleCollector)
    def get_inference_input_dict(self, policy_id: PolicyID) -> \
            Dict[str, TensorType]:
        policy = self.policy_map[policy_id]
        indices = self.forward_pass_indices[policy_id]
        view_reqs = policy.model.inference_view_requirements
        #return self.policy_collectors[
        #    policy_id].get_inference_input_dict(view_reqs)
        input_dict = {}
        for view_col, view_req in view_reqs.items():
            # Create the batch of data from the different buffers.
            data_col = view_req.data_col or view_col
            #if data_col not in self.buffers:
            #    self._build_buffers({data_col: view_req.space.sample()})

            #if self.time_major:
            #    input_dict[view_col] = self.buffers[data_col][indices]
            #else:
            #if isinstance(view_req.shift, (list, tuple)):
            #    time_indices = \
            #        np.array(view_req.shift) + np.array(indices[0])
            #    input_dict[view_col] = self.buffers[data_col][indices[1],
            #                                                  time_indices]
            #else:
            input_dict[view_col] = \
                self.buffers[data_col][indices[1], indices[0]]

        self._reset_inference_calls()

        return input_dict

    @override(_SampleCollector)
    def has_non_postprocessed_data(self) -> bool:
        return self.total_env_steps() > 0

    @override(_SampleCollector)
    def postprocess_trajectories_so_far(
            self, episode: Optional[MultiAgentEpisode] = None) -> None:
        """Apply policy postprocessors to any unprocessed rows.

        This pushes the postprocessed per-agent batches onto the per-policy
        builders, clearing per-agent state.

        Args:
            episode (Optional[MultiAgentEpisode]): The Episode object that
                holds this MultiAgentBatchBuilder object.
        """

        # Materialize the batches so far.
        pre_batches = {}
        for agent_id, builder in self.agent_builders.items():
            pre_batches[agent_id] = (
                self.policy_map[self.agent_to_policy[agent_id]],
                builder.build_and_reset())

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
            other_batches = pre_batches.copy()
            del other_batches[agent_id]
            policy = self.policy_map[self.agent_to_policy[agent_id]]
            if any(pre_batch["dones"][:-1]) or len(set(
                    pre_batch["eps_id"])) > 1:
                raise ValueError(
                    "Batches sent to postprocessing must only contain steps "
                    "from a single trajectory.", pre_batch)
            # Call the Policy's Exploration's postprocess method.
            post_batches[agent_id] = pre_batch
            if getattr(policy, "exploration", None) is not None:
                policy.exploration.postprocess_trajectory(
                    policy, post_batches[agent_id],
                    getattr(policy, "_sess", None))
            post_batches[agent_id] = policy.postprocess_trajectory(
                post_batches[agent_id], other_batches, episode)

        if log_once("after_post"):
            logger.info(
                "Trajectory fragment after postprocess_trajectory():\n\n{}\n".
                format(summarize(post_batches)))

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
            self.policy_collectors[self.agent_to_policy[agent_id]].add_batch(
                post_batch)

        self.agent_builders.clear()
        self.agent_to_policy.clear()

    @override(_SampleCollector)
    def check_missing_dones(self) -> None:
        for agent_id, builder in self.agent_builders.items():
            if builder.buffers["dones"][-1] is not True:
                raise ValueError(
                    "The environment terminated for all agents, but we still "
                    "don't have a last observation for "
                    "agent {} (policy {}). ".format(
                        agent_id, self.agent_to_policy[agent_id]) +
                    "Please ensure that you include the last observations "
                    "of all live agents when setting '__all__' done to True. "
                    "Alternatively, set no_done_at_end=True to allow this.")

    @override(_SampleCollector)
    def try_build(self, episode: Optional[MultiAgentEpisode] = None
                        ) -> MultiAgentBatch:
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

        self.postprocess_trajectories_so_far(episode)
        policy_batches = {}
        for policy_id, builder in self.policy_collectors.items():
            if builder.count > 0:
                policy_batches[policy_id] = builder.build_and_reset()
        old_count = self.count
        self.count = 0
        return MultiAgentBatch.wrap_as_needed(policy_batches, old_count)

    def _add_to_next_inference_call(self, agent_id):
        policy_id = self.agent_to_policy[agent_id]
        idx = self.forward_pass_size[policy_id]
        self.forward_pass_index_to_agent[idx] = agent_id
        self.agent_to_forward_pass_index[agent_id] = idx
        if idx == 0:
            self.forward_pass_indices[0].clear()
            self.forward_pass_indices[1].clear()
        self.forward_pass_indices[policy_id].append(agent_id)
        self.forward_pass_size[policy_id] += 1

    def _reset_inference_calls(self):
        for pid in self.forward_pass_size.keys():
            self.forward_pass_size[pid] = 0
