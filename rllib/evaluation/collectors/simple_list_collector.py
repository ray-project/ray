import collections
import logging
import numpy as np
from typing import List, Any, Dict, Optional, Tuple, TYPE_CHECKING, Union
import time #TODO

from ray.rllib.env.base_env import _DUMMY_AGENT_ID
from ray.rllib.evaluation.collectors.sample_collector import _SampleCollector
from ray.rllib.evaluation.episode import MultiAgentEpisode
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.annotations import override
from ray.rllib.utils.debug import summarize
from ray.rllib.utils.typing import AgentID, EpisodeID, EnvID, PolicyID, \
    TensorType
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.torch_ops import convert_to_non_torch_type
from ray.util.debug import log_once

_, tf, _ = try_import_tf()
torch, _ = try_import_torch()

if TYPE_CHECKING:
    from ray.rllib.agents.callbacks import DefaultCallbacks

logger = logging.getLogger(__name__)


def to_float_np_array(v: List[Any]) -> np.ndarray:
    if torch.is_tensor(v[0]):
        raise ValueError
        v = convert_to_non_torch_type(v)
    arr = np.array(v)
    if arr.dtype == np.float64:
        return arr.astype(np.float32)  # save some memory
    return arr


class _AgentCollector:

    _next_unroll_id = 0  # disambiguates unrolls within a single episode

    def __init__(self, shift_before: int = 0):
        self.buffers: Dict[str, List] = {}
        self.shift_before = max(shift_before, 1)
        self.count = 0

    def add_init_obs(self, init_obs: TensorType) -> None:
        if SampleBatch.OBS not in self.buffers:
            self._build_buffers(
                single_row={SampleBatch.OBS: init_obs,})
        self.buffers[SampleBatch.OBS].append(init_obs)

    def add_action_reward_next_obs(
            self, values: Dict[str, TensorType]) -> None:

        assert SampleBatch.OBS not in values
        values[SampleBatch.OBS] = values[SampleBatch.NEXT_OBS]
        del values[SampleBatch.NEXT_OBS]

        for k, v in values.items():
            if k not in self.buffers:
                self._build_buffers(single_row=values)
            self.buffers[k].append(v)
        self.count += 1

    def build(self, view_reqirements: Dict[str, ViewRequirement]) -> SampleBatch:
        # TODO: measure performance gains when using a UsageTrackingDict
        #  instead of a SampleBatch for postprocessing (this would eliminate
        #  copies (for creating this SampleBatch) of many unused columns for
        #  no reason (not used by postprocessor)).

        batch_data = {}
        np_data = {}
        for view_col, view_req in view_reqirements.items():
            # Create the batch of data from the different buffers.
            data_col = view_req.data_col or view_col
            # Some columns don't exist yet (get created during postprocessing).
            # -> skip.
            if data_col not in self.buffers:
                continue
            shift = view_req.shift - (1 if data_col == SampleBatch.OBS and self.shift_before is not None else 0)
            if data_col not in np_data:
                np_data[data_col] = to_float_np_array(self.buffers[data_col])
            if shift == 0:
                batch_data[view_col] = np_data[data_col][self.shift_before:]
            else:
                batch_data[view_col] = np_data[data_col][
                                       self.shift_before + shift:shift]
        batch = SampleBatch(batch_data)

        if SampleBatch.UNROLL_ID not in batch.data:
            batch.data[SampleBatch.UNROLL_ID] = np.repeat(
                _AgentCollector._next_unroll_id, batch.count)
            _AgentCollector._next_unroll_id += 1

        # This trajectory is continuing -> Copy data at the end (in the size of
        # self.shift_before) to the beginning of buffers and erase everything
        # else.
        if not self.buffers[SampleBatch.DONES][-1]:
            # Copy data to beginning of buffer and cut lists.
            if self.shift_before > 0:
                for k, data in self.buffers.items():
                    self.buffers[k] = data[-self.shift_before:]
            self.count = 0

        return batch

    def _build_buffers(self, single_row: Dict[str, TensorType]) -> None:
        for col, data in single_row.items():
            if col in self.buffers:
                continue
            shift = self.shift_before - (1 if col == SampleBatch.OBS else 0)
            # Python primitive -> np.array.
            if isinstance(data, (int, float, bool)):
                self.buffers[col] = [0 for _ in range(shift)]
            # np.ndarray, torch.Tensor, or tf.Tensor.
            else:
                shape = data.shape
                dtype = data.dtype
                if torch and isinstance(data, torch.Tensor):
                    self.buffers[col] = [torch.zeros(
                        shape, dtype=dtype, device=data.device) for _ in range(shift)]
                elif tf and isinstance(data, tf.Tensor):
                    self.buffers[col] = [tf.zeros(shape=shape, dtype=dtype) for _ in range(shift)]
                else:
                    self.buffers[col] = [np.zeros(shape=shape, dtype=dtype) for _ in range(shift)]


class _PolicyCollector:

    def __init__(self):
        self.buffers: Dict[str, List] = collections.defaultdict(list)
        self.count = 0

    def add_postprocessed_batch_for_training(
            self,
            batch: SampleBatch,
            view_requirements: Dict[str, ViewRequirement]) -> None:
        for view_col, data in batch.items():
            # Skip columns that are not used for training.
            if view_col in view_requirements and \
                    not view_requirements[view_col].used_for_training:
                continue
            self.buffers[view_col].extend(data)
        self.count += batch.count

    def build(self):
        batch = SampleBatch(self.buffers)
        assert SampleBatch.UNROLL_ID in batch.data
        self.buffers.clear()
        self.count = 0
        return batch


class _SimpleListCollector(_SampleCollector):
    """Util to build SampleBatches for each policy in a multi-agent env.

    Input data is per-agent, while output data is per-policy. There is an M:N
    mapping between agents and policies. We retain one local batch builder
    per agent. When an agent is done, then its local batch is appended into the
    corresponding policy batch for the agent's policy.
    """

    def __init__(self, policy_map: Dict[PolicyID, Policy],
                 clip_rewards: Union[bool, float],
                 callbacks: "DefaultCallbacks",
                 multiple_episodes_in_batch: bool = True,
                 rollout_fragment_length: int = 200):
        """Initialize a MultiAgentSampleBatchBuilder.

        Args:
            policy_map (Dict[str, Policy]): Maps policy ids to policy instances.
            clip_rewards (Union[bool, float]): Whether to clip rewards before
                postprocessing (at +/-1.0) or the actual value to +/- clip.
            callbacks (DefaultCallbacks): RLlib callbacks.
        """

        self.policy_map = policy_map
        self.clip_rewards = clip_rewards
        self.callbacks = callbacks
        self.multiple_episodes_in_batch = multiple_episodes_in_batch
        self.rollout_fragment_length = rollout_fragment_length
        self.large_batch_threshold: int = \
            max(1000, rollout_fragment_length * 10) if \
                rollout_fragment_length != float("inf") else 5000

        # Build each Policies' single collector.
        self.policy_collectors = {
            pid: _PolicyCollector() for pid in policy_map.keys()
        }
        self.policy_collectors_env_steps = 0
        # Whenever we observe a new episode+agent, add a new
        # _SingleTrajectoryCollector.
        self.agent_collectors: Dict[
            Tuple[EpisodeID, AgentID], _SingleTrajectoryCollector] = {}
        # Internal agent-to-policy map.
        self.agent_to_policy = {}

        # Agents to collect data from for the next forward pass (per policy).
        self.forward_pass_agent_keys = {pid: [] for pid in policy_map.keys()}
        self.forward_pass_size = {pid: 0 for pid in policy_map.keys()}
        # Maps index from the forward pass batch to (agent_id, episode_id,
        # env_id) tuple.
        self.forward_pass_index_info = {pid: {} for pid in policy_map.keys()}
        self.agent_key_to_forward_pass_index = {}

        # Maps episode ID to _EpisodeRecord objects.
        self.episode_steps: Dict[EpisodeID, int] = collections.defaultdict(int)
        self.episodes: Dict[EpisodeID, MultiAgentEpisode] = {}

    @override(_SampleCollector)
    def episode_step(self, episode_id):
        self.episode_steps[episode_id] += 1

        env_steps = self.policy_collectors_env_steps + self.episode_steps[
                    episode_id]
        if (env_steps > self.large_batch_threshold and
                log_once("large_batch_warning")):
            logger.warning(
                "More than {} observations for {} env steps ".format(
                    env_steps, env_steps) +
                "are buffered in the sampler. If this is more than you "
                "expected, check that that you set a horizon on your "
                "environment correctly and that it terminates at some point. "
                "Note: In multi-agent environments, `rollout_fragment_length` "
                "sets the batch size based on (across-agents) environment "
                "steps, not the steps of individual agents, which can result "
                "in unexpectedly large batches." +
                ("Also, you may be in evaluation waiting for your Env to "
                 "terminate (batch_mode=`complete_episodes`). Make sure it "
                 "does at some point."
                 if not self.multiple_episodes_in_batch else ""))

    @override(_SampleCollector)
    def add_init_obs(self, episode: MultiAgentEpisode, agent_id: AgentID,
                     env_id: EnvID,
                     policy_id: PolicyID,
                     init_obs: TensorType) -> None:
        # Make sure our mappings are up to date.
        agent_key = (episode.episode_id, agent_id)
        if agent_key not in self.agent_to_policy:
            self.agent_to_policy[agent_id] = policy_id
        else:
            assert self.agent_to_policy[agent_id] == policy_id

        # Add initial obs to Trajectory.
        assert agent_key not in self.agent_collectors
        # TODO: determine exact shift-before based on the view-req shifts.
        self.agent_collectors[agent_key] = _AgentCollector()
        self.agent_collectors[agent_key].add_init_obs(init_obs=init_obs)

        self.episodes[episode.episode_id] = episode

        self._add_to_next_inference_call(agent_key, env_id)

    @override(_SampleCollector)
    def add_action_reward_next_obs(self, episode_id: EpisodeID,
                                   agent_id: AgentID, env_id: EnvID,
                                   policy_id: PolicyID, agent_done: bool,
                                   values: Dict[str, TensorType]) -> None:
        # Make sure, episode/agent already has some (at least init) data.
        agent_key = (episode_id, agent_id)
        assert self.agent_to_policy[agent_id] == policy_id
        assert agent_key in self.agent_collectors

        # Include the current agent id for multi-agent algorithms.
        if agent_id != _DUMMY_AGENT_ID:
            values["agent_id"] = agent_id

        # Add action/reward/next-obs (and other data) to Trajectory.
        self.agent_collectors[agent_key].add_action_reward_next_obs(values)

        if agent_done:
            del self.agent_key_to_forward_pass_index[agent_key]
        else:
            self._add_to_next_inference_call(agent_key, env_id)

    @override(_SampleCollector)
    def total_env_steps(self) -> int:
        return sum(a.count for a in self.agent_collectors.values())

    @override(_SampleCollector)
    def get_inference_input_dict(self, policy_id: PolicyID) -> \
            Dict[str, TensorType]:
        policy = self.policy_map[policy_id]
        keys = self.forward_pass_agent_keys[policy_id]
        view_reqs = policy.model.inference_view_requirements
        input_dict = {}
        for view_col, view_req in view_reqs.items():
            # Create the batch of data from the different buffers.
            data_col = view_req.data_col or view_col
            time_indices = view_req.shift - 1
            input_dict[view_col] = np.array(
                [self.agent_collectors[k].buffers[data_col][time_indices]
                 for k in keys])

        self._reset_inference_calls(policy_id)

        return input_dict

    @override(_SampleCollector)
    def has_non_postprocessed_data(self) -> bool:
        return self.total_env_steps() > 0

    @override(_SampleCollector)
    def postprocess_episode(self,
                            episode: MultiAgentEpisode,
                            is_done: bool = False,
                            check_dones: bool = False,
                            perf_stats = None  #TEST
                            ):
                            #cut_at_env_step: Optional[int] = None) -> Optional[MultiAgentBatch]:
        # TODO: (sven) Once we implement multi-agent communication channels,
        #  we have to resolve the restriction of only sending other agent
        #  batches from the same policy to the postprocess methods.
        t = time.time()
        # Build SampleBatches for the given episode.
        pre_batches = {}
        for (episode_id, agent_id), collector in self.agent_collectors.items():
            # Build only the given episode.
            if episode_id != episode.episode_id:
                continue
            policy = self.policy_map[self.agent_to_policy[agent_id]]
            pre_batches[(episode_id, agent_id)] = (
                policy, collector.build(policy.view_requirements))
        t2 = time.time()
        perf_stats._agent_building += t2 - t
        #print("building {} agents took {}sec".format(len(pre_batches), t2 - t))

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

        for (episode_id, agent_id), (_, pre_batch) in pre_batches.items():
            # Entire episode is said to be done.
            if is_done:
                # Error if no DONE at end of this agent's trajectory.
                if check_dones and not pre_batch[SampleBatch.DONES][-1]:
                    raise ValueError(
                        "Episode {} terminated for all agents, but we still "
                        "don't have a last observation for agent {} (policy "
                        "{}). ".format(episode_id, agent_id,
                                       self.agent_to_policy[agent_id]) +
                        "Please ensure that you include the last observations "
                        "of all live agents when setting done[__all__] to "
                        "True. Alternatively, set no_done_at_end=True to "
                        "allow this.")
            # If (only this?) agent is done, erase its buffer entirely.
            if pre_batch[SampleBatch.DONES][-1]:
                del self.agent_collectors[(episode_id, agent_id)]

            other_batches = pre_batches.copy()
            del other_batches[(episode_id, agent_id)]
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
        t3 = time.time()
        perf_stats._postprocessing += t3 - t2

        # Append into policy batches and reset.
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
            # Add the postprocessed SampleBatch to the policy collectors for
            # training.
            t0 = time.time()
            self.policy_collectors[
                self.agent_to_policy[
                    agent_id]].add_postprocessed_batch_for_training(
                post_batch, policy.view_requirements)
            perf_stats._move_to_policy += time.time() - t0

        t1 = time.time()
        perf_stats._postprocess_and_move_to_policy += t1 - t

        env_steps = self.episode_steps[episode.episode_id]
        self.policy_collectors_env_steps += env_steps
        #print("all policy collectors env steps={}".format(self.policy_collectors_env_steps))

        if is_done:
            del self.episode_steps[episode.episode_id]
            del self.episodes[episode.episode_id]
        else:
            self.episode_steps[episode.episode_id] = 0

    def build_ma_batch(self, env_steps, perf_stats):
        t1 = time.time()
        ma_batch = MultiAgentBatch.wrap_as_needed(
            {pid: collector.build()
             for pid, collector in self.policy_collectors.items() if collector.count > 0},
            env_steps=env_steps)
        #print("built batch of env-steps={} and actual pol0-samples={}".format(
        #    env_steps, ma_batch.policy_batches["pol0"]["obs"].shape[0]))
        self.policy_collectors_env_steps = 0
        perf_stats._build_batches += time.time() - t1
        return ma_batch

    @override(_SampleCollector)
    def try_build_truncated_episode_multi_agent_batch(self, perf_stats  #TEST
                                                      ) -> Union[MultiAgentBatch, SampleBatch, None]:
        for episode_id, count in self.episode_steps.items():
            env_steps = self.policy_collectors_env_steps + count
            #print("trying (pol-col total={}) episode{}={}".format(self.policy_collectors_env_steps, episode_id, count))
            if env_steps >= self.rollout_fragment_length:
                t = time.time()
                if self.policy_collectors_env_steps < self.rollout_fragment_length:
                    self.postprocess_episode(
                        self.episodes[episode_id],
                        is_done=False,
                        perf_stats=perf_stats)
                else:
                    env_steps = self.policy_collectors_env_steps
                t1 = time.time()
                perf_stats._postprocess_and_move_to_policy += t1 - t
                ma_batch = self.build_ma_batch(
                    env_steps=env_steps, perf_stats=perf_stats)
                perf_stats._build_batches += time.time() - t1
                return ma_batch
        return None

    def _add_to_next_inference_call(self, agent_key, env_id):
        policy_id = self.agent_to_policy[agent_key[1]]
        idx = self.forward_pass_size[policy_id]
        self.forward_pass_index_info[policy_id][idx] = (agent_key, env_id)
        self.agent_key_to_forward_pass_index[agent_key] = idx
        if idx == 0:
            self.forward_pass_agent_keys[policy_id].clear()
        self.forward_pass_agent_keys[policy_id].append(agent_key)
        self.forward_pass_size[policy_id] += 1

    def _reset_inference_calls(self, policy_id):
        self.forward_pass_size[policy_id] = 0
