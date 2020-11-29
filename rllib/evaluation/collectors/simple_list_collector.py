import collections
import logging
import numpy as np
from typing import Any, List, Dict, Tuple, TYPE_CHECKING, Union

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
from ray.util.debug import log_once

_, tf, _ = try_import_tf()
torch, _ = try_import_torch()

if TYPE_CHECKING:
    from ray.rllib.agents.callbacks import DefaultCallbacks

logger = logging.getLogger(__name__)


def to_float_np_array(v: List[Any]) -> np.ndarray:
    if torch and torch.is_tensor(v[0]):
        raise ValueError
    arr = np.array(v)
    if arr.dtype == np.float64:
        return arr.astype(np.float32)  # save some memory
    return arr


class _AgentCollector:
    """Collects samples for one agent in one trajectory (episode).

    The agent may be part of a multi-agent environment. Samples are stored in
    lists including some possible automatic "shift" buffer at the beginning to
    be able to save memory when storing things like NEXT_OBS, PREV_REWARDS,
    etc.., which are specified using the trajectory view API.
    """

    _next_unroll_id = 0  # disambiguates unrolls within a single episode

    def __init__(self, shift_before: int = 0):
        self.shift_before = max(shift_before, 1)
        self.buffers: Dict[str, List] = {}
        # The simple timestep count for this agent. Gets increased by one
        # each time a (non-initial!) observation is added.
        self.count = 0

    def add_init_obs(self, episode_id: EpisodeID, agent_id: AgentID,
                     env_id: EnvID, init_obs: TensorType,
                     view_requirements: Dict[str, ViewRequirement]) -> None:
        """Adds an initial observation (after reset) to the Agent's trajectory.

        Args:
            episode_id (EpisodeID): Unique ID for the episode we are adding the
                initial observation for.
            agent_id (AgentID): Unique ID for the agent we are adding the
                initial observation for.
            env_id (EnvID): The environment index (in a vectorized setup).
            init_obs (TensorType): The initial observation tensor (after
            `env.reset()`).
            view_requirements (Dict[str, ViewRequirements])
        """
        if SampleBatch.OBS not in self.buffers:
            self._build_buffers(
                single_row={
                    SampleBatch.OBS: init_obs,
                    SampleBatch.EPS_ID: episode_id,
                    SampleBatch.AGENT_INDEX: agent_id,
                    "env_id": env_id,
                })
        self.buffers[SampleBatch.OBS].append(init_obs)

    def add_action_reward_next_obs(self, values: Dict[str, TensorType]) -> \
            None:
        """Adds the given dictionary (row) of values to the Agent's trajectory.

        Args:
            values (Dict[str, TensorType]): Data dict (interpreted as a single
                row) to be added to buffer. Must contain keys:
                SampleBatch.ACTIONS, REWARDS, DONES, and NEXT_OBS.
        """

        assert SampleBatch.OBS not in values
        values[SampleBatch.OBS] = values[SampleBatch.NEXT_OBS]
        del values[SampleBatch.NEXT_OBS]

        for k, v in values.items():
            if k not in self.buffers:
                self._build_buffers(single_row=values)
            self.buffers[k].append(v)
        self.count += 1

    def build(self, view_requirements: Dict[str, ViewRequirement]) -> \
            SampleBatch:
        """Builds a SampleBatch from the thus-far collected agent data.

        If the episode/trajectory has no DONE=True at the end, will copy
        the necessary n timesteps at the end of the trajectory back to the
        beginning of the buffers and wait for new samples coming in.
        SampleBatches created by this method will be ready for postprocessing
        by a Policy.

        Args:
            view_requirements (Dict[str, ViewRequirement]: The view
                requirements dict needed to build the SampleBatch from the raw
                buffers (which may have data shifts as well as mappings from
                view-col to data-col in them).
        Returns:
            SampleBatch: The built SampleBatch for this agent, ready to go into
                postprocessing.
        """

        # TODO: measure performance gains when using a UsageTrackingDict
        #  instead of a SampleBatch for postprocessing (this would eliminate
        #  copies (for creating this SampleBatch) of many unused columns for
        #  no reason (not used by postprocessor)).

        batch_data = {}
        np_data = {}
        for view_col, view_req in view_requirements.items():
            # Create the batch of data from the different buffers.
            data_col = view_req.data_col or view_col
            # Some columns don't exist yet (get created during postprocessing).
            # -> skip.
            if data_col not in self.buffers:
                continue
            # OBS are already shifted by -1 (the initial obs starts one ts
            # before all other data columns).
            shift = view_req.shift - \
                (1 if data_col == SampleBatch.OBS else 0)
            if data_col not in np_data:
                np_data[data_col] = to_float_np_array(self.buffers[data_col])
            # Shift is exactly 0: Send trajectory as is.
            if shift == 0:
                data = np_data[data_col][self.shift_before:]
            # Shift is positive: We still need to 0-pad at the end here.
            elif shift > 0:
                data = to_float_np_array(
                    self.buffers[data_col][self.shift_before + shift:] + [
                        np.zeros(
                            shape=view_req.space.shape,
                            dtype=view_req.space.dtype) for _ in range(shift)
                    ])
            # Shift is negative: Shift into the already existing and 0-padded
            # "before" area of our buffers.
            else:
                data = np_data[data_col][self.shift_before + shift:shift]
            if len(data) > 0:
                batch_data[view_col] = data
        batch = SampleBatch(batch_data)

        if SampleBatch.UNROLL_ID not in batch.data:
            # TODO: (sven) Once we have the additional
            #  model.preprocess_train_batch in place (attention net PR), we
            #  should not even need UNROLL_ID anymore:
            #  Add "if SampleBatch.UNROLL_ID in view_requirements:" here.
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
        """Builds the buffers for sample collection, given an example data row.

        Args:
            single_row (Dict[str, TensorType]): A single row (keys=column
                names) of data to base the buffers on.
        """
        for col, data in single_row.items():
            if col in self.buffers:
                continue
            shift = self.shift_before - (1 if col == SampleBatch.OBS else 0)
            # Python primitive or dict (e.g. INFOs).
            if isinstance(data, (int, float, bool, str, dict)):
                self.buffers[col] = [0 for _ in range(shift)]
            # np.ndarray, torch.Tensor, or tf.Tensor.
            else:
                shape = data.shape
                dtype = data.dtype
                if torch and isinstance(data, torch.Tensor):
                    self.buffers[col] = \
                        [torch.zeros(shape, dtype=dtype, device=data.device)
                         for _ in range(shift)]
                elif tf and isinstance(data, tf.Tensor):
                    self.buffers[col] = \
                        [tf.zeros(shape=shape, dtype=dtype)
                         for _ in range(shift)]
                else:
                    self.buffers[col] = \
                        [np.zeros(shape=shape, dtype=dtype)
                         for _ in range(shift)]


class _PolicyCollector:
    """Collects already postprocessed (single agent) samples for one policy.

    Samples come in through already postprocessed SampleBatches, which
    contain single episode/trajectory data for a single agent and are then
    appended to this policy's buffers.
    """

    def __init__(self):
        """Initializes a _PolicyCollector instance."""

        self.buffers: Dict[str, List] = collections.defaultdict(list)
        # The total timestep count for all agents that use this policy.
        # NOTE: This is not an env-step count (across n agents). AgentA and
        # agentB, both using this policy, acting in the same episode and both
        # doing n steps would increase the count by 2*n.
        self.count = 0

    def add_postprocessed_batch_for_training(
            self, batch: SampleBatch,
            view_requirements: Dict[str, ViewRequirement]) -> None:
        """Adds a postprocessed SampleBatch (single agent) to our buffers.

        Args:
            batch (SampleBatch): A single agent (one trajectory) SampleBatch
                to be added to the Policy's buffers.
            view_requirements (Dict[str, ViewRequirement]: The view
                requirements for the policy. This is so we know, whether a
                view-column needs to be copied at all (not needed for
                training).
        """
        for view_col, data in batch.items():
            # Skip columns that are not used for training.
            if view_col not in view_requirements or \
                    not view_requirements[view_col].used_for_training:
                continue
            self.buffers[view_col].extend(data)
        # Add the agent's trajectory length to our count.
        self.count += batch.count

    def build(self):
        """Builds a SampleBatch for this policy from the collected data.

        Also resets all buffers for further sample collection for this policy.

        Returns:
            SampleBatch: The SampleBatch with all thus-far collected data for
                this policy.
        """
        # Create batch from our buffers.
        batch = SampleBatch(self.buffers)
        assert SampleBatch.UNROLL_ID in batch.data
        # Clear buffers for future samples.
        self.buffers.clear()
        # Reset count to 0.
        self.count = 0
        return batch


class _PolicyCollectorGroup:
    def __init__(self, policy_map):
        self.policy_collectors = {
            pid: _PolicyCollector()
            for pid in policy_map.keys()
        }
        self.count = 0


class _SimpleListCollector(_SampleCollector):
    """Util to build SampleBatches for each policy in a multi-agent env.

    Input data is per-agent, while output data is per-policy. There is an M:N
    mapping between agents and policies. We retain one local batch builder
    per agent. When an agent is done, then its local batch is appended into the
    corresponding policy batch for the agent's policy.
    """

    def __init__(self,
                 policy_map: Dict[PolicyID, Policy],
                 clip_rewards: Union[bool, float],
                 callbacks: "DefaultCallbacks",
                 multiple_episodes_in_batch: bool = True,
                 rollout_fragment_length: int = 200):
        """Initializes a _SimpleListCollector instance.

        Args:
            policy_map (Dict[str, Policy]): Maps policy ids to policy
                instances.
            clip_rewards (Union[bool, float]): Whether to clip rewards before
                postprocessing (at +/-1.0) or the actual value to +/- clip.
            callbacks (DefaultCallbacks): RLlib callbacks.
        """

        self.policy_map = policy_map
        self.clip_rewards = clip_rewards
        self.callbacks = callbacks
        self.multiple_episodes_in_batch = multiple_episodes_in_batch
        self.rollout_fragment_length = rollout_fragment_length
        self.large_batch_threshold: int = max(
            1000, rollout_fragment_length *
            10) if rollout_fragment_length != float("inf") else 5000

        # Whenever we observe a new episode+agent, add a new
        # _SingleTrajectoryCollector.
        self.agent_collectors: Dict[Tuple[EpisodeID, AgentID],
                                    _AgentCollector] = {}
        # Internal agent-key-to-policy-id map.
        self.agent_key_to_policy_id = {}
        # Pool of used/unused PolicyCollectorGroups (attached to episodes for
        # across-episode multi-agent sample collection).
        self.policy_collector_groups = []

        # Agents to collect data from for the next forward pass (per policy).
        self.forward_pass_agent_keys = {pid: [] for pid in policy_map.keys()}
        self.forward_pass_size = {pid: 0 for pid in policy_map.keys()}

        # Maps episode ID to the (non-built) env steps taken in this episode.
        self.episode_steps: Dict[EpisodeID, int] = \
            collections.defaultdict(int)
        # Maps episode ID to MultiAgentEpisode.
        self.episodes: Dict[EpisodeID, MultiAgentEpisode] = {}

    @override(_SampleCollector)
    def episode_step(self, episode_id: EpisodeID) -> None:
        episode = self.episodes[episode_id]
        self.episode_steps[episode_id] += 1
        episode.length += 1
        assert episode.batch_builder is not None
        env_steps = episode.batch_builder.count
        num_observations = sum(
            c.count for c in episode.batch_builder.policy_collectors.values())

        if num_observations > self.large_batch_threshold and \
                log_once("large_batch_warning"):
            logger.warning(
                "More than {} observations in {} env steps for "
                "episode {} ".format(num_observations, env_steps, episode_id) +
                "are buffered in the sampler. If this is more than you "
                "expected, check that that you set a horizon on your "
                "environment correctly and that it terminates at some point. "
                "Note: In multi-agent environments, `rollout_fragment_length` "
                "sets the batch size based on (across-agents) environment "
                "steps, not the steps of individual agents, which can result "
                "in unexpectedly large batches." +
                ("Also, you may be waiting for your Env to "
                 "terminate (batch_mode=`complete_episodes`). Make sure it "
                 "does at some point."
                 if not self.multiple_episodes_in_batch else ""))

    @override(_SampleCollector)
    def add_init_obs(self, episode: MultiAgentEpisode, agent_id: AgentID,
                     env_id: EnvID, policy_id: PolicyID,
                     init_obs: TensorType) -> None:
        # Make sure our mappings are up to date.
        agent_key = (episode.episode_id, agent_id)
        if agent_key not in self.agent_key_to_policy_id:
            self.agent_key_to_policy_id[agent_key] = policy_id
        else:
            assert self.agent_key_to_policy_id[agent_key] == policy_id
        policy = self.policy_map[policy_id]
        view_reqs = policy.model.inference_view_requirements if \
            getattr(policy, "model", None) else policy.view_requirements

        # Add initial obs to Trajectory.
        assert agent_key not in self.agent_collectors
        # TODO: determine exact shift-before based on the view-req shifts.
        self.agent_collectors[agent_key] = _AgentCollector()
        self.agent_collectors[agent_key].add_init_obs(
            episode_id=episode.episode_id,
            agent_id=agent_id,
            env_id=env_id,
            init_obs=init_obs,
            view_requirements=view_reqs)

        self.episodes[episode.episode_id] = episode
        if episode.batch_builder is None:
            episode.batch_builder = self.policy_collector_groups.pop() if \
                self.policy_collector_groups else _PolicyCollectorGroup(
                self.policy_map)

        self._add_to_next_inference_call(agent_key)

    @override(_SampleCollector)
    def add_action_reward_next_obs(self, episode_id: EpisodeID,
                                   agent_id: AgentID, env_id: EnvID,
                                   policy_id: PolicyID, agent_done: bool,
                                   values: Dict[str, TensorType]) -> None:
        # Make sure, episode/agent already has some (at least init) data.
        agent_key = (episode_id, agent_id)
        assert self.agent_key_to_policy_id[agent_key] == policy_id
        assert agent_key in self.agent_collectors

        # Include the current agent id for multi-agent algorithms.
        if agent_id != _DUMMY_AGENT_ID:
            values["agent_id"] = agent_id

        # Add action/reward/next-obs (and other data) to Trajectory.
        self.agent_collectors[agent_key].add_action_reward_next_obs(values)

        if not agent_done:
            self._add_to_next_inference_call(agent_key)

    @override(_SampleCollector)
    def total_env_steps(self) -> int:
        return sum(a.count for a in self.agent_collectors.values())

    @override(_SampleCollector)
    def get_inference_input_dict(self, policy_id: PolicyID) -> \
            Dict[str, TensorType]:
        policy = self.policy_map[policy_id]
        keys = self.forward_pass_agent_keys[policy_id]
        buffers = {k: self.agent_collectors[k].buffers for k in keys}
        view_reqs = policy.model.inference_view_requirements if \
            getattr(policy, "model", None) else policy.view_requirements

        input_dict = {}
        for view_col, view_req in view_reqs.items():
            # Create the batch of data from the different buffers.
            data_col = view_req.data_col or view_col
            time_indices = \
                view_req.shift - (
                    1 if data_col in [SampleBatch.OBS, "t", "env_id",
                                      SampleBatch.EPS_ID,
                                      SampleBatch.AGENT_INDEX] else 0)
            data_list = []
            for k in keys:
                if data_col not in buffers[k]:
                    self.agent_collectors[k]._build_buffers({
                        data_col: view_req.space.sample()
                    })
                data_list.append(buffers[k][data_col][time_indices])
            input_dict[view_col] = np.array(data_list)

        self._reset_inference_calls(policy_id)

        return input_dict

    @override(_SampleCollector)
    def postprocess_episode(self,
                            episode: MultiAgentEpisode,
                            is_done: bool = False,
                            check_dones: bool = False,
                            build: bool = False) -> None:
        episode_id = episode.episode_id
        policy_collector_group = episode.batch_builder

        # TODO: (sven) Once we implement multi-agent communication channels,
        #  we have to resolve the restriction of only sending other agent
        #  batches from the same policy to the postprocess methods.
        # Build SampleBatches for the given episode.
        pre_batches = {}
        for (eps_id, agent_id), collector in self.agent_collectors.items():
            # Build only if there is data and agent is part of given episode.
            if collector.count == 0 or eps_id != episode_id:
                continue
            pid = self.agent_key_to_policy_id[(eps_id, agent_id)]
            policy = self.policy_map[pid]
            pre_batch = collector.build(policy.view_requirements)
            pre_batches[agent_id] = (policy, pre_batch)

        # Apply reward clipping before calling postprocessing functions.
        if self.clip_rewards is True:
            for _, (_, pre_batch) in pre_batches.items():
                pre_batch["rewards"] = np.sign(pre_batch["rewards"])
        elif self.clip_rewards:
            for _, (_, pre_batch) in pre_batches.items():
                pre_batch["rewards"] = np.clip(
                    pre_batch["rewards"],
                    a_min=-self.clip_rewards,
                    a_max=self.clip_rewards)

        post_batches = {}
        for agent_id, (_, pre_batch) in pre_batches.items():
            # Entire episode is said to be done.
            # Error if no DONE at end of this agent's trajectory.
            if is_done and check_dones and \
                    not pre_batch[SampleBatch.DONES][-1]:
                raise ValueError(
                    "Episode {} terminated for all agents, but we still don't "
                    "don't have a last observation for agent {} (policy "
                    "{}). ".format(
                        episode_id, agent_id, self.agent_key_to_policy_id[(
                            episode_id, agent_id)]) +
                    "Please ensure that you include the last observations "
                    "of all live agents when setting done[__all__] to "
                    "True. Alternatively, set no_done_at_end=True to "
                    "allow this.")
            # If (only this?) agent is done, erase its buffer entirely.
            if pre_batch[SampleBatch.DONES][-1]:
                del self.agent_collectors[(episode_id, agent_id)]

            other_batches = pre_batches.copy()
            del other_batches[agent_id]
            pid = self.agent_key_to_policy_id[(episode_id, agent_id)]
            policy = self.policy_map[pid]
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

        # Append into policy batches and reset.
        from ray.rllib.evaluation.rollout_worker import get_global_worker
        for agent_id, post_batch in sorted(post_batches.items()):
            pid = self.agent_key_to_policy_id[(episode_id, agent_id)]
            policy = self.policy_map[pid]
            self.callbacks.on_postprocess_trajectory(
                worker=get_global_worker(),
                episode=episode,
                agent_id=agent_id,
                policy_id=pid,
                policies=self.policy_map,
                postprocessed_batch=post_batch,
                original_batches=pre_batches)
            # Add the postprocessed SampleBatch to the policy collectors for
            # training.
            policy_collector_group.policy_collectors[
                pid].add_postprocessed_batch_for_training(
                    post_batch, policy.view_requirements)

        env_steps = self.episode_steps[episode_id]
        policy_collector_group.count += env_steps

        if is_done:
            del self.episode_steps[episode_id]
            del self.episodes[episode_id]
            # Make PolicyCollectorGroup available for more agent batches in
            # other episodes. Do not reset count to 0.
            self.policy_collector_groups.append(policy_collector_group)
        else:
            self.episode_steps[episode_id] = 0

        # Build a MultiAgentBatch from the episode and return.
        if build:
            return self._build_multi_agent_batch(episode)

    def _build_multi_agent_batch(self, episode: MultiAgentEpisode) -> \
            Union[MultiAgentBatch, SampleBatch]:

        ma_batch = {}
        for pid, collector in episode.batch_builder.policy_collectors.items():
            if collector.count > 0:
                ma_batch[pid] = collector.build()
        # Create the batch.
        ma_batch = MultiAgentBatch.wrap_as_needed(
            ma_batch, env_steps=episode.batch_builder.count)

        # PolicyCollectorGroup is empty.
        episode.batch_builder.count = 0

        return ma_batch

    @override(_SampleCollector)
    def try_build_truncated_episode_multi_agent_batch(self) -> \
            List[Union[MultiAgentBatch, SampleBatch]]:
        batches = []
        # Loop through ongoing episodes and see whether their length plus
        # what's already in the policy collectors reaches the fragment-len.
        for episode_id, episode in self.episodes.items():
            env_steps = episode.batch_builder.count + \
                        self.episode_steps[episode_id]
            # Reached the fragment-len -> We should build an MA-Batch.
            if env_steps >= self.rollout_fragment_length:
                assert env_steps == self.rollout_fragment_length
                # If we reached the fragment-len only because of `episode_id`
                # (still ongoing) -> postprocess `episode_id` first.
                if episode.batch_builder.count < self.rollout_fragment_length:
                    self.postprocess_episode(episode, is_done=False)
                # Build the MA-batch and return.
                batch = self._build_multi_agent_batch(episode=episode)
                batches.append(batch)
        return batches

    def _add_to_next_inference_call(
            self, agent_key: Tuple[EpisodeID, AgentID]) -> None:
        """Adds an Agent key (episode+agent IDs) to the next inference call.

        This makes sure that the agent's current data (in the trajectory) is
        used for generating the next input_dict for a
        `Policy.compute_actions()` call.

        Args:
            agent_key (Tuple[EpisodeID, AgentID]: A unique agent key (across
                vectorized environments).
        """
        pid = self.agent_key_to_policy_id[agent_key]
        idx = self.forward_pass_size[pid]
        if idx == 0:
            self.forward_pass_agent_keys[pid].clear()
        self.forward_pass_agent_keys[pid].append(agent_key)
        self.forward_pass_size[pid] += 1

    def _reset_inference_calls(self, policy_id: PolicyID) -> None:
        """Resets internal inference input-dict registries.

        Calling `self.get_inference_input_dict()` after this method is called
        would return an empty input-dict.

        Args:
            policy_id (PolicyID): The policy ID for which to reset the
                inference pointers.
        """
        self.forward_pass_size[policy_id] = 0
