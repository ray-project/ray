import collections
from gym.spaces import Space
import logging
import math
import numpy as np
import tree  # pip install dm_tree
from typing import Any, Dict, List, Tuple, TYPE_CHECKING, Union

from ray.rllib.env.base_env import _DUMMY_AGENT_ID
from ray.rllib.evaluation.collectors.sample_collector import SampleCollector
from ray.rllib.evaluation.episode import Episode
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.policy_map import PolicyMap
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.debug import summarize
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.spaces.space_utils import get_dummy_batch_for_space
from ray.rllib.utils.typing import (
    AgentID,
    EpisodeID,
    EnvID,
    PolicyID,
    TensorType,
    ViewRequirementsDict,
)
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

    def __init__(self, view_reqs, policy):
        self.policy = policy
        # Determine the size of the buffer we need for data before the actual
        # episode starts. This is used for 0-buffering of e.g. prev-actions,
        # or internal state inputs.
        self.shift_before = -min(
            (int(vr.shift.split(":")[0]) if isinstance(vr.shift, str) else vr.shift)
            - (1 if vr.data_col == SampleBatch.OBS or k == SampleBatch.OBS else 0)
            for k, vr in view_reqs.items()
        )

        # The actual data buffers. Keys are column names, values are lists
        # that contain the sub-components (e.g. for complex obs spaces) with
        # each sub-component holding a list of per-timestep tensors.
        # E.g.: obs-space = Dict(a=Discrete(2), b=Box((2,)))
        # buffers["obs"] = [
        #    [0, 1],  # <- 1st sub-component of observation
        #    [np.array([.2, .3]), np.array([.0, -.2])]  # <- 2nd sub-component
        # ]
        # NOTE: infos and state_out_... are not flattened due to them often
        # using custom dict values whose structure may vary from timestep to
        # timestep.
        self.buffers: Dict[str, List[List[TensorType]]] = {}
        # Maps column names to an example data item, which may be deeply
        # nested. These are used such that we'll know how to unflatten
        # the flattened data inside self.buffers when building the
        # SampleBatch.
        self.buffer_structs: Dict[str, Any] = {}
        # The episode ID for the agent for which we collect data.
        self.episode_id = None
        # The unroll ID, unique across all rollouts (within a RolloutWorker).
        self.unroll_id = None
        # The simple timestep count for this agent. Gets increased by one
        # each time a (non-initial!) observation is added.
        self.agent_steps = 0

    def add_init_obs(
        self,
        episode_id: EpisodeID,
        agent_index: int,
        env_id: EnvID,
        t: int,
        init_obs: TensorType,
    ) -> None:
        """Adds an initial observation (after reset) to the Agent's trajectory.

        Args:
            episode_id (EpisodeID): Unique ID for the episode we are adding the
                initial observation for.
            agent_index (int): Unique int index (starting from 0) for the agent
                within its episode. Not to be confused with AGENT_ID (Any).
            env_id (EnvID): The environment index (in a vectorized setup).
            t (int): The time step (episode length - 1). The initial obs has
                ts=-1(!), then an action/reward/next-obs at t=0, etc..
            init_obs (TensorType): The initial observation tensor (after
            `env.reset()`).
        """
        # Store episode ID + unroll ID, which will be constant throughout this
        # AgentCollector's lifecycle.
        self.episode_id = episode_id
        if self.unroll_id is None:
            self.unroll_id = _AgentCollector._next_unroll_id
            _AgentCollector._next_unroll_id += 1

        if SampleBatch.OBS not in self.buffers:
            self._build_buffers(
                single_row={
                    SampleBatch.OBS: init_obs,
                    SampleBatch.AGENT_INDEX: agent_index,
                    SampleBatch.ENV_ID: env_id,
                    SampleBatch.T: t,
                    SampleBatch.EPS_ID: self.episode_id,
                    SampleBatch.UNROLL_ID: self.unroll_id,
                }
            )

        # Append data to existing buffers.
        flattened = tree.flatten(init_obs)
        for i, sub_obs in enumerate(flattened):
            self.buffers[SampleBatch.OBS][i].append(sub_obs)
        self.buffers[SampleBatch.AGENT_INDEX][0].append(agent_index)
        self.buffers[SampleBatch.ENV_ID][0].append(env_id)
        self.buffers[SampleBatch.T][0].append(t)
        self.buffers[SampleBatch.EPS_ID][0].append(self.episode_id)
        self.buffers[SampleBatch.UNROLL_ID][0].append(self.unroll_id)

    def add_action_reward_next_obs(self, values: Dict[str, TensorType]) -> None:
        """Adds the given dictionary (row) of values to the Agent's trajectory.

        Args:
            values (Dict[str, TensorType]): Data dict (interpreted as a single
                row) to be added to buffer. Must contain keys:
                SampleBatch.ACTIONS, REWARDS, DONES, and NEXT_OBS.
        """
        if self.unroll_id is None:
            self.unroll_id = _AgentCollector._next_unroll_id
            _AgentCollector._next_unroll_id += 1

        # Next obs -> obs.
        assert SampleBatch.OBS not in values
        values[SampleBatch.OBS] = values[SampleBatch.NEXT_OBS]
        del values[SampleBatch.NEXT_OBS]

        # Make sure EPS_ID/UNROLL_ID stay the same for this agent.
        if SampleBatch.EPS_ID in values:
            assert values[SampleBatch.EPS_ID] == self.episode_id
            del values[SampleBatch.EPS_ID]
        self.buffers[SampleBatch.EPS_ID][0].append(self.episode_id)
        if SampleBatch.UNROLL_ID in values:
            assert values[SampleBatch.UNROLL_ID] == self.unroll_id
            del values[SampleBatch.UNROLL_ID]
        self.buffers[SampleBatch.UNROLL_ID][0].append(self.unroll_id)

        for k, v in values.items():
            if k not in self.buffers:
                self._build_buffers(single_row=values)
            # Do not flatten infos, state_out_ and (if configured) actions.
            # Infos/state-outs may be structs that change from timestep to
            # timestep.
            if (
                k == SampleBatch.INFOS
                or k.startswith("state_out_")
                or (
                    k == SampleBatch.ACTIONS
                    and not self.policy.config.get("_disable_action_flattening")
                )
            ):
                self.buffers[k][0].append(v)
            # Flatten all other columns.
            else:
                flattened = tree.flatten(v)
                for i, sub_list in enumerate(self.buffers[k]):
                    sub_list.append(flattened[i])
        self.agent_steps += 1

    def build(self, view_requirements: ViewRequirementsDict) -> SampleBatch:
        """Builds a SampleBatch from the thus-far collected agent data.

        If the episode/trajectory has no DONE=True at the end, will copy
        the necessary n timesteps at the end of the trajectory back to the
        beginning of the buffers and wait for new samples coming in.
        SampleBatches created by this method will be ready for postprocessing
        by a Policy.

        Args:
            view_requirements (ViewRequirementsDict): The view
                requirements dict needed to build the SampleBatch from the raw
                buffers (which may have data shifts as well as mappings from
                view-col to data-col in them).

        Returns:
            SampleBatch: The built SampleBatch for this agent, ready to go into
                postprocessing.
        """

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
            obs_shift = -1 if data_col == SampleBatch.OBS else 0

            # Keep an np-array cache so we don't have to regenerate the
            # np-array for different view_cols using to the same data_col.
            if data_col not in np_data:
                np_data[data_col] = [
                    to_float_np_array(d) for d in self.buffers[data_col]
                ]

            # Range of indices on time-axis, e.g. "-50:-1". Together with
            # the `batch_repeat_value`, this determines the data produced.
            # Example:
            #  batch_repeat_value=10, shift_from=-3, shift_to=-1
            #  buffer=[-3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
            #  resulting data=[[-3, -2, -1], [7, 8, 9]]
            #  Range of 3 consecutive items repeats every 10 timesteps.
            if view_req.shift_from is not None:
                # Batch repeat value > 1: Only repeat the shift_from/to range
                # every n timesteps.
                if view_req.batch_repeat_value > 1:
                    count = int(
                        math.ceil(
                            (len(np_data[data_col][0]) - self.shift_before)
                            / view_req.batch_repeat_value
                        )
                    )
                    data = [
                        np.asarray(
                            [
                                d[
                                    self.shift_before
                                    + (i * view_req.batch_repeat_value)
                                    + view_req.shift_from
                                    + obs_shift : self.shift_before
                                    + (i * view_req.batch_repeat_value)
                                    + view_req.shift_to
                                    + 1
                                    + obs_shift
                                ]
                                for i in range(count)
                            ]
                        )
                        for d in np_data[data_col]
                    ]
                # Batch repeat value = 1: Repeat the shift_from/to range at
                # each timestep.
                else:
                    d0 = np_data[data_col][0]
                    shift_win = view_req.shift_to - view_req.shift_from + 1
                    data_size = d0.itemsize * int(np.product(d0.shape[1:]))
                    strides = [
                        d0.itemsize * int(np.product(d0.shape[i + 1 :]))
                        for i in range(1, len(d0.shape))
                    ]
                    start = (
                        self.shift_before
                        - shift_win
                        + 1
                        + obs_shift
                        + view_req.shift_to
                    )
                    data = [
                        np.lib.stride_tricks.as_strided(
                            d[start : start + self.agent_steps],
                            [self.agent_steps, shift_win]
                            + [d.shape[i] for i in range(1, len(d.shape))],
                            [data_size, data_size] + strides,
                        )
                        for d in np_data[data_col]
                    ]
            # Set of (probably non-consecutive) indices.
            # Example:
            #  shift=[-3, 0]
            #  buffer=[-3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
            #  resulting data=[[-3, 0], [-2, 1], [-1, 2], [0, 3], [1, 4], ...]
            elif isinstance(view_req.shift, np.ndarray):
                data = [
                    d[self.shift_before + obs_shift + view_req.shift]
                    for d in np_data[data_col]
                ]
            # Single shift int value. Use the trajectory as-is, and if
            # `shift` != 0: shifted by that value.
            else:
                shift = view_req.shift + obs_shift

                # Batch repeat (only provide a value every n timesteps).
                if view_req.batch_repeat_value > 1:
                    count = int(
                        math.ceil(
                            (len(np_data[data_col][0]) - self.shift_before)
                            / view_req.batch_repeat_value
                        )
                    )
                    data = [
                        np.asarray(
                            [
                                d[
                                    self.shift_before
                                    + (i * view_req.batch_repeat_value)
                                    + shift
                                ]
                                for i in range(count)
                            ]
                        )
                        for d in np_data[data_col]
                    ]
                # Shift is exactly 0: Use trajectory as is.
                elif shift == 0:
                    data = [d[self.shift_before :] for d in np_data[data_col]]
                # Shift is positive: We still need to 0-pad at the end.
                elif shift > 0:
                    data = [
                        to_float_np_array(
                            np.concatenate(
                                [
                                    d[self.shift_before + shift :],
                                    [
                                        np.zeros(
                                            shape=view_req.space.shape,
                                            dtype=view_req.space.dtype,
                                        )
                                        for _ in range(shift)
                                    ],
                                ]
                            )
                        )
                        for d in np_data[data_col]
                    ]
                # Shift is negative: Shift into the already existing and
                # 0-padded "before" area of our buffers.
                else:
                    data = [
                        d[self.shift_before + shift : shift] for d in np_data[data_col]
                    ]

            if len(data) > 0:
                if data_col not in self.buffer_structs:
                    batch_data[view_col] = data[0]
                else:
                    batch_data[view_col] = tree.unflatten_as(
                        self.buffer_structs[data_col], data
                    )

        # Due to possible batch-repeats > 1, columns in the resulting batch
        # may not all have the same batch size.
        batch = SampleBatch(batch_data)

        # Adjust the seq-lens array depending on the incoming agent sequences.
        if self.policy.is_recurrent():
            seq_lens = []
            max_seq_len = self.policy.config["model"]["max_seq_len"]
            count = batch.count
            while count > 0:
                seq_lens.append(min(count, max_seq_len))
                count -= max_seq_len
            batch["seq_lens"] = np.array(seq_lens)
            batch.max_seq_len = max_seq_len

        # This trajectory is continuing -> Copy data at the end (in the size of
        # self.shift_before) to the beginning of buffers and erase everything
        # else.
        if not self.buffers[SampleBatch.DONES][0][-1]:
            # Copy data to beginning of buffer and cut lists.
            if self.shift_before > 0:
                for k, data in self.buffers.items():
                    # Loop through
                    for i in range(len(data)):
                        self.buffers[k][i] = data[i][-self.shift_before :]
            self.agent_steps = 0

        # Reset our unroll_id.
        self.unroll_id = None

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

            shift = self.shift_before - (
                1
                if col
                in [
                    SampleBatch.OBS,
                    SampleBatch.EPS_ID,
                    SampleBatch.AGENT_INDEX,
                    SampleBatch.ENV_ID,
                    SampleBatch.T,
                    SampleBatch.UNROLL_ID,
                ]
                else 0
            )

            # Store all data as flattened lists, except INFOS and state-out
            # lists. These are monolithic items (infos is a dict that
            # should not be further split, same for state-out items, which
            # could be custom dicts as well).
            if (
                col == SampleBatch.INFOS
                or col.startswith("state_out_")
                or (
                    col == SampleBatch.ACTIONS
                    and not self.policy.config.get("_disable_action_flattening")
                )
            ):
                self.buffers[col] = [[data for _ in range(shift)]]
            else:
                self.buffers[col] = [
                    [v for _ in range(shift)] for v in tree.flatten(data)
                ]
                # Store an example data struct so we know, how to unflatten
                # each data col.
                self.buffer_structs[col] = data


class _PolicyCollector:
    """Collects already postprocessed (single agent) samples for one policy.

    Samples come in through already postprocessed SampleBatches, which
    contain single episode/trajectory data for a single agent and are then
    appended to this policy's buffers.
    """

    def __init__(self, policy: Policy):
        """Initializes a _PolicyCollector instance.

        Args:
            policy (Policy): The policy object.
        """

        self.batches = []
        self.policy = policy
        # The total timestep count for all agents that use this policy.
        # NOTE: This is not an env-step count (across n agents). AgentA and
        # agentB, both using this policy, acting in the same episode and both
        # doing n steps would increase the count by 2*n.
        self.agent_steps = 0

    def add_postprocessed_batch_for_training(
        self, batch: SampleBatch, view_requirements: ViewRequirementsDict
    ) -> None:
        """Adds a postprocessed SampleBatch (single agent) to our buffers.

        Args:
            batch (SampleBatch): An individual agent's (one trajectory)
                SampleBatch to be added to the Policy's buffers.
            view_requirements (ViewRequirementsDict): The view
                requirements for the policy. This is so we know, whether a
                view-column needs to be copied at all (not needed for
                training).
        """
        # Add the agent's trajectory length to our count.
        self.agent_steps += batch.count
        # And remove columns not needed for training.
        for view_col, view_req in view_requirements.items():
            if view_col in batch and not view_req.used_for_training:
                del batch[view_col]
        self.batches.append(batch)

    def build(self):
        """Builds a SampleBatch for this policy from the collected data.

        Also resets all buffers for further sample collection for this policy.

        Returns:
            SampleBatch: The SampleBatch with all thus-far collected data for
                this policy.
        """
        # Create batch from our buffers.
        batch = SampleBatch.concat_samples(self.batches)
        # Clear batches for future samples.
        self.batches = []
        # Reset agent steps to 0.
        self.agent_steps = 0
        return batch


class _PolicyCollectorGroup:
    def __init__(self, policy_map):
        self.policy_collectors = {
            pid: _PolicyCollector(policy) for pid, policy in policy_map.items()
        }
        # Total env-steps (1 env-step=up to N agents stepped).
        self.env_steps = 0
        # Total agent steps (1 agent-step=1 individual agent (out of N)
        # stepped).
        self.agent_steps = 0


class SimpleListCollector(SampleCollector):
    """Util to build SampleBatches for each policy in a multi-agent env.

    Input data is per-agent, while output data is per-policy. There is an M:N
    mapping between agents and policies. We retain one local batch builder
    per agent. When an agent is done, then its local batch is appended into the
    corresponding policy batch for the agent's policy.
    """

    def __init__(
        self,
        policy_map: PolicyMap,
        clip_rewards: Union[bool, float],
        callbacks: "DefaultCallbacks",
        multiple_episodes_in_batch: bool = True,
        rollout_fragment_length: int = 200,
        count_steps_by: str = "env_steps",
    ):
        """Initializes a SimpleListCollector instance."""

        super().__init__(
            policy_map,
            clip_rewards,
            callbacks,
            multiple_episodes_in_batch,
            rollout_fragment_length,
            count_steps_by,
        )

        self.large_batch_threshold: int = (
            max(1000, self.rollout_fragment_length * 10)
            if self.rollout_fragment_length != float("inf")
            else 5000
        )

        # Whenever we observe a new episode+agent, add a new
        # _SingleTrajectoryCollector.
        self.agent_collectors: Dict[Tuple[EpisodeID, AgentID], _AgentCollector] = {}
        # Internal agent-key-to-policy-id map.
        self.agent_key_to_policy_id = {}
        # Pool of used/unused PolicyCollectorGroups (attached to episodes for
        # across-episode multi-agent sample collection).
        self.policy_collector_groups = []

        # Agents to collect data from for the next forward pass (per policy).
        self.forward_pass_agent_keys = {pid: [] for pid in self.policy_map.keys()}
        self.forward_pass_size = {pid: 0 for pid in self.policy_map.keys()}

        # Maps episode ID to the (non-built) env steps taken in this episode.
        self.episode_steps: Dict[EpisodeID, int] = collections.defaultdict(int)
        # Maps episode ID to the (non-built) individual agent steps in this
        # episode.
        self.agent_steps: Dict[EpisodeID, int] = collections.defaultdict(int)
        # Maps episode ID to Episode.
        self.episodes: Dict[EpisodeID, Episode] = {}

    @override(SampleCollector)
    def episode_step(self, episode: Episode) -> None:
        episode_id = episode.episode_id
        # In the rase case that an "empty" step is taken at the beginning of
        # the episode (none of the agents has an observation in the obs-dict
        # and thus does not take an action), we have seen the episode before
        # and have to add it here to our registry.
        if episode_id not in self.episodes:
            self.episodes[episode_id] = episode
        else:
            assert episode is self.episodes[episode_id]
        self.episode_steps[episode_id] += 1
        episode.length += 1

        # In case of "empty" env steps (no agent is stepping), the builder
        # object may still be None.
        if episode.batch_builder:
            env_steps = episode.batch_builder.env_steps
            num_individual_observations = sum(
                c.agent_steps for c in episode.batch_builder.policy_collectors.values()
            )

            if num_individual_observations > self.large_batch_threshold and log_once(
                "large_batch_warning"
            ):
                logger.warning(
                    "More than {} observations in {} env steps for "
                    "episode {} ".format(
                        num_individual_observations, env_steps, episode_id
                    )
                    + "are buffered in the sampler. If this is more than you "
                    "expected, check that that you set a horizon on your "
                    "environment correctly and that it terminates at some "
                    "point. Note: In multi-agent environments, "
                    "`rollout_fragment_length` sets the batch size based on "
                    "(across-agents) environment steps, not the steps of "
                    "individual agents, which can result in unexpectedly "
                    "large batches."
                    + (
                        "Also, you may be waiting for your Env to "
                        "terminate (batch_mode=`complete_episodes`). Make sure "
                        "it does at some point."
                        if not self.multiple_episodes_in_batch
                        else ""
                    )
                )

    @override(SampleCollector)
    def add_init_obs(
        self,
        episode: Episode,
        agent_id: AgentID,
        env_id: EnvID,
        policy_id: PolicyID,
        t: int,
        init_obs: TensorType,
    ) -> None:
        # Make sure our mappings are up to date.
        agent_key = (episode.episode_id, agent_id)
        self.agent_key_to_policy_id[agent_key] = policy_id
        policy = self.policy_map[policy_id]

        # Add initial obs to Trajectory.
        assert agent_key not in self.agent_collectors
        # TODO: determine exact shift-before based on the view-req shifts.
        self.agent_collectors[agent_key] = _AgentCollector(
            policy.view_requirements, policy
        )
        self.agent_collectors[agent_key].add_init_obs(
            episode_id=episode.episode_id,
            agent_index=episode._agent_index(agent_id),
            env_id=env_id,
            t=t,
            init_obs=init_obs,
        )

        self.episodes[episode.episode_id] = episode
        if episode.batch_builder is None:
            episode.batch_builder = (
                self.policy_collector_groups.pop()
                if self.policy_collector_groups
                else _PolicyCollectorGroup(self.policy_map)
            )

        self._add_to_next_inference_call(agent_key)

    @override(SampleCollector)
    def add_action_reward_next_obs(
        self,
        episode_id: EpisodeID,
        agent_id: AgentID,
        env_id: EnvID,
        policy_id: PolicyID,
        agent_done: bool,
        values: Dict[str, TensorType],
    ) -> None:
        # Make sure, episode/agent already has some (at least init) data.
        agent_key = (episode_id, agent_id)
        assert self.agent_key_to_policy_id[agent_key] == policy_id
        assert agent_key in self.agent_collectors

        self.agent_steps[episode_id] += 1

        # Include the current agent id for multi-agent algorithms.
        if agent_id != _DUMMY_AGENT_ID:
            values["agent_id"] = agent_id

        # Add action/reward/next-obs (and other data) to Trajectory.
        self.agent_collectors[agent_key].add_action_reward_next_obs(values)

        if not agent_done:
            self._add_to_next_inference_call(agent_key)

    @override(SampleCollector)
    def total_env_steps(self) -> int:
        # Add the non-built ongoing-episode env steps + the already built
        # env-steps.
        return sum(self.episode_steps.values()) + sum(
            pg.env_steps for pg in self.policy_collector_groups.values()
        )

    @override(SampleCollector)
    def total_agent_steps(self) -> int:
        # Add the non-built ongoing-episode agent steps (still in the agent
        # collectors) + the already built agent steps.
        return sum(a.agent_steps for a in self.agent_collectors.values()) + sum(
            pg.agent_steps for pg in self.policy_collector_groups.values()
        )

    @override(SampleCollector)
    def get_inference_input_dict(self, policy_id: PolicyID) -> Dict[str, TensorType]:
        policy = self.policy_map[policy_id]
        keys = self.forward_pass_agent_keys[policy_id]
        batch_size = len(keys)

        # Return empty batch, if no forward pass to do.
        if batch_size == 0:
            return SampleBatch()

        buffers = {}
        for k in keys:
            collector = self.agent_collectors[k]
            buffers[k] = collector.buffers
        # Use one agent's buffer_structs (they should all be the same).
        buffer_structs = self.agent_collectors[keys[0]].buffer_structs

        input_dict = {}
        for view_col, view_req in policy.view_requirements.items():
            # Not used for action computations.
            if not view_req.used_for_compute_actions:
                continue

            # Create the batch of data from the different buffers.
            data_col = view_req.data_col or view_col
            delta = (
                -1
                if data_col
                in [
                    SampleBatch.OBS,
                    SampleBatch.ENV_ID,
                    SampleBatch.EPS_ID,
                    SampleBatch.AGENT_INDEX,
                    SampleBatch.T,
                ]
                else 0
            )
            # Range of shifts, e.g. "-100:0". Note: This includes index 0!
            if view_req.shift_from is not None:
                time_indices = (view_req.shift_from + delta, view_req.shift_to + delta)
            # Single shift (e.g. -1) or list of shifts, e.g. [-4, -1, 0].
            else:
                time_indices = view_req.shift + delta

            # Loop through agents and add up their data (batch).
            data = None
            for k in keys:
                # Buffer for the data does not exist yet: Create dummy
                # (zero) data.
                if data_col not in buffers[k]:
                    if view_req.data_col is not None:
                        space = policy.view_requirements[view_req.data_col].space
                    else:
                        space = view_req.space

                    if isinstance(space, Space):
                        fill_value = get_dummy_batch_for_space(
                            space,
                            batch_size=0,
                        )
                    else:
                        fill_value = space

                    self.agent_collectors[k]._build_buffers({data_col: fill_value})

                if data is None:
                    data = [[] for _ in range(len(buffers[keys[0]][data_col]))]

                # `shift_from` and `shift_to` are defined: User wants a
                # view with some time-range.
                if isinstance(time_indices, tuple):
                    # `shift_to` == -1: Until the end (including(!) the
                    # last item).
                    if time_indices[1] == -1:
                        for d, b in zip(data, buffers[k][data_col]):
                            d.append(b[time_indices[0] :])
                    # `shift_to` != -1: "Normal" range.
                    else:
                        for d, b in zip(data, buffers[k][data_col]):
                            d.append(b[time_indices[0] : time_indices[1] + 1])
                # Single index.
                else:
                    for d, b in zip(data, buffers[k][data_col]):
                        d.append(b[time_indices])

            np_data = [np.array(d) for d in data]
            if data_col in buffer_structs:
                input_dict[view_col] = tree.unflatten_as(
                    buffer_structs[data_col], np_data
                )
            else:
                input_dict[view_col] = np_data[0]

        self._reset_inference_calls(policy_id)

        return SampleBatch(
            input_dict,
            seq_lens=np.ones(batch_size, dtype=np.int32)
            if "state_in_0" in input_dict
            else None,
        )

    @override(SampleCollector)
    def postprocess_episode(
        self,
        episode: Episode,
        is_done: bool = False,
        check_dones: bool = False,
        build: bool = False,
    ) -> Union[None, SampleBatch, MultiAgentBatch]:
        episode_id = episode.episode_id
        policy_collector_group = episode.batch_builder

        # TODO: (sven) Once we implement multi-agent communication channels,
        #  we have to resolve the restriction of only sending other agent
        #  batches from the same policy to the postprocess methods.
        # Build SampleBatches for the given episode.
        pre_batches = {}
        for (eps_id, agent_id), collector in self.agent_collectors.items():
            # Build only if there is data and agent is part of given episode.
            if collector.agent_steps == 0 or eps_id != episode_id:
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
                    a_max=self.clip_rewards,
                )

        post_batches = {}
        for agent_id, (_, pre_batch) in pre_batches.items():
            # Entire episode is said to be done.
            # Error if no DONE at end of this agent's trajectory.
            if is_done and check_dones and not pre_batch[SampleBatch.DONES][-1]:
                raise ValueError(
                    "Episode {} terminated for all agents, but we still "
                    "don't have a last observation for agent {} (policy "
                    "{}). ".format(
                        episode_id,
                        agent_id,
                        self.agent_key_to_policy_id[(episode_id, agent_id)],
                    )
                    + "Please ensure that you include the last observations "
                    "of all live agents when setting done[__all__] to "
                    "True. Alternatively, set no_done_at_end=True to "
                    "allow this."
                )

            # Skip a trajectory's postprocessing (and thus using it for training),
            # if its agent's info exists and contains the training_enabled=False
            # setting (used by our PolicyClients).
            last_info = episode.last_info_for(agent_id)
            if last_info and not last_info.get("training_enabled", True):
                if is_done:
                    agent_key = (episode_id, agent_id)
                    del self.agent_key_to_policy_id[agent_key]
                    del self.agent_collectors[agent_key]
                continue

            if len(pre_batches) > 1:
                other_batches = pre_batches.copy()
                del other_batches[agent_id]
            else:
                other_batches = {}
            pid = self.agent_key_to_policy_id[(episode_id, agent_id)]
            policy = self.policy_map[pid]
            if (
                any(pre_batch[SampleBatch.DONES][:-1])
                or len(set(pre_batch[SampleBatch.EPS_ID])) > 1
            ):
                raise ValueError(
                    "Batches sent to postprocessing must only contain steps "
                    "from a single trajectory.",
                    pre_batch,
                )
            # Call the Policy's Exploration's postprocess method.
            post_batches[agent_id] = pre_batch
            if getattr(policy, "exploration", None) is not None:
                policy.exploration.postprocess_trajectory(
                    policy, post_batches[agent_id], policy.get_session()
                )
            post_batches[agent_id].set_get_interceptor(None)
            post_batches[agent_id] = policy.postprocess_trajectory(
                post_batches[agent_id], other_batches, episode
            )

        if log_once("after_post"):
            logger.info(
                "Trajectory fragment after postprocess_trajectory():\n\n{}\n".format(
                    summarize(post_batches)
                )
            )

        # Append into policy batches and reset.
        from ray.rllib.evaluation.rollout_worker import get_global_worker

        for agent_id, post_batch in sorted(post_batches.items()):
            agent_key = (episode_id, agent_id)
            pid = self.agent_key_to_policy_id[agent_key]
            policy = self.policy_map[pid]
            self.callbacks.on_postprocess_trajectory(
                worker=get_global_worker(),
                episode=episode,
                agent_id=agent_id,
                policy_id=pid,
                policies=self.policy_map,
                postprocessed_batch=post_batch,
                original_batches=pre_batches,
            )

            # Add the postprocessed SampleBatch to the policy collectors for
            # training.
            # PID may be a newly added policy. Just confirm we have it in our
            # policy map before proceeding with adding a new _PolicyCollector()
            # to the group.
            if pid not in policy_collector_group.policy_collectors:
                assert pid in self.policy_map
                policy_collector_group.policy_collectors[pid] = _PolicyCollector(policy)
            policy_collector_group.policy_collectors[
                pid
            ].add_postprocessed_batch_for_training(post_batch, policy.view_requirements)

            if is_done:
                del self.agent_key_to_policy_id[agent_key]
                del self.agent_collectors[agent_key]

        if policy_collector_group:
            env_steps = self.episode_steps[episode_id]
            policy_collector_group.env_steps += env_steps
            agent_steps = self.agent_steps[episode_id]
            policy_collector_group.agent_steps += agent_steps

        if is_done:
            del self.episode_steps[episode_id]
            del self.agent_steps[episode_id]
            del self.episodes[episode_id]

            # Make PolicyCollectorGroup available for more agent batches in
            # other episodes. Do not reset count to 0.
            if policy_collector_group:
                self.policy_collector_groups.append(policy_collector_group)
        else:
            self.episode_steps[episode_id] = self.agent_steps[episode_id] = 0

        # Build a MultiAgentBatch from the episode and return.
        if build:
            return self._build_multi_agent_batch(episode)

    def _build_multi_agent_batch(
        self, episode: Episode
    ) -> Union[MultiAgentBatch, SampleBatch]:

        ma_batch = {}
        for pid, collector in episode.batch_builder.policy_collectors.items():
            if collector.agent_steps > 0:
                ma_batch[pid] = collector.build()
        # Create the batch.
        ma_batch = MultiAgentBatch.wrap_as_needed(
            ma_batch, env_steps=episode.batch_builder.env_steps
        )

        # PolicyCollectorGroup is empty.
        episode.batch_builder.env_steps = 0
        episode.batch_builder.agent_steps = 0

        return ma_batch

    @override(SampleCollector)
    def try_build_truncated_episode_multi_agent_batch(
        self,
    ) -> List[Union[MultiAgentBatch, SampleBatch]]:
        batches = []
        # Loop through ongoing episodes and see whether their length plus
        # what's already in the policy collectors reaches the fragment-len
        # (abiding to the unit used: env-steps or agent-steps).
        for episode_id, episode in self.episodes.items():
            # Measure batch size in env-steps.
            if self.count_steps_by == "env_steps":
                built_steps = (
                    episode.batch_builder.env_steps if episode.batch_builder else 0
                )
                ongoing_steps = self.episode_steps[episode_id]
            # Measure batch-size in agent-steps.
            else:
                built_steps = (
                    episode.batch_builder.agent_steps if episode.batch_builder else 0
                )
                ongoing_steps = self.agent_steps[episode_id]

            # Reached the fragment-len -> We should build an MA-Batch.
            if built_steps + ongoing_steps >= self.rollout_fragment_length:
                if self.count_steps_by != "agent_steps":
                    assert built_steps + ongoing_steps == self.rollout_fragment_length
                # If we reached the fragment-len only because of `episode_id`
                # (still ongoing) -> postprocess `episode_id` first.
                if built_steps < self.rollout_fragment_length:
                    self.postprocess_episode(episode, is_done=False)
                # If there is a builder for this episode,
                # build the MA-batch and add to return values.
                if episode.batch_builder:
                    batch = self._build_multi_agent_batch(episode=episode)
                    batches.append(batch)
                # No batch-builder:
                # We have reached the rollout-fragment length w/o any agent
                # steps! Warn that the environment may never request any
                # actions from any agents.
                elif log_once("no_agent_steps"):
                    logger.warning(
                        "Your environment seems to be stepping w/o ever "
                        "emitting agent observations (agents are never "
                        "requested to act)!"
                    )

        return batches

    def _add_to_next_inference_call(self, agent_key: Tuple[EpisodeID, AgentID]) -> None:
        """Adds an Agent key (episode+agent IDs) to the next inference call.

        This makes sure that the agent's current data (in the trajectory) is
        used for generating the next input_dict for a
        `Policy.compute_actions()` call.

        Args:
            agent_key (Tuple[EpisodeID, AgentID]: A unique agent key (across
                vectorized environments).
        """
        pid = self.agent_key_to_policy_id[agent_key]

        # PID may be a newly added policy (added on the fly during training).
        # Just confirm we have it in our policy map before proceeding with
        # forward_pass_size=0.
        if pid not in self.forward_pass_size:
            assert pid in self.policy_map
            self.forward_pass_size[pid] = 0
            self.forward_pass_agent_keys[pid] = []

        idx = self.forward_pass_size[pid]
        assert idx >= 0
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
