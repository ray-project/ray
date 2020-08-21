import logging
import numpy as np
from typing import Dict, Optional

from ray.rllib.evaluation.episode import MultiAgentEpisode
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.typing import AgentID, EnvID, EpisodeID, TensorType

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()

logger = logging.getLogger(__name__)


class _PerPolicySampleCollector:
    """A class for efficiently collecting samples for a single (fixed) policy.

    Can be used by a _MultiAgentSampleCollector for its different policies.
    """

    def __init__(self,
                 num_agents: Optional[int] = None,
                 num_timesteps: Optional[int] = None,
                 time_major: bool = True,
                 shift_before: int = 0,
                 shift_after: int = 0):
        """Initializes a _PerPolicySampleCollector object.

        Args:
            num_agents (int): The max number of agent slots to pre-allocate
                in the buffer.
            num_timesteps (int): The max number of timesteps to pre-allocate
                in the buffer.
            time_major (Optional[bool]): Whether to preallocate buffers and
                collect samples in time-major fashion (TxBx...).
            shift_before (int): The additional number of time slots to
                pre-allocate at the beginning of a time window (for possible
                underlying data column shifts, e.g. PREV_ACTIONS).
            shift_after (int): The additional number of time slots to
                pre-allocate at the end of a time window (for possible
                underlying data column shifts, e.g. NEXT_OBS).
        """

        self.num_agents = num_agents or 100
        self.num_timesteps = num_timesteps
        self.time_major = time_major
        # `shift_before must at least be 1 for the init obs timestep.
        self.shift_before = max(shift_before, 1)
        self.shift_after = shift_after

        # The offset on the agent dim to start the next SampleBatch build from.
        self.sample_batch_offset = 0

        # The actual underlying data-buffers.
        self.buffers = {}
        self.postprocessed_agents = [False] * self.num_agents

        # Next agent-slot to be used by a new agent/env combination.
        self.agent_slot_cursor = 0
        # Maps agent/episode ID/chunk-num to an agent slot.
        self.agent_key_to_slot = {}
        # Maps agent/episode ID to the last chunk-num.
        self.agent_key_to_chunk_num = {}
        # Maps agent slot number to agent keys.
        self.slot_to_agent_key = [None] * self.num_agents
        # Maps agent/episode ID/chunk-num to a time step cursor.
        self.agent_key_to_timestep = {}

        # Total timesteps taken in the env over all agents since last reset.
        self.timesteps_since_last_reset = 0

        # Indices (T,B) to pick from the buffers for the next forward pass.
        self.forward_pass_indices = [[], []]
        self.forward_pass_size = 0
        # Maps index from the forward pass batch to (agent_id, episode_id,
        # env_id) tuple.
        self.forward_pass_index_to_agent_info = {}
        self.agent_key_to_forward_pass_index = {}

    def add_init_obs(self, episode_id: EpisodeID, agent_id: AgentID,
                     env_id: EnvID, chunk_num: int,
                     init_obs: TensorType) -> None:
        """Adds a single initial observation (after env.reset()) to the buffer.

        Args:
            episode_id (EpisodeID): Unique ID for the episode we are adding the
                initial observation for.
            agent_id (AgentID): Unique ID for the agent we are adding the
                initial observation for.
            env_id (EnvID): The env ID to which `init_obs` belongs.
            chunk_num (int): The time-chunk number (0-based). Some episodes
                may last for longer than self.num_timesteps and therefore
                have to be chopped into chunks.
            init_obs (TensorType): Initial observation (after env.reset()).
        """
        agent_key = (agent_id, episode_id, chunk_num)
        agent_slot = self.agent_slot_cursor
        self.agent_key_to_slot[agent_key] = agent_slot
        self.agent_key_to_chunk_num[agent_key[:2]] = chunk_num
        self.slot_to_agent_key[agent_slot] = agent_key
        self._next_agent_slot()

        if SampleBatch.OBS not in self.buffers:
            self._build_buffers(
                single_row={
                    SampleBatch.OBS: init_obs,
                    SampleBatch.EPS_ID: episode_id,
                    SampleBatch.AGENT_INDEX: agent_id,
                    "env_id": env_id,
                })
        if self.time_major:
            self.buffers[SampleBatch.OBS][self.shift_before-1, agent_slot] = \
                init_obs
        else:
            self.buffers[SampleBatch.OBS][agent_slot, self.shift_before-1] = \
                init_obs
        self.agent_key_to_timestep[agent_key] = self.shift_before

        self._add_to_next_inference_call(agent_key, env_id, agent_slot,
                                         self.shift_before - 1)

    def add_action_reward_next_obs(
            self, episode_id: EpisodeID, agent_id: AgentID, env_id: EnvID,
            agent_done: bool, values: Dict[str, TensorType]) -> None:
        """Add the given dictionary (row) of values to this batch.

        Args:
            episode_id (EpisodeID): Unique ID for the episode we are adding the
                values for.
            agent_id (AgentID): Unique ID for the agent we are adding the
                values for.
            env_id (EnvID): The env ID to which the given data belongs.
            agent_done (bool): Whether next obs should not be used for an
                upcoming inference call. Default: False = next-obs should be
                used for upcoming inference.
            values (Dict[str, TensorType]): Data dict (interpreted as a single
                row) to be added to buffer. Must contain keys:
                SampleBatch.ACTIONS, REWARDS, DONES, and NEXT_OBS.
        """
        assert (SampleBatch.ACTIONS in values and SampleBatch.REWARDS in values
                and SampleBatch.NEXT_OBS in values
                and SampleBatch.DONES in values)

        assert SampleBatch.OBS not in values
        values[SampleBatch.OBS] = values[SampleBatch.NEXT_OBS]
        del values[SampleBatch.NEXT_OBS]

        chunk_num = self.agent_key_to_chunk_num[(agent_id, episode_id)]
        agent_key = (agent_id, episode_id, chunk_num)
        agent_slot = self.agent_key_to_slot[agent_key]
        ts = self.agent_key_to_timestep[agent_key]
        for k, v in values.items():
            if k not in self.buffers:
                self._build_buffers(single_row=values)
            if self.time_major:
                self.buffers[k][ts, agent_slot] = v
            else:
                self.buffers[k][agent_slot, ts] = v
        self.agent_key_to_timestep[agent_key] += 1

        # Time-axis is "full" -> Cut-over to new chunk (only if not DONE).
        if self.agent_key_to_timestep[
            agent_key] - self.shift_before == self.num_timesteps and \
                not values[SampleBatch.DONES]:
            self._new_chunk_from(agent_slot, agent_key,
                                 self.agent_key_to_timestep[agent_key])

        self.timesteps_since_last_reset += 1

        if not agent_done:
            self._add_to_next_inference_call(agent_key, env_id, agent_slot, ts)

    def get_inference_input_dict(self, view_reqs: Dict[str, ViewRequirement]
                                 ) -> Dict[str, TensorType]:
        """Returns an input_dict for an (inference) forward pass.

        The input_dict can then be used for action computations inside a
        Policy via `Policy.compute_actions_from_input_dict()`.

        Args:
            view_reqs (Dict[str, ViewRequirement]): The view requirements
                dict to use.

        Returns:
            Dict[str, TensorType]: The input_dict to be passed into the ModelV2
                for inference/training.

        Examples:
            >>> obs, r, done, info = env.step(action)
            >>> collector.add_action_reward_next_obs(12345, 0, "pol0", {
            ...     "action": action, "obs": obs, "reward": r, "done": done
            ... })
            >>> input_dict = collector.get_inference_input_dict(policy.model)
            >>> action = policy.compute_actions_from_input_dict(input_dict)
            >>> # repeat
        """
        input_dict = {}
        for view_col, view_req in view_reqs.items():
            # Create the batch of data from the different buffers.
            data_col = view_req.data_col or view_col
            if data_col not in self.buffers:
                self._build_buffers({data_col: view_req.space.sample()})

            indices = self.forward_pass_indices
            if self.time_major:
                input_dict[view_col] = self.buffers[data_col][indices]
            else:
                if isinstance(view_req.shift, (list, tuple)):
                    time_indices = \
                        np.array(view_req.shift) + np.array(indices[0])
                    input_dict[view_col] = self.buffers[data_col][indices[1],
                                                                  time_indices]
                else:
                    input_dict[view_col] = \
                        self.buffers[data_col][indices[1], indices[0]]

        self._reset_inference_call()

        return input_dict

    def get_postprocessing_sample_batches(
            self,
            episode: MultiAgentEpisode,
            view_reqs: Dict[str, ViewRequirement]) -> \
            Dict[AgentID, SampleBatch]:
        """Returns a SampleBatch object ready for postprocessing.

        Args:
            episode (MultiAgentEpisode): The MultiAgentEpisode object to
                get the to-be-postprocessed SampleBatches for.
            view_reqs (Dict[str, ViewRequirement]): The view requirements dict
                to use for creating the SampleBatch from our buffers.

        Returns:
            Dict[AgentID, SampleBatch]: The sample batch objects to be passed
                to `Policy.postprocess_trajectory()`.
        """
        # Loop through all agents and create a SampleBatch
        # (as "view"; no copying).

        # Construct the SampleBatch-dict.
        sample_batch_data = {}

        range_ = self.agent_slot_cursor - self.sample_batch_offset
        if range_ < 0:
            range_ = self.num_agents + range_
        for i in range(range_):
            agent_slot = self.sample_batch_offset + i
            if agent_slot >= self.num_agents:
                agent_slot = agent_slot % self.num_agents
            # Do not postprocess the same slot twice.
            if self.postprocessed_agents[agent_slot]:
                continue
            agent_key = self.slot_to_agent_key[agent_slot]
            # Skip other episodes (if episode provided).
            if episode and agent_key[1] != episode.episode_id:
                continue
            end = self.agent_key_to_timestep[agent_key]
            # Do not build any empty SampleBatches.
            if end == self.shift_before:
                continue
            self.postprocessed_agents[agent_slot] = True

            assert agent_key not in sample_batch_data
            sample_batch_data[agent_key] = {}
            batch = sample_batch_data[agent_key]

            for view_col, view_req in view_reqs.items():
                data_col = view_req.data_col or view_col
                # Skip columns that will only get added through postprocessing
                # (these may not even exist yet).
                if data_col not in self.buffers:
                    continue

                shift = view_req.shift
                if data_col == SampleBatch.OBS:
                    shift -= 1

                batch[view_col] = self.buffers[data_col][
                    self.shift_before + shift:end + shift, agent_slot]

        batches = {}
        for agent_key, data in sample_batch_data.items():
            batches[agent_key] = SampleBatch(data)
        return batches

    def get_train_sample_batch_and_reset(self, view_reqs) -> SampleBatch:
        """Returns the accumulated sample batche for this policy.

        This is usually called to collect samples for policy training.

        Returns:
            SampleBatch: Returns the accumulated sample batch for this
                policy.
        """
        seq_lens_w_0s = [
            self.agent_key_to_timestep[k] - self.shift_before
            for k in self.slot_to_agent_key if k is not None
        ]
        # We have an agent-axis buffer "rollover" (new SampleBatch will be
        # built from last n agent records plus first m agent records in
        # buffer).
        if self.agent_slot_cursor < self.sample_batch_offset:
            rollover = -(self.num_agents - self.sample_batch_offset)
            seq_lens_w_0s = seq_lens_w_0s[rollover:] + seq_lens_w_0s[:rollover]
        first_zero_len = len(seq_lens_w_0s)
        if seq_lens_w_0s[-1] == 0:
            first_zero_len = seq_lens_w_0s.index(0)
            # Assert that all zeros lie at the end of the seq_lens array.
            assert all(seq_lens_w_0s[i] == 0
                       for i in range(first_zero_len, len(seq_lens_w_0s)))

        t_start = self.shift_before
        t_end = t_start + self.num_timesteps

        # The agent_slot cursor that points to the newest agent-slot that
        # actually already has at least 1 timestep of data (thus it excludes
        # just-rolled over chunks (which only have the initial obs in them)).
        valid_agent_cursor = \
            (self.agent_slot_cursor -
             (len(seq_lens_w_0s) - first_zero_len)) % self.num_agents

        # Construct the view dict.
        view = {}
        for view_col, view_req in view_reqs.items():
            data_col = view_req.data_col or view_col
            assert data_col in self.buffers
            # For OBS, indices must be shifted by -1.
            shift = view_req.shift
            shift += 0 if data_col != SampleBatch.OBS else -1
            # If agent_slot has been rolled-over to beginning, we have to copy
            # here.
            if valid_agent_cursor < self.sample_batch_offset:
                time_slice = self.buffers[data_col][t_start + shift:t_end +
                                                    shift]
                one_ = time_slice[:, self.sample_batch_offset:]
                two_ = time_slice[:, :valid_agent_cursor]
                if torch and isinstance(time_slice, torch.Tensor):
                    view[view_col] = torch.cat([one_, two_], dim=1)
                else:
                    view[view_col] = np.concatenate([one_, two_], axis=1)
            else:
                view[view_col] = \
                    self.buffers[data_col][
                    t_start + shift:t_end + shift,
                    self.sample_batch_offset:valid_agent_cursor]

        # Copy all still ongoing trajectories to new agent slots
        # (including the ones that just started (are seq_len=0)).
        new_chunk_args = []
        for i, seq_len in enumerate(seq_lens_w_0s):
            if seq_len < self.num_timesteps:
                agent_slot = (self.sample_batch_offset + i) % self.num_agents
                if not self.buffers[SampleBatch.
                                    DONES][seq_len - 1 +
                                           self.shift_before][agent_slot]:
                    agent_key = self.slot_to_agent_key[agent_slot]
                    new_chunk_args.append(
                        (agent_slot, agent_key,
                         self.agent_key_to_timestep[agent_key]))
        # Cut out all 0 seq-lens.
        seq_lens = seq_lens_w_0s[:first_zero_len]
        batch = SampleBatch(
            view, _seq_lens=np.array(seq_lens), _time_major=self.time_major)

        # Reset everything for new data.
        self.postprocessed_agents = [False] * self.num_agents
        self.agent_key_to_slot.clear()
        self.agent_key_to_chunk_num.clear()
        self.slot_to_agent_key = [None] * self.num_agents
        self.agent_key_to_timestep.clear()
        self.timesteps_since_last_reset = 0
        self.forward_pass_size = 0
        self.sample_batch_offset = self.agent_slot_cursor

        for args in new_chunk_args:
            self._new_chunk_from(*args)

        return batch

    def _build_buffers(self, single_row: Dict[str, TensorType]) -> None:
        """Builds the internal data buffers based on a single given row.

        This may be called several times in the lifetime of this instance
        to add new columns to the buffer. Columns in `single_row` that already
        exist in the buffer will be ignored.

        Args:
            single_row (Dict[str, TensorType]): A single datarow with one or
                more columns (str as key, np.ndarray|tensor as data) to be used
                as template to build the pre-allocated buffer.
        """
        time_size = self.num_timesteps + self.shift_before + self.shift_after
        for col, data in single_row.items():
            if col in self.buffers:
                continue
            base_shape = (time_size, self.num_agents) if self.time_major else \
                (self.num_agents, time_size)
            # Python primitive -> np.array.
            if isinstance(data, (int, float, bool)):
                t_ = type(data)
                dtype = np.float32 if t_ == float else \
                    np.int32 if type(data) == int else np.bool_
                self.buffers[col] = np.zeros(shape=base_shape, dtype=dtype)
            # np.ndarray, torch.Tensor, or tf.Tensor.
            else:
                shape = base_shape + data.shape
                dtype = data.dtype
                if torch and isinstance(data, torch.Tensor):
                    self.buffers[col] = torch.zeros(
                        *shape, dtype=dtype, device=data.device)
                elif tf and isinstance(data, tf.Tensor):
                    self.buffers[col] = tf.zeros(shape=shape, dtype=dtype)
                else:
                    self.buffers[col] = np.zeros(shape=shape, dtype=dtype)

    def _next_agent_slot(self):
        """Starts a new agent slot at the end of the agent-axis.

        Also makes sure, the new slot is not taken yet.
        """
        self.agent_slot_cursor += 1
        if self.agent_slot_cursor >= self.num_agents:
            self.agent_slot_cursor = 0
        # Just make sure, there is space in our buffer.
        assert self.slot_to_agent_key[self.agent_slot_cursor] is None

    def _new_chunk_from(self, agent_slot, agent_key, timestep):
        """Creates a new time-window (chunk) given an agent.

        The agent may already have an unfinished episode going on (in a
        previous chunk). The end of that previous chunk will be copied to the
        beginning of the new one for proper data-shift handling (e.g.
        PREV_ACTIONS/REWARDS).

        Args:
            agent_slot (int): The agent to start a new chunk for (from an
                ongoing episode (chunk)).
            agent_key (Tuple[AgentID, EpisodeID, int]): The internal key to
                identify an active agent in some episode.
            timestep (int): The timestep in the old chunk being continued.
        """
        new_agent_slot = self.agent_slot_cursor
        # Increase chunk num by 1.
        new_agent_key = agent_key[:2] + (agent_key[2] + 1, )
        # Copy relevant timesteps at end of old chunk into new one.
        if self.time_major:
            for k in self.buffers.keys():
                self.buffers[k][0:self.shift_before, new_agent_slot] = \
                    self.buffers[k][
                    timestep - self.shift_before:timestep, agent_slot]
        else:
            for k in self.buffers.keys():
                self.buffers[k][new_agent_slot, 0:self.shift_before] = \
                    self.buffers[k][
                    agent_slot, timestep - self.shift_before:timestep]

        self.agent_key_to_slot[new_agent_key] = new_agent_slot
        self.agent_key_to_chunk_num[new_agent_key[:2]] = new_agent_key[2]
        self.slot_to_agent_key[new_agent_slot] = new_agent_key
        self._next_agent_slot()
        self.agent_key_to_timestep[new_agent_key] = self.shift_before

    def _add_to_next_inference_call(self, agent_key, env_id, agent_slot,
                                    timestep):
        """Registers given T and B (agent_slot) for get_inference_input_dict.

        Calling `get_inference_input_dict` will produce an input_dict (for
        Policy.compute_actions_from_input_dict) with all registered agent/time
        indices and then automatically reset the registry.

        Args:
            agent_key (Tuple[AgentID, EpisodeID, int]): The internal key to
                identify an active agent in some episode.
            env_id (EnvID): The env ID of the given agent.
            agent_slot (int): The agent_slot to register (B axis).
            timestep (int): The timestep to register (T axis).
        """
        idx = self.forward_pass_size
        self.forward_pass_index_to_agent_info[idx] = (agent_key[0],
                                                      agent_key[1], env_id)
        self.agent_key_to_forward_pass_index[agent_key[:2]] = idx
        if self.forward_pass_size == 0:
            self.forward_pass_indices[0].clear()
            self.forward_pass_indices[1].clear()
        self.forward_pass_indices[0].append(timestep)
        self.forward_pass_indices[1].append(agent_slot)
        self.forward_pass_size += 1

    def _reset_inference_call(self):
        """Resets indices for the next inference call.

        After calling this, new calls to `add_init_obs()` and
        `add_action_reward_next_obs()` will count for the next input_dict
        returned by `get_inference_input_dict()`.
        """
        self.forward_pass_size = 0
