import logging
import numpy as np
from typing import Dict, Optional

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.types import AgentID, EpisodeID, PolicyID, TensorType

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()

logger = logging.getLogger(__name__)


def to_float_array(v):
    arr = np.array(v)
    if arr.dtype == np.float64:
        return arr.astype(np.float32)  # save some memory
    return arr


class RolloutSampleCollector:
    """
    """

    def __init__(self, num_agents: Optional[int] = None, num_timesteps: Optional[int] = None,
                 shift_before=1, shift_after=1, policy_id=None):
        """Initializes a ... object.

        Args:
        """
        self.policy_id = policy_id
        self.num_agents = num_agents or 100
        self.num_timesteps = num_timesteps
        assert num_timesteps, "Only supports RNN style PolicyTrajectories for now!"
        self.shift_before = shift_before
        self.shift_after = shift_after

        # The offset on the agent dim to start the next SampleBatch build from.
        self.sample_batch_offset = 0

        self.buffers = {}

        self.seq_lens = []

        # Next agent-slot to be used by a new agent/env combination.
        self.agent_dim_cursor = 0
        # Maps agent/env IDs to an agent slot.
        self.agent_key_to_slot = {}
        # Maps agent/env IDs to a time step cursor.
        self.agent_key_to_timestep = {}

        # Total timesteps taken in the env over all agents since last reset.
        self.timesteps_since_last_reset = 0

        # Indices (T,B) to pick from the buffers for the next forward pass.
        self.forward_pass_indices = [[], []]
        self.forward_pass_size = 0
        # Maps index from the forward pass batch to (agent_id, episode_id) tuple.
        self.forward_pass_index_to_agent_key = {}
        self.agent_key_to_forward_pass_index = {}

    def add_init_obs(self,
                     episode_id: EpisodeID,
                     agent_id: AgentID,
                     init_obs: TensorType) -> None:
        """Adds a single initial observation (after env.reset()) to the buffer.

        #Stores it in self.initial_obs.

        Args:
            episode_id (EpisodeID): Unique id for the episode we are adding the initial
                observation for.
            agent_id (AgentID): Unique id for the agent we are adding the
                initial observation for.
            init_obs (TensorType): Initial observation (after env.reset()).
        """
        agent_key = (agent_id, episode_id)
        agent_slot = self.agent_dim_cursor
        self.agent_key_to_slot[agent_key] = agent_slot
        self.agent_dim_cursor += 1

        if SampleBatch.OBS not in self.buffers:
            self._build_buffers(single_row={SampleBatch.OBS: init_obs})
        self.buffers[SampleBatch.OBS][self.shift_before - 1][agent_slot] = init_obs
        self.agent_key_to_timestep[agent_key] = 1

    def add_action_reward_next_obs(self,
                                   episode_id: EpisodeID,
                                   agent_id: AgentID,
                                   values: Dict[str, TensorType]) -> None:
        """Add the given dictionary (row) of values to this batch.

        Args:
            episode_id (EpisodeID): Unique id for the episode we are adding the initial
                observation for.
            agent_id (AgentID): Unique id for the agent we are adding the
                initial observation for.
            policy_id (PolicyID): Unique id for policy controlling the agent.
            values (Dict[str, TensorType]): Data dict (interpreted as a single
                row) to be added to buffer. Must contain keys:
                SampleBatch.ACTIONS, REWARDS, DONES, and OBS.
        """
        #assert self.initial_obs is not None
        assert (SampleBatch.ACTIONS in values and SampleBatch.REWARDS in values
                and SampleBatch.NEXT_OBS in values)
        #assert episode_id == self.episode_id
        #assert agent_id == self.agent_id
        #assert policy_id == self.policy_id

        if SampleBatch.NEXT_OBS in values:
            assert SampleBatch.CUR_OBS not in values
            values[SampleBatch.CUR_OBS] = values[SampleBatch.NEXT_OBS]
            del values[SampleBatch.NEXT_OBS]

        # Only obs exists so far in buffers:
        # Initialize all other columns.
        #if len(self.buffers) == 0:
        #    self._build_buffers(single_row=values)

        agent_key = (agent_id, episode_id)
        agent_slot = self.agent_key_to_slot[agent_key]
        ts = self.agent_key_to_timestep[agent_key]
        for k, v in values.items():
            if k not in self.buffers:
                self._build_buffers(single_row=values)
            self.buffers[k][ts][agent_slot] = v
        self.agent_key_to_timestep[agent_key] += 1

        # Extend (re-alloc) buffers if full.
        if self.agent_key_to_timestep[agent_key] == self.num_timesteps:
            # TODO: assign new agent slot to this agent.
            raise NotImplementedError  #self._extend_buffers(values)

        self.timesteps_since_last_reset += 1

    def add_to_forward_pass(self, episode_id, agent_id):
        agent_key = (agent_id, episode_id)
        b = self.agent_key_to_slot[agent_key]
        t = self.agent_key_to_timestep[agent_key]
        idx = self.forward_pass_size
        self.forward_pass_index_to_agent_key[idx] = agent_key
        self.agent_key_to_forward_pass_index[agent_key] = idx
        self.forward_pass_indices[0].append(t)
        self.forward_pass_indices[1].append(b)
        self.forward_pass_size += 1

    def reset_forward_pass(self):
        self.forward_pass_size = 0
        #self.forward_pass_indices[0].clear()
        #self.forward_pass_indices[1].clear()
        #self.forward_pass_index_to_agent_key.clear()
        #self.agent_key_to_forward_pass_index.clear()

    def get_sample_batch_and_reset(self) -> SampleBatch:
        """Returns a SampleBatch carrying all previously added data.

        If a reset happens and the trajectory is not done yet, we'll keep the
        entire ongoing trajectory in memory for Model view requirement purposes
        and only actually free the data, once the episode ends.

        Returns:
            SampleBatch: A SampleBatch containing data for the Policy.
        """

        # Convert all our data to numpy arrays, compress float64 to float32,
        # and add the last observation data as well (always one more obs than
        # all other columns due to the additional obs returned by Env.reset()).
        data = {}
        for k, v in self.buffers.items():
            data[k] = to_float_array(
                v[:,self.sample_batch_offset:self.agent_dim_cursor])  #, reduce_floats=True)
        batch = SampleBatch(
            data,
            _initial_inputs=self._inputs,
            _seq_lens=np.array(self.seq_lens) if self.seq_lens else None)

        assert SampleBatch.UNROLL_ID in batch.data

        # Leave buffers as-is and move the sample_batch offset to cursor.
        # Then build next sample_batch from sample_batch_offset on.
        self.sample_batch_offset = self.agent_dim_cursor
        self.seq_lens = []
        return batch

    def _build_buffers(self, single_row) -> None:
        """
        Args:
        """
        time_size = self.num_timesteps + self.shift_before + self.shift_after
        for col, data in single_row.items():
            if col in self.buffers:
                continue
            # Python primitive -> np.array.
            if isinstance(data, (int, float, bool)):
                shape = (time_size, self.num_agents, )
                t_ = type(data)
                dtype = np.float32 if t_ == float else \
                    np.int32 if type(data) == int else np.bool_
                self.buffers[col] = np.zeros(shape=shape, dtype=dtype)
            # np.ndarray, torch.Tensor, or tf.Tensor.
            else:
                shape = (time_size, self.num_agents, ) + data.shape
                dtype = data.dtype
                if torch and isinstance(data, torch.Tensor):
                    self.buffers[col] = torch.zeros(
                        *shape, dtype=dtype, device=data.device)
                elif tf and isinstance(data, tf.Tensor):
                    self.buffers[col] = tf.zeros(shape=shape, dtype=dtype)
                else:
                    self.buffers[col] = np.zeros(shape=shape, dtype=dtype)

    def _extend_buffers(self, sample_batch):
        """Extends the buffers on the batch dimension.

        Args:
            sample_batch (SampleBatch): SampleBatch to determine sizes and
                dtypes of the data columns to be preallocated (zero-filled)
                in case of a new (larger) buffer creation.
        """
        raise NotImplementedError
        sample_batch_size = self.cursor - self.sample_batch_offset
        # SampleBatch to-be-built-next starts in first half of the buffer ->
        # Reallocate a new buffer and copy the currently ongoing SampleBatch
        # into the new buffer.
        if self.sample_batch_offset < self.buffer_size / 2:
            # Double actual horizon.
            self.buffer_size *= 2
            # Store currently ongoing trajectory and build a new buffer.
            old_buffers = self.buffers
            self.buffers = {}
            self._build_buffers(sample_batch)
            # Copy the still ongoing trajectory into the new buffer.
            for col, data in old_buffers.items():
                self.buffers[col][:sample_batch_size] = \
                    data[self.sample_batch_offset:self.cursor]
        # Do an efficient memory swap: Move current SampleBatch
        # to-be-built-next simply to the beginning of the buffer
        # (no reallocation/zero-padding necessary).
        else:
            for col, data in self.buffers.items():
                self.buffers[col][:sample_batch_size] = self.buffers[col][
                    self.sample_batch_offset:self.cursor]

        # Set all pointers to their correct new values.
        self.sample_batch_offset = 0
        self.cursor = sample_batch_size
