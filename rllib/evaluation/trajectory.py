import logging
import numpy as np
from typing import Dict, Optional

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.types import AgentID, EnvID, PolicyID, TensorType

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()

logger = logging.getLogger(__name__)


def to_float_array(v):
    if torch and isinstance(v[0], torch.Tensor):
        arr = torch.stack(v).numpy()  # np.array([s.numpy() for s in v])
    else:
        arr = np.array(v)
    if arr.dtype == np.float64:
        return arr.astype(np.float32)  # save some memory
    return arr


class Trajectory:
    """A trajectory of a (single) agent throughout one episode.

    Note: This is an experimental class only used when
    `config._use_trajectory_view_api` = True.

    Collects all data produced by the environment during stepping of the agent
    as well as all model outputs associated with the agent's Policy into
    pre-allocated buffers of n timesteps capacity (`self.buffer_size`).
    NOTE: A Trajectory object may contain remainders of a previous trajectory,
    however, these are only kept for avoiding memory re-allocations. A
    convenience cursor and offset-pointers allow for only "viewing" the
    currently ongoing trajectory.
    Memory re-allocation into larger buffers (`self.buffer_size *= 2`) only
    happens if unavoidable (in case the buffer is full AND the currently
    ongoing trajectory (episode) takes more than half of the buffer). In all
    other cases, the same buffer is used for succeeding episodes/trejactories
    (even for different agents).
    """

    # Disambiguate unrolls within a single episode.
    _next_unroll_id = 0

    def __init__(self, buffer_size: Optional[int] = None):
        """Initializes a Trajectory object.

        Args:
            buffer_size (Optional[int]): The max number of timesteps to
                fit into one buffer column. When re-allocating
        """
        # The current occupant (agent X in env Y using policy Z) of our
        # buffers.
        self.env_id: EnvID = None
        self.agent_id: AgentID = None
        self.policy_id: PolicyID = None

        # Determine the size of the initial buffers.
        self.buffer_size = buffer_size or 1000
        # The actual buffer holding dict (by column name (str) ->
        # numpy/torch/tf tensors).
        self.buffers = {}

        # Holds the initial observation data.
        self.initial_obs = None

        # Cursor into the preallocated buffers. This is where all new data
        # gets inserted.
        self.cursor: int = 0
        # The offset inside our buffer where the current trajectory starts.
        self.trajectory_offset: int = 0
        # The offset inside our buffer, from where to build the next
        # SampleBatch.
        self.sample_batch_offset: int = 0

    @property
    def timestep(self) -> int:
        """The timestep in the (currently ongoing) trajectory/episode."""
        return self.cursor - self.trajectory_offset

    def add_init_obs(self,
                     env_id: EnvID,
                     agent_id: AgentID,
                     policy_id: PolicyID,
                     init_obs: TensorType) -> None:
        """Adds a single initial observation (after env.reset()) to the buffer.

        Stores it in self.initial_obs.

        Args:
            env_id (EnvID): Unique id for the episode we are adding the initial
                observation for.
            agent_id (AgentID): Unique id for the agent we are adding the
                initial observation for.
            policy_id (PolicyID): Unique id for policy controlling the agent.
            init_obs (TensorType): Initial observation (after env.reset()).
        """
        self.env_id = env_id
        self.agent_id = agent_id
        self.policy_id = policy_id
        self.initial_obs = init_obs

    def add_action_reward_next_obs(self,
                                   env_id: EnvID,
                                   agent_id: AgentID,
                                   policy_id: PolicyID,
                                   values: Dict[str, TensorType]) -> None:
        """Add the given dictionary (row) of values to this batch.

        Args:
            env_id (EnvID): Unique id for the episode we are adding the initial
                observation for.
            agent_id (AgentID): Unique id for the agent we are adding the
                initial observation for.
            policy_id (PolicyID): Unique id for policy controlling the agent.
            values (Dict[str, TensorType]): Data dict (interpreted as a single
                row) to be added to buffer. Must contain keys:
                SampleBatch.ACTIONS, REWARDS, DONES, and OBS.
        """
        assert self.initial_obs is not None
        assert (SampleBatch.ACTIONS in values and SampleBatch.REWARDS in values
                and SampleBatch.NEXT_OBS in values)
        assert env_id == self.env_id
        assert agent_id == self.agent_id
        assert policy_id == self.policy_id

        # Only obs exists so far in buffers:
        # Initialize all other columns.
        if len(self.buffers) == 0:
            self._build_buffers(single_row=values)

        for k, v in values.items():
            self.buffers[k][self.cursor] = v
        self.cursor += 1

        # Extend (re-alloc) buffers if full.
        if self.cursor == self.buffer_size:
            self._extend_buffers(values)

    def get_sample_batch_and_reset(self) -> SampleBatch:
        """Returns a SampleBatch carrying all previously added data.

        If a reset happens and the trajectory is not done yet, we'll keep the
        entire ongoing trajectory in memory for Model view requirement purposes
        and only actually free the data, once the episode ends.

        Returns:
            SampleBatch: The SampleBatch containing this agent's data for the
                entire trajectory (so far). The trajectory may not be
                terminated yet. This SampleBatch object will contain a
                `_last_obs` property, which contains the last observation for
                this agent. This should be used by postprocessing functions
                instead of the SampleBatch.NEXT_OBS field, which is deprecated.
        """
        assert SampleBatch.UNROLL_ID not in self.buffers

        # Convert all our data to numpy arrays, compress float64 to float32,
        # and add the last observation data as well (always one more obs than
        # all other columns due to the additional obs returned by Env.reset()).
        data = {}
        for k, v in self.buffers.items():
            data[k] = to_float_array(
                v[self.sample_batch_offset:self.cursor])

        # Add unroll ID column to batch if non-existent.
        uid = Trajectory._next_unroll_id
        data[SampleBatch.UNROLL_ID] = np.repeat(
            uid, self.cursor - self.sample_batch_offset)

        inputs = {uid: {}}
        if "t" in self.buffers:
            if self.buffers["t"][self.sample_batch_offset] > 0:
                for k in self.buffers.keys():
                    inputs[uid][k] = \
                        self.buffers[k][self.sample_batch_offset - 1]
            else:
                inputs[uid][SampleBatch.NEXT_OBS] = self.initial_obs
        else:
            inputs[uid][SampleBatch.NEXT_OBS] = self.initial_obs

        Trajectory._next_unroll_id += 1

        batch = SampleBatch(data, _initial_inputs=inputs)

        # If done at end -> We can reset our buffers entirely.
        if self.buffers[SampleBatch.DONES][self.cursor - 1]:
            # Set self.timestep to 0 -> new trajectory w/o re-alloc (not yet,
            # only ever re-alloc when necessary).
            self.trajectory_offset = self.sample_batch_offset = self.cursor
        # No done at end -> leave trajectory_offset as is (trajectory is still
        # ongoing), but move the sample_batch offset to cursor.
        else:
            self.sample_batch_offset = self.cursor
        return batch

    def _build_buffers(self, single_row):
        """Creates zero-filled pre-allocated numpy buffers for data collection.

        Except for the obs-column, which should already be initialized (done
        on call to `self.add_initial_observation()`).

        Args:
            single_row (Dict[str,np.ndarray]): Dict of column names (keys) and
                sample numpy data (values). Note: Only one of `single_data` or
                `data_batch` must be provided.
        """
        for col, data in single_row.items():
            # Skip already initialized ones, e.g. 'obs' if used with
            # add_initial_observation.
            if col in self.buffers:
                continue
            self.buffers[col] = [None] * self.buffer_size

    def _extend_buffers(self, single_row):
        """Extends the buffers (depending on trajectory state/length).

        - Extend all buffer lists (x2) if trajectory starts at 0 (trajectory is
            longer than current self.buffer_size).
        - Trajectory starts in first half of buffer: Create new buffer lists
            (2x buffer sizes) and move Trajectory to beginning of new buffer.
        - Trajectory starts in last half of buffer: Leave buffer as is, but
            move trajectory to very front (cursor=0).

        Args:
            single_row (dict): Data dict example to use in case we have to
                re-build buffer.
        """
        traj_length = self.cursor - self.trajectory_offset

        # Trajectory starts at 0 (meaning episodes are longer than current
        # `self.buffer_size` -> Simply do a resize (enlarge) on each column
        # in the buffer.
        if self.trajectory_offset == 0:
            # Double actual horizon.
            for col, data in self.buffers.items():
                self.buffers[col].extend([None] * self.buffer_size)
            self.buffer_size *= 2

        # Trajectory starts in first half of the buffer -> Reallocate a new
        # buffer and copy the currently ongoing trajectory into the new buffer.
        elif self.trajectory_offset < self.buffer_size / 2:
            # Double actual horizon.
            self.buffer_size *= 2
            # Store currently ongoing trajectory and build a new buffer.
            old_buffers = self.buffers
            self.buffers = {}
            self._build_buffers(single_row)
            # Copy the still ongoing trajectory into the new buffer.
            for col, data in old_buffers.items():
                self.buffers[col][:traj_length] = data[self.trajectory_offset:
                                                       self.cursor]

        # Do an efficient memory swap: Move current trajectory simply to
        # the beginning of the buffer (no reallocation/None-padding necessary).
        else:
            for col, data in self.buffers.items():
                self.buffers[col][:traj_length] = self.buffers[col][
                    self.trajectory_offset:self.cursor]

        # Set all pointers to their correct new values.
        self.sample_batch_offset = (
            self.sample_batch_offset - self.trajectory_offset)
        self.trajectory_offset = 0
        self.cursor = traj_length
