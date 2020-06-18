import logging
import numpy as np
from typing import Union

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy

tf = try_import_tf()
torch, _ = try_import_torch()

logger = logging.getLogger(__name__)


@PublicAPI
class Trajectory:
    """A trajectory of a (single) agent throughout one episode.

    Collects all data produced by the environment during stepping of the agent
    as well as all model outputs associated with the agent's Policy.
    NOTE: A Trajectory object may contain remainders of a previous trajectory,
    however, these are only kept for avoiding memory re-allocations. A
    convenience cursor and offset allow for only "viewing" the currently
    ongoing trajectory.
    Pre-allocation happens over a given `horizon` range of timesteps. `horizon`
    may be float("inf"), in which case, we will allocate for some fixed
    n timesteps and double (re-allocate) the buffers each time this limit is
    reached.
    """

    # Disambiguate unrolls within a single episode.
    _next_unroll_id = 0

    @PublicAPI
    def __init__(self, env_id, agent_id, horizon: Union[int,float]):
        """Initializes a Trajectory object.

        Args:
            env_id (str): The env's ID for which we want to store the initial
                observation.
            agent_id (str): The agent's ID whose observation we want to store.
            horizon (Union[int,float]): The max number of timesteps to sample
                in one rollout. Use float("inf") for an unlimited/unknown
                horizon.
        """
        self.env_id = env_id
        self.agent_id = agent_id

        self.horizon = self.actual_horizon = horizon
        if self.horizon == float("inf"):
            self.actual_horizon = 1000

        self.has_initial_obs = False

        self.buffers = {}

        # Cursor into the preallocated buffers. This is where all new data
        # gets inserted.
        self.cursor = 0
        # The offset to be added to self.cursor to get the start of the
        # currently ongoing trajectory.
        self.trajectory_offset = 0

    @property
    def timestep(self):
        # The timestep in the (currently ongoing) trajectory.
        return self.cursor - self.trajectory_offset

    @PublicAPI
    def add_init_obs(self, init_obs):
        """Adds a single initial observation (after env.reset()) to the buffer.

        Args:
            init_obs (any): Initial observation (after env.reset()).
        """
        # Our buffer should be empty when we add the first observation.
        assert self.has_initial_obs is False
        #assert len(self.buffers) == 0 and self.timestep == 0
        self.has_initial_obs = True
        # Build the buffer only for "obs" (needs +1 time step slot for the
        # last observation). Only increase `self.timestep` once we get the
        # other-than-obs data (which will include the next obs).
        obs_buffer = np.zeros(
            shape=(self.actual_horizon + 1,) + init_obs.shape,
            dtype=init_obs.dtype)
        obs_buffer[0] = init_obs
        self.buffers[SampleBatch.OBS] = obs_buffer

    @PublicAPI
    def add_action_reward_next_obs(self, values):
        """Add the given dictionary (row) of values to this batch.

        Args:
            values (Dict[str,any]): Data dict (interpreted as a single row)
                to be added to buffer. Must contain keys: SampleBatch.ACTIONS,
                REWARDS, DONES, and OBS.
        """
        assert self.has_initial_obs is True
        # Only obs exists so far in buffers:
        # Initialize all other columns.
        if len(self.buffers) == 1:
            assert SampleBatch.OBS in self.buffers
            self._build_buffers(single_data=values)

        for k, v in values.items():
            t = self.timestep + (0 if self.has_initial_obs is False or
                                      k != SampleBatch.OBS else 1)
            self.buffers[k][t] = v
        self.cursor += 1

        # Extend (re-alloc) buffers if full and horizon == +inf.
        if self.cursor == self.actual_horizon and \
                self.horizon == float("inf"):
            self._extend_buffers()

    @PublicAPI
    def add_sample_batch(self, sample_batch):
        """Add the given batch of values to this batch.

        Args:
            sample_batch (SampleBatch): The SampleBatch whose data to insert
                into this Trajectory's preallocated buffers.
        """
        # Only obs exists so far in buffers:
        # Initialize all other columns.
        if len(self.buffers) <= 1:
            self._build_buffers(batched_data=sample_batch)

        ts = sample_batch.count
        # Extend (re-alloc) buffers if full and horizon == +inf.
        if self.timestep + ts >= self.actual_horizon and \
                self.horizon == float("inf"):
            self._extend_buffers()

        for k, column in sample_batch.items():
            # For obs column: Always add it to the next timestep b/c obs is
            # initialized with a value before everything else (at env.reset()).
            t = self.timestep
            self.buffers[k][t:t+ts] = column
            if sample_batch.last_obs is not None and k == SampleBatch.OBS:
                self.has_initial_obs = True
                self.buffers[k][t+ts] = sample_batch.last_obs
        self.cursor += ts

    @PublicAPI
    def build_and_reset(self):
        """Returns a SampleBatch carrying all previously added data."""

        data = {}
        for k, v in self.buffers.items():
            data[k] = convert_to_numpy(v[:self.timestep], reduce_floats=True)
        last_obs = convert_to_numpy(
            self.buffers[SampleBatch.OBS][self.timestep],
            reduce_floats=True)
        batch = SampleBatch(data, _last_obs=last_obs)

        # Add unroll ID column to batch if non-existent.
        if SampleBatch.UNROLL_ID not in batch.data:
            batch.data[SampleBatch.UNROLL_ID] = np.repeat(
                Trajectory._next_unroll_id, batch.count)
            Trajectory._next_unroll_id += 1

        self.buffers = {}  #TEST: try this to allow buffers to stick around some more in case other objects are referencing them
                           # instead of: self.buffers.clear()
        self.timestep = 0
        return batch

    def _build_buffers(self, single_data=None, batched_data=None):
        """Creates zero-filled pre-allocated numpy buffers for data collection.

        Except for the obs-column, which should already be initialized (done
        on call to `self.add_initial_observation()`).

        Args:
            single_data (Dict[str,np.ndarray]): Dict of column names (keys) and
                sample numpy data (values). Note: Only one of `single_data` or
                `data_batch` must be provided.
            batched_data (Dict[str,np.ndarray]): Dict of column names (keys)
                and (batched) sample numpy data (values). Note: Only one of
                `single_data` or `data_batch` must be provided.
        """
        for col, data in (
                single_data.items() if single_data else batched_data.items()):
            # Skip already initialized ones, e.g. 'obs' if used with
            # add_initial_observation.
            if col in self.buffers:
                continue
            next_obs_add = 1 if col == SampleBatch.OBS else 0
            # Primitive.
            if isinstance(data, (int, float, bool)):
                shape = (self.actual_horizon + next_obs_add, )
                t_ = type(data)
                dtype = np.float32 if t_ == float else \
                    np.int32 if type(data) == int else np.bool_
                self.buffers[col] = np.zeros(shape=shape, dtype=dtype)
            # np.ndarray, torch.Tensor, or tf.Tensor.
            else:
                shape = (self.actual_horizon + next_obs_add, ) + \
                    (data.shape if single_data else data.shape[1:])
                dtype = data.dtype
                if torch and isinstance(data, torch.Tensor):
                    self.buffers[col] = torch.zeros(
                        *shape, dtype=dtype, device=data.device)
                elif tf and isinstance(data, tf.Tensor):
                    self.buffers[col] = tf.zeros(shape=shape, dtype=dtype)
                else:
                    self.buffers[col] = np.zeros(shape=shape, dtype=dtype)

    def _extend_buffers(self):
        raise NotImplementedError
