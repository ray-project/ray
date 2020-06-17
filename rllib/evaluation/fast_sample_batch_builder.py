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
class _FastSampleBatchBuilder:
    """Util to build a SampleBatch from pre-allocated (per-column) buffers.

    Pre-allocation happens over a given `horizon` range of timesteps. `horizon`
    may be float("inf"), in which case, we will allocate for some fixed
    n timesteps and double (re-allocate) the buffers each time this limit is
    reached.
    """

    # Disambiguate unrolls within a single episode.
    _next_unroll_id = 0

    @PublicAPI
    def __init__(self, horizon: Union[int,float]):
        """Initializes a _FastSampleBatchBuilder object.

        Args:
            horizon (Union[int,float]): The max number of timesteps to sample
                in one rollout. Use float("inf") for an unlimited/unknown
                horizon.
        """
        self.horizon = self.actual_horizon = horizon
        if self.horizon == float("inf"):
            self.actual_horizon = 1000
        self.name_observation_col = SampleBatch.OBS
        self.buffers = {}
        self.timestep = 0
        #self.count = 0

    def add_initial_observation(self, env_id, agent_id, observation):
        """Adds a single initial observation (after env.reset()) to the buffer.

        Args:
            env_id (str): The env's ID for which we want to store the initial
                observation.
            agent_id (str): The agent's ID whose observation we want to store.
            obs (any): Initial observation (after env.reset()).
        """
        # Our buffer should be empty when we add the first observation.
        assert len(self.buffers) == 0 and self.timestep == 0
        self.buffers["env_id"] = env_id
        self.buffers["agent_id"] = agent_id
        # Build the buffer only for "obs" (needs +1 time step slot for the
        # last observation). Only increase `self.timestep` once we get the
        # other-than-obs data (which will include the next obs).
        obs_buffer = np.zeros(
            shape=(self.actual_horizon + 1,) + observation.shape,
            dtype=observation.dtype)
        obs_buffer[0] = observation
        self.buffers[self.name_observation_col] = obs_buffer
        

    @PublicAPI
    def add_values(self, **values):
        """Add the given dictionary (row) of values to this batch.

        Args:
            **values (Dict[str,any]): Data dict (interpreted as a single row)
                to be added to buffer.
        """
        # Only obs, env_id, agent_id exist so far in buffers:
        # Initialize all other columns.
        if len(self.buffers) == 3:
            self._build_buffers(single_data=values)

        for k, v in values.items():
            t = self.timestep + (0 if k != self.name_observation_col else 1)
            self.buffers[k][t] = v
        self.timestep += 1

        # Extend (re-alloc) buffers if full and horizon == +inf.
        if self.timestep == self.actual_horizon and \
                self.horizon == float("inf"):
            self._extend_buffers()

    @PublicAPI
    def add_batch(self, batch):
        """Add the given batch of values to this batch.

        Args:
            batch (SampleBatch): The SampleBatch whose data to insert into this
                SampleBatchBuilder's preallocated buffers.
        """
        # Only obs, env_id, agent_id exist so far in buffers:
        # Initialize all other columns.
        if len(self.buffers) == 3:
            self._build_buffers(batched_data=batch)

        ts = batch.count
        # Extend (re-alloc) buffers if full and horizon == +inf.
        if self.timestep + ts >= self.actual_horizon and \
                self.horizon == float("inf"):
            self._extend_buffers()

        for k, column in batch.items():
            # For obs column: Always add it to the next timestep b/c obs is
            # initialized with a value before everything else (at env.reset()).
            t = self.time_step + (0 if k != self.name_observation_col else 1)
            self.buffers[k][t:t+ts] = column
        self.time_step += ts

    @PublicAPI
    def build_and_reset(self):
        """Returns a SampleBatch carrying all previously added data."""

        batch = SampleBatch(
            {k: convert_to_numpy(v, reduce_floats=True)
             for k, v in self.buffers.items() if k not in ["env_id", "agent_id"]})

        # Add unroll ID column to batch if non-existent.
        if SampleBatch.UNROLL_ID not in batch.data:
            batch.data[SampleBatch.UNROLL_ID] = np.repeat(
                _FastSampleBatchBuilder._next_unroll_id, batch.count)
            _FastSampleBatchBuilder._next_unroll_id += 1

        self.buffers.clear()
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
        assert len(self.buffers) == 3  # obs, env_id, agent_id

        for col, data in (
                single_data.items() if single_data else batched_data.items()):
            # Skip `obs`: Already initialized.
            if col == self.name_observation_col:
                continue
            # Primitive.
            if isinstance(data, (int, float, bool)):
                shape = (self.actual_horizon, )
                t_ = type(data)
                dtype = np.float32 if t_ == float else \
                    np.int32 if type(data) == int else np.bool_
                self.buffers[col] = np.zeros(shape=shape, dtype=dtype)
            # np.ndarray, torch.Tensor, or tf.Tensor.
            else:
                shape = (self.actual_horizon,) + \
                    (data.shape if single_data else data.shape[1:])
                dtype = data.dtype
                if torch and isinstance(data, torch.Tensor):
                    self.buffers[col] = torch.zeros(
                        *shape, dtype=dtype, device=data.device)
                elif tf and isinstance(data, tf.Tensor):
                    self.buffers[col] = tf.zeros(shape=shape, dtype=dtype)
                else:
                    self.buffers[col] = np.zeros(shape=shape, dtype=dtype)
