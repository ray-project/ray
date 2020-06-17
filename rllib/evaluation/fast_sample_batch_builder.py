import logging
import numpy as np
from typing import Union

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import PublicAPI

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
        self.initial_obs = None
        self.buffers = {}
        self.time_step = 0

    def add_initial_observation(self, obs):
        """Adds a single initial observation (after env.reset()) to the buffer.

        Args:
            obs (any): Initial observation (after env.reset()).
        """
        # Our buffer should be empty when we add the first observation.
        assert self.initial_obs is None and self.time_step == 0
        # Keep timestep as is (0). Only increase once we get the other-than-obs
        # data (which will include the next obs).
        self.initial_obs = obs

    @PublicAPI
    def add_values(self, **values):
        """Add the given dictionary (row) of values to this batch.

        Args:
            **values (Dict[str,any]): Data dict (interpreted as a single row)
                to be added to buffer.
        """
        if len(self.buffers) == 0:
            self._build_buffers(single_data=values)

        t = self.time_step
        for k, v in values.items():
            self.buffers[k][t] = v
        self.time_step += 1

        # Extend (re-alloc) buffers if full and horizon == +inf.
        if self.time_step == self.actual_horizon and \
                self.horizon == float("inf"):
            self._extend_buffers()

    @PublicAPI
    def add_batch(self, batch):
        """Add the given batch of values to this batch.

        Args:
            batch (SampleBatch): The SampleBatch whose data to insert into this
                SampleBatchBuilder's preallocated buffers.
        """
        if len(self.buffers) == 0:
            self._build_buffers(batched_data=values)

        ts = batch.count
        # Extend (re-alloc) buffers if full and horizon == +inf.
        if self.time_step + ts >= self.actual_horizon and \
                self.horizon == float("inf"):
            self._extend_buffers()

        for k, column in batch.items():
            # For "obs" column: Always add it to the next timestep b/c obs is
            # initialized with a value before everything else (at env.reset()).
            t = self.time_step + (0 if k != "obs" else 1)
            self.buffers[k][t:t+ts] = column
        self.time_step += ts

    @PublicAPI
    def build_and_reset(self):
        """Returns a SampleBatch carrying all previously added data."""

        batch = SampleBatch(
            {k: to_float_array(v)
             for k, v in self.buffers.items()})

        # Add unroll ID column to batch if non-existent.
        if SampleBatch.UNROLL_ID not in batch.data:
            batch.data[SampleBatch.UNROLL_ID] = np.repeat(
                _FastSampleBatchBuilder._next_unroll_id, batch.count)
            _FastSampleBatchBuilder._next_unroll_id += 1

        self.buffers.clear()
        self.time_step = 0
        return batch

    def _build_buffers(self, single_data=None, batched_data=None):
        """Creates zero-filled pre-allocated numpy buffers for data collection.

        Args:
            single_data (Dict[str,np.ndarray]): Dict of column names (keys) and
                sample numpy data (values). Note: Only one of `single_data` or
                `data_batch` must be provided.
            batched_data (Dict[str,np.ndarray]): Dict of column names (keys) and
                (batched) sample numpy data (values). Note: Only one of
                `single_data` or `data_batch` must be provided.
        """
        self.buffers = {}
        for col, data in (
                single_data.items() if single_data else batched_data.items()):
            shape = (self.actual_horizon,) + (data.shape if single_data else
                                              data.shape[1:])
            self.buffers[col] = np.zeros(shape=shape, dtype=data.dtype)

        # Fill in our initial obs.
        self.buffers["obs"][0] = self.initial_obs
