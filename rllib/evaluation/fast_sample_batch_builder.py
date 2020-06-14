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
        self.buffers = None
        self.time_step = 0

    @PublicAPI
    def add_values(self, **values):
        """Add the given dictionary (row) of values to this batch."""

        # Create buffers if non-existent.
        if self.buffers is None:
            self._build_buffers(values)

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

        if self.buffers is None:
            self._build_buffers(batch)

        t = self.time_step
        ts = batch.count

        # Extend (re-alloc) buffers if full and horizon == +inf.
        if t + ts >= self.actual_horizon and \
                self.horizon == float("inf"):
            self._extend_buffers()

        for k, column in batch.items():
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

    def _build_buffers(self, single_data=None, data_batch=None):
        """Creates zero-filled pre-allocated numpy buffers for data collection.

        Args:
            single_data (Dict[str,np.ndarray]): Dict of column names (keys) and
                sample numpy data (values). Note: Only one of `single_data` or
                `data_batch` must be provided.
            data_batch (Dict[str,np.ndarray]): Dict of column names (keys) and
                (batched) sample numpy data (values). Note: Only one of
                `single_data` or `data_batch` must be provided.
        """
        self.buffers = {}
        for k, d in (
                single_data.items() if single_data else data_batch.items()):
            shape = (self.actual_horizon,) + (d.shape if single_data else
                                              d.shape[1:])
            self.buffers[k] = np.zeros(shape=shape, dtype=d.dtype)
