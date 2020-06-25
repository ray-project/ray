import logging
import numpy as np
from typing import Optional

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy

tf = try_import_tf()
torch, _ = try_import_torch()

logger = logging.getLogger(__name__)


@PublicAPI
class PolicyTrajectories:
    """Aggregator for many single-agent SampleBatches (all using same Policy).

    Each incoming SampleBatch must be from a Trajectory object (collecting
    data for one single agent).
    Also, all incoming data must be for the same Policy.

    Pre-allocation happens over a given `buffer_size` range of timesteps.
    When the buffers are full, we will attempt to move the currently
    accumulating batch in an efficient manner to either a new buffer or
    - if possible - to the beginning of the same buffer (to avoid
    re-allocation).
    """

    @PublicAPI
    def __init__(self, buffer_size: Optional[int]):
        """Initializes a PolicyTrajectories object.

        Args:
            buffer_size (Optional[int]): The max number of timesteps to
                fit into one buffer column.
        """
        # Determine the size of the initial buffers.
        self.buffer_size = buffer_size or 1000
        self.buffers = {}

        self.policy_id = None

        self.cursor = 0
        self.sample_batch_offset = 0

    @PublicAPI
    def add_sample_batch(self, sample_batch):
        """Add the given batch of values to this batch.

        Args:
            sample_batch (SampleBatch): The SampleBatch whose data to insert
                into this Trajectory's preallocated buffers.
        """

        assert len(set(sample_batch[SampleBatch.AGENT_INDEX])) == 1

        if not self.buffers:
            self._build_buffers(sample_batch)

        ts = sample_batch.count
        # Extend (re-alloc) buffers if full.
        if self.cursor + ts >= self.buffer_size:
            self._extend_buffers(sample_batch)

        for k, column in sample_batch.items():
            self.buffers[k][self.cursor:self.cursor + ts] = column

        self.cursor += ts

    @PublicAPI
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
            data[k] = convert_to_numpy(
                v[self.sample_batch_offset:self.cursor], reduce_floats=True)
        batch = SampleBatch(data)

        assert SampleBatch.UNROLL_ID in batch.data

        # Leave buffers as-is and move the sample_batch offset to cursor.
        # Then build next sample_batch from sample_batch_offset on.
        self.sample_batch_offset = self.cursor
        return batch

    def _build_buffers(self, sample_batch: SampleBatch) -> None:
        """Creates zero-filled pre-allocated numpy buffers for data collection.

        Args:
            sample_batch (SampleBatch): SampleBatch to determine sizes and
                dtypes of the data columns to be preallocated (zero-filled).
        """
        for col, data in sample_batch.items():
            # Primitive.
            if isinstance(data, (int, float, bool)):
                shape = (self.buffer_size, )
                t_ = type(data)
                dtype = np.float32 if t_ == float else \
                    np.int32 if type(data) == int else np.bool_
                self.buffers[col] = np.zeros(shape=shape, dtype=dtype)
            # np.ndarray, torch.Tensor, or tf.Tensor.
            else:
                shape = (self.buffer_size, ) + data.shape[1:]
                dtype = data.dtype
                if torch and isinstance(data, torch.Tensor):
                    self.buffers[col] = torch.zeros(
                        *shape, dtype=dtype, device=data.device)
                elif tf and isinstance(data, tf.Tensor):
                    self.buffers[col] = tf.zeros(shape=shape, dtype=dtype)
                else:
                    self.buffers[col] = np.zeros(shape=shape, dtype=dtype)

    def _extend_buffers(self, sample_batch):
        """Extends the buffer or moves current data from the end to the start.

        Args:
            sample_batch (SampleBatch): SampleBatch to determine sizes and
                dtypes of the data columns to be preallocated (zero-filled)
                in case of a new (larger) buffer creation.
        """
        sample_batch_size = self.cursor - self.sample_batch_offset
        # SampleBatch to-b-built-next starts in first half of the buffer ->
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
