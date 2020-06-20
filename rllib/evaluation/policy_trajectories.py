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
class PolicyTrajectories:
    """A builder class for different Trajectory data used by the same Policy.

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
    def __init__(self, horizon: Union[int, float]):
        """Initializes a PolicyTrajectories object.

        Args:
        """
        # Determine the size of the initial buffers.
        self.horizon = self.actual_horizon = horizon
        if self.horizon == float("inf"):
            self.actual_horizon = 1000

        self.buffers = {}
        # Holds the last observations per added SampleBatch (per env_id,
        # per agent?).
        self.last_obs = {}
        self.cursor = 0

    @PublicAPI
    def add_sample_batch(self, agent_id, sample_batch):
        """Add the given batch of values to this batch.

        Args:
            sample_batch (SampleBatch): The SampleBatch whose data to insert
                into this Trajectory's preallocated buffers.
        """
        if not self.buffers:
            self._build_buffers(sample_batch)

        ts = sample_batch.count
        # Extend (re-alloc) buffers if full and horizon == +inf.
        if self.cursor + ts >= self.actual_horizon and \
                self.horizon == float("inf"):
            self._extend_buffers()

        for k, column in sample_batch.items():
            # For obs column: Always add it to the next timestep b/c obs is
            # initialized with a value before everything else (at env.reset()).
            t = self.cursor
            self.buffers[k][t:t + ts] = column

        if sample_batch.last_obs is not None:
            self.last_obs[agent_id] = sample_batch.last_obs

        self.cursor += ts

    @PublicAPI
    def get_sample_batch_and_reset(self) -> SampleBatch:
        """Returns a SampleBatch carrying all previously added data.

        If a reset happens and the trajectory is not done yet, we'll keep the
        entire ongoing trajectory in memory for Model view requirement purposes
        and only actually free the data, once the episode ends.

        Returns:
            SampleBatch: The SampleBatch containing this agent's data for the
                entire trajectory (so far). The trajectory may not be
                terminated yet.
        """

        # Convert all our data to numpy arrays, compress float64 to float32,
        # and add the last observation data as well (always one more obs than
        # all other columns due to the additional obs returned by Env.reset()).
        data = {}
        for k, v in self.buffers.items():
            data[k] = convert_to_numpy(v[:self.cursor], reduce_floats=True)
        batch = SampleBatch(data, _last_obs=self.last_obs)

        assert SampleBatch.UNROLL_ID in batch.data

        # If done at end -> We can reset our buffers entirely.
        self.buffers.clear()
        self.cursor = self.trajectory_offset = 0
        return batch

    def _build_buffers(self, sample_batch: SampleBatch) -> None:
        """Creates zero-filled pre-allocated numpy buffers for data collection.

        Args:
            sample_batch (SampleBatch):
        """
        for col, data in sample_batch.items():
            # Primitive.
            if isinstance(data, (int, float, bool)):
                shape = (self.actual_horizon, )
                t_ = type(data)
                dtype = np.float32 if t_ == float else \
                    np.int32 if type(data) == int else np.bool_
                self.buffers[col] = np.zeros(shape=shape, dtype=dtype)
            # np.ndarray, torch.Tensor, or tf.Tensor.
            else:
                shape = (self.actual_horizon, ) + data.shape[1:]
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
