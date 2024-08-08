import asyncio
from typing import Optional, TypeVar

import ray

T = TypeVar("T", bound=Optional[object])


@ray.remote(num_cpus=0)  # type: ignore
class SynchronizationActor:
    """A Ray actor that synchronizes the workers in a distributed training job.

    This actor forms a synchronization barrier on a group of processes.
    Every time a worker calls the broadcast_from_rank_zero method,
    the counter is incremented. When the counter equals to the world size,
    the actor notifies all the workers to continue.
    """

    def __init__(self):
        self._counter: int = 0
        self._world_size: Optional[int] = None
        self._condition = asyncio.Condition()
        self._reduced_data = None

    def get_counter(self):
        """Returns the current value of the counter."""
        return self._counter

    def get_world_size(self):
        """Returns the current value of the world_size."""
        return self._world_size

    def get_reduced_data(self):
        """Returns the current value of the reduced_data."""
        return self._reduced_data

    def _clear_states_with_return_val(self):
        """Clears the states of the actor. When the last worker has
        called the _clear_states method, the actor clears its states
        """
        ret_val = self._reduced_data
        self._counter -= 1
        if self._counter == 0:
            self._reduced_data = None
            self._world_size = None
        return ret_val

    async def broadcast_from_rank_zero(
        self, world_rank: int, world_size: int, data: T
    ) -> T:
        """Broadcasts a data from the worker with rank 0 to all other workers.

        This method is a coroutine that blocks until all workers have called this
        method  with the their data. The data from the worker with rank 0 will
        be returned.
        """
        # Ensures that all global states manipulation is done within the async context
        # manager which makes the condition variable awaiting and the counter
        # incrementing an atomic operation.
        async with self._condition:
            if not self._world_size:
                self._world_size = world_size
            elif world_size != self._world_size:
                raise ValueError(
                    f"Expects all callers to provide the same world size. \
                    Got {world_size} and expected {self._world_size}."
                )
            if world_rank == 0:
                self._reduced_data = data
            if self._counter < self._world_size:
                self._counter += 1
            if self._counter == self._world_size:
                self._condition.notify_all()
            else:
                await self._condition.wait()
            ret_val = self._clear_states_with_return_val()
            return ret_val

    # TODO: Implement a general consensus_from_votes method that takes a callable
    # reduce_fn and a list of votes from each worker. The method returns the consensus
