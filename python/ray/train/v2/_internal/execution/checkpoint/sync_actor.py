import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Collection, Dict, List, Optional, Set, Tuple, TypeVar

import ray
from ray.train.v2._internal.constants import (
    COLLECTIVE_WARN_INTERVAL_S_ENV_VAR,
    DEFAULT_COLLECTIVE_TIMEOUT_S,
    DEFAULT_COLLECTIVE_WARN_INTERVAL_S,
)
from ray.train.v2._internal.exceptions import BroadcastCollectiveTimeoutError
from ray.train.v2._internal.util import wait_with_logging

T = TypeVar("T", bound=Optional[object])
logger = logging.getLogger(__name__)


class SynchronizationBarrierResetError(Exception):
    """Raised when the synchronization barrier is reset, e.g. due to a worker failure."""

    pass


BROADCAST_PERIODIC_WARNING = """
`{caller_method_name}` has not been called by all {world_size} workers in the group.
The workers have been waiting for {max_time_elapsed_s:.2f} s for the following ranks to join the `{caller_method_name}` call: {missing_ranks}.
Also ensure that workers are not hanging on other operations, causing them to miss this synchronization barrier.
You can set the {warn_interval_env_var} environment variable to change the frequency of this warning (current value: {warn_interval_s} s).
"""


@ray.remote(num_cpus=0)  # type: ignore
class SynchronizationActor:
    """A Ray actor that synchronizes the workers in a distributed training job.

    This actor forms a synchronization barrier on a group of processes.
    Every time a worker calls the broadcast_from_rank_zero method, its rank is
    recorded. Once every *expected* rank has joined, the actor notifies all the
    workers to continue.

    The expected set defaults to all ``world_size`` ranks. During a node
    preemption the controller may narrow it to the surviving ranks via
    :meth:`set_expected_ranks`, so that a rank already reclaimed by the cloud
    provider does not strand the healthy ranks at the barrier.
    """

    def __init__(
        self,
        timeout_s: Optional[float] = DEFAULT_COLLECTIVE_TIMEOUT_S,
        warn_interval_s: float = DEFAULT_COLLECTIVE_WARN_INTERVAL_S,
    ):
        self._counter: int = 0
        self._world_size: int = 0
        # Ranks currently inside the barrier.
        self._arrived_ranks: Set[int] = set()
        # Sequence numbers of the collectives that each rank is currently inside.
        self._arrived_seq: Dict[int, Optional[int]] = {}
        # Whether the collective currently at the barrier opted in to
        # relaxation. Currently only Ray Train's own preemption and checkpoint
        # collectives do.
        self._relaxable_collective: bool = False
        # Ranks that must join before the barrier releases. None means strict barrier.
        self._expected_ranks: Optional[Set[int]] = None
        # Sequence number of the collective that most recently released the barrier.
        self._released_seq: Optional[int] = None
        self._condition = asyncio.Condition()
        self._reduced_data = None
        self._reset = False
        # The time when workers from different ranks
        # enters the synchronization barrier.
        self._sync_start_times: List[Optional[float]] = []
        # The timeout in seconds for the synchronization barrier.
        self._timeout_s: Optional[float] = timeout_s
        # The interval in seconds to log a warning when waiting for the barrier.
        self._warn_interval_s: float = warn_interval_s

    def get_counter(self):
        """Returns the current value of the counter."""
        return self._counter

    def get_world_size(self):
        """Returns the current value of the world_size."""
        return self._world_size

    def get_reduced_data(self):
        """Returns the current value of the reduced_data."""
        return self._reduced_data

    def get_expected_ranks(self) -> Optional[List[int]]:
        """The relaxed expected-rank set, or None if all ranks are required."""
        if self._expected_ranks is None:
            return None
        return sorted(self._expected_ranks)

    async def set_expected_ranks(self, ranks: Optional[Collection[int]]) -> bool:
        """Release the barrier once `ranks` have joined, instead of every rank.

        Args:
            ranks: The ranks that must join before the barrier releases, or
                None to require every rank in the world.

        Returns:
            True if the expected set was applied.
        """
        async with self._condition:
            if ranks is None:
                self._expected_ranks = None
                return True

            expected = set(ranks)
            if 0 not in expected:
                logger.warning(
                    "Refusing to relax the synchronization barrier to ranks %s: "
                    "rank 0 is the only writer of the broadcast payload, so "
                    "releasing without it would broadcast None to every "
                    "survivor. Keeping the strict barrier.",
                    sorted(expected),
                )
                return False

            self._expected_ranks = expected
            logger.info(
                "Synchronization barrier relaxed to expect ranks %s.",
                sorted(expected),
            )

            # Release existing waiters if the expected ranks have already arrived.
            if self._world_size:
                should_release, release_seq = self._barrier_release(
                    self._relaxable_collective
                )
                if should_release:
                    self._released_seq = release_seq
                    self._condition.notify_all()
            return True

    def _effective_expected_ranks(self) -> Set[int]:
        """The ranks that must join before the barrier releases."""
        if self._expected_ranks is None:
            return set(range(self._world_size))
        return self._expected_ranks

    def _barrier_release(self, relaxable: bool) -> Tuple[bool, Optional[int]]:
        """Whether to release the barrier, and which collective is released.

        Args:
            relaxable: Whether the collective being evaluated opted in to
                relaxation.

        Returns:
            ``(False, None)`` to keep waiting. Otherwise ``(True, seq)``, where
            ``seq`` is the collective being released, or ``None`` when the
            barrier released without consulting sequence numbers.
        """
        if self._expected_ranks is None or not relaxable:
            if len(self._arrived_ranks) < self._world_size:
                return False, None
            return True, None

        if not self._expected_ranks.issubset(self._arrived_ranks):
            return False, None
        seqs = {self._arrived_seq.get(rank) for rank in self._expected_ranks}
        if len(seqs) != 1:
            return False, None
        return True, seqs.pop()

    def _clear_states(self, world_rank: int):
        """Clears the states of the actor. When the last worker has
        called the _clear_states method, the actor clears its states
        """
        self._counter -= 1
        self._arrived_ranks.discard(world_rank)
        self._arrived_seq.pop(world_rank, None)
        if self._counter == 0:
            self._reduced_data = None
            self._world_size = 0
            self._arrived_ranks.clear()
            self._arrived_seq.clear()
            self._relaxable_collective = False
            self._released_seq = None
            self._reset = False
            self._condition.notify_all()

    async def _setup_or_validate_collective_op(self, world_size: int):
        """The setup method for the synchronization actor if it is not setup yet.
        It initializes the world size and the start times for the
        synchronization barrier.
        """
        # Wait for previous collective reset to finish.
        await self._condition.wait_for(lambda: not self._reset)
        if self._world_size == 0:
            self._world_size = world_size
            self._sync_start_times = [None] * world_size
        elif world_size != self._world_size:
            raise ValueError(
                f"Expected all callers to provide the same world size. \
                Got {world_size} and expected {self._world_size}."
            )

    @asynccontextmanager
    async def _broadcast_collective_context_manager(
        self,
        world_rank: int,
        world_size: int,
        data: T,
        collective_seq: Optional[int] = None,
        relaxable: bool = False,
    ):
        """A context manager that ensures the synchronization barrier is lifted
        after the block of code is executed.
        """
        try:
            await self._setup_or_validate_collective_op(world_size)
            if world_rank == 0:
                self._reduced_data = data
            if self._counter < self._world_size:
                self._counter += 1
            self._arrived_ranks.add(world_rank)
            self._arrived_seq[world_rank] = collective_seq
            self._relaxable_collective = relaxable
            yield
        finally:
            self._clear_states(world_rank)

    def _get_time_elapsed(self) -> Optional[float]:
        """Return the time elapsed since the first worker entered the barrier.
        If no workers have entered the barrier, returns None.
        """
        start_times = [t for t in self._sync_start_times if t is not None]
        if not start_times:
            return None

        return asyncio.get_event_loop().time() - min(start_times)

    def get_released_seq(self) -> Optional[int]:
        """The collective most recently released, for tests and debugging."""
        return self._released_seq

    def _get_missing_ranks(self) -> List[int]:
        """Returns the expected ranks that have not entered the barrier."""
        return sorted(self._effective_expected_ranks() - self._arrived_ranks)

    def _generate_broadcast_periodic_warning(self, caller_method_name: str) -> str:
        """Generates the warning message for the broadcast periodic warning."""

        return BROADCAST_PERIODIC_WARNING.format(
            caller_method_name=caller_method_name,
            world_size=self._world_size,
            max_time_elapsed_s=self._get_time_elapsed(),
            missing_ranks=self._get_missing_ranks(),
            warn_interval_env_var=COLLECTIVE_WARN_INTERVAL_S_ENV_VAR,
            warn_interval_s=self._warn_interval_s,
        )

    async def reset(self):
        """Reset the synchronization barrier, unblocking any waiting workers.

        If no workers are currently at the barrier, this is a no-op.
        Waiting workers will raise SynchronizationBarrierResetError.
        The actor remains alive and usable for subsequent barriers.
        """
        async with self._condition:
            if self._counter == 0:
                return
            self._reset = True
            self._condition.notify_all()

    async def broadcast_from_rank_zero(
        self,
        world_rank: int,
        world_size: int,
        data: T,
        caller_method_name: str,
        collective_seq: Optional[int] = None,
        relaxable: bool = False,
    ) -> T:
        """Broadcasts a data from the worker with rank 0 to all other workers.

        This method is a coroutine that blocks until all workers have called this
        method  with the their data. The data from the worker with rank 0 will
        be returned.

        Args:
            world_rank: The rank of the worker that calls this method.
            world_size: The total number of workers in the group.
            data: The data to broadcast.
            caller_method_name: The name of the method that calls this method.
            collective_seq: Identifies which collective this call belongs to.
                Every worker enters the same collectives in the same order, so
                these agree across workers. Defaults to None, which restores
                the unsequenced behavior.
            relaxable: Whether this collective may be released without the
                ranks excluded by :meth:`set_expected_ranks`. Currently only Ray Train's
                own preemption and checkpoint collectives opt in; the public
                ``ray.train.collective.*`` APIs keep their all-rank contract
                even while a preemption is in progress. Defaults to False.

        Returns:
            The data broadcasted from the worker with rank 0.
        """
        # TODO: resolve https://github.com/ray-project/ray/pull/54066#discussion_r2180657435
        # We couldn't reproduce the issue but the asyncio docs don't say it can't happen.

        # Ensures that all global states manipulation is done within the async context
        # manager which makes the condition variable awaiting and the counter
        # incrementing an atomic operation.
        async with self._condition:
            async with self._broadcast_collective_context_manager(
                world_rank, world_size, data, collective_seq, relaxable
            ):
                # Once every expected rank has joined, notify all the workers to continue.
                should_release, release_seq = self._barrier_release(relaxable)
                if should_release:
                    self._released_seq = release_seq
                    self._condition.notify_all()
                    return self._reduced_data
                use_seq = (
                    relaxable
                    and self._expected_ranks is not None
                    and collective_seq is not None
                )
                try:
                    current_time = asyncio.get_event_loop().time()
                    self._sync_start_times[world_rank] = current_time
                    await wait_with_logging(
                        self._condition,
                        predicate=(
                            lambda: self._reset or self._released_seq == collective_seq
                        )
                        if use_seq
                        else None,
                        generate_warning_message=(
                            lambda: self._generate_broadcast_periodic_warning(
                                caller_method_name
                            )
                        )
                        if world_rank == 0
                        else None,
                        warn_interval_s=self._warn_interval_s,
                        timeout_s=self._timeout_s,
                    )
                    if self._reset:
                        raise SynchronizationBarrierResetError(
                            "Synchronization barrier was reset, likely due "
                            "to a worker failure and replica group replacement."
                        )
                    return self._reduced_data
                except (asyncio.TimeoutError, TimeoutError) as e:
                    raise BroadcastCollectiveTimeoutError(
                        time_elapsed=self._get_time_elapsed(),
                        missing_ranks=self._get_missing_ranks(),
                        timeout_s=self._timeout_s,
                    ) from e

    # TODO: Implement a general consensus_from_votes method that takes a callable
    # reduce_fn and a list of votes from each worker. The method returns the consensus
