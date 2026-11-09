"""Per-dataset object-store accounting for the actor-only streaming executor.

Call the mutating APIs in this order. Queries
(``live_object_store`` / ``cumulative_object_store`` / ``node_capacity``)
are safe anytime on the executor thread.

::

    set_node_view                  # once per scheduling tick
    drain_consumed_blocks          # once per tick; applies queued consumptions
         |
         |  maybe_register_actor   # first sighting of an actor on a node
         |       |
         |       |  on_task_submitted
         |       |       |
         |       |       |-- on_new_output          # each pulled block
         |       |       |-- on_block_consumed      # consumer thread (queued)
         |       |       |                          # or submit-time input withdraw
         |       |       |
         |       |  on_task_completed               # drops in-flight inputs
         |       |
         |  deregister_actor       # downscale / death
         |
    finalize_dataset               # end of the execution

An actor lives on one node for its whole life. A dead actor is deregistered
like a downscaled one.
"""

from __future__ import annotations

import logging
import math
import threading
from abc import ABC
from dataclasses import dataclass, field
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Protocol,
    Set,
    Tuple,
    runtime_checkable,
)

from typing_extensions import override

import ray
from ray.data._internal.execution.execution_flags import (
    CORE_ACTOR_BACKPRESSURE_NUM_OBJECTS_PER_YIELD,
    actor_only_backend_enabled,
)
from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    NodeIdStr,
    PhysicalOperator,
)
from ray.data._internal.execution.interfaces.common import ActorId
from ray.data._internal.execution.interfaces.distribution_tracker import (
    DistributionTracker,
)
from ray.data.block import Block, BlockMetadata
from ray.types import ObjectRef

logger = logging.getLogger(__name__)

# Fallback average-bytes-per-output when no samples exist yet (128 MiB).
DEFAULT_BYTES_PER_OUTPUT = 128 << 20


def get_alive_nodes_uncached() -> Dict[NodeIdStr, ExecutionResources]:
    """Fresh ``ray.nodes()`` snapshot; no TTL cache.

    Use this when a miss against a stale/incomplete view must see nodes that
    just joined (e.g. actor landed before the coordinator allocation cache
    warmed).
    """
    node_resources: Dict[NodeIdStr, ExecutionResources] = {}
    for node_info in ray.nodes():
        if not node_info["alive"]:
            continue
        node_id = node_info["NodeID"]
        raw = node_info["Resources"]
        resources = ExecutionResources.from_resource_dict(raw)
        node_resources[node_id] = resources
    return node_resources


@runtime_checkable
class ActorPoolAccounting(Protocol):
    """The part of an actor pool that ``ResourceBank`` needs in order to size
    how much object store its prebuffered outputs uses."""

    def max_actor_concurrency(self) -> int:
        ...


@runtime_checkable
class ActorPoolOperator(Protocol):
    """An operator that submits its tasks to a pool of actors.

    A protocol rather than a base class, so operators that don't run on actors
    (read / input / limit) simply fail the `isinstance` check and get no
    prebuffer term.
    """

    _ray_remote_args: Dict[str, Any]
    _ray_actor_task_remote_args: Dict[str, Any]

    @property
    def actor_pool(self) -> ActorPoolAccounting:
        ...


@dataclass(kw_only=True, slots=True, repr=False, eq=False)
class UniqueBlockMetadata:
    # The operator that produced this block
    op: PhysicalOperator
    bm: BlockMetadata
    task_id: ray.TaskID | None = None
    # Held back by Ray Data lineage reconstruction until the child task's whole
    # input set is re-produced (see ``ResourceBank.on_output_withheld``).
    withheld_for_reconstruction: bool = False


@dataclass(kw_only=True, slots=True, repr=False, eq=False)
class CumulativeObjStore:
    """These object store stats should be non-decreasing.
    Fine-grained breakdown of how the object store is being used
    """

    # Outputs (blocks produced from tasks)
    output_bytes: DistributionTracker = field(default_factory=DistributionTracker)
    output_rows: DistributionTracker = field(default_factory=DistributionTracker)

    # Inputs (blocks consumed in tasks)
    input_bytes: DistributionTracker = field(default_factory=DistributionTracker)
    input_rows: DistributionTracker = field(default_factory=DistributionTracker)

    # breakdown of which nodes are local, which are remote
    input_bytes_local: DistributionTracker = field(default_factory=DistributionTracker)
    input_bytes_remote: DistributionTracker = field(default_factory=DistributionTracker)

    def on_input_submitted(
        self,
        *,
        bm: BlockMetadata,
        remote: bool,
    ):
        size_bytes = bm.size_bytes or 0
        if remote:
            self.input_bytes_remote.add_sample(size_bytes)
        else:
            self.input_bytes_local.add_sample(size_bytes)

        self.input_rows.add_sample(bm.num_rows or 0)
        self.input_bytes.add_sample(size_bytes)

    def on_new_output(
        self,
        *,
        bm: BlockMetadata,
    ):
        self.output_bytes.add_sample(bm.size_bytes or 0)
        self.output_rows.add_sample(bm.num_rows or 0)

    @staticmethod
    def _merged_tracker(
        *, left: DistributionTracker, right: DistributionTracker
    ) -> DistributionTracker:
        tracker = DistributionTracker()
        tracker.merge(left)
        tracker.merge(right)
        return tracker

    def __add__(self, other: "CumulativeObjStore") -> "CumulativeObjStore":
        return CumulativeObjStore(
            output_bytes=self._merged_tracker(
                left=self.output_bytes, right=other.output_bytes
            ),
            output_rows=self._merged_tracker(
                left=self.output_rows, right=other.output_rows
            ),
            input_bytes=self._merged_tracker(
                left=self.input_bytes, right=other.input_bytes
            ),
            input_rows=self._merged_tracker(
                left=self.input_rows, right=other.input_rows
            ),
            input_bytes_local=self._merged_tracker(
                left=self.input_bytes_local, right=other.input_bytes_local
            ),
            input_bytes_remote=self._merged_tracker(
                left=self.input_bytes_remote, right=other.input_bytes_remote
            ),
        )


@dataclass(kw_only=True, slots=True, repr=False, eq=False)
class LiveObjStore:
    """The current snapshot of each object store, INCLUDING
    prebuffered bytes from _generator_backpressure_num_objects.

    ::

        input ──task──► prebuffered ──pull──► pulled ──consume──► (gone)
           │
           └─drop (task done)

    Counting each copy of a block exactly once puts one requirement on the
    caller. When a block is handed to a task on the same node it was produced
    on, no second copy is made, so the caller must report the producer's copy as
    consumed (`on_block_consumed`) at the same time the task's input is counted.
    When the block goes to a task on another node, Ray really does make a second
    copy, so both are counted and the block shows up twice cluster-wide.
    """

    # Inputs (blocks being consumed in tasks, aka in-flight)
    num_input_blocks: int = 0
    num_input_bytes: int = 0

    # Prebuffered outputs (generated but not yet pulled from generators)
    num_prebuffered_output_blocks: int = 0
    num_prebuffered_output_bytes: int = 0
    num_prebuffered_output_rows: int = 0

    # Pulled outputs (blocks pulled from generators, not yet consumed downstream, actor alive)
    num_pulled_output_blocks: int = 0
    num_pulled_output_bytes: int = 0
    num_pulled_output_rows: int = 0

    # The part of the pulled outputs withheld by lineage reconstruction: no
    # consumer can drain them until their child's whole input set is back.
    num_withheld_output_blocks: int = 0
    num_withheld_output_bytes: int = 0
    num_withheld_output_rows: int = 0

    def on_input_submitted(
        self,
        *,
        bm: BlockMetadata,
    ):
        self.num_input_bytes += bm.size_bytes or 0
        self.num_input_blocks += 1

    def on_new_output(
        self,
        *,
        bm: BlockMetadata,
    ):
        """Add one pulled output"""
        size_bytes = bm.size_bytes or 0
        num_rows = bm.num_rows or 0
        self.num_pulled_output_blocks += 1
        self.num_pulled_output_bytes += size_bytes
        self.num_pulled_output_rows += num_rows

    def on_input_dropped(
        self,
        *,
        task_info: TaskInfo,
    ):
        self.num_input_blocks -= task_info.live.num_input_blocks
        self.num_input_bytes -= task_info.live.num_input_bytes

    def add(self, other: "LiveObjStore") -> None:
        """Add in place"""
        self.num_input_blocks += other.num_input_blocks
        self.num_input_bytes += other.num_input_bytes
        self.num_prebuffered_output_blocks += other.num_prebuffered_output_blocks
        self.num_prebuffered_output_bytes += other.num_prebuffered_output_bytes
        self.num_prebuffered_output_rows += other.num_prebuffered_output_rows
        self.num_pulled_output_blocks += other.num_pulled_output_blocks
        self.num_pulled_output_bytes += other.num_pulled_output_bytes
        self.num_pulled_output_rows += other.num_pulled_output_rows
        self.num_withheld_output_blocks += other.num_withheld_output_blocks
        self.num_withheld_output_bytes += other.num_withheld_output_bytes
        self.num_withheld_output_rows += other.num_withheld_output_rows

    def subtract(self, other: "LiveObjStore") -> None:
        """Subtract in place"""
        self.num_input_blocks -= other.num_input_blocks
        self.num_input_bytes -= other.num_input_bytes
        self.num_prebuffered_output_blocks -= other.num_prebuffered_output_blocks
        self.num_prebuffered_output_bytes -= other.num_prebuffered_output_bytes
        self.num_prebuffered_output_rows -= other.num_prebuffered_output_rows
        self.num_pulled_output_blocks -= other.num_pulled_output_blocks
        self.num_pulled_output_bytes -= other.num_pulled_output_bytes
        self.num_pulled_output_rows -= other.num_pulled_output_rows
        self.num_withheld_output_blocks -= other.num_withheld_output_blocks
        self.num_withheld_output_bytes -= other.num_withheld_output_bytes
        self.num_withheld_output_rows -= other.num_withheld_output_rows

    def charge_withheld_block(self, *, bm: BlockMetadata, delta: int) -> None:
        """Charge (``delta=1``) or uncharge (``delta=-1``) one pulled output to
        the withheld counts. It stays counted as pulled either way."""
        self.num_withheld_output_blocks += delta
        self.num_withheld_output_bytes += delta * (bm.size_bytes or 0)
        self.num_withheld_output_rows += delta * (bm.num_rows or 0)

    def on_output_consumed(
        self,
        *,
        bm: BlockMetadata,
    ):
        """When an input is consumed from a downstream worker, it frees up space for the
        upstream actor. That's why we decrement the output, since it's
        also known as the input to the downstream worker."""
        size_bytes = bm.size_bytes or 0
        num_rows = bm.num_rows or 0
        self.num_pulled_output_blocks -= 1
        self.num_pulled_output_bytes -= size_bytes
        self.num_pulled_output_rows -= num_rows

    def all_outputs_consumed(self) -> bool:
        return self.num_pulled_output_blocks + self.num_prebuffered_output_blocks == 0

    def __add__(self, other: "LiveObjStore") -> "LiveObjStore":
        return LiveObjStore(
            num_pulled_output_blocks=self.num_pulled_output_blocks
            + other.num_pulled_output_blocks,
            num_pulled_output_bytes=self.num_pulled_output_bytes
            + other.num_pulled_output_bytes,
            num_pulled_output_rows=self.num_pulled_output_rows
            + other.num_pulled_output_rows,
            num_prebuffered_output_blocks=self.num_prebuffered_output_blocks
            + other.num_prebuffered_output_blocks,
            num_prebuffered_output_bytes=self.num_prebuffered_output_bytes
            + other.num_prebuffered_output_bytes,
            num_prebuffered_output_rows=self.num_prebuffered_output_rows
            + other.num_prebuffered_output_rows,
            num_input_blocks=self.num_input_blocks + other.num_input_blocks,
            num_input_bytes=self.num_input_bytes + other.num_input_bytes,
            num_withheld_output_blocks=self.num_withheld_output_blocks
            + other.num_withheld_output_blocks,
            num_withheld_output_bytes=self.num_withheld_output_bytes
            + other.num_withheld_output_bytes,
            num_withheld_output_rows=self.num_withheld_output_rows
            + other.num_withheld_output_rows,
        )

    def clone(self) -> "LiveObjStore":
        """A detached snapshot, so query APIs never hand out live state."""
        return LiveObjStore() + self

    def total_bytes(self) -> int:
        return self.get_input_bytes() + self.get_output_bytes()

    def total_blocks(self) -> int:
        return self.get_input_blocks() + self.get_output_blocks()

    def __repr__(self) -> str:
        num_outputs = self.num_pulled_output_blocks + self.num_prebuffered_output_blocks
        return (
            f"input={self.num_input_blocks}, outputs={num_outputs}, "
            f"prebuffered={self.num_prebuffered_output_blocks}"
        )

    def get_output_bytes(self) -> int:
        return self.num_pulled_output_bytes + self.num_prebuffered_output_bytes

    def get_output_blocks(self) -> int:
        return self.num_pulled_output_blocks + self.num_prebuffered_output_blocks

    def get_input_bytes(self) -> int:
        return self.num_input_bytes

    def get_input_blocks(self) -> int:
        return self.num_input_blocks


@runtime_checkable
class TracksObjStore(Protocol):
    def on_task_submitted(
        self,
        *,
        target_actor_id: ActorId | None,
        task_id: ray.TaskID | None,
        target_node_id: NodeIdStr,
        bms: List[BlockMetadata],
    ):
        ...

    def on_new_output(self, *, bm: BlockMetadata, task_id: ray.TaskID | None):
        ...

    def on_task_completed(self, *, task_id: ray.TaskID):
        ...

    def on_output_consumed(self, *, bm: BlockMetadata, task_id: ray.TaskID | None):
        ...


@dataclass(kw_only=True, slots=True, repr=False, eq=False)
class TaskInfo:
    # None for actorless ops (read/input/limit). They generate
    # output through ray.put, or other means
    actor_id: ActorId | None
    task_id: ray.TaskID | None
    node_id: NodeIdStr
    # Set once the task's generator is exhausted. The task -> actor mapping is
    # only safe to drop once the task has completed AND every output block it
    # produced has been consumed downstream.
    completed: bool = False

    live: LiveObjStore = field(default_factory=LiveObjStore)


@dataclass(kw_only=True, slots=True, repr=False, eq=False)
class ActorInfo:
    node_id: NodeIdStr
    # NOTE: This is supposed to act like a immutable
    # reference, so don't update it in this class
    op_stats: "PerOpStats"

    downscaled: bool

    # Number of tasks currently running on this actor.
    num_tasks_in_flight: int = 0

    live: LiveObjStore = field(default_factory=LiveObjStore)

    def clone(self) -> "ActorInfo":
        """A copy of this entry for another scope, sharing `op_stats`."""
        return ActorInfo(
            node_id=self.node_id,
            op_stats=self.op_stats,
            downscaled=self.downscaled,
            num_tasks_in_flight=self.num_tasks_in_flight,
            live=self.live.clone(),
        )

    def to_downscaled(self):
        # Stop counting what this actor already produced. Those blocks are
        # still in the object store until they are consumed, but a downscaled
        # actor with unconsumed outputs is rare enough that tracking it is not
        # worth the bookkeeping.
        self.downscaled = True
        self.live.num_pulled_output_blocks = 0
        self.live.num_pulled_output_bytes = 0
        self.live.num_pulled_output_rows = 0
        self.live.num_withheld_output_blocks = 0
        self.live.num_withheld_output_bytes = 0
        self.live.num_withheld_output_rows = 0

    def prebuffered_obj_store_estimate(self) -> LiveObjStore:
        """The estimated object store contribution of this actor's
        prebuffered (generated-but-not-yet-pulled) outputs."""
        if self.num_tasks_in_flight == 0:
            return LiveObjStore()

        prebuffered = self.op_stats.estimate_npo_per_actor()
        bpo = self.op_stats.bytes_per_output_estimate()
        rpo = self.op_stats.average_rows_per_output() or 1
        return LiveObjStore(
            num_prebuffered_output_blocks=prebuffered,
            num_prebuffered_output_bytes=int(prebuffered * bpo),
            num_prebuffered_output_rows=int(prebuffered * rpo),
        )

    def live_obj_store_estimate(self) -> LiveObjStore:
        """This actor's estimated live object store usage: its pulled ``.live``
        plus its prebuffered estimate (see ``prebuffered_obj_store_estimate``)."""

        return self.live + self.prebuffered_obj_store_estimate()


@dataclass(kw_only=True)
class ActorRegistry(TracksObjStore):
    """Registry of the actors tracked (plus their tasks)."""

    # Currently-registered actors. An entry leaves `live` either when the
    # actor is downscaled (moved to `downscaled`).
    actor_info_map: Dict[ActorId, ActorInfo] = field(default_factory=dict)
    # NOTE: Although an actor-pool operator should not support downscaling an actor
    # until its inputs are dropped, that concept is something that is bound to
    # Ray Data, meaning it is perfectly feasible to have a downscaled actor with
    # unconsumed outputs. Actors that die unexpectedly are tracked here too.
    downscaled_actors: Dict[ActorId, ActorInfo] = field(default_factory=dict)

    # When None, technically there is no task , but it's to represent
    # the input operators that use ray.put (InputDataBuffer)
    task_info_map: Dict[ray.TaskID | None, TaskInfo] = field(default_factory=dict)

    def on_register_actor(
        self,
        *,
        actor_id: ActorId,
        actor_info: ActorInfo,
    ):
        assert actor_id not in self.actor_info_map
        assert actor_id not in self.downscaled_actors
        self.actor_info_map[actor_id] = actor_info

    def on_deregister_actor(
        self,
        *,
        actor_id: ActorId,
    ):
        if actor_id in self.actor_info_map:
            actor_info = self.actor_info_map.pop(actor_id)
            actor_info.to_downscaled()
            self.downscaled_actors[actor_id] = actor_info

    @override
    def on_task_submitted(
        self,
        *,
        target_actor_id: ActorId | None,
        task_id: ray.TaskID | None,
        target_node_id: NodeIdStr,
        bms: List[BlockMetadata],
    ):
        live = LiveObjStore()
        # An actor that is draining (downscaled but not yet released) can still
        # be handed work, so look it up the same way every other path does
        # rather than indexing ``actor_info_map`` directly.
        actor_info = (
            self.get_actor_info(actor_id=target_actor_id)
            if target_actor_id is not None
            else None
        )
        if actor_info is not None:
            for bm in bms:
                live.on_input_submitted(bm=bm)
                actor_info.live.on_input_submitted(bm=bm)
            actor_info.num_tasks_in_flight += 1

        self.task_info_map[task_id] = TaskInfo(
            actor_id=target_actor_id,
            task_id=task_id,
            node_id=target_node_id,
            live=live,
        )

    @override
    def on_new_output(self, *, bm: BlockMetadata, task_id: ray.TaskID | None):
        task_info = self.task_info_map[task_id]
        task_info.live.on_new_output(bm=bm)
        actor_info = self._actor_for_task(task_info=task_info)
        if actor_info is not None:
            actor_info.live.on_new_output(bm=bm)

    @override
    def on_task_completed(self, *, task_id: ray.TaskID):
        task_info = self.task_info_map[task_id]
        task_info.completed = True
        if task_info.actor_id is not None:
            actor_info = self.get_actor_info(actor_id=task_info.actor_id)
            if actor_info is not None:
                actor_info.live.on_input_dropped(task_info=task_info)
                # Relocation leaves in-flight count as-is (core retries the
                # same tasks). A completion after the actor was already
                # drained must not drive it negative.
                actor_info.num_tasks_in_flight = max(
                    0, actor_info.num_tasks_in_flight - 1
                )
            else:
                # else: the actor was released before its last task reported in
                # (it finished draining after being scaled down), so there is
                # nothing left to update here.
                logger.debug(
                    f"Task with task_id={task_id} finished after its actor was released."
                )
        task_info.live.on_input_dropped(task_info=task_info)
        if task_info.live.all_outputs_consumed():
            self.task_info_map.pop(task_id, None)

    @override
    def on_output_consumed(self, *, bm: BlockMetadata, task_id: ray.TaskID | None):
        task_info = self.task_info_map.get(task_id)
        if task_info is None:
            return
        task_info.live.on_output_consumed(bm=bm)
        actor_info = self._actor_for_task(task_info=task_info)
        if actor_info is not None:
            if actor_info.live.num_pulled_output_blocks == 0:
                # `to_downscaled` dropped this actor's outputs, so the block
                # being consumed now was never counted here. Subtracting it
                # would take the actor negative, which reads as extra output
                # budget rather than none.
                logger.debug(
                    f"Consumed an output of actor_id={task_info.actor_id} that "
                    "is no longer counted against it (it was downscaled while "
                    "the output was still in flight)."
                )
            else:
                actor_info.live.on_output_consumed(bm=bm)

        if task_info.live.all_outputs_consumed() and (
            task_info.completed or task_info.actor_id is None
        ):
            self.task_info_map.pop(task_info.task_id, None)

    def charge_withheld_block(
        self, *, bm: BlockMetadata, task_id: ray.TaskID | None, delta: int
    ) -> None:
        task_info = self.get_task_info(task_id=task_id)
        if task_info is None:
            return
        actor_info = self._actor_for_task(task_info=task_info)
        if actor_info is None:
            return
        if delta < 0 and actor_info.live.num_withheld_output_blocks == 0:
            # `to_downscaled` already dropped this actor's outputs.
            return
        actor_info.live.charge_withheld_block(bm=bm, delta=delta)

    def get_task_info(self, *, task_id: ray.TaskID | None) -> TaskInfo | None:
        if task_id is None:
            return None
        return self.task_info_map.get(task_id)

    def has_task_info(self, *, task_id: ray.TaskID | None) -> bool:
        return task_id is not None and task_id in self.task_info_map

    def _actor_for_task(self, *, task_info: TaskInfo) -> ActorInfo | None:
        """The actor entry a task's blocks count against, if this scope has one"""
        if task_info.actor_id is None:
            return None
        return self.get_actor_info(actor_id=task_info.actor_id)

    def get_actor_info(self, *, actor_id: ActorId) -> ActorInfo | None:
        actor_info = self.actor_info_map.get(actor_id, None)
        if actor_info is None:
            return self.downscaled_actors.get(actor_id, None)
        return actor_info

    def task_to_actor(self, *, task_id: ray.TaskID | None) -> ActorId | None:
        task_info = self.get_task_info(task_id=task_id)
        if task_info is None:
            return None
        return task_info.actor_id


@dataclass(kw_only=True)
class PerScopeStats(TracksObjStore):
    """A scope (an operator, a node, or an operator on a node) that aggregates
    object-store usage and owns an actor registry and a task registry.
    """

    # Registry of this scope's actors (plus the tasks those actors run).
    actors: ActorRegistry = field(default_factory=ActorRegistry)

    cumulative: CumulativeObjStore = field(default_factory=CumulativeObjStore)
    live: LiveObjStore = field(default_factory=LiveObjStore)

    @override
    def on_task_submitted(
        self,
        *,
        target_actor_id: ActorId | None,
        task_id: ray.TaskID | None,
        target_node_id: NodeIdStr,
        bms: List[BlockMetadata],
    ):
        self.actors.on_task_submitted(
            target_actor_id=target_actor_id,
            task_id=task_id,
            target_node_id=target_node_id,
            bms=bms,
        )
        for bm in bms:
            remote = is_cross_node_transfer(bm=bm, target_node_id=target_node_id)
            self.cumulative.on_input_submitted(bm=bm, remote=remote)
            self.live.on_input_submitted(bm=bm)

    @override
    def on_new_output(self, *, bm: BlockMetadata, task_id: ray.TaskID | None):
        # NOTE: task_id is None on input data sources (like input_data_buffer)
        # Their object refs are ray.put(paths), rather than task generated.
        if self.actors.has_task_info(task_id=task_id):
            self.actors.on_new_output(bm=bm, task_id=task_id)
        self.cumulative.on_new_output(bm=bm)
        self.live.on_new_output(bm=bm)

    @override
    def on_task_completed(
        self,
        *,
        task_id: ray.TaskID,
    ):
        task_info = self.actors.get_task_info(task_id=task_id)
        assert task_info is not None, f"ray.TaskID is {task_id}"
        self.live.on_input_dropped(task_info=task_info)
        self.actors.on_task_completed(task_id=task_id)

    @override
    def on_output_consumed(self, *, bm: BlockMetadata, task_id: ray.TaskID | None):
        self.live.on_output_consumed(bm=bm)
        if self.actors.has_task_info(task_id=task_id):
            self.actors.on_output_consumed(bm=bm, task_id=task_id)

    def actor_ids(self) -> Set[ActorId]:
        """All actor ids tracked by this scope (live + downscaled)."""
        return self.actors.actor_info_map.keys() | self.actors.downscaled_actors.keys()

    def actor_info(self) -> List[ActorInfo]:
        """All actor entries tracked by this scope (live + downscaled)."""
        return [
            *self.actors.actor_info_map.values(),
            *self.actors.downscaled_actors.values(),
        ]

    def average_bytes_per_output(self) -> float | None:
        if self.cumulative.output_bytes.num_samples > 0:
            return self.cumulative.output_bytes.mean or None
        return None

    def average_rows_per_output(self) -> float | None:
        if self.cumulative.output_rows.num_samples > 0:
            return self.cumulative.output_rows.mean
        return None

    def all_outputs_consumed(self) -> bool:
        return self.live.all_outputs_consumed()

    def live_obj_store_estimate(self) -> LiveObjStore:
        """This scope's estimated live object store usage.

        An estimate because Ray Data cannot see how full a generator's prebuffer
        is, so we assume it is full for every actor of this scope that has a
        task in flight.

        Every scope counts those actors from this bank's own entries. That is
        what makes an operator's estimate equal the sum of its per-(operator,
        node) estimates; counting them from the actor pool instead would let the
        two disagree, since the pool tracks its actors separately from what this
        bank was told.
        """
        estimate = self.live.clone()
        for actor_info in self.actor_info():
            estimate += actor_info.prebuffered_obj_store_estimate()
        return estimate


@dataclass(kw_only=True)
class PerNodeStats(PerScopeStats):

    node_id: NodeIdStr


@dataclass(kw_only=True)
class PerOpStats(PerScopeStats):

    op: PhysicalOperator
    # Num_Prebuffered_Objects, static
    npo: int

    def estimate_npo_per_actor(self) -> int:
        """Estimating the number of prebuffered objects (npo), by assuming
        worst case its filled with objects"""
        if self.npo >= 0:
            return self.npo
        # TODO(Justin): Probably can unify parts of OpRuntimeMetrics
        num_blocks = self.op.metrics.num_task_outputs_generated
        num_tasks_submitted = self.op.metrics.num_tasks_submitted
        if num_tasks_submitted > 0:
            return max(1, math.ceil(num_blocks / num_tasks_submitted))
        return 1

    def bytes_per_output_estimate(self) -> float:
        """Average bytes one output block of this operator produces"""
        return self.average_bytes_per_output() or DEFAULT_BYTES_PER_OUTPUT


@dataclass(kw_only=True)
class PerOpNodeStats(PerOpStats):
    """One operator's slice of one node.

    Subclasses ``PerOpStats`` to reuse the prebuffer sizing helpers. Its actor
    registry holds exactly that operator's actors on ``node_id``, so the
    inherited ``live_obj_store_estimate`` already restricts itself to them.
    """

    node_id: NodeIdStr


@dataclass(kw_only=True)
class ClusterStats:

    per_node: Dict[NodeIdStr, PerNodeStats] = field(default_factory=dict)
    per_operator: Dict[PhysicalOperator, PerOpStats] = field(default_factory=dict)
    per_op_node: Dict[Tuple[PhysicalOperator, NodeIdStr], PerOpNodeStats] = field(
        default_factory=dict
    )
    actor_to_operator: Dict[ActorId, PerOpStats] = field(default_factory=dict)

    def on_register_node(self, *, node_id: NodeIdStr) -> PerNodeStats:
        node_stats = self.per_node.get(node_id)
        if node_stats is None:
            node_stats = PerNodeStats(node_id=node_id)
            self.per_node[node_id] = node_stats
        return node_stats

    def on_register_op(self, *, op: PhysicalOperator) -> PerOpStats:
        op_stats = self.per_operator.get(op)
        if op_stats is None:
            op_stats = PerOpStats(
                op=op,
                npo=num_prebuffered_objects(op=op),
            )
            self.per_operator[op] = op_stats
        return op_stats

    def on_register_op_node(
        self, *, op: PhysicalOperator, node_id: NodeIdStr
    ) -> PerOpNodeStats:
        key = (op, node_id)
        op_node_stats = self.per_op_node.get(key)
        if op_node_stats is None:
            op_node_stats = PerOpNodeStats(
                op=op,
                node_id=node_id,
                npo=num_prebuffered_objects(op=op),
            )
            self.per_op_node[key] = op_node_stats
        return op_node_stats

    def on_maybe_register_actor(
        self,
        *,
        node_id: NodeIdStr,
        op: PhysicalOperator,
        actor_id: ActorId,
    ):

        node_stats = self.on_register_node(node_id=node_id)
        op_stats = self.on_register_op(op=op)
        op_node_stats = self.on_register_op_node(op=op, node_id=node_id)
        actor_info = op_stats.actors.get_actor_info(actor_id=actor_id)
        self.actor_to_operator[actor_id] = op_stats

        if actor_info is not None:
            # Already registered; actors never change nodes.
            assert actor_info.node_id == node_id, (
                f"actor {actor_id} registered on {actor_info.node_id} "
                f"was seen on {node_id}"
            )
            return

        # First sighting -> register the actor on its node.
        actor_info = ActorInfo(
            node_id=node_id,
            op_stats=op_stats,
            downscaled=False,
        )
        op_stats.actors.on_register_actor(actor_id=actor_id, actor_info=actor_info)
        node_stats.actors.on_register_actor(
            actor_id=actor_id, actor_info=actor_info.clone()
        )
        op_node_stats.actors.on_register_actor(
            actor_id=actor_id, actor_info=actor_info.clone()
        )

    def on_task_submitted(
        self,
        *,
        actor_id: ActorId,
        task_id: ray.TaskID,
        input_bms: List[BlockMetadata],
    ):
        # Bind this task_id to the actor that will run it.
        op_stats = self.actor_to_operator.get(actor_id)
        actor_info = (
            op_stats.actors.get_actor_info(actor_id=actor_id)
            if op_stats is not None
            else None
        )
        assert op_stats is not None and actor_info is not None, (
            f"actor_id={actor_id} was handed a task before it was registered "
            "(or after it was released); call maybe_register_actor first"
        )
        node_stats = self.on_register_node(node_id=actor_info.node_id)
        op_node_stats = self.on_register_op_node(
            op=op_stats.op, node_id=actor_info.node_id
        )

        # Per-task bookkeeping (in-flight count, registries) -- once per scope.
        for scope in (op_stats, node_stats, op_node_stats):
            scope.on_task_submitted(
                target_actor_id=actor_id,
                task_id=task_id,
                target_node_id=actor_info.node_id,
                bms=input_bms,
            )

    def on_new_output(
        self,
        *,
        op: PhysicalOperator,
        bm: BlockMetadata,
        task_id: ray.TaskID | None,
    ):
        node_id = bm.node_id
        node_stats = self.on_register_node(node_id=node_id)
        op_stats = self.on_register_op(op=op)
        op_node_stats = self.on_register_op_node(op=op, node_id=node_id)
        for scope in (op_stats, node_stats, op_node_stats):
            scope.on_new_output(bm=bm, task_id=task_id)

    def maybe_release_actor(
        self,
        *,
        actor_id: ActorId,
        actor_info: ActorInfo,
        op_stats: PerOpStats,
    ):
        """Drop a downscaled actor's tracking entries once it is fully drained.

        The node scopes are looked up from the actor entry itself, so a caller
        cannot pass a stale pair after a restart moved the actor.
        """
        if not (
            actor_info.downscaled
            and actor_info.num_tasks_in_flight == 0
            and actor_info.live.all_outputs_consumed()
        ):
            return
        node_stats = self.per_node.get(actor_info.node_id)
        op_node_stats = self.per_op_node.get((op_stats.op, actor_info.node_id))
        for scope in (op_stats, node_stats, op_node_stats):
            if scope is not None:
                scope.actors.downscaled_actors.pop(actor_id, None)
        # Drop the reverse map entry too, otherwise it grows unbounded as
        # actors are churned by autoscaling.
        self.actor_to_operator.pop(actor_id, None)

    def on_task_completed(self, *, op: PhysicalOperator, task_id: ray.TaskID):
        op_stats = self.per_operator[op]
        task_info = op_stats.actors.get_task_info(task_id=task_id)
        assert task_info is not None

        # Each scope's ActorRegistry.on_task_completed sets `completed` on its
        # own TaskInfo. Relocation re-homes the per-node copies, so resolve the
        # node scopes through the task's current node rather than assuming the
        # one it was submitted on.
        scopes: List[PerScopeStats] = [op_stats]
        node_stats = self.per_node.get(task_info.node_id)
        op_node_stats = self.per_op_node.get((op, task_info.node_id))
        for scope in (node_stats, op_node_stats):
            if scope is not None and scope.actors.has_task_info(task_id=task_id):
                scopes.append(scope)
        for scope in scopes:
            scope.on_task_completed(task_id=task_id)

        actor_id = task_info.actor_id
        if actor_id is None:
            return

        actor_info = op_stats.actors.get_actor_info(actor_id=actor_id)
        if actor_info is None:
            # The actor's tracking entries were already dropped (e.g. a fully
            # drained downscaled actor); nothing left to release.
            return
        self.maybe_release_actor(
            actor_id=actor_id, actor_info=actor_info, op_stats=op_stats
        )

    def on_block_consumed(
        self,
        *,
        bm: BlockMetadata,
        op: PhysicalOperator,
        task_id: ray.TaskID | None,
    ):
        node_id = bm.node_id
        op_stats = self.per_operator[op]
        task_info = op_stats.actors.get_task_info(task_id=task_id)
        actor_id = task_info.actor_id if task_info is not None else None

        # Independent per-scope object store + registry GC. Every scope here
        # was registered by the matching ``on_new_output``.
        for scope in (
            op_stats,
            self.per_node.get(node_id),
            self.per_op_node.get((op, node_id)),
        ):
            if scope is not None:
                scope.on_output_consumed(bm=bm, task_id=task_id)

        if actor_id is not None:
            actor_info = op_stats.actors.get_actor_info(actor_id=actor_id)
            if actor_info is not None:
                self.maybe_release_actor(
                    actor_id=actor_id, actor_info=actor_info, op_stats=op_stats
                )

    def charge_withheld_block(
        self,
        *,
        bm: BlockMetadata,
        op: PhysicalOperator,
        task_id: ray.TaskID | None,
        delta: int,
    ) -> None:
        node_id = bm.node_id
        for scope in (
            self.per_operator.get(op),
            self.per_node.get(node_id),
            self.per_op_node.get((op, node_id)),
        ):
            if scope is None:
                continue
            scope.live.charge_withheld_block(bm=bm, delta=delta)
            scope.actors.charge_withheld_block(bm=bm, task_id=task_id, delta=delta)

    def on_deregister_actor(self, *, actor_id: ActorId):
        """Mark the actor into the deregistered state. However, we may continue
        to track the downscaled actor, because its outputs may still be alive. In this
        case, we rely on maybe_release_actor to fully stop tracking that actor"""
        op_stats = self.actor_to_operator.get(actor_id)
        actor_info = (
            op_stats.actors.get_actor_info(actor_id=actor_id)
            if op_stats is not None
            else None
        )
        if actor_info is None:
            # The actor never ran a task (e.g. it was scaled down while idle, so
            # it was never registered) or has already been fully released. There
            # is nothing tracked to deregister.
            return

        assert op_stats is not None and op_stats is actor_info.op_stats
        node_id = actor_info.node_id
        for scope in (
            op_stats,
            self.per_node.get(node_id),
            self.per_op_node.get((op_stats.op, node_id)),
        ):
            if scope is not None:
                scope.actors.on_deregister_actor(actor_id=actor_id)

        # The actor may already be fully drained (all tasks done, all outputs
        # consumed) at downscale time, in which case nothing else will trigger
        # cleanup -- so attempt it here too.
        self.maybe_release_actor(
            actor_id=actor_id, actor_info=actor_info, op_stats=op_stats
        )

    def clear(self) -> None:
        """Remove all tracked information for this dataset execution."""
        self.per_node.clear()
        self.per_operator.clear()
        self.actor_to_operator.clear()
        self.per_op_node.clear()


class ResourceBankBase(ABC):
    """Interface for the object-store accounting bank.

    Every method raises `NotImplementedError` by default, so a test double can
    override just the ones it needs and any unexpected call fails loudly instead
    of being silently swallowed the way a bare mock would.
    """

    def maybe_register_actor(
        self,
        *,
        op: PhysicalOperator,
        actor_id: ActorId,
        node_id: NodeIdStr,
    ) -> None:
        raise NotImplementedError

    def on_task_submitted(
        self,
        *,
        actor_id: ActorId,
        task_id: ray.TaskID,
        input_bms: List[BlockMetadata],
    ) -> None:
        raise NotImplementedError

    def on_new_output(
        self,
        *,
        op: PhysicalOperator,
        ref: ObjectRef[Block],
        bm: BlockMetadata,
        task_id: ray.TaskID | None = None,
    ) -> None:
        raise NotImplementedError

    def on_task_completed(self, *, op: PhysicalOperator, task_id: ray.TaskID) -> None:
        raise NotImplementedError

    def on_block_consumed(self, *, input_ref: ObjectRef[Block]) -> None:
        raise NotImplementedError

    def drain_consumed_blocks(self) -> None:
        raise NotImplementedError

    def on_output_withheld(self, *, ref: ObjectRef[Block]) -> None:
        raise NotImplementedError

    def reserve_headroom(self, *, node_id: NodeIdStr, num_bytes: int) -> int:
        raise NotImplementedError

    def release_headroom(self, *, node_id: NodeIdStr, num_bytes: int) -> None:
        raise NotImplementedError

    def free_object_store_bytes(self, *, node_id: NodeIdStr) -> float:
        raise NotImplementedError

    def on_output_released(self, *, ref: ObjectRef[Block]) -> None:
        raise NotImplementedError

    def deregister_actor(self, *, actor_id: ActorId) -> None:
        raise NotImplementedError

    def get_actor_id_from_task_id(
        self, *, op: PhysicalOperator, task_id: ray.TaskID | None
    ) -> ActorId | None:
        raise NotImplementedError

    def node_capacity(self, *, node_id: NodeIdStr) -> ExecutionResources:
        raise NotImplementedError

    def set_node_view(self, *, node_view: Dict[NodeIdStr, ExecutionResources]) -> None:
        raise NotImplementedError

    def live_object_store(
        self,
        *,
        op: PhysicalOperator | None = None,
        node_id: NodeIdStr | None = None,
        actor_id: ActorId | None = None,
    ) -> LiveObjStore:
        raise NotImplementedError

    def cumulative_object_store(
        self,
        *,
        op: PhysicalOperator | None = None,
        node_id: NodeIdStr | None = None,
    ) -> CumulativeObjStore:
        raise NotImplementedError

    def finalize_dataset(self, *, ops: Set[PhysicalOperator]) -> None:
        raise NotImplementedError

    def node_ids(self) -> Set[NodeIdStr]:
        raise NotImplementedError

    def op_node_ids(self) -> Set[Tuple[PhysicalOperator, NodeIdStr]]:
        raise NotImplementedError


class ResourceBank(ResourceBankBase):
    """Object-store accounting across three scopes: operators, nodes, and
    operators-on-a-node (the join of the two).

    Scoped to a single dataset execution, owned by the streaming executor.
    """

    def __init__(self):
        self.stats: ClusterStats = ClusterStats()
        # Key by ObjectRef hex string so ResourceBank accounting never keeps an
        # extra Python ObjectRef alive and delays Ray Core ref-count cleanup.
        self.live_block_refs: Dict[str, UniqueBlockMetadata] = {}
        # This class should only be double-threaded, one thread for the
        # streaming executor loop, the other is the consumer thread (get_output_blocking)
        # Rather than creating a lock to guard each public method of the ResourceBank,
        # I decided to only protect the consumed_refs, since it's the only method that
        # should be called in the consumer thread.
        self._consumed_refs: List[str] = []
        self._consumed_refs_lock: threading.Lock = threading.Lock()
        # To have 1 source of truth of the shape of the cluster, we set this value
        # at the start of each scheduling loop.
        self._node_view: Optional[Dict[NodeIdStr, ExecutionResources]] = None
        # Object-store bytes escape hatches have granted from each node's free
        # space and not yet given back (see ``free_object_store_bytes``).
        self._reserved_headroom_bytes: Dict[NodeIdStr, int] = {}

    @override
    def maybe_register_actor(
        self,
        *,
        op: PhysicalOperator,
        actor_id: ActorId,
        node_id: NodeIdStr,
    ) -> None:
        """Register ``actor_id`` on ``node_id`` on first sighting; later
        sightings do nothing (actors never change nodes)."""
        if not actor_only_backend_enabled():
            raise NotImplementedError

        self.stats.on_maybe_register_actor(
            node_id=node_id,
            actor_id=actor_id,
            op=op,
        )

    @override
    def on_task_submitted(
        self,
        *,
        actor_id: ActorId,
        task_id: ray.TaskID,
        input_bms: List[BlockMetadata],
    ) -> None:
        """Keep track of which task belongs to which actor."""
        if not actor_only_backend_enabled():
            raise NotImplementedError

        self.stats.on_task_submitted(
            actor_id=actor_id,
            task_id=task_id,
            input_bms=input_bms,
        )

    @override
    def on_new_output(
        self,
        *,
        op: PhysicalOperator,
        ref: ObjectRef[Block],
        bm: BlockMetadata,
        task_id: ray.TaskID | None = None,
    ):
        """Called whenever there is new output from a task."""
        if not actor_only_backend_enabled():
            raise NotImplementedError

        ref_key = ref.hex()  # pyrefly: ignore[missing-attribute]
        if ref_key in self.live_block_refs:
            # A streaming generator derives its return ids from (task_id,
            # put_index), so a task that Ray Core retries on the same (live)
            # actor yields the very same ObjectRefs again. That is still one
            # object, just rebuilt. Subtract the previous one before adding this
            # one, or the block would be counted twice.
            self._apply_consumed_ref(ref_key=ref_key)
        self.stats.on_new_output(op=op, bm=bm, task_id=task_id)
        self.live_block_refs[ref_key] = UniqueBlockMetadata(
            op=op,
            bm=bm,
            task_id=task_id,
        )

    @override
    def on_task_completed(self, *, op: PhysicalOperator, task_id: ray.TaskID):
        """Called whenever task_id is completed"""
        if not actor_only_backend_enabled():
            raise NotImplementedError

        op_stats = self.stats.per_operator.get(op)
        if op_stats is None or not op_stats.actors.has_task_info(task_id=task_id):
            return
        self.stats.on_task_completed(op=op, task_id=task_id)

    @override
    def on_block_consumed(self, *, input_ref: ObjectRef[Block]):
        """Report that ``input_ref`` is no longer needed downstream.

        This is the only method called from outside the executor thread
        (consumers call it from `OpState.get_output_blocking`), so it only adds
        the ref to a queue; `drain_consumed_blocks` does the real work later on
        the executor thread.

        Safe to call twice for the same ref, which happens normally: once when
        an actor takes over a local input at submit time, and again when the
        task finishes. The second report is ignored.
        """
        if not actor_only_backend_enabled():
            raise NotImplementedError

        with self._consumed_refs_lock:
            self._consumed_refs.append(
                input_ref.hex()  # pyrefly: ignore[missing-attribute]
            )

    @override
    def drain_consumed_blocks(self) -> None:
        """Apply queued block-consumption reports. Executor thread ONLY.

        Takes the queue under its lock, then applies each report without any
        further locking, which is safe because the executor thread is the only
        thing that writes `stats` and `live_block_refs`.
        """
        if not actor_only_backend_enabled():
            raise NotImplementedError

        with self._consumed_refs_lock:
            if not self._consumed_refs:
                return
            refs = self._consumed_refs
            self._consumed_refs = []

        for ref_key in refs:
            self._apply_consumed_ref(ref_key=ref_key)

    @override
    def on_output_withheld(self, *, ref: ObjectRef[Block]) -> None:
        """Mark an already-registered output as withheld by lineage
        reconstruction. It stays counted as pulled (the bytes are still in the
        object store), and is also counted as withheld, so callers can tell
        output a consumer can drain from output that waits on its siblings."""
        if not actor_only_backend_enabled():
            raise NotImplementedError
        self._set_withheld(ref_key=ref.hex(), withheld=True)  # pyrefly: ignore

    @override
    def reserve_headroom(self, *, node_id: NodeIdStr, num_bytes: int) -> int:
        """Set aside up to ``num_bytes`` of ``node_id``'s free object store for
        an escape-hatch grant, so later grants don't count it as free again.
        Capped at the node's free bytes, so reservations never exceed the
        object store. Returns the bytes reserved."""
        if not actor_only_backend_enabled():
            raise NotImplementedError
        free = max(0, int(self.free_object_store_bytes(node_id=node_id)))
        reserved = max(0, min(num_bytes, free))
        if reserved < num_bytes:
            logger.warning(
                "Headroom reservation on node %s capped at its free object store: "
                "asked for %d bytes, reserved %d.",
                node_id,
                num_bytes,
                reserved,
            )
        if reserved:
            self._reserved_headroom_bytes[node_id] = (
                self._reserved_headroom_bytes.get(node_id, 0) + reserved
            )
        return reserved

    @override
    def release_headroom(self, *, node_id: NodeIdStr, num_bytes: int) -> None:
        if not actor_only_backend_enabled():
            raise NotImplementedError
        remaining = self._reserved_headroom_bytes.get(node_id, 0) - num_bytes
        if remaining > 0:
            self._reserved_headroom_bytes[node_id] = remaining
        else:
            self._reserved_headroom_bytes.pop(node_id, None)

    @override
    def free_object_store_bytes(self, *, node_id: NodeIdStr) -> float:
        """Object store on ``node_id`` that neither holds Data blocks (pulled,
        prebuffered, or task inputs) nor is reserved by an escape hatch."""
        if not actor_only_backend_enabled():
            raise NotImplementedError
        usage = self.live_object_store(node_id=node_id)
        return (
            self.node_capacity(node_id=node_id).object_store_memory
            - usage.num_pulled_output_bytes
            - usage.num_prebuffered_output_bytes
            - usage.num_input_bytes
            - self._reserved_headroom_bytes.get(node_id, 0)
        )

    @override
    def on_output_released(self, *, ref: ObjectRef[Block]) -> None:
        """Clear the withheld mark: the child's input set is complete and the
        block is queued for it."""
        if not actor_only_backend_enabled():
            raise NotImplementedError
        self._set_withheld(ref_key=ref.hex(), withheld=False)  # pyrefly: ignore

    def _set_withheld(self, *, ref_key: str, withheld: bool) -> None:
        unique_bm = self.live_block_refs.get(ref_key)
        if unique_bm is None or unique_bm.withheld_for_reconstruction == withheld:
            return
        unique_bm.withheld_for_reconstruction = withheld
        self.stats.charge_withheld_block(
            bm=unique_bm.bm,
            op=unique_bm.op,
            task_id=unique_bm.task_id,
            delta=1 if withheld else -1,
        )

    def _apply_consumed_ref(self, *, ref_key: str) -> None:
        """Subtract one tracked ref. Unknown or already-dropped refs do nothing."""
        unique_bm = self.live_block_refs.get(ref_key)
        if unique_bm is None:
            return
        self._set_withheld(ref_key=ref_key, withheld=False)
        del self.live_block_refs[ref_key]
        self.stats.on_block_consumed(
            bm=unique_bm.bm,
            op=unique_bm.op,
            task_id=unique_bm.task_id,
        )

    @override
    def deregister_actor(self, *, actor_id: ActorId) -> None:
        """Drop a downscaled actor from the live indexes."""
        if not actor_only_backend_enabled():
            raise NotImplementedError

        self.stats.on_deregister_actor(actor_id=actor_id)

    @override
    def get_actor_id_from_task_id(
        self, *, op: PhysicalOperator, task_id: ray.TaskID | None
    ) -> ActorId | None:
        """Given a task_id, returns the actor_id that generated that task_id"""
        if not actor_only_backend_enabled():
            raise NotImplementedError

        op_stats = self.stats.per_operator.get(op)
        if op_stats is None:
            return None
        return op_stats.actors.task_to_actor(task_id=task_id)

    @override
    def set_node_view(self, *, node_view: Dict[NodeIdStr, ExecutionResources]) -> None:
        """Set this tick's per-node allocation snapshot (source of truth).

        Called once at the top of each scheduling step so the bank and the sizer
        agree on the node view for the whole tick.
        """
        if not actor_only_backend_enabled():
            raise NotImplementedError

        self._node_view = node_view

    @override
    def node_capacity(self, *, node_id: NodeIdStr) -> ExecutionResources:
        if not actor_only_backend_enabled():
            raise NotImplementedError

        if self._node_view is not None:
            res = self._node_view.get(node_id)
            if res is not None:
                return res

        # Missing from coordinator allocation (cold cache, soft affinity, or
        # untargeted initial_size actors) or sizer-off: use a fresh cluster
        # snapshot so a just-joined landing node is visible.
        return get_alive_nodes_uncached().get(node_id, ExecutionResources.zero())

    @override
    def live_object_store(
        self,
        *,
        op: PhysicalOperator | None = None,
        node_id: NodeIdStr | None = None,
        actor_id: ActorId | None = None,
    ) -> LiveObjStore:
        """Returns the object store based on the scope.
        Will go from most scoped (actor_id) to least scoped (op).
        This function does not validate requests (ie actor_id on wrong node_id)"""
        if not actor_only_backend_enabled():
            raise NotImplementedError

        if actor_id is not None:
            op_stats = self.stats.actor_to_operator.get(actor_id)
            # Protect against pending actors.
            if op_stats is None:
                return LiveObjStore()
            actor_info = op_stats.actors.get_actor_info(actor_id=actor_id)
            if actor_info is None:
                return LiveObjStore()
            return actor_info.live_obj_store_estimate()
        elif node_id is not None and op is not None:
            op_node_stats = self.stats.per_op_node.get((op, node_id))
            if op_node_stats is None:
                return LiveObjStore()
            return op_node_stats.live_obj_store_estimate()
        elif node_id is not None:
            node_stats = self.stats.per_node.get(node_id)
            if node_stats is None:
                return LiveObjStore()
            return node_stats.live_obj_store_estimate()
        elif op is not None:
            if op in self.stats.per_operator:
                # TODO(justin): May need to have this check for entities too.
                return self.stats.per_operator[op].live_obj_store_estimate()
            return LiveObjStore()
        else:
            assert op is None and node_id is None and actor_id is None
            bytes_output = LiveObjStore()
            for _, stats in self.stats.per_operator.items():
                bytes_output += self.live_object_store(op=stats.op)
            return bytes_output

    @override
    def cumulative_object_store(
        self,
        *,
        op: PhysicalOperator | None = None,
        node_id: NodeIdStr | None = None,
    ) -> CumulativeObjStore:
        """Cumulative totals for the requested scope, from most scoped
        ((op, node_id)) to least (no argument sums every operator).

        Do not mutate the result. Asking for a single scope hands back the
        bank's own totals rather than a copy, because copying has to merge a
        sketch per field and metrics export calls this once per (operator, node)
        on every scheduling tick.
        """
        if not actor_only_backend_enabled():
            raise NotImplementedError

        if node_id is not None and op is not None:
            op_node_stats = self.stats.per_op_node.get((op, node_id))
            if op_node_stats is None:
                return CumulativeObjStore()
            return op_node_stats.cumulative
        elif node_id is not None:
            node_stats = self.stats.per_node.get(node_id)
            if node_stats is None:
                return CumulativeObjStore()
            return node_stats.cumulative
        elif op is not None:
            op_stats = self.stats.per_operator.get(op)
            if op_stats is None:
                return CumulativeObjStore()
            return op_stats.cumulative
        else:
            assert op is None and node_id is None
            bytes_output = CumulativeObjStore()
            for _, stats in self.stats.per_operator.items():
                bytes_output += stats.cumulative
            return bytes_output

    @override
    def finalize_dataset(self, *, ops: Set[PhysicalOperator]) -> None:
        """Clear the space in the ResourceBank"""
        if not actor_only_backend_enabled():
            raise NotImplementedError

        # Apply any buffered consumptions before tearing down. Runs on the
        # executor thread, same as everything below.
        self.drain_consumed_blocks()
        # ResourceBank is scoped to one dataset execution, so finalization drops
        # all tracked stats and refs regardless of which operators are passed in.
        self.stats.clear()
        self.live_block_refs = {}
        self._reserved_headroom_bytes = {}
        # Drop any reports that arrived after the drain above (e.g. a straggler
        # consumer); they reference now-released refs and would otherwise
        # accumulate unbounded across executions.
        with self._consumed_refs_lock:
            self._consumed_refs = []

    @override
    def node_ids(self) -> Set[NodeIdStr]:
        if not actor_only_backend_enabled():
            raise NotImplementedError

        return set(self.stats.per_node.keys())

    @override
    def op_node_ids(self) -> Set[Tuple[PhysicalOperator, NodeIdStr]]:
        """Every (operator, node) pair that has been observed this execution.

        Sparse: a pair appears once the operator first places an actor or lands
        an output on that node, and then stays for the rest of the execution.
        """
        if not actor_only_backend_enabled():
            raise NotImplementedError

        return set(self.stats.per_op_node.keys())


def is_cross_node_transfer(*, bm: BlockMetadata, target_node_id: str) -> bool:
    """If the block meta were to transfer to actor_node_id, would it be
    a locality hit? Returns True if non-local, False for local"""
    return bm.node_id != target_node_id


def _prebuffer_blocks_from_object_cap(*, raw: Any) -> int:
    """Convert a Ray generator backpressure cap from object units to blocks.

    Both ``_actor_generator_backpressure_num_objects`` and
    ``_generator_backpressure_num_objects`` are stored in Ray Core as object
    counts. Map actors with grouped yields emit one logical block as
    ``objects_per_yield`` objects (block + metadata).
    """
    if raw is None:
        return -1
    raw = int(raw)
    if raw < 0:
        return -1
    objects_per_yield = CORE_ACTOR_BACKPRESSURE_NUM_OBJECTS_PER_YIELD
    if raw % objects_per_yield != 0:
        # The cap comes from user-supplied `ray_remote_args`, so a value that is
        # not a whole number of yields is a misconfiguration, not a broken
        # invariant. Over-reserving prebuffer space is safe; crashing the
        # executor over an accounting hint is not.
        logger.warning(
            f"Generator backpressure cap ({raw}) is not a multiple of "
            f"{objects_per_yield}, the number of objects Ray Data yields per "
            "block. Rounding up to the next whole block."
        )
    return math.ceil(raw / objects_per_yield)


def num_prebuffered_objects(*, op: PhysicalOperator) -> int:
    ray_remote_args = {}
    ray_actor_task_remote_args = {}
    max_concurrency = 0
    if isinstance(op, ActorPoolOperator):
        ray_remote_args = op._ray_remote_args
        ray_actor_task_remote_args = op._ray_actor_task_remote_args
        max_concurrency = op.actor_pool.max_actor_concurrency()

    agbno = _prebuffer_blocks_from_object_cap(
        raw=ray_remote_args.get("_actor_generator_backpressure_num_objects")
    )
    gbno = _prebuffer_blocks_from_object_cap(
        raw=ray_actor_task_remote_args.get("_generator_backpressure_num_objects")
    )

    if agbno >= 0 and gbno >= 0:
        return min(agbno, gbno * max_concurrency)
    elif agbno >= 0:
        return agbno
    elif gbno >= 0:
        return gbno * max_concurrency

    return -1
