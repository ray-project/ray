"""Pre-scheduling hints for sizing the next consumer task.

Producers (typically the operator that knows the *output* cost of the work
it is about to hand downstream — e.g. a download op that has measured file
sizes) stage a :class:`SchedulingHints` via :func:`stage_scheduling_hints`.
The next ``_map_task`` yield captures it onto the per-block wire envelope
(``BlockMetadataWithSchema``) and the driver lifts it onto the
``RefBundle``'s ``BlockEntry`` for the next operator to consume.

These hints are **prospective**: they describe what the next consumer is
expected to do with this block. They are deliberately kept separate from
``BlockMetadata`` (which describes the block's measured, retrospective
state) so the type system surfaces the difference between "this is what
the block is" and "this is what we forecast the next op will need."

No operator stages or reads hints in this initial landing — the infra is
shipped first and adopted by Download (and others) in follow-on PRs.
"""

from dataclasses import dataclass
from typing import Optional

from ray.data._internal.execution.interfaces.task_context import TaskContext


@dataclass(frozen=True)
class SchedulingHints:
    """Producer forecast for sizing the next consumer task.

    All fields default to ``None`` so producers fill only the axes they can
    forecast. Consumers ignore axes they don't care about — a Download op
    that only cares about ``memory`` simply skips the rest. New hint axes
    (cpu, gpu, locality, scheduling strategy) can be added here without
    touching ``BlockMetadata`` or the producer/consumer call sites.
    """

    # Worker memory (bytes) the downstream task processing this block is
    # expected to need. Plug directly into ``ray.remote(memory=...)``.
    memory: Optional[int] = None
    # Future, additive:
    # num_cpus: Optional[float] = None
    # num_gpus: Optional[float] = None
    # preferred_node_id: Optional[str] = None
    # scheduling_strategy: Optional[str] = None


def stage_scheduling_hints(hints: SchedulingHints) -> None:
    """Stage a per-yield ``SchedulingHints`` on the current ``TaskContext``.

    Call this immediately before yielding a block from a map transform.
    ``_map_task`` reads-and-clears the hints after the yield (see
    :meth:`TaskContext.consume_next_block_scheduling_hints`), so calling
    this again before each subsequent yield is required — a stale value
    would silently mis-tag later blocks.

    Calls without an active ``TaskContext`` (e.g. direct unit-test
    invocation of a transform) are silent no-ops so tests do not need to
    construct fake contexts.
    """
    ctx = TaskContext.get_current()
    if ctx is None:
        return
    ctx.next_block_scheduling_hints = hints


def stage_memory_hint(memory: int) -> None:
    """Convenience wrapper for the single-axis memory hint case.

    Equivalent to ``stage_scheduling_hints(SchedulingHints(memory=memory))``.
    Non-positive values are dropped silently — a zero-memory request would
    be meaningless to the scheduler.
    """
    if memory <= 0:
        return
    stage_scheduling_hints(SchedulingHints(memory=memory))
