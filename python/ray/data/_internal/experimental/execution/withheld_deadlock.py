"""Deadlock detection for outputs withheld by lineage reconstruction.

A reconstructed block is withheld until its child's whole input set is back,
and it stays charged to the actor that made it. Actor selection ignores this:
it places a reconstruction task by locality and free slots, blind to how much
of an actor's output budget is already held by withheld blocks. So a task of
plan P can land on an actor whose budget is full of P's withheld blocks, and
then it can't emit until P's set is released, which needs that task to
finish. Such cycles never resolve by waiting; this module finds them.

Within one operator a plan's withheld set is released when the operator's last
task of that plan finishes, so every wait is on the operator's own actors (or
on upstream input, which the upstream operator's own check covers):

- an actor whose budget would stay exhausted even after every drainable output
  is consumed (withheld plus prebuffered outputs fill it) waits on the plans it
  holds blocks for: any of them completing frees part of its budget;
- a plan completes once its running tasks finish, which needs their actors to
  progress, and once its tasks not yet started get a task slot.

Everything else gets resolved by waiting: drainable outputs are consumed
downstream (or the output-limit hatch handles a consumer that can't start).
So the operator is deadlocked exactly when some stuck actors can't progress
under any amount of waiting -- a cycle such as an actor waiting on its own
plan, two actors waiting on each other's plans, or a plan whose next task needs
a slot that only stuck actors hold. ``find_withheld_deadlock`` computes that
set, and picks the actors to unblock: the producers the stuck actors wait on.
Room given to an actor that only waits on another actor's output is wasted.

Examples, for one operator with actors A and B and reconstruction plans P and
Q. "Holds P" means the actor's budget is full of P's withheld blocks; "runs P"
means it runs one of P's tasks that hasn't finished yet.

- A holds P; B runs P and can still pull. Not a deadlock: B finishes, P's set
  is released, and A's budget frees up. Nothing is granted.
- A holds P and runs P. A waits on itself: P's task can't pull its next output
  until P's set is released, which needs that task to finish. Deadlocked {A},
  target A. The grant lets the task finish and P's set is released.
- A holds P and runs Q; B holds Q and runs P. Each waits on the other.
  Deadlocked {A, B}; both are targets. Granting A lets Q's task finish, which
  releases Q's set, frees B, and lets B finish P's task.
- A holds P and runs nothing of P; B holds P and runs P. Deadlocked {A, B},
  target B only: room for A changes nothing, since A just waits on B. Once B
  finishes P's task, P's set is released and both are freed.
- A is the operator's only actor. A holds P; P's next task hasn't started,
  and A's stalled tasks fill all of A's task slots. Deadlocked {A}, target A:
  the grant lets one of A's tasks finish, which frees a slot for P's task.
  With a second actor B that has a free slot, or tasks that will finish, P's
  task runs on B and nothing is deadlocked.

The hatch grants one target per pass and re-checks on the next pass, so a
chain of deadlocks is broken one link at a time.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, FrozenSet, List, Set

from ray.data._internal.execution.interfaces.common import ActorId

if TYPE_CHECKING:
    from ray.data._internal.execution.lineage_tracker import ReconstructionPlanId

__all__ = ["ActorWaits", "PlanWaits", "WithheldDeadlock", "find_withheld_deadlock"]


@dataclass(frozen=True)
class ActorWaits:
    actor_id: ActorId
    # The budget stays exhausted after its drainable outputs are consumed.
    stuck: bool
    # Plans this actor holds withheld blocks for.
    withheld_plans: FrozenSet[ReconstructionPlanId] = frozenset()
    num_tasks_in_flight: int = 0


@dataclass(frozen=True)
class PlanWaits:
    plan_id: ReconstructionPlanId
    # Actors running this plan's tasks in the operator.
    running_actor_ids: FrozenSet[ActorId] = frozenset()
    # Some task the plan still needs hasn't started yet.
    has_unstarted_tasks: bool = False


@dataclass
class WithheldDeadlock:
    # Stuck actors that no amount of waiting will unblock.
    deadlocked: Set[ActorId] = field(default_factory=set)
    # Deadlocked actors to unblock, best first.
    targets: List[ActorId] = field(default_factory=list)
    # Plans that won't complete by waiting. Their withheld blocks stay put
    # until a grant breaks the deadlock, so they don't count as budget a
    # peer frees up by waiting.
    stalled_plans: Set[ReconstructionPlanId] = field(default_factory=set)


def find_withheld_deadlock(
    actors: List[ActorWaits], plans: List[PlanWaits], has_free_task_slot: bool
) -> WithheldDeadlock:
    """Find the stuck actors that can never progress, and whom to unblock.

    Progress is the least fixed point: an actor that isn't stuck progresses; a
    plan completes when all its running actors progress (a running actor that
    has left the pool counts as progressing) and, if it has tasks not yet
    started, a task slot is or will be free; a stuck actor progresses once any
    plan it holds blocks for completes. The stuck actors left over are
    deadlocked.

    Targets are deadlocked actors running tasks of plans that deadlocked
    actors wait on, ranked by how many deadlocked actors wait on them. When
    none qualifies, the plans wait on task slots held by deadlocked actors;
    then any deadlocked actor with tasks in flight frees a slot once unblocked.
    """
    by_id: Dict[ActorId, ActorWaits] = {a.actor_id: a for a in actors}
    progressing: Set[ActorId] = {a.actor_id for a in actors if not a.stuck}
    completing: Set[ReconstructionPlanId] = set()

    changed = True
    while changed:
        changed = False
        slot_will_free = has_free_task_slot or any(
            by_id[a].num_tasks_in_flight > 0 for a in progressing
        )
        for plan in plans:
            if plan.plan_id in completing:
                continue
            if all(
                a in progressing or a not in by_id for a in plan.running_actor_ids
            ) and (not plan.has_unstarted_tasks or slot_will_free):
                completing.add(plan.plan_id)
                changed = True
        for actor in actors:
            if actor.actor_id in progressing:
                continue
            if actor.withheld_plans & completing:
                progressing.add(actor.actor_id)
                changed = True

    deadlocked = {a.actor_id for a in actors if a.actor_id not in progressing}
    if not deadlocked:
        return WithheldDeadlock()

    waiters = Counter(
        plan_id for a in deadlocked for plan_id in by_id[a].withheld_plans
    )
    score: Counter = Counter()
    for plan in plans:
        for actor_id in plan.running_actor_ids & deadlocked:
            score[actor_id] += waiters[plan.plan_id]
    targets = [a for a, s in score.items() if s > 0]
    if not targets:
        targets = [a for a in deadlocked if by_id[a].num_tasks_in_flight > 0]
    targets.sort(key=lambda a: (-score[a], str(a)))
    stalled_plans = {p.plan_id for p in plans if p.plan_id not in completing}
    return WithheldDeadlock(
        deadlocked=deadlocked, targets=targets, stalled_plans=stalled_plans
    )
