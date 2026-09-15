VOID: the negative control churned harder than the treatment arms.

  die_healthy (NO outage)  max_epoch=14  num_restarts=13
  outage_10                max_epoch=12  num_restarts=11
  outage_30                max_epoch=11  num_restarts=10

The control was required to show exactly one restart and epoch +1. It showed
thirteen. By the rule pre-registered in experiments/C28.md that voids the run --
and the direction is the diagnosis: restarts are ANTI-correlated with outage
duration, so the outage cannot be the cause.

CAUSE: the rig, and specifically the suicide mechanism.

`JobCoordinator` is declared `@ray.remote(max_restarts=-1, max_task_retries=-1)`.
The sampler calls `h.die.remote()`, which enters `os._exit(1)` and never
returns, so from Ray's point of view that task was IN FLIGHT when the actor
died. With `max_task_retries=-1` Ray replays it on the restarted actor, which
kills itself again, which triggers another restart, and so on -- for as long as
the calling sampler process stays alive to resubmit. The arms differ only in how
much of the sampler's lifetime was left after the outage, which is why the
control (whose sampler window was longest post-death) churned the most.

CONSEQUENCE FOR ITERATION 16: the `epochs=[18]` observation that raised C28 in
the first place has the same cause. It was my rig killing the actor eighteen
times, not R4 thrashing under a GCS outage. C28 was raised on a misreading, and
the claim must be re-tested rather than treated as confirmed-in-spirit.

WHAT SURVIVES AS AN OBSERVATION (not as evidence, since the run is void): the
ledger key count sat at 3 in every arm through 10-13 restarts, and the recovered
`done` sets stayed contiguous with no duplicates. If that holds in a valid run it
is clause 1 of C28.

REAL LESSON, worth keeping regardless: `max_task_retries=-1` REPLAYS in-flight
tasks on a restarted actor. For R4 that is mostly desirable -- `run()` resuming
is the point -- but it means every coordinator method must be idempotent under
replay, including any administrative method. That is a design constraint the
doc does not currently state.

FIX: `die()` now starts a timer thread and returns immediately, so the task
completes normally and is never a retry candidate.
