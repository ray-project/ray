VOID: blind rig. The positive control did not fire.

  baseline p99 (idle_a/idle_b)   6.16 / 9.57 ms  -> noise floor 43%
  design   (26 writes/s)         9.23 ms  (+17%)
  overload (2069 writes/s)       9.42 ms  (+20%)

The pre-registered rule is explicit: if `overload` does not produce a measurable
regression the rig cannot see regressions at all, and `design`'s clean result
means nothing. Both deltas sit below the 43% noise floor, so nothing here is
distinguishable from run-to-run variation.

Three signs the measurement, not the system, is at fault:

1. The noise floor (43%) is larger than the effect size the claim cares about
   (25%). Two IDENTICAL arms differed by more than any treatment did.
2. `overload` produced a LOWER p95 (2.46 ms vs 4.09 ms idle) and a LOWER actor
   round-trip p99 (12.96 ms vs 22.01 ms). Load making things faster is not a
   physical result; it means the arms are dominated by something other than the
   load -- warm-up and scheduling drift across sequentially-run arms.
3. The victim slept 10 ms between operations, so it issued only ~80 ops/s and
   spent almost all its time asleep. Its latency distribution is measuring
   wake-up jitter, not GCS.

What is worth carrying forward, as a hypothesis rather than a result: a single
loader sustained 2069 writes/s -- well above the ~593/s aggregate figure from
stage B -- without visibly disturbing the victim. GCS internal_kv may simply not
be a contended resource at any rate R4 could generate. That is a good story for
the design, which is exactly why it must not be accepted from a rig that cannot
detect the opposite.

REDESIGN for the next iteration:
  - victim runs back-to-back with no sleep (thousands of samples/s)
  - baseline arms INTERLEAVED between load arms, so each load arm is compared
    against its temporal neighbour and drift cancels
  - load swept (26 / 200 / 1000 / 4000 / 8 parallel loaders) to find the rate at
    which regression appears, which calibrates the rig AND maps the headroom
  - an INDEPENDENT positive control that does not depend on the ledger at all:
    a loader writing multi-megabyte values, which must move p99 if the victim
    metric works
