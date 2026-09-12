PARTIAL: clauses 1 and 3 measured; clause 2 STARVED, not tested.

Two numbers gave it away.

1. `async_outage` completed 91 units during the outage window -- IDENTICAL to
   `async_nokill`'s 91. Identical counts across a supposedly decisive parameter
   is the same signature as the inert stale-tail cap in iteration 10.

   The cause is an arithmetic error in the pre-registration, not a rig defect.
   The queue holds BATCHES, not units, so its capacity is Q*W = 64*3 = 192
   units, while a 35 s outage at ~2.8 units/s only needs ~98. The buffer was
   roughly twice the size of the test. Max queued reached 31 batches (~93
   units), which matches the outage almost exactly and confirms the writer WAS
   blocked -- the queue simply never came close to its bound.

   So clause 1 is measured, and measured strongly: with sufficient buffer the
   outage becomes INVISIBLE to the work loop. But the pre-registered band of
   50-75 units was wrong, and the pre-registered negative control NC-queue-bound
   explicitly says that if work continues without the queue bounding it, the run
   cannot prove clause 2.

2. Both crash arms reported redo = 0, async and sync alike, because at the
   moment of the kill the queue had already drained (executed 75, durable 75).
   A redo-window measurement taken when nothing is in flight measures nothing.
   That is a STARVED control, the same failure as NC13 in iteration 10.

Valid from this run:
  - clause 1: work continues through an outage (91 units vs the sync baseline's
    ZERO in runs/20260910-063616-C7inplace2)
  - clause 3: contiguous, duplicate-free, no writer errors, in all four arms;
    async_nokill completed all 400 units cleanly (NC-nokill green as required)

NOT established: clause 2, the redo bound. Needs Q small enough to saturate
inside the outage (Q*W << units produced during the outage) and a coordinator
crash taken WHILE the queue is backed up.
