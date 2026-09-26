VOID: the negative control was green when it was required to be red.

The `prefixed_noreconcile` arm was supposed to demonstrate that deterministic
naming is useless unless somebody explicitly reconciles. It reported orphans=4
and created_now=4, which LOOKED like the required leak. It was not one.

Phase 2 recreated the missing children with `get_if_exists=True` and the SAME
deterministic names, so Ray handed back the EXISTING actors. The pids prove it:

  child-2  pid 994628 before -> 994628 after     (same actor)
  child-3  pid 994668 -> 994668
  child-4  pid 994705 -> 994705
  child-5  pid 994742 -> 994742

Nothing was orphaned and nothing was duplicated. The arm labelled those four as
orphans purely because their names were absent from `adopted_names`, which is a
bookkeeping artifact of the rig, not an observation about the cluster.

So the rig did not measure what it claimed, and by the rule pre-registered in
experiments/C5.md ("if the control does not leak, the rig is not measuring what
it claims, and the run is VOID") nothing may be concluded from the other arms
either.

The finding underneath is real and better than the prediction:
`get_if_exists=True` combined with deterministic, scope-prefixed names IS a
reconciliation mechanism on its own. No listing pass is needed. That belongs in
the design, but it has to be measured deliberately rather than inferred from a
broken control.

The `opaque` arm remains sound and is in fact the correct negative control: its
four unrecorded children were genuinely orphaned and genuinely duplicated, with
four distinct new pids (994011/994047/994085/994125) beside four still-live
originals. Re-run restructures the arms around that.
