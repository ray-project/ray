#!/usr/bin/env python3
"""
Machine-checked derivation of the hash-shuffle design (design/hash-shuffle-bypass.md §0).

Method (per §0): NOT a Nash equilibrium — it is constraint propagation over
fundamental mutual-exclusion AXIOMS, then Pareto selection by the workload
OBJECTIVE. We encode the axioms as boolean clauses, assert an objective, and let
Z3 ENUMERATE every satisfying assignment over the decision variables. A variable
that is constant across all models is FORCED (derived); one that varies is FREE.

This is honest by construction: the proof is only as good as the encoding, so the
encoding is kept readable and each clause is tagged with its axiom (X1..X8). If a
choice we *claimed* was forced is actually free, the enumeration will show it.

Run:  python3 design/shuffle_design_derivation.py
Needs: pip install z3-solver
"""
from z3 import Bool, Solver, Implies, Or, And, Not, sat, PbEq

# ---------------------------------------------------------------- decision vars
V = {n: Bool(n) for n in [
    "handle_put",        # handle created via ray.put  (mapper-owned)        [Bundle 1]
    "handle_return",     # handle is a normal task return (caller-owned)     [Bundle 2]
    "cleanup_refcount",  # cleanup via owner refcount->0 callback            [path B]
    "cleanup_executor",  # cleanup via Ray Data executor Release             [path A]
    "serve_pernode",     # fetch served by a per-node actor
    "serve_perworker",   # fetch served on each worker's CoreWorker server
    "sidechannel",       # reducer fetch via side-channel (bytes -> userspace)
    "rayget",            # reducer fetch via ray.get (RefBundle dataflow)
    "bulk_file",         # bulk stored in node-local files
    "bulk_plasma",       # bulk stored in plasma (ray.put)
    "actor_owns_inmem",  # the per-node actor owns the in-mem bytes (in write path)
    "holepunch",         # per-partition early reclaim via fallocate PUNCH_HOLE
    "core_change",       # design requires a Ray-core change
    "auto_recovery",     # recovery is core-automatic (vs executor-orchestrated)
]}
g = lambda n: V[n]

# ---------------------------------------------------------------- structural choices
def base_constraints(s):
    # The handle is NOT an exclusive choice: you may BOTH ray.put an anchor AND
    # return a reconstructable handle (the "double-return" hybrid, §4.10). So this
    # is at-least-one, not exactly-one. (An earlier model used PbEq(...)==1 here and
    # thereby silently excluded double-return from the design space.)
    s.add(Or(g("handle_put"), g("handle_return")))
    # exactly-one decisions
    s.add(PbEq([(g("cleanup_refcount"), 1), (g("cleanup_executor"), 1)], 1))
    s.add(PbEq([(g("serve_pernode"), 1),    (g("serve_perworker"), 1)], 1))
    s.add(PbEq([(g("sidechannel"), 1),      (g("rayget"), 1)], 1))
    # at-least-one bulk tier (both = mixed, allowed)
    s.add(Or(g("bulk_file"), g("bulk_plasma")))

    # ---- AXIOMS (each tagged; see §0.1) ----
    # X1: mapper-owned handle is INELIGIBLE_PUT (not reconstructable); a refcount
    #     callback needs the mapper to OWN the handle; core-auto recovery needs the
    #     handle to be a (reconstructable) RETURN.
    s.add(Implies(g("cleanup_refcount"), g("handle_put")))         # X1
    s.add(Implies(g("auto_recovery"),    g("handle_return")))      # X1
    # X3: no-2x  <->  side-channel (out-of-band read). (rayget => 2x)
    #     no2x is asserted as a goal, so here just bind it to the mechanism:
    #     enforced in objective() via sidechannel.
    # X5: controlled spill on a plasma tier needs a core change (uncontrolled otherwise)
    #     -> only bites if both controlled-spill goal and bulk_plasma; folded into X7-style.
    # X7: a refcount cleanup hook is a Ray-core change.
    s.add(Implies(g("cleanup_refcount"), g("core_change")))        # X7
    # X8: per-node serving cannot reuse the per-worker CoreWorker server.
    #     (pernode_scale goal => serve_pernode; enforced in objective())
    # X2: an in-memory tier survives worker death ONLY if a long-lived owner (the
    #     actor) owns it -> actor in the write path.
    s.add(Implies(g("actor_owns_inmem"), g("bulk_plasma")))        # X2 (only meaningful w/ plasma)

def objective(s, fits, want_no2x=True, want_ft=True, low_core=True,
              pernode_scale=True, want_auto_recovery=False):
    # scenario input
    if fits:
        s.add(g("bulk_plasma"))                 # data fits -> in-memory tier usable
    else:
        s.add(g("bulk_file"))                   # X2/X5: doesn't fit -> must use files
        s.add(Not(g("bulk_plasma")))            # durable tier is files (no plasma bulk)
    # goals
    if want_no2x:
        s.add(g("sidechannel"))                 # X3: no-2x <=> side-channel
    if low_core:
        s.add(Not(g("core_change")))            # "low core invasion" hard constraint
    if pernode_scale:
        s.add(g("serve_pernode"))               # X8: scaling/survive-death serving
    if want_auto_recovery:
        s.add(g("auto_recovery"))               # X1 -> forces handle_return
    # want_ft: durable data must be recoverable. files survive worker death (X2);
    # a plasma-only durable tier would need actor_owns_inmem. Encode:
    if want_ft:
        s.add(Or(g("bulk_file"), And(g("bulk_plasma"), g("actor_owns_inmem"))))

REPORT = ["sidechannel", "rayget", "cleanup_executor", "cleanup_refcount",
          "serve_pernode", "serve_perworker", "bulk_file", "bulk_plasma",
          "handle_return", "handle_put", "holepunch", "core_change",
          "auto_recovery", "actor_owns_inmem"]

def analyze(title, **objkw):
    s = Solver(); base_constraints(s); objective(s, **objkw)
    if s.check() != sat:
        print(f"\n=== {title} ===\n  UNSAT — objective is infeasible under the axioms."); return
    # enumerate all models projected onto REPORT vars
    models = []
    while s.check() == sat:
        m = s.model()
        assign = {n: bool(m.eval(g(n), model_completion=True)) for n in REPORT}
        models.append(assign)
        s.add(Or([g(n) != assign[n] for n in REPORT]))   # block this projection
    forced, free = {}, []
    for n in REPORT:
        vals = {a[n] for a in models}
        if len(vals) == 1: forced[n] = vals.pop()
        else: free.append(n)
    print(f"\n=== {title} ===")
    print(f"  satisfying designs (over reported vars): {len(models)}")
    print(f"  FORCED:")
    for n, v in forced.items():
        print(f"     {n:18} = {v}")
    print(f"  FREE (not pinned by axioms+objective): {free or '(none)'}")

if __name__ == "__main__":
    analyze("PB corpus: doesn't fit, low-core, FT, no-2x, per-node",
            fits=False)
    analyze("PB corpus + WANT core-automatic recovery",
            fits=False, want_auto_recovery=True)
    analyze("Small data: fits in memory, FT, no-2x (no low-core demand)",
            fits=True, low_core=False, pernode_scale=False)
