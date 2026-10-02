# Stage A rig — crash/interleaving model checker

Stdlib Python 3.8+, no dependencies, no Ray. See [`../poc-plan.md`](../poc-plan.md)
for why the first rig is an enumerator rather than a cluster.

```
kvfake.py       a model of GCS internal_kv, exactly as weak as the real thing
protocol.py     the progress-ledger protocol (M1/M2/M3), written once, as a generator
                of KV operations; negative controls are flags on Cfg, not forks
checker.py      the enumerator: crash / lost-ack injection over every schedule
                within a fault budget, invariants checked after every mutation
run_checker.py  harness -> runs/<id>/{results.json,stdout.log,config.json}
```

Run it:

```bash
cd design/ray-driver-resume-kv-ledger/poc
python3 run_checker.py --experiment C6 --out ../runs/$(date +%Y%m%d-%H%M%S)-C6
```

## Invariants

| id | what it catches | can a fault-free run violate it? |
|---|---|---|
| INV-DUR  | an acked unit is not recoverable | yes |
| INV-PTR  | `base_ptr` names a base that does not exist | yes |
| INV-GC   | keys below the winning epoch survive a clean startup | yes |
| INV-LIVE | no instance ever completed | **no — needs `lost_ack`** |
| INV-REDO | more work re-executed than `crashes x W` | **no — needs a crash** |

The last column is the important one. Four of the nine negative controls go red
with **zero** faults injected, which means on their own they prove only that the
invariant checker works — not that the fault model does. INV-LIVE and INV-REDO
exist so that a green result cannot be explained by injectors that do nothing.

## Determinism

The search is a deterministic depth-first enumeration of fault plans. There is
no randomness and therefore no seed: the same command produces the same traces,
and a counterexample is fully described by its plan (`[i0#7:crash_after, ...]`).
