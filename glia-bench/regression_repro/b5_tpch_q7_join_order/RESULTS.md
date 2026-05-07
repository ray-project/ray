# b5_tpch_q7 — devpod results

## Target regression

Friend's `comparison.md` shows `tpch_q7_autoscaling` regressing **−12.2%**
on glia (147s) vs master (129s). Friend's logs flagged a different op
ordering (master moves a `ReadParquet` from position 11 → 15, interleaving
it with joins). Suspected cause: logical-op refactor stack
(#62321 / #62400 / #62568 / #63090 / #63107).

## Devpod result

| Tree | Walls (3 reps) | Mean | vs master |
|---|---|---|---|
| **glia (m6_fixed)** | [80.07, 82.20, 80.10] | **80.79s** | +100% |
| **master nightly** | [39.82, 39.90, 41.42] | **40.38s** | (baseline) |

**Reproduces at 2× wall — much larger than upstream's 12% gap.**

## Diagnosis: not join-order, it's block-format

The plans **match** between the two trees:

```
ReadParquet→SplitBlocks(9) -> ReadParquet→SplitBlocks(24) -> ReadParquet→SplitBlocks(48)
  -> Join -> Join -> ReadParquet→SplitBlocks(6) -> Join
  -> ReadParquet→SplitBlocks(12) -> ReadParquet→SplitBlocks(48) -> Join -> Join
  -> Filter→MapBatches -> HashAggregate
```

So the regime hit on devpod is *not* the optimizer-reordering one the friend
saw at sf100. Per-op stats reveal the actual driver:

| | glia | master | ratio |
|---|---|---|---|
| All 5 Joins | "executed in 72.25s" | "executed in 33.54s" | 2.15× |
| HashAggregate | 72.25s | 33.54s | 2.15× |
| **Filter→MapBatches output_size_bytes/block** | **134 MB** | **70 MB** | **1.91×** |
| Output rows/block | 11.5 M | 11.5 M | 1.0× |

Same row count, ~half the bytes. That signature matches commit
**#61733 `[Data] Deprecate Pandas: Convert Pandas UDF batches as Arrow blocks
internally`**. Master converts UDF batches to Arrow internally; glia stores
them as Pandas. The 2× larger blocks then propagate through the hash-shuffle
for every Join and the HashAggregate. The wall delta we see is dominated by
this block-size cascade.

Master's `DataContext` confirms: `'batch_to_block_arrow_format': 'True'`
(only present on master).

## Implication for the rebase

The 2× wall reduction we measure is mostly attributable to #61733, which is
a self-contained block-format change. Rebasing m6_fixed onto master will
pick this up automatically.

The friend's specific 12% wall delta on `tpch_q7_autoscaling` (with a
slightly different plan ordering) likely combines:
- This block-format effect (small at the friend's scale because they
  presumably already serialize through Arrow at most points)
- Plan-ordering improvements from the logical-operator refactor stack
- Optimizer ordering visible at higher cluster scale

We can't isolate the plan-reordering portion on devpod; both trees produce
identical plans here. The friend's join-order observation may need a larger,
more constrained DAG (or sf100 scale) to surface.

## Validity check

- Reproduces direction (master faster) ✓
- Within-tree variance: ~1–2s; 40s gap is ≫ noise ✓
- Plans identical ⇒ regression is execution-time, not planner ✓
- Output-bytes ratio gives clean attribution to #61733 ✓
