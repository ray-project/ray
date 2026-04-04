import ray
from ray.data.context import DataContext, ShuffleStrategy

results = []
for i in range(30):
    ctx = DataContext.get_current()
    ctx._shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    ds = ray.data.from_items([{"id": n} for n in range(101)])
    ds = ds.repartition(target_num_rows_per_block=20, strict=True).materialize()

    counts = []
    for ref_bundle in ds.iter_internal_ref_bundles():
        for _, metadata in ref_bundle.blocks:
            counts.append(metadata.num_rows)

    last_is_remainder = counts[-1] == 1
    results.append(last_is_remainder)
    print(f"Run {i+1:2d}: {counts}  → last==1? {last_is_remainder}")

fails = results.count(False)
print(f"\n{fails}/30 runs would have failed the test")
