"""Single-process A/B: same Parquet file decoded by the arrow-rs crate and by
PyArrow (both as ~5-chunk tables like the release read op), then Ray
serialization (pickle only) and ray.put (pickle + copy into plasma) timed.
Usage: python ser_micro.py <parquet_path> [batch_rows]"""
import sys
import time
import gc
import os
import pyarrow as pa
import pyarrow.parquet as pq
import ray
import ray_data_arrow_rs as rs

p = sys.argv[1]
pf = pq.ParquetFile(p)
RGS = (
    [int(x) for x in os.environ["RG"].split(",")]
    if os.environ.get("RG")
    else list(range(pf.metadata.num_row_groups))
)
n = sum(pf.metadata.row_group(i).num_rows for i in RGS)
print(
    "file rows",
    pf.metadata.num_rows,
    "row_groups",
    pf.metadata.num_row_groups,
    "using",
    RGS,
    "rows",
    n,
)
bs = int(sys.argv[2]) if len(sys.argv) > 2 else max(1, n // 5)


def t_pa():
    return pa.Table.from_batches(list(pf.iter_batches(batch_size=bs, row_groups=RGS)))


def t_rs():
    h = rs.open_parquet_file(p, page_index=False)
    r = h.read_row_groups(row_groups=RGS, batch_size=bs, k=1)
    rbr = pa.RecordBatchReader.from_stream(r)
    return pa.Table.from_batches(list(rbr))


def describe(name, t):
    print(
        f"[{name}] rows={t.num_rows} nbytes={t.nbytes/1e6:.1f}MB chunks/col={t.column(0).num_chunks}"
    )
    oddities = []
    for col in t.columns:
        for ch in col.chunks:
            if ch.offset != 0:
                oddities.append(
                    (col._name if hasattr(col, "_name") else "?", "offset", ch.offset)
                )
            for b in ch.buffers():
                if b is not None and b.address % 64:
                    oddities.append(("align", b.address % 64))
    print(f"   non-zero offsets / misaligned buffers: {len(oddities)} {oddities[:3]}")
    return t


def timeit(label, fn, reps=4):
    ts = []
    for _ in range(reps):
        gc.collect()
        t0 = time.perf_counter()
        r = fn()
        ts.append(time.perf_counter() - t0)
        del r
    print(f"   {label:<28} " + " ".join(f"{x*1000:7.1f}ms" for x in ts))


ray.init(address="auto", log_to_driver=False) if os.environ.get(
    "USE_EXISTING"
) else ray.init(
    address="local", num_cpus=2, include_dashboard=False, log_to_driver=False
)
ctx = ray._private.worker.global_worker.get_serialization_context()
for name, mk in (("pyarrow", t_pa), ("arrow-rs", t_rs)):
    t0 = time.perf_counter()
    t = mk()
    print(f"\n[{name}] decode {time.perf_counter()-t0:.2f}s")
    describe(name, t)
    timeit("serialize (pickle only)", lambda: ctx.serialize(t))
    timeit("ray.put (pickle+plasma)", lambda: ray.put(t))
    tc = t.combine_chunks()  # copies into pyarrow's pool, 1 chunk
    timeit("ray.put combine_chunks", lambda: ray.put(tc))
    tcopy = pa.Table.from_batches(
        [
            pa.RecordBatch.from_arrays(
                [c.take(pa.array(range(len(c)))) for c in b.columns],
                names=b.schema.names,
            )
            for b in t.to_batches()
        ]
    )
    timeit("ray.put pa-realloc copy", lambda: ray.put(tcopy))
    t = tc = tcopy = None  # noqa: F841 - release before the next arm
ray.shutdown()
