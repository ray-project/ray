"""In-worker A/B: N concurrent Ray tasks each decode 5 row groups of a local
lineitem file (crate or PyArrow) into a ~5-chunk table and time ray.put of it.
Usage: python put_micro.py <parquet> <num_cpus> [tasks_per_arm]"""
import sys
import time
import os
import statistics as st
import resource
import pyarrow.parquet as pq
import ray

p, ncpu = sys.argv[1], int(sys.argv[2])
ntasks = int(sys.argv[3]) if len(sys.argv) > 3 else ncpu


@ray.remote(num_cpus=1)
def task(arm, path, rgs, bs):
    import pyarrow as pa
    import pyarrow.parquet as pq
    import time

    t0 = time.perf_counter()
    if arm == "pa":
        pf = pq.ParquetFile(path)
        t = pa.Table.from_batches(list(pf.iter_batches(batch_size=bs, row_groups=rgs)))
    else:
        import ray_data_arrow_rs as rs

        h = rs.open_parquet_file(path, page_index=False)
        r = h.read_row_groups(row_groups=rgs, batch_size=bs, k=1)
        t = pa.Table.from_batches(list(pa.RecordBatchReader.from_stream(r)))
        if arm == "rs_combine":
            t = t.combine_chunks()
    t1 = time.perf_counter()
    ru0 = resource.getrusage(resource.RUSAGE_SELF)
    c0 = time.process_time()
    ray.put(t)
    t2 = time.perf_counter()
    ru1 = resource.getrusage(resource.RUSAGE_SELF)
    c1 = time.process_time()
    return {
        "decode": t1 - t0,
        "put": t2 - t1,
        "nbytes": t.nbytes,
        "chunks": t.column(0).num_chunks,
        "pid": os.getpid(),
        "cpu": c1 - c0,
        "minflt": ru1.ru_minflt - ru0.ru_minflt,
        "majflt": ru1.ru_majflt - ru0.ru_majflt,
        "nivcsw": ru1.ru_nivcsw - ru0.ru_nivcsw,
        "nvcsw": ru1.ru_nvcsw - ru0.ru_nvcsw,
    }


ray.init(address="local", num_cpus=ncpu, include_dashboard=False, log_to_driver=False)
nrg = pq.ParquetFile(p).metadata.num_row_groups
rows = pq.ParquetFile(p).metadata.row_group(0).num_rows
windows = [list(range(i, min(i + 5, nrg))) for i in range(0, nrg - 4, 5)]
arms = sys.argv[4].split(",") if len(sys.argv) > 4 else ["pa", "rs", "rs_combine"]
for arm in arms:
    t0 = time.perf_counter()
    refs = [task.remote(arm, p, windows[i % len(windows)], rows) for i in range(ntasks)]
    res = ray.get(refs)
    wall = time.perf_counter() - t0
    puts = sorted(r["put"] for r in res)
    decs = [r["decode"] for r in res]
    print(
        f"[{arm:10s} cpus={ncpu} tasks={ntasks}] wall {wall:5.1f}s  put p50 {st.median(puts)*1000:7.0f}ms mean {st.mean(puts)*1000:7.0f}ms max {puts[-1]*1000:7.0f}ms | decode p50 {st.median(decs):.2f}s | put cpu p50 {st.median(r['cpu'] for r in res)*1000:6.0f}ms minflt p50 {st.median(r['minflt'] for r in res):7.0f} majflt {st.median(r['majflt'] for r in res):4.0f} nvcsw {st.median(r['nvcsw'] for r in res):5.0f} nivcsw {st.median(r['nivcsw'] for r in res):5.0f} | {res[0]['nbytes']/1e6:.0f}MB chunks {res[0]['chunks']} workers {len({r['pid'] for r in res})}",
        flush=True,
    )
    del refs, res
ray.shutdown()
