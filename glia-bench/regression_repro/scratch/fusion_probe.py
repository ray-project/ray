"""Probe what gates Read+Map fusion."""
import os, sys, tempfile
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

# Many small files → high natural block count → split_factor=1
TMPDIR = "/tmp/fusion_probe_files"
N_FILES = 250
N_ROWS = 10_000

if not os.path.exists(TMPDIR):
    os.makedirs(TMPDIR)
    rng = np.random.default_rng(0)
    for i in range(N_FILES):
        t = pa.table({"col0": rng.integers(0, 100, N_ROWS, dtype=np.int64)})
        pq.write_table(t, f"{TMPDIR}/p{i:04d}.parquet")
print(f"Files ready: {N_FILES}")

import ray
print(f"ray.__file__: {ray.__file__}")
print(f"ray.__version__: {ray.__version__}")

ray.init(num_cpus=8, log_to_driver=False, logging_level="WARNING", include_dashboard=False)

# Test 1: batch_size=int
ds = ray.data.read_parquet(TMPDIR).map_batches(lambda b: b, batch_size=10_000)
list(ds.iter_internal_ref_bundles())
import re
plan_text = ds.stats()
m = re.findall(r"^Operator \d+ (.+?):", plan_text, re.MULTILINE)
print(f"\n[batch_size=10_000]: {len(m)} ops")
for op in m: print(f"  {op}")

# Test 2: batch_size="auto"
ray.shutdown()
ray.init(num_cpus=8, log_to_driver=False, logging_level="WARNING", include_dashboard=False)
ds = ray.data.read_parquet(TMPDIR).map_batches(lambda b: b, batch_size="auto")
list(ds.iter_internal_ref_bundles())
plan_text = ds.stats()
m = re.findall(r"^Operator \d+ (.+?):", plan_text, re.MULTILINE)
print(f"\n[batch_size='auto']: {len(m)} ops")
for op in m: print(f"  {op}")

# Test 3: no batch_size
ray.shutdown()
ray.init(num_cpus=8, log_to_driver=False, logging_level="WARNING", include_dashboard=False)
ds = ray.data.read_parquet(TMPDIR).map_batches(lambda b: b)
list(ds.iter_internal_ref_bundles())
plan_text = ds.stats()
m = re.findall(r"^Operator \d+ (.+?):", plan_text, re.MULTILINE)
print(f"\n[no batch_size]: {len(m)} ops")
for op in m: print(f"  {op}")
