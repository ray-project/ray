import ray
import time

class FakeGPU:
    def __init__(self): pass
    def __call__(self, b): time.sleep(0.005); return b

ray.init(num_cpus=8, log_to_driver=False, logging_level="DEBUG", include_dashboard=False)
print(f"ray.__file__: {ray.__file__}")
ds = ray.data.range(100_000)
ds = ds.map_batches(lambda b: b, batch_size=10_000)
ds = ds.map_batches(FakeGPU, batch_size=2_000, num_cpus=1, concurrency=(4,4))
list(ds.iter_internal_ref_bundles())
print("\n\nSTATS:")
print(ds.stats()[:2000])
