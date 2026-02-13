"""
Reproduction for GitHub Issue #59914:
[Core] Excessive number of small objects stored in memory

The in-process memory store accumulates small objects without automatic
cleanup when using ray.get() on actor task results in a loop. This causes
the owner process memory to grow unboundedly.

Objects < 100KB are stored inline in the in-process memory store (heap)
rather than in Plasma. The store has no eviction mechanism - objects are
only deleted when their reference count drops to zero and Python GC runs.
"""
import ray
import time
import os
import gc
import psutil

ray.init()

@ray.remote
class DataProducer:
    """Actor that returns small objects (< 100KB each, stored in-process)."""
    def produce(self, size=256):
        # Return a small float array - well under 100KB threshold
        # Each call creates a new small object in the caller's in-process store
        return [float(i) for i in range(size)]

producer = DataProducer.remote()

process = psutil.Process(os.getpid())

# Measure baseline memory
gc.collect()
time.sleep(0.5)
baseline_rss = process.memory_info().rss / (1024 * 1024)
print(f"Baseline RSS: {baseline_rss:.1f} MB")

NUM_BATCHES = 20
CALLS_PER_BATCH = 500

print(f"\nPerforming {NUM_BATCHES} batches of {CALLS_PER_BATCH} ray.get() calls each...")
print("Each call returns a small object (~2KB) stored in the in-process memory store.\n")

for batch in range(NUM_BATCHES):
    # Submit and retrieve many small objects
    refs = [producer.produce.remote() for _ in range(CALLS_PER_BATCH)]
    results = ray.get(refs)

    # Explicitly delete all references
    del refs
    del results

    # Force garbage collection
    gc.collect()

    current_rss = process.memory_info().rss / (1024 * 1024)
    growth = current_rss - baseline_rss
    total_calls = (batch + 1) * CALLS_PER_BATCH
    print(f"  Batch {batch + 1:2d}: {total_calls:5d} calls, "
          f"RSS: {current_rss:.1f} MB (growth: {growth:+.1f} MB)")

final_rss = process.memory_info().rss / (1024 * 1024)
total_growth = final_rss - baseline_rss
total_calls = NUM_BATCHES * CALLS_PER_BATCH

print(f"\nFinal RSS: {final_rss:.1f} MB")
print(f"Total growth: {total_growth:.1f} MB over {total_calls} calls")
print(f"Growth per call: ~{total_growth * 1024 / total_calls:.1f} KB")

if total_growth > 20:
    print("\nBUG REPRODUCED: Significant memory growth despite deleting all references")
    print("and running gc.collect(). The in-process memory store is not releasing objects.")
elif total_growth > 5:
    print("\nPARTIAL REPRODUCTION: Moderate memory growth detected.")
    print("The in-process store may be holding some objects longer than expected.")
else:
    print("\nMemory growth is minimal - GC may be working adequately for this workload.")
    print("The issue is more pronounced with higher call rates and long-running services.")

ray.shutdown()
