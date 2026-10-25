import numpy as np

import ray
from ray._common.test_utils import wait_for_condition


def run_object_ref_borrowing_workload(total_num_cpus, smoke):
    """Run object ref borrowing workload.

    This test checks that borrowed refs
    remain valid even with node failures or transient network failures.
    """

    @ray.remote(num_cpus=1, max_retries=-1)
    def create_object(size_mb):
        data = np.zeros(size_mb * 1024 * 1024, dtype=np.uint8)
        return data

    @ray.remote(num_cpus=1, max_retries=-1)
    def borrow_object(borrowed_refs):
        data = ray.get(borrowed_refs[0])
        return len(data)

    # For smoke mode, run fewer iterations
    if smoke:
        NUM_ITERATIONS = 10
    else:
        NUM_ITERATIONS = 2000
    OBJECT_SIZE_MB = 10

    print(f"Starting borrowing test with {NUM_ITERATIONS * 2} total tasks")
    print(f"Object size: {OBJECT_SIZE_MB}MB per object")
    print(f"Expected total data: {NUM_ITERATIONS * 2 * OBJECT_SIZE_MB / 1024:.2f} GB")

    total_completed = 0
    total_bytes = 0

    for i in range(NUM_ITERATIONS):
        ref = create_object.remote(OBJECT_SIZE_MB)
        task_ref = borrow_object.remote([ref])
        size = ray.get(task_ref)
        total_completed += 1
        total_bytes += size

    refs = []
    for i in range(NUM_ITERATIONS):
        ref = create_object.remote(OBJECT_SIZE_MB)
        refs.append(borrow_object.remote([ref]))
    sizes = ray.get(refs)
    total_completed += len(sizes)
    total_bytes += sum(sizes)

    print("All tasks completed:")
    print(f"  Total tasks completed: {total_completed} (expected {NUM_ITERATIONS * 2})")
    print(f"  Total data processed: {total_bytes / (1024**3):.2f} GB")

    expected_total_tasks = NUM_ITERATIONS * 2
    assert (
        total_completed == expected_total_tasks
    ), f"Expected {expected_total_tasks} completions, got {total_completed}"
    expected_total_bytes = expected_total_tasks * OBJECT_SIZE_MB * 1024 * 1024
    assert (
        total_bytes == expected_total_bytes
    ), f"Expected {expected_total_bytes} bytes, got {total_bytes}"

    # Consistency check
    wait_for_condition(
        lambda: (
            ray.cluster_resources().get("CPU", 0)
            == ray.available_resources().get("CPU", 0)
        ),
        timeout=60,
    )
