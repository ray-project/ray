from math import ceil
import ray
import time
import psutil


@ray.remote
def allocate_memory(
    total_allocate_bytes: int,
    num_chunks: int = 10,
    allocate_interval_s: float = 0,
) -> int:
    chunks = []
    # divide by 8 as each element in the array occupies 8 bytes
    bytes_per_chunk = total_allocate_bytes / 8 / num_chunks
    for _ in range(num_chunks):
        chunks.append([0] * ceil(bytes_per_chunk))
        time.sleep(allocate_interval_s)
    return 1


def get_additional_bytes_to_reach_memory_usage_pct(pct: float) -> int:
    node_mem = psutil.virtual_memory()
    used = node_mem.total - node_mem.available
    bytes_needed = node_mem.total * pct - used
    assert bytes_needed > 0, "node has less memory than what is requested"
    return int(bytes_needed)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-tasks",
        help="number of tasks to process in total",
        default="20",
        type=int,
    )

    parser.add_argument(
        "--mem-pct-per-task",
        help="memory to allocate per task as a fraction of the node's available memory",
        default="0.4",
        type=float,
    )

    args = parser.parse_args()

    cpu_per_task = 1

    bytes_per_task = get_additional_bytes_to_reach_memory_usage_pct(
        args.mem_pct_per_task
    )

    start = time.time()
    task_refs = [
        allocate_memory.options(num_cpus=cpu_per_task).remote(
            total_allocate_bytes=bytes_per_task, allocate_interval_s=1
        )
        for _ in range(args.num_tasks)
    ]
    results = [ray.get(ref) for ref in task_refs]
    end = time.time()

    print(f"processed {args.num_tasks} tasks in {end-start} seconds")
