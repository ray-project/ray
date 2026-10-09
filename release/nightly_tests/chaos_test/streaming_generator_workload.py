import numpy as np

import ray
from ray._common.test_utils import wait_for_condition
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


def run_streaming_generator_workload(total_num_cpus, smoke):
    """Run streaming generator workload.

    Spreads streaming generators across the nodes to ensure
    chaos events affect the generators. Tests that streaming generators
    work correctly with retries when there are node failures or transient
    network failures.
    """

    @ray.remote(num_cpus=1, max_retries=-1)
    def streaming_generator(num_items, item_size_mb):
        for i in range(num_items):
            data = np.zeros(item_size_mb * 1024 * 1024, dtype=np.uint8)
            yield data

    @ray.remote(num_cpus=1, max_retries=-1)
    def consume_streaming_generator(num_items, item_size_mb):
        gen = streaming_generator.remote(num_items, item_size_mb)

        count = 0
        total_bytes = 0
        for item_ref in gen:
            data = ray.get(item_ref)
            count += 1
            total_bytes += data.nbytes

        return (count, total_bytes)

    # Get alive nodes to distribute generators across the cluster
    alive_nodes = [n for n in ray.nodes() if n.get("Alive", False)]
    NUM_GENERATORS = 2 * len(alive_nodes)

    # For smoke mode, run fewer items
    if smoke:
        ITEMS_PER_GENERATOR = 10
    else:
        ITEMS_PER_GENERATOR = 500
    ITEM_SIZE_MB = 10

    print(
        f"Starting {NUM_GENERATORS} concurrent streaming generators "
        f"({ITEMS_PER_GENERATOR} items of {ITEM_SIZE_MB}MB each)"
    )
    print(
        f"Expected total data: "
        f"{NUM_GENERATORS * ITEMS_PER_GENERATOR * ITEM_SIZE_MB / 1024:.2f} GB"
    )

    # Distribute generators across nodes to maximize chaos impact
    tasks = []
    for i in range(NUM_GENERATORS):
        node = alive_nodes[i % len(alive_nodes)]
        node_id = node["NodeID"]

        task = consume_streaming_generator.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=node_id, soft=True
            )
        ).remote(ITEMS_PER_GENERATOR, ITEM_SIZE_MB)
        tasks.append(task)

    results = ray.get(tasks)

    total_items = sum(count for count, _ in results)
    total_bytes = sum(bytes_val for _, bytes_val in results)

    print("All generators completed:")
    print(
        f"  Total items: {total_items} (expected {NUM_GENERATORS * ITEMS_PER_GENERATOR})"
    )
    print(f"  Total data: {total_bytes / (1024**3):.2f} GB")

    assert (
        total_items == NUM_GENERATORS * ITEMS_PER_GENERATOR
    ), f"Expected {NUM_GENERATORS * ITEMS_PER_GENERATOR} items, got {total_items}"

    # Consistency check
    wait_for_condition(
        lambda: (
            ray.cluster_resources().get("CPU", 0)
            == ray.available_resources().get("CPU", 0)
        ),
        timeout=60,
    )
