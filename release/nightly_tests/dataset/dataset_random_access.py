import time
import random
import os
import json


def main():
    if "SMOKE_TEST" in os.environ:
        # On a laptop:
        # Multiget throughput: 13236.238851667515 keys / second
        # Single get throughput: 3035.6024155680234 keys / second
        nrow = 100_000_000
        nclient = 1
        batch_size = 1000
        parallelism = 20
        num_workers = 1
        run_time = 3
    else:
        # On a 20-node cluster:
        # Multiget throughput: 516600.7075172231 keys / second
        # Single get throughput: 84203.5265246087 keys / second
        nrow = 10_000_000_000
        nclient = 40
        batch_size = 100000
        parallelism = 200
        num_workers = 400
        run_time = 15

    ds = ray.data.range_table(nrow, parallelism=parallelism)
    rmap = ds.to_random_access_dataset("value", num_workers=num_workers)

    print("Multiget throughput: ", end="")
    start = time.time()

    @ray.remote(scheduling_strategy="SPREAD")
    def client():
        total = 0
        rand_values = [random.randint(0, nrow) for _ in range(batch_size)]
        while time.time() - start < run_time:
            rmap.multiget(rand_values)
            total += batch_size
        return total

    total = sum(ray.get([client.remote() for _ in range(nclient)]))
    multiget_qps = total / (time.time() - start)
    print(multiget_qps, "keys / second")

    print("Single get throughput: ", end="")
    start = time.time()

    @ray.remote(scheduling_strategy="SPREAD")
    def client():
        total = 0
        while time.time() - start < run_time:
            ray.get([rmap.get_async(random.randint(0, nrow)) for _ in range(1000)])
            total += 1000
        return total

    total = sum(ray.get([client.remote() for _ in range(nclient)]))
    get_qps = total / (time.time() - start)
    print(get_qps, "keys / second")

    return get_qps, multiget_qps


if __name__ == "__main__":
    import ray

    print("Connecting to Ray cluster...")
    ray.init(address="auto")

    start = time.time()
    get_qps, multiget_qps = main()
    delta = time.time() - start

    print(f"success! total time {delta}")
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        f.write(
            json.dumps(
                {
                    "perf_metrics": [
                        {
                            "perf_metric_name": "get_qps",
                            "perf_metric_value": get_qps,
                            "perf_metric_type": "THROUGHPUT",
                        },
                        {
                            "perf_metric_name": "multiget_qps",
                            "perf_metric_value": multiget_qps,
                            "perf_metric_type": "THROUGHPUT",
                        },
                    ],
                    "success": 1,
                }
            )
        )
