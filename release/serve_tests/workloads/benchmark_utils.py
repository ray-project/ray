import asyncio
import time
import numpy as np


async def measure_latency_ms(async_fn, args, expected_output, num_requests=10):
    # warmup for 1sec
    start = time.time()
    while time.time() - start < 1:
        await async_fn(args)

    latency_stats = []
    for _ in range(num_requests):
        start = time.time()
        await async_fn(args) == expected_output
        end = time.time()
        latency_stats.append((end - start) * 1000)

    return latency_stats


async def measure_throughput_tps(async_fn, args, expected_output, duration_secs=10):
    # warmup for 1sec
    start = time.time()
    while time.time() - start < 1:
        await async_fn(args)

    tps_stats = []
    for _ in range(duration_secs):
        start = time.time()
        request_completed = 0
        while time.time() - start < 1:
            await async_fn(args) == expected_output
            request_completed += 1
        tps_stats.append(request_completed)

    return tps_stats


async def benchmark_throughput_tps(
    dag_handle,
    expected,
    duration_secs=10,
    num_clients=1,
):
    """Call deployment handle in a blocking for loop from multiple clients."""
    client_tasks = [measure_throughput_tps for _ in range(num_clients)]

    throughput_stats_tps_list = await asyncio.gather(
        *[
            client_task(
                dag_handle.predict.remote,
                0,
                expected,
                duration_secs=duration_secs,
            )
            for client_task in client_tasks
        ]
    )
    throughput_stats_tps = []
    for client_rst in throughput_stats_tps_list:
        throughput_stats_tps.extend(client_rst)

    mean = round(np.mean(throughput_stats_tps), 2)
    std = round(np.std(throughput_stats_tps), 2)
    return mean, std


async def benchmark_latency_ms(dag_handle, expected, num_requests=100, num_clients=1):
    """Call deployment handle in a blocking for loop from multiple clients."""
    client_tasks = [measure_latency_ms for _ in range(num_clients)]

    latency_stats_ms_list = await asyncio.gather(
        *[
            client_task(
                dag_handle.predict.remote,
                0,
                expected,
                num_requests=num_requests,
            )
            for client_task in client_tasks
        ]
    )
    latency_stats_ms = []
    for client_rst in latency_stats_ms_list:
        latency_stats_ms.extend(client_rst)

    mean = round(np.mean(latency_stats_ms), 2)
    std = round(np.std(latency_stats_ms), 2)
    return mean, std
