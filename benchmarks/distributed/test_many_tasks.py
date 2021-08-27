import asyncio
import click
import json
import os
import ray
import time
import tqdm
import numpy as np


@ray.remote(num_cpus=0)
class SignalActor:
    def __init__(self):
        self.timestamps = []

    def reset(self, num_expected):
        self.start_time = time.time()
        self.num_expected = num_expected

    def send(self):
        self.timestamps.append(time.time() - self.start_time)
        assert len(self.timestamps) <= self.num_expected

    def get_timestamps(self):
        return self.timestamps


@ray.remote
def task(i, signal, semaphore):
    signal.send.remote()
    ref = semaphore.acquire.remote()
    ready = False
    while not ready:
        ready, _ = ray.wait([ref], timeout=0)
        time.sleep(1)


@ray.remote(num_cpus=0)
class Semaphore:
    def __init__(self, value=1):
        self._sema = asyncio.Semaphore(value=value)

    async def acquire(self):
        await self._sema.acquire()

    async def release(self):
        self._sema.release()

    async def locked(self):
        return self._sema.locked()


def wait_for_condition(condition_predictor, timeout=10, retry_interval_ms=100):
    """Wait until a condition is met or time out with an exception.

    Args:
        condition_predictor: A function that predicts the condition.
        timeout: Maximum timeout in seconds.
        retry_interval_ms: Retry interval in milliseconds.

    Raises:
        RuntimeError: If the condition is not met before the timeout expires.
    """
    start = time.time()
    while time.time() - start <= timeout:
        if condition_predictor():
            return
        time.sleep(retry_interval_ms / 1000.0)
    raise RuntimeError("The condition wasn't met before the timeout expired.")


def test_max_running_tasks(num_tasks):
    cpus_per_task = 0.25

    semaphore_actor = Semaphore.remote()
    ray.get(semaphore_actor.acquire.remote())
    signal_actor = SignalActor.remote()
    ray.get(signal_actor.reset.remote(num_tasks))

    refs = [
        task.options(num_cpus=cpus_per_task).remote(i, signal_actor,
                                                    semaphore_actor)
        for i in tqdm.trange(num_tasks, desc="Launching tasks")
    ]

    timestamps = []
    with tqdm.tqdm(
            total=num_tasks, desc="Waiting for tasks to be scheduled") as pbar:
        while len(timestamps) < num_tasks:
            new_timestamps = ray.get(signal_actor.get_timestamps.remote())
            pbar.update(len(new_timestamps) - len(timestamps))
            timestamps = new_timestamps
            time.sleep(1)

    for _ in range(num_tasks):
        semaphore_actor.release.remote()

    for _ in tqdm.trange(num_tasks, desc="Ensuring all tasks have finished"):
        done, refs = ray.wait(refs)
        assert ray.get(done[0]) is None

    timestamps = ray.get(signal_actor.get_timestamps.remote())
    print(f"p50: {np.percentile(timestamps, 50)}s")
    print(f"p95: {np.percentile(timestamps, 95)}s")
    print(f"p100: {np.percentile(timestamps, 100)}s")

    return timestamps


def no_resource_leaks():
    return ray.available_resources() == ray.cluster_resources()


@click.command()
@click.option(
    "--num-tasks", required=True, type=int, help="Number of tasks to launch.")
@click.option(
    "--local",
    is_flag=True,
    type=bool,
    default=False,
    help="Whether to run the test locally, with a simulated cluster.")
def test(num_tasks, local):
    if local:
        num_cpus = 2 * num_tasks // 4
        ray.init(num_cpus=num_cpus)
    else:
        ray.init(address="auto")

    wait_for_condition(no_resource_leaks)
    start_time = time.time()
    timestamps = test_max_running_tasks(num_tasks)
    end_time = time.time()
    wait_for_condition(no_resource_leaks)

    max_timestamp = max(timestamps)
    rate = num_tasks / max_timestamp

    print(f"Success! Started {num_tasks} tasks in {max_timestamp}s. "
          f"({rate} tasks/s)")

    if "TEST_OUTPUT_JSON" in os.environ:
        out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
        results = {
            "tasks_per_second": rate,
            "num_tasks": num_tasks,
            "time": end_time - start_time,
            "success": "1"
        }
        json.dump(results, out_file)


if __name__ == "__main__":
    test()
