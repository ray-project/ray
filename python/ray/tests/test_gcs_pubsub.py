import asyncio
import os
import re
import sys
import threading
from typing import Dict, Tuple

import pytest

import ray
from ray._common.test_utils import SignalActor, wait_for_condition
from ray._private.gcs_pubsub import (
    GcsAioResourceUsageSubscriber,
)


def test_publish_and_subscribe_error_info(ray_start_regular):
    address_info = ray_start_regular
    gcs_server_addr = address_info["gcs_address"]

    subscriber = ray._raylet.GcsErrorSubscriber(address=gcs_server_addr)
    subscriber.subscribe()

    gcs_client = ray._raylet.GcsClient(address=gcs_server_addr)
    gcs_client.publish_error(b"aaa_id", "", "test error message 1")
    gcs_client.publish_error(b"bbb_id", "", "test error message 2")

    (key_id1, err1) = subscriber.poll()
    assert key_id1 == b"aaa_id"
    assert err1["error_message"] == "test error message 1"
    (key_id2, err2) = subscriber.poll()
    assert key_id2 == b"bbb_id"
    assert err2["error_message"] == "test error message 2"

    subscriber.close()


def test_publish_and_subscribe_logs(ray_start_regular):
    address_info = ray_start_regular
    gcs_server_addr = address_info["gcs_address"]

    subscriber = ray._raylet.GcsLogSubscriber(address=gcs_server_addr)
    subscriber.subscribe()

    gcs_client = ray._raylet.GcsClient(address=gcs_server_addr)
    log_batch = {
        "ip": "127.0.0.1",
        "pid": 1234,
        "job": "0001",
        "is_err": False,
        "lines": ["line 1", "line 2"],
        "actor_name": "test actor",
        "task_name": "test task",
    }
    gcs_client.publish_logs(log_batch)

    # PID is treated as string.
    log_batch["pid"] = "1234"
    assert subscriber.poll() == log_batch

    subscriber.close()


@pytest.mark.asyncio
async def test_aio_publish_and_subscribe_resource_usage(ray_start_regular):
    address_info = ray_start_regular
    gcs_server_addr = address_info["gcs_address"]

    subscriber = GcsAioResourceUsageSubscriber(address=gcs_server_addr)
    await subscriber.subscribe()

    gcs_client = ray._raylet.GcsClient(address=gcs_server_addr)
    await gcs_client.async_publish_node_resource_usage("aaa_id", '{"cpu": 1}')
    await gcs_client.async_publish_node_resource_usage("bbb_id", '{"cpu": 2}')

    assert await subscriber.poll() == ("aaa_id", '{"cpu": 1}')
    assert await subscriber.poll() == ("bbb_id", '{"cpu": 2}')

    await subscriber.close()


@pytest.mark.asyncio
async def test_aio_poll_no_leaks(ray_start_regular):
    """Test that polling doesn't leak memory."""
    ctx = ray_start_regular
    gcs_server_addr = ctx.address_info["gcs_address"]

    subscriber = GcsAioResourceUsageSubscriber(address=gcs_server_addr)
    await subscriber.subscribe()

    for _ in range(10000):
        subscriber.poll()
        # There should only be 1 task, but use 10 as a buffer.
        assert len(asyncio.all_tasks()) < 10

    await subscriber.close()


def test_two_subscribers(ray_start_regular):
    """Tests concurrently subscribing to two channels work."""

    address_info = ray_start_regular
    gcs_server_addr = address_info["gcs_address"]

    num_messages = 100

    errors = []
    error_subscriber = ray._raylet.GcsErrorSubscriber(address=gcs_server_addr)
    # Make sure subscription is registered before publishing starts.
    error_subscriber.subscribe()

    def receive_errors():
        while len(errors) < num_messages:
            _, msg = error_subscriber.poll()
            errors.append(msg)

    t1 = threading.Thread(target=receive_errors)
    t1.start()

    logs = []
    log_subscriber = ray._raylet.GcsLogSubscriber(address=gcs_server_addr)
    # Make sure subscription is registered before publishing starts.
    log_subscriber.subscribe()

    log_str_pattern = re.compile("^log ([0-9]+)$")

    def receive_logs():
        while len(logs) < num_messages:
            log_batch = log_subscriber.poll()
            if log_str_pattern.match(log_batch["lines"][0]):
                logs.append(log_batch)

    t2 = threading.Thread(target=receive_logs)
    t2.start()

    gcs_client = ray._raylet.GcsClient(address=gcs_server_addr)
    for i in range(0, num_messages):
        gcs_client.publish_error(b"msg_id", "", f"error {i}")
        gcs_client.publish_logs(
            {
                "ip": "127.0.0.1",
                "pid": "gcs",
                "job": "0001",
                "is_err": False,
                "lines": [f"log {i}"],
                "actor_name": "test actor",
                "task_name": "test task",
            }
        )

    t1.join(timeout=10)
    assert len(errors) == num_messages, str(errors)
    assert not t1.is_alive(), str(errors)

    t2.join(timeout=10)
    assert len(logs) == num_messages, str(logs)
    assert not t2.is_alive(), str(logs)

    for i in range(0, num_messages):
        assert errors[i]["error_message"] == f"error {i}", str(errors)
        assert logs[i]["lines"][0] == f"log {i}", str(logs)


def _publisher_channel_stats(gcs_log_path: str) -> Dict[str, Tuple[int, int]]:
    """Parse per-channel publisher subscription stats from the GCS debug dump.

    Returns a dict mapping channel name to (current all-entity subscribers,
    current keyed subscription keys), taking the latest dumped block for each
    channel. Returns an empty dict if the GCS log file doesn't exist yet.
    """
    if not os.path.exists(gcs_log_path):
        return {}
    with open(gcs_log_path, encoding="utf-8") as f:
        log = f.read()
    pattern = re.compile(
        r"(\w+_CHANNEL)\n"
        r"- cumulative published messages: \d+\n"
        r"- cumulative published bytes: \d+\n"
        r"- current buffered bytes: \d+\n"
        r"- current all-entity subscribers: (\d+)\n"
        r"- current keyed subscription keys: (\d+)"
    )
    stats = {}
    for m in pattern.finditer(log):
        stats[m.group(1)] = (int(m.group(2)), int(m.group(3)))
    return stats


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_nodes": 3,
            "num_cpus": 2,
            "_system_config": {
                "event_stats_print_interval_ms": 200,
                "event_stats": True,
            },
        }
    ],
    indirect=True,
)
def test_pubsub_subscriptions_bounded_for_regular_cluster(ray_start_cluster):
    """Asserts the number of pub-sub channel subscriptions to ensure that
    there are no unexpected increases due to unintended changes. If you find
    yourself increasing a count for any reason, please carefully think
    through the performance impact of your change before modifying this.
    """
    num_nodes = 3
    num_workers = 6
    cluster = ray_start_cluster
    cluster.wait_for_nodes()

    # Spin up several workers. Each task blocks on the signal until all of them
    # are running, so Ray is forced to start a distinct worker process per task
    # instead of reusing a couple of them. This is what makes the assertions
    # below meaningful: subscription counts must stay O(num_nodes) and must not
    # grow with the number of workers.
    signal = SignalActor.remote()

    @ray.remote(num_cpus=0.5)
    def wait_for_signal():
        ray.get(signal.wait.remote())
        return os.getpid()

    refs = [wait_for_signal.remote() for _ in range(num_workers)]
    wait_for_condition(
        lambda: ray.get(signal.cur_num_waiters.remote()) == num_workers, timeout=60
    )
    ray.get(signal.send.remote())
    assert len(set(ray.get(refs))) == num_workers

    # One backpressured generator actor: the generator executor takes out a
    # keyed subscription on the worker death channel for the driver (its owner).
    @ray.remote(_actor_generator_backpressure_num_objects=2)
    class Gen:
        def f(self):
            for i in range(6):
                yield i

    g = Gen.remote()
    assert [ray.get(ref) for ref in g.f.remote()] == list(range(6))

    session_dir = ray._private.worker.global_worker.node.address_info["session_dir"]
    gcs_log_path = os.path.join(session_dir, "logs", "gcs_server.out")

    def check():
        stats = _publisher_channel_stats(gcs_log_path)
        if "GCS_WORKER_DELTA_CHANNEL" not in stats:
            # No debug dump with publisher stats has landed yet.
            return False
        assert stats["GCS_WORKER_DELTA_CHANNEL"] == (num_nodes, 1), stats
        assert stats["GCS_JOB_CHANNEL"] == (num_nodes, 0), stats
        actor_all, actor_keyed = stats["GCS_ACTOR_CHANNEL"]
        assert actor_all <= 1 and actor_keyed >= 1, stats
        assert stats["GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL"][1] == 0, stats
        return True

    wait_for_condition(check, timeout=30)


if __name__ == "__main__":

    sys.exit(pytest.main(["-sv", __file__]))
