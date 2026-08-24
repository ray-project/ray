import asyncio
import os
import re
import sys
import tempfile
import threading
import time
from typing import Dict, NamedTuple, Optional

import pytest

import ray
from ray._common.test_utils import wait_for_condition
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


class ChannelStats(NamedTuple):
    """Current subscription counts for a single pub-sub channel."""

    # Subscribers receiving every message on the channel.
    all_entity_subscribers: int
    # Distinct entity keys with at least one keyed subscriber.
    keyed_subscription_keys: int
    # Subscribers holding at least one keyed subscription. Distinct from
    # keyed_subscription_keys: any number of subscribers can watch the same
    # key, so only this count catches keyed subscriptions that scale with the
    # number of workers.
    keyed_subscribers: int


class PublisherStats(NamedTuple):
    """Current state of one publisher from the GCS debug dump."""

    # Long-polling connections into the publisher. There is one per subscriber
    # process regardless of how many channels or keys it subscribes to, so this
    # is the channel-agnostic connection count.
    long_polling_subscribers: int
    channels: Dict[str, ChannelStats]


# The GCS dumps two publishers (the GCS publisher and the observability
# publisher) into the same debug block, separated by a blank line, so each
# block has to be parsed on its own before picking out the one we want.
_PUBLISHER_BLOCK_PATTERN = re.compile(
    r"Publisher:\n- current long-polling subscribers: (\d+)\n(.*?)(?=\n\n)",
    re.DOTALL,
)

_CHANNEL_PATTERN = re.compile(
    r"(\w+_CHANNEL)\n"
    r"- cumulative published messages: \d+\n"
    r"- cumulative published bytes: \d+\n"
    r"- current buffered bytes: \d+\n"
    r"- current all-entity subscribers: (\d+)\n"
    r"- current keyed subscription keys: (\d+)\n"
    r"- current keyed subscribers: (\d+)"
)


def _gcs_publisher_stats(gcs_log_path: str) -> Optional[PublisherStats]:
    """Parse the latest GCS publisher stats out of the GCS debug dump.

    Args:
        gcs_log_path: Path to the gcs_server.out log file.

    Returns:
        Stats from the most recent GCS publisher dump, or None if the log file
        doesn't exist yet or no such dump has landed.
    """
    if not os.path.exists(gcs_log_path):
        return None
    with open(gcs_log_path, encoding="utf-8") as f:
        log = f.read()
    latest = None
    for block in _PUBLISHER_BLOCK_PATTERN.finditer(log):
        channels = {
            m.group(1): ChannelStats(int(m.group(2)), int(m.group(3)), int(m.group(4)))
            for m in _CHANNEL_PATTERN.finditer(block.group(2))
        }
        # Identify the GCS publisher (rather than the observability publisher)
        # by a channel only it registers.
        if "GCS_WORKER_DELTA_CHANNEL" in channels:
            latest = PublisherStats(int(block.group(1)), channels)
    return latest


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

    # Spin up several workers and block them, so Ray is forced to create a new
    # worker per task. We avoid a signal actor here because a worker holding an
    # actor handle takes out a subscription of its own, which would make the
    # counts below scale with num_workers and mask what this test guards.
    barrier_dir = tempfile.mkdtemp()

    @ray.remote(num_cpus=0.5)
    def wait_for_peers(expected: int, barrier_dir: str) -> int:
        pid = os.getpid()
        with open(os.path.join(barrier_dir, str(pid)), "w"):
            pass
        # Bounded so a scheduling hiccup fails the test instead of hanging it.
        for _ in range(600):
            if len(os.listdir(barrier_dir)) >= expected:
                return pid
            time.sleep(0.1)
        raise AssertionError(f"only {len(os.listdir(barrier_dir))}/{expected} started")

    pids = ray.get(
        [wait_for_peers.remote(num_workers, barrier_dir) for _ in range(num_workers)]
    )
    assert len(set(pids)) == num_workers, pids

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

    # num_nodes raylets + the driver + the generator actor's worker + 2 from the
    # dashboard head. The dashboard counts twice because its node-info and actor
    # subscribers each mint their own subscriber id (see _SubscriberBase.__init__
    # in gcs_pubsub.py), so one process holds two long-poll connections.
    # The exact value matters less than the invariant it pins: it must not grow
    # with num_workers (verified unchanged at num_workers=6 and 12).
    expected_long_polling_subscribers = num_nodes + 4

    def check():
        stats = _gcs_publisher_stats(gcs_log_path)
        if stats is None:
            # No debug dump with publisher stats has landed yet.
            return False
        channels = stats.channels
        assert (
            stats.long_polling_subscribers == expected_long_polling_subscribers
        ), stats
        # Tuples below are (all-entity subscribers, keyed keys, keyed subscribers).
        # One raylet each subscribes to all worker deltas, and the generator
        # executor holds a single keyed subscription on its owner (the driver).
        assert channels["GCS_WORKER_DELTA_CHANNEL"] == (num_nodes, 1, 1), stats
        # One raylet each; nothing subscribes to individual jobs.
        assert channels["GCS_JOB_CHANNEL"] == (num_nodes, 0, 0), stats
        # The driver holds the one actor handle, so one key with one subscriber.
        assert channels["GCS_ACTOR_CHANNEL"] == (1, 1, 1), stats
        # Raylets plus the driver watch node liveness. Workers must not: a
        # per-worker subscription here would put this at O(num_workers).
        assert channels["GCS_NODE_ADDRESS_AND_LIVENESS_CHANNEL"] == (
            num_nodes + 1,
            0,
            0,
        ), stats
        assert channels["GCS_NODE_INFO_CHANNEL"] == (1, 0, 0), stats
        return True

    wait_for_condition(check, timeout=30)


if __name__ == "__main__":

    sys.exit(pytest.main(["-sv", __file__]))
