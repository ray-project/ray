import os

from datetime import datetime
from time import sleep

import ray
import pytest

from ray.dashboard.node_stats import NodeStats
from ray.ray_constants import REDIS_DEFAULT_PASSWORD
from ray.test_utils import wait_for_condition


def start_node_stats(redis_address):
    redis_password = REDIS_DEFAULT_PASSWORD
    node_stats = NodeStats(redis_address, redis_password)
    node_stats.start()
    # Wait for node stats to fire up.
    MAX_START_TIME_S = 30
    t_start = datetime.now()
    while True:
        try:
            stats = node_stats.get_node_stats()
            client_stats = stats and stats.get("clients")
            if not client_stats:
                sleep(3)
                if (datetime.now() - t_start).seconds > MAX_START_TIME_S:
                    pytest.fail("Node stats took too long to start up")
                continue
            break
        except Exception:
            continue
    return node_stats


def test_basic(ray_start_with_dashboard):
    """Dashboard test that starts a Ray cluster with a dashboard server running,
    then hits the dashboard API and asserts that it receives sensible data."""
    node_stats = start_node_stats(ray_start_with_dashboard["redis_address"])
    stats = node_stats.get_node_stats()
    client_stats = stats and stats.get("clients")

    assert len(client_stats) == 1
    client = client_stats[0]
    assert len(client["workers"]) == 1


def test_log_and_error_messages(ray_start_with_dashboard):
    node_stats = start_node_stats(ray_start_with_dashboard["redis_address"])

    LOG_MESSAGE = "LOG_MESSAGE"
    LOG_TIMES = 3

    @ray.remote
    def generate_log():
        for _ in range(LOG_TIMES):
            print(LOG_MESSAGE)
        return os.getpid()

    pid = str(ray.get(generate_log.remote()))
    stats = node_stats.get_node_stats()
    client_stats = stats and stats.get("clients")
    assert len(client_stats) == 1, "Client stats is not available."
    hostname = client_stats[0]["hostname"]

    wait_for_condition(
        lambda: len(node_stats.get_logs(hostname, pid)[pid]) == 3)

    @ray.remote
    class Actor:
        def get_pid(self):
            return os.getpid()

        def generate_error(self):
            raise Exception(LOG_MESSAGE)

    actor = Actor.remote()
    pid = str(ray.get(actor.get_pid.remote()))
    actor.generate_error.remote()
    wait_for_condition(
        lambda: len(node_stats.get_errors(hostname, pid)[pid]) == 1)
