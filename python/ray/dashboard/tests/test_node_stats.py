from ray.dashboard.node_stats import NodeStats
from ray.ray_constants import REDIS_DEFAULT_PASSWORD
from datetime import datetime
from time import sleep
import pytest


def test_basic(ray_start_with_dashboard):
    """Dashboard test that starts a Ray cluster with a dashboard server running,
    then hits the dashboard API and asserts that it receives sensible data."""
    redis_address = ray_start_with_dashboard["redis_address"]
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
    assert len(client_stats) == 1
    client = client_stats[0]
    assert len(client["workers"]) == 1
