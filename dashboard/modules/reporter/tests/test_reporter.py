import os
import sys
import logging
import requests
import time

import pytest
import ray
from ray import ray_constants
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.test_utils import (format_web_url, RayTestTimeoutException,
                            wait_until_server_available, wait_for_condition,
                            fetch_prometheus)

logger = logging.getLogger(__name__)


def test_profiling(shutdown_only):
    addresses = ray.init(include_dashboard=True, num_cpus=6)

    @ray.remote(num_cpus=2)
    class Actor:
        def getpid(self):
            return os.getpid()

    c = Actor.remote()
    actor_pid = ray.get(c.getpid.remote())

    webui_url = addresses["webui_url"]
    assert (wait_until_server_available(webui_url) is True)
    webui_url = format_web_url(webui_url)

    start_time = time.time()
    launch_profiling = None
    while True:
        # Sometimes some startup time is required
        if time.time() - start_time > 15:
            raise RayTestTimeoutException(
                "Timed out while collecting profiling stats, "
                f"launch_profiling: {launch_profiling}")
        launch_profiling = requests.get(
            webui_url + "/api/launch_profiling",
            params={
                "ip": ray.nodes()[0]["NodeManagerAddress"],
                "pid": actor_pid,
                "duration": 5
            }).json()
        if launch_profiling["result"]:
            profiling_info = launch_profiling["data"]["profilingInfo"]
            break
        time.sleep(1)
    logger.info(profiling_info)


def test_node_physical_stats(enable_test_module, shutdown_only):
    addresses = ray.init(include_dashboard=True, num_cpus=6)

    @ray.remote(num_cpus=1)
    class Actor:
        def getpid(self):
            return os.getpid()

    actors = [Actor.remote() for _ in range(6)]
    actor_pids = ray.get([actor.getpid.remote() for actor in actors])
    actor_pids = set(actor_pids)

    webui_url = addresses["webui_url"]
    assert (wait_until_server_available(webui_url) is True)
    webui_url = format_web_url(webui_url)

    def _check_workers():
        try:
            resp = requests.get(webui_url +
                                "/test/dump?key=node_physical_stats")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True
            node_physical_stats = result["data"]["nodePhysicalStats"]
            assert len(node_physical_stats) == 1
            current_stats = node_physical_stats[addresses["node_id"]]
            # Check Actor workers
            current_actor_pids = set()
            for worker in current_stats["workers"]:
                if "ray::Actor" in worker["cmdline"][0]:
                    current_actor_pids.add(worker["pid"])
            assert current_actor_pids == actor_pids
            # Check raylet cmdline
            assert "raylet" in current_stats["cmdline"][0]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_workers, timeout=10)


def test_prometheus_physical_stats_record(enable_test_module, shutdown_only):
    addresses = ray.init(include_dashboard=True, num_cpus=1)
    metrics_export_port = addresses["metrics_export_port"]
    addr = addresses["raylet_ip_address"]
    prom_addresses = [f"{addr}:{metrics_export_port}"]

    def test_case_stats_exist():
        components_dict, metric_names, metric_samples = fetch_prometheus(
            prom_addresses)
        return all([
            "ray_node_cpu" in metric_names, "ray_node_mem" in metric_names,
            "ray_raylet_cpu" in metric_names, "ray_raylet_mem" in metric_names
        ])

    def test_case_ip_correct():
        components_dict, metric_names, metric_samples = fetch_prometheus(
            prom_addresses)
        raylet_proc = ray.worker._global_node.all_processes[
            ray_constants.PROCESS_TYPE_RAYLET][0]
        raylet_pid = None
        # Find the raylet pid recorded in the tag.
        for sample in metric_samples:
            if sample.name == "ray_raylet_cpu":
                raylet_pid = sample.labels["pid"]
                break
        return str(raylet_proc.process.pid) == str(raylet_pid)

    wait_for_condition(test_case_stats_exist, retry_interval_ms=1000)
    wait_for_condition(test_case_ip_correct, retry_interval_ms=1000)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
