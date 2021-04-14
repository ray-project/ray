import os
import sys
import logging
import requests
import time

import pytest
import ray
from ray import ray_constants
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.new_dashboard.utils import Bunch
from ray.new_dashboard.modules.reporter.reporter_agent import ReporterAgent
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
            "ray_node_cpu_utilization" in metric_names,
            "ray_node_cpu_count" in metric_names,
            "ray_node_mem_used" in metric_names,
            "ray_node_mem_available" in metric_names,
            "ray_node_mem_total" in metric_names,
            "ray_raylet_cpu" in metric_names, "ray_raylet_mem" in metric_names,
            "ray_node_disk_usage" in metric_names,
            "ray_node_disk_free" in metric_names,
            "ray_node_disk_utilization_percentage" in metric_names,
            "ray_node_network_sent" in metric_names,
            "ray_node_network_received" in metric_names,
            "ray_node_network_send_speed" in metric_names,
            "ray_node_network_receive_speed" in metric_names
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


def test_report_stats():
    class ReporterAgentDummy(object):
        pass

    obj = ReporterAgentDummy()
    obj._is_head_node = True

    test_stats = {
        "now": 1614826393.975763,
        "hostname": "fake_hostname.local",
        "ip": "127.0.0.1",
        "cpu": 57.4,
        "cpus": (8, 4),
        "mem": (17179869184, 5723353088, 66.7, 9234341888),
        "workers": [{
            "memory_info": Bunch(
                rss=55934976, vms=7026937856, pfaults=15354, pageins=0),
            "cpu_percent": 0.0,
            "cmdline": [
                "ray::IDLE", "", "", "", "", "", "", "", "", "", "", ""
            ],
            "create_time": 1614826391.338613,
            "pid": 7174,
            "cpu_times": Bunch(
                user=0.607899328,
                system=0.274044032,
                children_user=0.0,
                children_system=0.0)
        }],
        "raylet": {
            "memory_info": Bunch(
                rss=18354176, vms=6921486336, pfaults=6206, pageins=3),
            "cpu_percent": 0.0,
            "cmdline": ["fake raylet cmdline"],
            "create_time": 1614826390.274854,
            "pid": 7153,
            "cpu_times": Bunch(
                user=0.03683138,
                system=0.035913716,
                children_user=0.0,
                children_system=0.0)
        },
        "bootTime": 1612934656.0,
        "loadAvg": ((4.4521484375, 3.61083984375, 3.5400390625), (0.56, 0.45,
                                                                  0.44)),
        "disk": {
            "/": Bunch(
                total=250790436864,
                used=11316781056,
                free=22748921856,
                percent=33.2),
            "/tmp": Bunch(
                total=250790436864,
                used=209532035072,
                free=22748921856,
                percent=90.2)
        },
        "gpus": [],
        "network": (13621160960, 11914936320),
        "network_speed": (8.435062128545095, 7.378462703142336),
    }

    cluster_stats = {
        "autoscaler_report": {
            "active_nodes": {
                "head_node": 1,
                "worker-node-0": 2
            },
            "failed_nodes": [],
            "pending_launches": {},
            "pending_nodes": []
        }
    }

    records = ReporterAgent._record_stats(obj, test_stats, cluster_stats)
    assert len(records) == 16
    # Test stats without raylets
    test_stats["raylet"] = {}
    records = ReporterAgent._record_stats(obj, test_stats, cluster_stats)
    assert len(records) == 14
    # Test stats with gpus
    test_stats["gpus"] = [{
        "utilization_gpu": 1,
        "memory_used": 100,
        "memory_total": 1000
    }]
    records = ReporterAgent._record_stats(obj, test_stats, cluster_stats)
    assert len(records) == 18
    # Test stats without autoscaler report
    cluster_stats = {}
    records = ReporterAgent._record_stats(obj, test_stats, cluster_stats)
    assert len(records) == 16


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
