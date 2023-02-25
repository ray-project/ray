import logging
import os
import sys

import pytest
import requests
import numpy as np
import time
import copy

from collections import defaultdict
from multiprocessing import Process
from unittest.mock import MagicMock

import psutil
import ray
from mock import patch
from ray._private import ray_constants
from ray._private.test_utils import (
    fetch_prometheus,
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
)
from ray.dashboard.modules.reporter.reporter_agent import ReporterAgent
from ray.dashboard.tests.conftest import *  # noqa
from ray.dashboard.utils import Bunch

try:
    import prometheus_client
except ImportError:
    prometheus_client = None

logger = logging.getLogger(__name__)

STATS_TEMPLATE = {
    "now": 1614826393.975763,
    "hostname": "fake_hostname.local",
    "ip": "127.0.0.1",
    "cpu": 57.4,
    "cpus": (8, 4),
    "mem": (17179869184, 5723353088, 66.7, 9234341888),
    "workers": [
        {
            "memory_info": Bunch(
                rss=55934976, vms=7026937856, pfaults=15354, pageins=0
            ),
            "memory_full_info": Bunch(uss=51428381),
            "cpu_percent": 0.0,
            "cmdline": ["ray::IDLE", "", "", "", "", "", "", "", "", "", "", ""],
            "create_time": 1614826391.338613,
            "pid": 7174,
            "cpu_times": Bunch(
                user=0.607899328,
                system=0.274044032,
                children_user=0.0,
                children_system=0.0,
            ),
        }
    ],
    "raylet": {
        "memory_info": Bunch(rss=18354176, vms=6921486336, pfaults=6206, pageins=3),
        "cpu_percent": 0.0,
        "cmdline": ["fake raylet cmdline"],
        "create_time": 1614826390.274854,
        "pid": 7153,
        "cpu_times": Bunch(
            user=0.03683138,
            system=0.035913716,
            children_user=0.0,
            children_system=0.0,
        ),
    },
    "agent": {
        "memory_info": Bunch(rss=18354176, vms=6921486336, pfaults=6206, pageins=3),
        "cpu_percent": 0.0,
        "cmdline": ["fake raylet cmdline"],
        "create_time": 1614826390.274854,
        "pid": 7154,
        "cpu_times": Bunch(
            user=0.03683138,
            system=0.035913716,
            children_user=0.0,
            children_system=0.0,
        ),
    },
    "bootTime": 1612934656.0,
    "loadAvg": ((4.4521484375, 3.61083984375, 3.5400390625), (0.56, 0.45, 0.44)),
    "disk_io": (100, 100, 100, 100),
    "disk_io_speed": (100, 100, 100, 100),
    "disk": {
        "/": Bunch(
            total=250790436864, used=11316781056, free=22748921856, percent=33.2
        ),
        "/tmp": Bunch(
            total=250790436864, used=209532035072, free=22748921856, percent=90.2
        ),
    },
    "gpus": [],
    "network": (13621160960, 11914936320),
    "network_speed": (8.435062128545095, 7.378462703142336),
}


def random_work():
    import time

    for _ in range(10000):
        time.sleep(0.1)
        np.random.rand(5 * 1024 * 1024)  # 40 MB


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
    assert wait_until_server_available(webui_url) is True
    webui_url = format_web_url(webui_url)

    def _check_workers():
        try:
            resp = requests.get(webui_url + "/test/dump?key=node_physical_stats")
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


@pytest.mark.skipif(prometheus_client is None, reason="prometheus_client not installed")
def test_prometheus_physical_stats_record(enable_test_module, shutdown_only):
    addresses = ray.init(include_dashboard=True, num_cpus=1)
    metrics_export_port = addresses["metrics_export_port"]
    addr = addresses["raylet_ip_address"]
    prom_addresses = [f"{addr}:{metrics_export_port}"]

    def test_case_stats_exist():
        components_dict, metric_names, metric_samples = fetch_prometheus(prom_addresses)
        return all(
            [
                "ray_node_cpu_utilization" in metric_names,
                "ray_node_cpu_count" in metric_names,
                "ray_node_mem_used" in metric_names,
                "ray_node_mem_available" in metric_names,
                "ray_node_mem_total" in metric_names,
                "ray_component_cpu_percentage" in metric_names,
                "ray_component_rss_mb" in metric_names,
                "ray_component_uss_mb" in metric_names,
                "ray_node_disk_io_read" in metric_names,
                "ray_node_disk_io_write" in metric_names,
                "ray_node_disk_io_read_count" in metric_names,
                "ray_node_disk_io_write_count" in metric_names,
                "ray_node_disk_io_read_speed" in metric_names,
                "ray_node_disk_io_write_speed" in metric_names,
                "ray_node_disk_read_iops" in metric_names,
                "ray_node_disk_write_iops" in metric_names,
                "ray_node_disk_usage" in metric_names,
                "ray_node_disk_free" in metric_names,
                "ray_node_disk_utilization_percentage" in metric_names,
                "ray_node_network_sent" in metric_names,
                "ray_node_network_received" in metric_names,
                "ray_node_network_send_speed" in metric_names,
                "ray_node_network_receive_speed" in metric_names,
            ]
        )

    def test_case_ip_correct():
        components_dict, metric_names, metric_samples = fetch_prometheus(prom_addresses)
        raylet_proc = ray._private.worker._global_node.all_processes[
            ray_constants.PROCESS_TYPE_RAYLET
        ][0]
        raylet_pid = None
        # Find the raylet pid recorded in the tag.
        for sample in metric_samples:
            if (
                sample.name == "ray_component_cpu_percentage"
                and sample.labels["Component"] == "raylet"
            ):
                raylet_pid = sample.labels["pid"]
                break
        return str(raylet_proc.process.pid) == str(raylet_pid)

    wait_for_condition(test_case_stats_exist, retry_interval_ms=1000)
    wait_for_condition(test_case_ip_correct, retry_interval_ms=1000)


@pytest.mark.skipif(
    prometheus_client is None,
    reason="prometheus_client must be installed.",
)
def test_prometheus_export_worker_and_memory_stats(enable_test_module, shutdown_only):
    addresses = ray.init(include_dashboard=True, num_cpus=1)
    metrics_export_port = addresses["metrics_export_port"]
    addr = addresses["raylet_ip_address"]
    prom_addresses = [f"{addr}:{metrics_export_port}"]

    @ray.remote
    def f():
        return 1

    ret = f.remote()
    ray.get(ret)

    def test_worker_stats():
        _, metric_names, metric_samples = fetch_prometheus(prom_addresses)
        expected_metrics = [
            "ray_component_cpu_percentage",
            "ray_component_rss_mb",
            "ray_component_uss_mb",
        ]
        for metric in expected_metrics:
            if metric not in metric_names:
                raise RuntimeError(
                    f"Metric {metric} not found in exported metric names"
                )
        return True

    wait_for_condition(test_worker_stats, retry_interval_ms=1000)


def test_report_stats():
    dashboard_agent = MagicMock()
    agent = ReporterAgent(dashboard_agent)
    # Assume it is a head node.
    agent._is_head_node = True

    cluster_stats = {
        "autoscaler_report": {
            "active_nodes": {"head_node": 1, "worker-node-0": 2},
            "failed_nodes": [],
            "pending_launches": {},
            "pending_nodes": [],
        }
    }

    records = agent._record_stats(STATS_TEMPLATE, cluster_stats)
    assert len(records) == 29
    # Test stats without raylets
    STATS_TEMPLATE["raylet"] = {}
    records = agent._record_stats(STATS_TEMPLATE, cluster_stats)
    assert len(records) == 27
    # Test stats with gpus
    STATS_TEMPLATE["gpus"] = [
        {"utilization_gpu": 1, "memory_used": 100, "memory_total": 1000}
    ]
    records = agent._record_stats(STATS_TEMPLATE, cluster_stats)
    assert len(records) == 31
    # Test stats without autoscaler report
    cluster_stats = {}
    records = agent._record_stats(STATS_TEMPLATE, cluster_stats)
    assert len(records) == 29


def test_report_per_component_stats():
    dashboard_agent = MagicMock()
    agent = ReporterAgent(dashboard_agent)
    # Assume it is a head node.
    agent._is_head_node = True

    # Generate stats.
    test_stats = copy.deepcopy(STATS_TEMPLATE)
    idle_stats = {
        "memory_info": Bunch(
            rss=55934976, vms=7026937856, uss=1234567, pfaults=15354, pageins=0
        ),
        "memory_full_info": Bunch(uss=51428381),
        "cpu_percent": 5.0,
        "cmdline": ["ray::IDLE", "", "", "", "", "", "", "", "", "", "", ""],
        "create_time": 1614826391.338613,
        "pid": 7174,
        "cpu_times": Bunch(
            user=0.607899328,
            system=0.274044032,
            children_user=0.0,
            children_system=0.0,
        ),
    }
    func_stats = {
        "memory_info": Bunch(rss=55934976, vms=7026937856, pfaults=15354, pageins=0),
        "memory_full_info": Bunch(uss=51428381),
        "cpu_percent": 6.0,
        "cmdline": ["ray::func", "", "", "", "", "", "", "", "", "", "", ""],
        "create_time": 1614826391.338613,
        "pid": 7175,
        "cpu_times": Bunch(
            user=0.607899328,
            system=0.274044032,
            children_user=0.0,
            children_system=0.0,
        ),
    }
    raylet_stast = {
        "memory_info": Bunch(rss=18354176, vms=6921486336, pfaults=6206, pageins=3),
        "memory_full_info": Bunch(uss=51428381),
        "cpu_percent": 4.0,
        "cmdline": ["fake raylet cmdline"],
        "create_time": 1614826390.274854,
        "pid": 7153,
        "cpu_times": Bunch(
            user=0.03683138,
            system=0.035913716,
            children_user=0.0,
            children_system=0.0,
        ),
    }
    agent_stats = {
        "memory_info": Bunch(rss=18354176, vms=6921486336, pfaults=6206, pageins=3),
        "memory_full_info": Bunch(uss=51428381),
        "cpu_percent": 6.0,
        "cmdline": ["fake raylet cmdline"],
        "create_time": 1614826390.274854,
        "pid": 7156,
        "cpu_times": Bunch(
            user=0.03683138,
            system=0.035913716,
            children_user=0.0,
            children_system=0.0,
        ),
    }

    test_stats["workers"] = [idle_stats, func_stats]
    test_stats["raylet"] = raylet_stast
    test_stats["agent"] = agent_stats

    cluster_stats = {
        "autoscaler_report": {
            "active_nodes": {"head_node": 1, "worker-node-0": 2},
            "failed_nodes": [],
            "pending_launches": {},
            "pending_nodes": [],
        }
    }

    def get_uss_and_cpu_records(records):
        component_uss_mb_records = defaultdict(list)
        component_cpu_percentage_records = defaultdict(list)
        for record in records:
            name = record.gauge.name
            if name == "component_uss_mb":
                comp = record.tags["Component"]
                component_uss_mb_records[comp].append(record)
            if name == "component_cpu_percentage":
                comp = record.tags["Component"]
                component_cpu_percentage_records[comp].append(record)
        return component_uss_mb_records, component_cpu_percentage_records

    """
    Test basic case.
    """
    records = agent._record_stats(test_stats, cluster_stats)
    uss_records, cpu_records = get_uss_and_cpu_records(records)

    def verify_metrics_values(uss_records, cpu_records, comp, uss, cpu_percent):
        """Verify the component exists and match the resource usage."""
        assert comp in uss_records
        assert comp in cpu_records
        uss_metrics = uss_records[comp][0].value
        cpu_percnet_metrics = cpu_records[comp][0].value
        assert uss_metrics == uss
        assert cpu_percnet_metrics == cpu_percent

    stats_map = {
        "raylet": raylet_stast,
        "agent": agent_stats,
        "ray::IDLE": idle_stats,
        "ray::func": func_stats,
    }
    # Verify metrics are correctly reported with a component name.
    for comp, stats in stats_map.items():
        verify_metrics_values(
            uss_records,
            cpu_records,
            comp,
            float(stats["memory_full_info"].uss) / 1.0e6,
            stats["cpu_percent"],
        )

    """
    Test metrics are resetted (report metrics with values 0) when
    the proc doesn't exist anymore.
    """
    # Verify the metrics are reset after ray::func is killed.
    test_stats["workers"] = [idle_stats]
    records = agent._record_stats(test_stats, cluster_stats)
    uss_records, cpu_records = get_uss_and_cpu_records(records)
    verify_metrics_values(
        uss_records,
        cpu_records,
        "ray::IDLE",
        float(idle_stats["memory_full_info"].uss) / 1.0e6,
        idle_stats["cpu_percent"],
    )

    comp = "ray::func"
    stats = func_stats
    # Value should be reset since func doesn't exist anymore.
    verify_metrics_values(
        uss_records,
        cpu_records,
        "ray::func",
        0,
        0,
    )

    """
    Verify worker names are only reported when they start with ray::.
    """
    # Verify if the command doesn't start with ray::, metrics are not reported.
    unknown_stats = {
        "memory_info": Bunch(rss=55934976, vms=7026937856, pfaults=15354, pageins=0),
        "memory_full_info": Bunch(uss=51428381),
        "cpu_percent": 6.0,
        "cmdline": ["python mock", "", "", "", "", "", "", "", "", "", "", ""],
        "create_time": 1614826391.338613,
        "pid": 7175,
        "cpu_times": Bunch(
            user=0.607899328,
            system=0.274044032,
            children_user=0.0,
            children_system=0.0,
        ),
    }
    test_stats["workers"] = [idle_stats, unknown_stats]

    records = agent._record_stats(test_stats, cluster_stats)
    uss_records, cpu_records = get_uss_and_cpu_records(records)
    assert "python mock" not in uss_records
    assert "python mock" not in cpu_records


@pytest.mark.parametrize("enable_k8s_disk_usage", [True, False])
def test_enable_k8s_disk_usage(enable_k8s_disk_usage: bool):
    """Test enabling display of K8s node disk usage when in a K8s pod."""
    with patch.multiple(
        "ray.dashboard.modules.reporter.reporter_agent",
        IN_KUBERNETES_POD=True,
        ENABLE_K8S_DISK_USAGE=enable_k8s_disk_usage,
    ):
        root_usage = ReporterAgent._get_disk_usage()["/"]
        if enable_k8s_disk_usage:
            # Since K8s disk usage is enabled, we shouuld get non-dummy values.
            assert root_usage.total != 1
            assert root_usage.free != 1
        else:
            # Unless K8s disk usage display is enabled, we should get dummy values.
            assert root_usage.total == 1
            assert root_usage.free == 1


def test_reporter_worker_cpu_percent():
    raylet_dummy_proc_f = psutil.Process
    agent_mock = Process(target=random_work)
    children = [Process(target=random_work) for _ in range(2)]

    class ReporterAgentDummy(object):
        _workers = {}

        def _get_raylet_proc(self):
            return raylet_dummy_proc_f()

        def _get_agent_proc(self):
            return psutil.Process(agent_mock.pid)

        def _generate_worker_key(self, proc):
            return (proc.pid, proc.create_time())

    obj = ReporterAgentDummy()

    try:
        agent_mock.start()
        for child_proc in children:
            child_proc.start()
        children_pids = {p.pid for p in children}
        workers = ReporterAgent._get_workers(obj)
        # In the first run, the percent should be 0.
        assert all([worker["cpu_percent"] == 0.0 for worker in workers])
        for _ in range(10):
            time.sleep(0.1)
            workers = ReporterAgent._get_workers(obj)
            workers_pids = {w["pid"] for w in workers}

            # Make sure all children are registered.
            for pid in children_pids:
                assert pid in workers_pids

            for worker in workers:
                if worker["pid"] in children_pids:
                    # Subsequent run shouldn't be 0.
                    worker["cpu_percent"] > 0

        # Kill one of the child procs and test it is cleaned.
        print("killed ", children[0].pid)
        children[0].kill()
        wait_for_condition(lambda: not children[0].is_alive())
        workers = ReporterAgent._get_workers(obj)
        workers_pids = {w["pid"] for w in workers}
        assert children[0].pid not in workers_pids
        assert children[1].pid in workers_pids

        children[1].kill()
        wait_for_condition(lambda: not children[1].is_alive())
        workers = ReporterAgent._get_workers(obj)
        workers_pids = {w["pid"] for w in workers}
        assert children[0].pid not in workers_pids
        assert children[1].pid not in workers_pids

    except Exception as e:
        logger.exception(e)
        raise
    finally:
        for child_proc in children:
            if child_proc.is_alive():
                child_proc.kill()
        if agent_mock.is_alive():
            agent_mock.kill()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
