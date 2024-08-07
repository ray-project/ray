import copy
import logging
import os
import sys
import time
from collections import defaultdict
from multiprocessing import Process
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import requests
from google.protobuf import text_format

import ray
from ray._private import ray_constants
from ray._private.metrics_agent import fix_grpc_metric
from ray._private.test_utils import (
    fetch_prometheus,
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
)
from ray.core.generated.metrics_pb2 import Metric
from ray.dashboard.modules.reporter.reporter_agent import ReporterAgent
from ray.dashboard.tests.conftest import *  # noqa
from ray.dashboard.utils import Bunch

import psutil

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
    "shm": 456,
    "workers": [
        {
            "memory_info": Bunch(
                rss=55934976, vms=7026937856, pfaults=15354, pageins=0
            ),
            "memory_full_info": Bunch(uss=51428381),
            "cpu_percent": 0.0,
            "num_fds": 10,
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
        "num_fds": 10,
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
        "num_fds": 10,
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


def test_fix_grpc_metrics():
    """
    A real metric output from gcs_server, with name prefixed with "grpc.io/" and 1
    distribution time series. It has 45 buckets, first of which bounds = 0.0.
    """
    metric_textproto = (
        'metric_descriptor { name: "grpc.io/server/server_latency" description: "Time '
        "between first byte of request received to last byte of response sent, or "
        'terminal error" unit: "ms" label_keys { key: "grpc_server_method" } label_keys'
        ' { key: "Component" } label_keys { key: "WorkerId" } label_keys { key: '
        '"Version" } label_keys { key: "NodeAddress" } label_keys { key: "SessionName" '
        "} } timeseries { start_timestamp { seconds: 1693693592 } label_values { value:"
        ' "ray.rpc.NodeInfoGcsService/RegisterNode" } label_values { value: '
        '"gcs_server" } label_values { } label_values { value: "3.0.0.dev0" } '
        'label_values { value: "127.0.0.1" } label_values { value: '
        '"session_2023-09-02_15-26-32_589652_23265" } points { timestamp { seconds: '
        "1693693602 } distribution_value { count: 1 sum: 0.266 bucket_options { "
        "explicit { bounds: 0.0 bounds: 0.01 bounds: 0.05 bounds: 0.1 bounds: 0.3 "
        "bounds: 0.6 bounds: 0.8 bounds: 1.0 bounds: 2.0 bounds: 3.0 bounds: 4.0 "
        "bounds: 5.0 bounds: 6.0 bounds: 8.0 bounds: 10.0 bounds: 13.0 bounds: 16.0 "
        "bounds: 20.0 bounds: 25.0 bounds: 30.0 bounds: 40.0 bounds: 50.0 bounds: 65.0 "
        "bounds: 80.0 bounds: 100.0 bounds: 130.0 bounds: 160.0 bounds: 200.0 bounds: "
        "250.0 bounds: 300.0 bounds: 400.0 bounds: 500.0 bounds: 650.0 bounds: 800.0 "
        "bounds: 1000.0 bounds: 2000.0 bounds: 5000.0 bounds: 10000.0 bounds: 20000.0 "
        "bounds: 50000.0 bounds: 100000.0 } } buckets { } buckets { } buckets { } "
        "buckets { } buckets { count: 1 } buckets { } buckets { } buckets { } buckets {"
        " } buckets { } buckets { } buckets { } buckets { } buckets { } buckets { } "
        "buckets { } buckets { } buckets { } buckets { } buckets { } buckets { } "
        "buckets { } buckets { } buckets { } buckets { } buckets { } buckets { } "
        "buckets { } buckets { } buckets { } buckets { } buckets { } buckets { } "
        "buckets { } buckets { } buckets { } buckets { } buckets { } buckets { } "
        "buckets { } buckets { } buckets { } } } }"
    )

    metric = Metric()
    text_format.Parse(metric_textproto, metric)

    expected_fixed_metric = Metric()
    expected_fixed_metric.CopyFrom(metric)
    expected_fixed_metric.metric_descriptor.name = "grpc_io_server_server_latency"
    expected_fixed_metric.timeseries[0].points[
        0
    ].distribution_value.bucket_options.explicit.bounds[0] = 0.0000001

    fix_grpc_metric(metric)
    assert metric == expected_fixed_metric


@pytest.fixture
def enable_grpc_metrics_collection():
    os.environ["RAY_enable_grpc_metrics_collection_for"] = "gcs"
    yield
    os.environ.pop("RAY_enable_grpc_metrics_collection_for", None)


@pytest.mark.skipif(prometheus_client is None, reason="prometheus_client not installed")
def test_prometheus_physical_stats_record(
    enable_grpc_metrics_collection, enable_test_module, shutdown_only
):
    addresses = ray.init(include_dashboard=True, num_cpus=1)
    metrics_export_port = addresses["metrics_export_port"]
    addr = addresses["raylet_ip_address"]
    prom_addresses = [f"{addr}:{metrics_export_port}"]

    def test_case_stats_exist():
        _, metric_descriptors, _ = fetch_prometheus(prom_addresses)
        metric_names = metric_descriptors.keys()
        predicates = [
            "ray_node_cpu_utilization" in metric_names,
            "ray_node_cpu_count" in metric_names,
            "ray_node_mem_used" in metric_names,
            "ray_node_mem_available" in metric_names,
            "ray_node_mem_total" in metric_names,
            "ray_node_mem_total" in metric_names,
            "ray_component_rss_mb" in metric_names,
            "ray_component_uss_mb" in metric_names,
            "ray_component_num_fds" in metric_names,
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
            "ray_grpc_io_client_sent_bytes_per_rpc_bucket" in metric_names,
        ]
        if sys.platform == "linux" or sys.platform == "linux2":
            predicates.append("ray_node_mem_shared_bytes" in metric_names)
        return all(predicates)

    def test_case_ip_correct():
        _, _, metric_samples = fetch_prometheus(prom_addresses)
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
        _, metric_descriptors, _ = fetch_prometheus(prom_addresses)
        metric_names = metric_descriptors.keys()
        expected_metrics = [
            "ray_component_cpu_percentage",
            "ray_component_rss_mb",
            "ray_component_uss_mb",
            "ray_component_num_fds",
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

    records = agent._to_records(STATS_TEMPLATE, cluster_stats)
    for record in records:
        name = record.gauge.name
        val = record.value
        if name == "node_mem_shared_bytes":
            assert val == STATS_TEMPLATE["shm"]
        print(record.gauge.name)
        print(record)
    assert len(records) == 36
    # Test stats without raylets
    STATS_TEMPLATE["raylet"] = {}
    records = agent._to_records(STATS_TEMPLATE, cluster_stats)
    assert len(records) == 32
    # Test stats with gpus
    STATS_TEMPLATE["gpus"] = [
        {"utilization_gpu": 1, "memory_used": 100, "memory_total": 1000, "index": 0}
    ]
    records = agent._to_records(STATS_TEMPLATE, cluster_stats)
    assert len(records) == 36
    # Test stats without autoscaler report
    cluster_stats = {}
    records = agent._to_records(STATS_TEMPLATE, cluster_stats)
    assert len(records) == 34


def test_report_stats_gpu():
    dashboard_agent = MagicMock()
    agent = ReporterAgent(dashboard_agent)
    # Assume it is a head node.
    agent._is_head_node = True
    # GPUstats query output example.
    """
    {'index': 0,
    'uuid': 'GPU-36e1567d-37ed-051e-f8ff-df807517b396',
    'name': 'NVIDIA A10G',
    'utilization_gpu': 1,
    'memory_used': 0,
    'memory_total': 22731,
    'processes': []}
    """
    GPU_MEMORY = 22731
    STATS_TEMPLATE["gpus"] = [
        {
            "index": 0,
            "uuid": "GPU-36e1567d-37ed-051e-f8ff-df807517b396",
            "name": "NVIDIA A10G",
            "utilization_gpu": 0,
            "memory_used": 0,
            "memory_total": GPU_MEMORY,
            "processes": [],
        },
        {
            "index": 1,
            "uuid": "GPU-36e1567d-37ed-051e-f8ff-df807517b397",
            "name": "NVIDIA A10G",
            "utilization_gpu": 1,
            "memory_used": 1,
            "memory_total": GPU_MEMORY,
            "processes": [],
        },
        {
            "index": 2,
            "uuid": "GPU-36e1567d-37ed-051e-f8ff-df807517b398",
            "name": "NVIDIA A10G",
            "utilization_gpu": 2,
            "memory_used": 2,
            "memory_total": GPU_MEMORY,
            "processes": [],
        },
        # No name.
        {
            "index": 3,
            "uuid": "GPU-36e1567d-37ed-051e-f8ff-df807517b398",
            "utilization_gpu": 3,
            "memory_used": 3,
            "memory_total": GPU_MEMORY,
            "processes": [],
        },
        # No index
        {
            "uuid": "GPU-36e1567d-37ed-051e-f8ff-df807517b398",
            "name": "NVIDIA A10G",
            "utilization_gpu": 3,
            "memory_used": 3,
            "memory_total": 22731,
            "processes": [],
        },
    ]
    gpu_metrics_aggregatd = {
        "node_gpus_available": 0,
        "node_gpus_utilization": 0,
        "node_gram_used": 0,
        "node_gram_available": 0,
    }
    records = agent._to_records(STATS_TEMPLATE, {})
    # If index is not available, we don't emit metrics.
    num_gpu_records = 0
    for record in records:
        if record.gauge.name in gpu_metrics_aggregatd:
            num_gpu_records += 1
    assert num_gpu_records == 16

    ip = STATS_TEMPLATE["ip"]
    gpu_records = defaultdict(list)
    for record in records:
        if record.gauge.name in gpu_metrics_aggregatd:
            gpu_records[record.gauge.name].append(record)

    for name, records in gpu_records.items():
        records.sort(key=lambda e: e.tags["GpuIndex"])
        index = 0
        for record in records:
            if record.tags["GpuIndex"] == "3":
                assert record.tags == {"ip": ip, "GpuIndex": "3"}
            else:
                assert record.tags == {
                    "ip": ip,
                    # The tag value must be string for prometheus.
                    "GpuIndex": str(index),
                    "GpuDeviceName": "NVIDIA A10G",
                }

            if name == "node_gram_available":
                assert record.value == GPU_MEMORY - index
            elif name == "node_gpus_available":
                assert record.value == 1
            else:
                assert record.value == index

            gpu_metrics_aggregatd[name] += record.value
            index += 1

    assert gpu_metrics_aggregatd["node_gpus_available"] == 4
    assert gpu_metrics_aggregatd["node_gpus_utilization"] == 6
    assert gpu_metrics_aggregatd["node_gram_used"] == 6
    assert gpu_metrics_aggregatd["node_gram_available"] == GPU_MEMORY * 4 - 6


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
        "num_fds": 11,
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
        "num_fds": 12,
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
        "num_fds": 13,
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
        "num_fds": 14,
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

    def get_uss_and_cpu_and_num_fds_records(records):
        component_uss_mb_records = defaultdict(list)
        component_cpu_percentage_records = defaultdict(list)
        component_num_fds_records = defaultdict(list)
        for record in records:
            name = record.gauge.name
            if name == "component_uss_mb":
                comp = record.tags["Component"]
                component_uss_mb_records[comp].append(record)
            if name == "component_cpu_percentage":
                comp = record.tags["Component"]
                component_cpu_percentage_records[comp].append(record)
            if name == "component_num_fds":
                comp = record.tags["Component"]
                component_num_fds_records[comp].append(record)
        return (
            component_uss_mb_records,
            component_cpu_percentage_records,
            component_num_fds_records,
        )

    """
    Test basic case.
    """
    records = agent._to_records(test_stats, cluster_stats)
    uss_records, cpu_records, num_fds_records = get_uss_and_cpu_and_num_fds_records(
        records
    )

    def verify_metrics_values(
        uss_records, cpu_records, num_fds_records, comp, uss, cpu_percent, num_fds
    ):
        """Verify the component exists and match the resource usage."""
        assert comp in uss_records
        assert comp in cpu_records
        assert comp in num_fds_records
        uss_metrics = uss_records[comp][0].value
        cpu_percnet_metrics = cpu_records[comp][0].value
        num_fds_metrics = num_fds_records[comp][0].value
        assert uss_metrics == uss
        assert cpu_percnet_metrics == cpu_percent
        assert num_fds_metrics == num_fds

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
            num_fds_records,
            comp,
            float(stats["memory_full_info"].uss) / 1.0e6,
            stats["cpu_percent"],
            stats["num_fds"],
        )

    """
    Test metrics are resetted (report metrics with values 0) when
    the proc doesn't exist anymore.
    """
    # Verify the metrics are reset after ray::func is killed.
    test_stats["workers"] = [idle_stats]
    records = agent._to_records(test_stats, cluster_stats)
    uss_records, cpu_records, num_fds_records = get_uss_and_cpu_and_num_fds_records(
        records
    )
    verify_metrics_values(
        uss_records,
        cpu_records,
        num_fds_records,
        "ray::IDLE",
        float(idle_stats["memory_full_info"].uss) / 1.0e6,
        idle_stats["cpu_percent"],
        idle_stats["num_fds"],
    )

    comp = "ray::func"
    stats = func_stats
    # Value should be reset since func doesn't exist anymore.
    verify_metrics_values(
        uss_records,
        cpu_records,
        num_fds_records,
        "ray::func",
        0,
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
        "num_fds": 8,
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

    records = agent._to_records(test_stats, cluster_stats)
    uss_records, cpu_records, num_fds_records = get_uss_and_cpu_and_num_fds_records(
        records
    )
    assert "python mock" not in uss_records
    assert "python mock" not in cpu_records
    assert "python mock" not in num_fds_records


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


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
@pytest.mark.skipif(sys.platform == "win32", reason="No py-spy on Windows.")
@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="Fails on OSX: https://github.com/ray-project/ray/issues/30114",
)
def test_get_task_traceback_running_task(shutdown_only):
    """
    Verify that Ray can get the traceback for a running task.

    """
    address_info = ray.init()
    webui_url = format_web_url(address_info["webui_url"])

    @ray.remote
    def f():
        pass

    @ray.remote
    def long_running_task():
        print("Long-running task began.")
        time.sleep(1000)
        print("Long-running task completed.")

    ray.get([f.remote() for _ in range(5)])

    task = long_running_task.remote()

    params = {
        "task_id": task.task_id().hex(),
        "attempt_number": 0,
        "node_id": ray.get_runtime_context().node_id.hex(),
    }

    def verify():
        resp = requests.get(f"{webui_url}/task/traceback", params=params)
        print(f"resp.text {type(resp.text)}: {resp.text}")

        assert "Process" in resp.text
        return True

    wait_for_condition(verify, timeout=20)


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
@pytest.mark.skipif(sys.platform == "win32", reason="No memray on Windows.")
@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="Fails on OSX, requires memray & lldb installed in osx image",
)
def test_get_memory_profile_running_task(shutdown_only):
    """
    Verify that we can get the memory profile for a running task.

    """
    address_info = ray.init()
    webui_url = format_web_url(address_info["webui_url"])

    @ray.remote
    def f():
        pass

    @ray.remote
    def long_running_task():
        print("Long-running task began.")
        time.sleep(1000)
        print("Long-running task completed.")

    ray.get([f.remote() for _ in range(5)])

    task = long_running_task.remote()

    params = {
        "task_id": task.task_id().hex(),
        "attempt_number": 0,
        "node_id": ray.get_runtime_context().node_id.hex(),
        "duration": 5,
    }

    def verify():
        resp = requests.get(f"{webui_url}/memory_profile", params=params)
        print(f"resp.text {type(resp.text)}: {resp.text}")

        assert resp.status_code == 200
        assert "memray" in resp.text
        return True

    wait_for_condition(verify, timeout=20)


TASK = {
    "task_id": "32d950ec0ccf9d2affffffffffffffffffffffff01000000",
    "attempt_number": 0,
    "node_id": "ffffffffffffffffffffffffffffffffffffffff01000000",
}


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
@pytest.mark.skipif(sys.platform == "win32", reason="No py-spy on Windows.")
@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="Fails on OSX: https://github.com/ray-project/ray/issues/30114",
)
def test_get_task_traceback_non_running_task(shutdown_only):
    """
    Verify that Ray throws an error for a non-running task.
    """

    # The sleep is needed since it seems a previous shutdown could be not yet
    # done when the next test starts. This prevents a previous cluster to be
    # connected the current test session.

    address_info = ray.init()
    webui_url = format_web_url(address_info["webui_url"])

    @ray.remote
    def f():
        pass

    ray.get([f.remote() for _ in range(5)])

    params = {
        "task_id": TASK["task_id"],
        "attempt_number": TASK["attempt_number"],
        "node_id": TASK["node_id"],
    }

    # Make sure the API works.
    def verify():
        with pytest.raises(requests.exceptions.HTTPError) as exc_info:
            resp = requests.get(f"{webui_url}/task/traceback", params=params)
            resp.raise_for_status()
        assert isinstance(exc_info.value, requests.exceptions.HTTPError)
        return True

    wait_for_condition(verify, timeout=10)


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
@pytest.mark.skipif(sys.platform == "win32", reason="No py-spy on Windows.")
@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="Fails on OSX: https://github.com/ray-project/ray/issues/30114",
)
def test_get_cpu_profile_non_running_task(shutdown_only):
    """
    Verify that we throw an error for a non-running task.
    """
    address_info = ray.init()
    webui_url = format_web_url(address_info["webui_url"])

    @ray.remote
    def f():
        pass

    ray.get([f.remote() for _ in range(5)])

    params = {
        "task_id": TASK["task_id"],
        "attempt_number": TASK["attempt_number"],
        "node_id": TASK["node_id"],
    }

    # Make sure the API works.
    def verify():
        with pytest.raises(requests.exceptions.HTTPError) as exc_info:
            resp = requests.get(f"{webui_url}/task/cpu_profile", params=params)
            resp.raise_for_status()
        assert isinstance(exc_info.value, requests.exceptions.HTTPError)
        return True

    wait_for_condition(verify, timeout=10)


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
@pytest.mark.skipif(sys.platform == "win32", reason="No py-spy on Windows.")
@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="Fails on OSX, requires memray & lldb installed in osx image",
)
def test_task_get_memory_profile_missing_params(shutdown_only):
    """
    Verify that we throw an error for a non-running task.
    """
    address_info = ray.init()
    webui_url = format_web_url(address_info["webui_url"])

    @ray.remote
    def f():
        pass

    ray.get([f.remote() for _ in range(5)])

    missing_node_id_params = {
        "task_id": TASK["task_id"],
        "attempt_number": TASK["attempt_number"],
    }

    # Make sure the API works.
    def verify():
        resp = requests.get(
            f"{webui_url}/memory_profile", params=missing_node_id_params
        )
        content = resp.content.decode("utf-8")
        assert "task's node id is required" in content, content
        return True

    wait_for_condition(verify, timeout=10)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
