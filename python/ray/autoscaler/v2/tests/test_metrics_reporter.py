import os
import sys
from typing import List

import pytest

from ray.autoscaler._private.prom_metrics import AutoscalerPrometheusMetrics
from ray.autoscaler.v2.instance_manager.config import NodeTypeConfig
from ray.autoscaler.v2.metrics_reporter import AutoscalerMetricsReporter
from ray.autoscaler.v2.tests.util import create_instance
from ray.core.generated.instance_manager_pb2 import Instance


def _get_metrics(metrics, name) -> List[float]:
    sample_values = []
    for x in metrics:
        for sample in x.samples:
            if sample.name == name:
                sample_values.append(sample.value)
    return sample_values


def test_report_nodes_resources():
    """
    Test that the metrics reporter reports the correct number of nodes and resources
    """
    reporter = AutoscalerMetricsReporter(
        AutoscalerPrometheusMetrics(session_name="test")
    )
    node_type_configs = {
        "type_1": NodeTypeConfig(
            name="type_1",
            max_worker_nodes=10,
            min_worker_nodes=1,
            resources={"CPU": 1},
        ),
        "type_2": NodeTypeConfig(
            name="type_2",
            max_worker_nodes=10,
            min_worker_nodes=1,
            resources={"GPU": 1},
        ),
    }

    _i = 0

    def id():
        nonlocal _i
        _i += 1
        return f"i-{_i}"

    terminating_type_1 = create_instance(
        id(), status=Instance.TERMINATING, instance_type="type_1"
    )
    terminating_type_2 = create_instance(
        id(), status=Instance.TERMINATING, instance_type="type_2"
    )

    instances = [
        # Active = 3
        create_instance(id(), status=Instance.RAY_RUNNING, instance_type="type_1"),
        create_instance(
            id(), status=Instance.RAY_STOP_REQUESTED, instance_type="type_1"
        ),
        create_instance(id(), status=Instance.RAY_STOPPING, instance_type="type_1"),
        create_instance(id(), status=Instance.RAY_RUNNING, instance_type="type_2"),
        # Pending
        create_instance(id(), status=Instance.QUEUED, instance_type="type_1"),
        create_instance(id(), status=Instance.REQUESTED, instance_type="type_1"),
        create_instance(id(), status=Instance.RAY_INSTALLING, instance_type="type_1"),
        create_instance(id(), status=Instance.ALLOCATED, instance_type="type_1"),
        create_instance(id(), status=Instance.RAY_INSTALLING, instance_type="type_2"),
        create_instance(id(), status=Instance.ALLOCATED, instance_type="type_2"),
        # Terminating
        terminating_type_1,
        terminating_type_2,
    ]
    reporter.report_instances(instances, node_type_configs)

    assert _get_metrics(
        reporter._prom_metrics.active_nodes.labels(
            SessionName="test", NodeType="type_1"
        ).collect(),
        "autoscaler_active_nodes",
    ) == [3]
    assert _get_metrics(
        reporter._prom_metrics.pending_nodes.labels(
            SessionName="test", NodeType="type_1"
        ).collect(),
        "autoscaler_pending_nodes",
    ) == [4]

    assert _get_metrics(
        reporter._prom_metrics.recently_failed_nodes.labels(
            SessionName="test", NodeType="type_1"
        ).collect(),
        "autoscaler_recently_failed_nodes",
    ) == [1]

    # Test that resources are reported correctly
    reporter.report_resources(instances, node_type_configs)

    assert _get_metrics(
        reporter._prom_metrics.cluster_resources.labels(
            SessionName="test", resource="CPU"
        ).collect(),
        "autoscaler_cluster_resources",
    ) == [3]

    assert _get_metrics(
        reporter._prom_metrics.cluster_resources.labels(
            SessionName="test", resource="GPU"
        ).collect(),
        "autoscaler_cluster_resources",
    ) == [1]

    assert _get_metrics(
        reporter._prom_metrics.pending_resources.labels(
            SessionName="test", resource="CPU"
        ).collect(),
        "autoscaler_pending_resources",
    ) == [4]

    assert _get_metrics(
        reporter._prom_metrics.pending_resources.labels(
            SessionName="test", resource="GPU"
        ).collect(),
        "autoscaler_pending_resources",
    ) == [2]


def test_report_nodes_resources_handles_deleted_node_type():
    reporter = AutoscalerMetricsReporter(
        AutoscalerPrometheusMetrics(session_name="test_deleted")
    )
    node_type_configs = {
        "current_type": NodeTypeConfig(
            name="current_type",
            max_worker_nodes=10,
            min_worker_nodes=1,
            resources={"CPU": 2},
        ),
    }

    instances = [
        create_instance(
            "current-running",
            status=Instance.RAY_RUNNING,
            instance_type="current_type",
        ),
        create_instance(
            "deleted-terminated",
            status=Instance.TERMINATED,
            instance_type="deleted_type",
        ),
        create_instance(
            "deleted-terminating",
            status=Instance.TERMINATING,
            instance_type="deleted_type",
        ),
        create_instance(
            "deleted-pending",
            status=Instance.QUEUED,
            instance_type="deleted_type",
        ),
    ]

    reporter.report_instances(instances, node_type_configs)
    reporter.report_resources(instances, node_type_configs)

    assert _get_metrics(
        reporter._prom_metrics.active_nodes.labels(
            SessionName="test_deleted", NodeType="current_type"
        ).collect(),
        "autoscaler_active_nodes",
    ) == [1]
    assert _get_metrics(
        reporter._prom_metrics.pending_nodes.labels(
            SessionName="test_deleted", NodeType="current_type"
        ).collect(),
        "autoscaler_pending_nodes",
    ) == [0]
    assert _get_metrics(
        reporter._prom_metrics.recently_failed_nodes.labels(
            SessionName="test_deleted", NodeType="current_type"
        ).collect(),
        "autoscaler_recently_failed_nodes",
    ) == [0]
    assert _get_metrics(
        reporter._prom_metrics.active_nodes.labels(
            SessionName="test_deleted", NodeType="deleted_type"
        ).collect(),
        "autoscaler_active_nodes",
    ) == [0]
    assert _get_metrics(
        reporter._prom_metrics.pending_nodes.labels(
            SessionName="test_deleted", NodeType="deleted_type"
        ).collect(),
        "autoscaler_pending_nodes",
    ) == [1]
    assert _get_metrics(
        reporter._prom_metrics.recently_failed_nodes.labels(
            SessionName="test_deleted", NodeType="deleted_type"
        ).collect(),
        "autoscaler_recently_failed_nodes",
    ) == [1]
    assert _get_metrics(
        reporter._prom_metrics.cluster_resources.labels(
            SessionName="test_deleted", resource="CPU"
        ).collect(),
        "autoscaler_cluster_resources",
    ) == [2]


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
