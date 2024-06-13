import os
import sys
from typing import List

import pytest

from ray.autoscaler._private.prom_metrics import AutoscalerPrometheusMetrics
from ray.autoscaler.v2.instance_manager.config import NodeTypeConfig
from ray.autoscaler.v2.metrics_reporter import AutoscalerMetricsReporter
from ray.autoscaler.v2.tests.util import create_instance
from ray.core.generated.instance_manager_pb2 import Instance


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
        create_instance(id(), status=Instance.TERMINATING, instance_type="type_1"),
        create_instance(id(), status=Instance.TERMINATING, instance_type="type_2"),
        # Terminated
        create_instance(id(), status=Instance.TERMINATED, instance_type="type_1"),
        create_instance(id(), status=Instance.TERMINATED, instance_type="type_2"),
    ]

    reporter.report_instances(instances, node_type_configs)

    def _get_metrics(metrics, name) -> List[float]:
        sample_values = []
        for x in metrics:
            for sample in x.samples:
                if sample.name == name:
                    sample_values.append(sample.value)
        return sample_values

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
    assert _get_metrics(
        reporter._prom_metrics.stopped_nodes.collect(),
        "autoscaler_stopped_nodes_total",
    ) == [2]

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


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
