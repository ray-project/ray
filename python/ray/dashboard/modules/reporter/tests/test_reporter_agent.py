import asyncio
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

import pytest
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2

from ray.dashboard.modules.reporter.reporter_agent import ReporterAgent


def test_export_processes_metrics_off_event_loop() -> None:
    request = metrics_service_pb2.ExportMetricsServiceRequest()
    scope_metrics = request.resource_metrics.add().scope_metrics.add()
    histogram = scope_metrics.metrics.add()
    histogram.histogram.data_points.add().count = 1
    gauge = scope_metrics.metrics.add()
    gauge.gauge.data_points.add().as_double = 1.0

    agent = object.__new__(ReporterAgent)
    metric_calls: list[tuple[str, int]] = []

    def record_histogram(_: object) -> None:
        metric_calls.append(("histogram", threading.get_ident()))

    def record_number(_: object) -> None:
        metric_calls.append(("number", threading.get_ident()))

    agent._export_histogram_data = record_histogram
    agent._export_number_data = record_number

    with ThreadPoolExecutor(max_workers=1) as executor:
        agent._otlp_ingest_executor = executor
        asyncio.run(
            ReporterAgent.Export(
                agent,
                request=request,
                context=MagicMock(),
            )
        )

    assert [kind for kind, _ in metric_calls] == ["histogram", "number"]
    assert all(thread_id != threading.get_ident() for _, thread_id in metric_calls)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
