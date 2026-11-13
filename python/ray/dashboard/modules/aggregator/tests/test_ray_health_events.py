import base64
import json
import sys

import pytest

import ray
from ray._private.test_utils import (
    wait_for_condition,
    wait_for_dashboard_agent_available,
)
from ray.dashboard.tests.conftest import *  # noqa

_HEALTH_EVENT_PORT = 12349

_EVENT_ENV_VARS = {
    "RAY_enable_ray_event": "1",
    "RAY_ENABLE_PYTHON_RAY_EVENT_TYPES": "HEALTH_EVENT",
}


@pytest.fixture(scope="session")
def httpserver_listen_address():
    return ("127.0.0.1", _HEALTH_EVENT_PORT)


def _health_events(httpserver):
    events = []
    for req, _ in httpserver.log:
        events.extend(e for e in json.loads(req.data) if "healthEvent" in e)
    return events


def test_ray_health_events(ray_start_cluster, httpserver):
    cluster = ray_start_cluster
    cluster.add_node(
        env_vars={
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": f"http://127.0.0.1:{_HEALTH_EVENT_PORT}",
            "RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES": "HEALTH_EVENT",
            **_EVENT_ENV_VARS,
        },
        _system_config={
            "enable_ray_event": True,
        },
    )
    cluster.wait_for_nodes()
    head_node_id = cluster.head_node.node_id

    ray.init(address=cluster.address)
    wait_for_dashboard_agent_available(cluster)

    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)

    # Emit from a worker, the way the HealthManager does from the Train controller.
    @ray.remote(runtime_env={"env_vars": _EVENT_ENV_VARS})
    def emit_health_events():
        from ray.core.generated.events_health_event_pb2 import (
            HealthFinding,
            NcclHangFinding,
        )
        from ray.train.v2._internal.health.event_exporter import (
            HealthEventExporter,
        )

        exporter = HealthEventExporter()
        finding_id = exporter.emit_new_finding(
            "nccl_hang_detector",
            summary="1 NCCL communicator stalled",
            started_at=1000.0,
            details=NcclHangFinding(
                run_id="run-1",
                attempt_id="attempt-1",
                communicators=[
                    NcclHangFinding.Communicator(
                        communicator_id="0xabc",
                        state=NcclHangFinding.Communicator.CONFIRMED,
                    )
                ],
            ),
            artifacts=[
                HealthFinding.HealthArtifact(name="ras_text_report", text="report")
            ],
        )
        decision_id = exporter.emit_decision(
            "REATTEMPT", reason="NCCL hang confirmed", finding_ids=[finding_id]
        )
        exporter.emit_stopped_finding(finding_id)
        exporter.shutdown()
        return finding_id, decision_id

    finding_id, decision_id = ray.get(emit_health_events.remote())

    wait_for_condition(lambda: len(_health_events(httpserver)) >= 3, timeout=20)
    events = sorted(
        _health_events(httpserver),
        key=lambda e: (
            e["healthEvent"]["checkerName"],
            int(e["healthEvent"]["sequence"]),
        ),
    )
    assert len(events) == 3

    for event in events:
        assert event["eventType"] == "HEALTH_EVENT"
        assert event["sourceType"] == "RAY_TRAIN"
        assert event["sessionName"]
        assert base64.b64decode(event["nodeId"]).hex() == head_node_id

    decision, new, stopped = events
    assert decision["severity"] == "WARNING"
    assert decision["message"] == "NCCL hang confirmed"
    assert decision["healthEvent"]["checkerName"] == "health_manager"
    # The aggregator prints every field, including empty ones.
    assert decision["healthEvent"]["decision"] == {
        "decisionId": decision_id,
        "action": "REATTEMPT",
        "reason": "NCCL hang confirmed",
        "evaluators": [],
        "findingIds": [finding_id],
        "targetNodeIds": [],
        "targetRanks": [],
        "onDemandProbes": [],
    }

    assert new["severity"] == "WARNING"
    assert new["message"] == "1 NCCL communicator stalled"
    finding = new["healthEvent"]["finding"]
    assert finding["findingId"] == finding_id
    assert finding["state"] == "NEW"
    assert finding["artifacts"] == [{"name": "ras_text_report", "text": "report"}]
    assert finding["ncclHang"]["runId"] == "run-1"
    assert finding["ncclHang"]["attemptId"] == "attempt-1"
    assert finding["ncclHang"]["communicators"][0]["state"] == "CONFIRMED"

    assert stopped["healthEvent"]["finding"]["state"] == "STOPPED"
    assert stopped["healthEvent"]["finding"]["artifacts"] == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
