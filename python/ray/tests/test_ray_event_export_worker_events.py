import json
import logging
import textwrap

import pytest

from ray._common.network_utils import find_free_port
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import run_string_as_driver_nonblocking

logger = logging.getLogger(__name__)


_EVENT_AGGREGATOR_AGENT_TARGET_PORT = find_free_port()
_EVENT_AGGREGATOR_AGENT_TARGET_IP = "127.0.0.1"
_EVENT_AGGREGATOR_AGENT_TARGET_ADDR = (
    f"http://{_EVENT_AGGREGATOR_AGENT_TARGET_IP}:{_EVENT_AGGREGATOR_AGENT_TARGET_PORT}"
)


@pytest.fixture(scope="module")
def httpserver_listen_address():
    return (_EVENT_AGGREGATOR_AGENT_TARGET_IP, _EVENT_AGGREGATOR_AGENT_TARGET_PORT)


# Worker lifecycle events are produced by GCS, gated on RAY_enable_ray_event.
_cluster_with_aggregator_target = pytest.mark.parametrize(
    ("preserve_proto_field_name", "ray_start_cluster_head_with_env_vars"),
    [
        pytest.param(
            preserve_proto_field_name,
            {
                # pin the cpu count so the worker pool stays small and predictable
                "num_cpus": 1,
                "env_vars": {
                    "RAY_enable_ray_event": "1",
                    "RAY_ray_events_report_interval_ms": 100,
                    "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": _EVENT_AGGREGATOR_AGENT_TARGET_ADDR,
                    "RAY_DASHBOARD_AGGREGATOR_AGENT_PRESERVE_PROTO_FIELD_NAME": (
                        "1" if preserve_proto_field_name is True else "0"
                    ),
                },
            },
        )
        for preserve_proto_field_name in [True, False]
    ],
    indirect=["ray_start_cluster_head_with_env_vars"],
)


def get_and_validate_events(httpserver, validation_func):
    event_data = []
    for http_log in httpserver.log:
        req, _ = http_log
        data = json.loads(req.data)
        event_data.extend(data)

    try:
        validation_func(event_data)
        return True
    except Exception:
        return False


def run_driver_script_and_wait_for_events(script, httpserver, validation_func):
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)
    run_string_as_driver_nonblocking(script)
    wait_for_condition(lambda: get_and_validate_events(httpserver, validation_func))


def _keys(preserve_proto_field_name: bool) -> dict:
    """JSON key names for the worker events under either proto->JSON naming mode."""
    if preserve_proto_field_name:
        return dict(
            event_type="event_type",
            wle="worker_lifecycle_event",
            wde="worker_definition_event",
            worker_id="worker_id",
            worker_type="worker_type",
            state_transitions="state_transitions",
            death_info="death_info",
            exit_type="exit_type",
        )
    return dict(
        event_type="eventType",
        wle="workerLifecycleEvent",
        wde="workerDefinitionEvent",
        worker_id="workerId",
        worker_type="workerType",
        state_transitions="stateTransitions",
        death_info="deathInfo",
        exit_type="exitType",
    )


class TestWorkerLifecycleEvents:
    @_cluster_with_aggregator_target
    def test_worker_lifecycle_exported_and_driver_excluded(
        self,
        ray_start_cluster_head_with_env_vars,
        httpserver,
        preserve_proto_field_name,
    ):
        script = textwrap.dedent(
            """
            import ray
            ray.init()

            @ray.remote
            def t():
                return 1
            ray.get(t.remote())

            @ray.remote
            class A:
                def ping(self):
                    return "ok"
            a = A.remote()
            ray.get(a.ping.remote())
            ray.kill(a)
            """
        )

        k = _keys(preserve_proto_field_name)

        def validate_events(events):
            lifecycle_events = [
                e for e in events if e.get(k["event_type"]) == "WORKER_LIFECYCLE_EVENT"
            ]
            definition_events = [
                e for e in events if e.get(k["event_type"]) == "WORKER_DEFINITION_EVENT"
            ]
            assert lifecycle_events, "no WORKER_LIFECYCLE_EVENT received"
            assert definition_events, "no WORKER_DEFINITION_EVENT received"

            # The static identity lives on the definition event. No worker event
            # should describe a driver (drivers are covered by driver job events).
            for e in definition_events:
                d = e[k["wde"]]
                assert d.get(k["worker_type"]) != "DRIVER"
                assert d.get(k["worker_id"])

            saw_alive = False
            saw_killed_dead = False
            for e in lifecycle_events:
                w = e[k["wle"]]
                # Identity is always present
                assert w.get(k["worker_id"])
                for st in w.get(k["state_transitions"], []):
                    if st.get("state") == "ALIVE":
                        saw_alive = True
                    if st.get("state") == "DEAD":
                        death_info = st.get(k["death_info"], {})
                        if death_info.get(k["exit_type"]) == "INTENDED_SYSTEM_EXIT":
                            saw_killed_dead = True

            assert saw_alive, "no ALIVE worker transition observed"
            assert (
                saw_killed_dead
            ), "no DEAD worker transition with INTENDED_SYSTEM_EXIT (ray.kill)"

            # the worker_id of every lifecycle event must match a definition event
            definition_worker_ids = {
                e[k["wde"]][k["worker_id"]] for e in definition_events
            }
            lifecycle_worker_ids = {
                e[k["wle"]][k["worker_id"]] for e in lifecycle_events
            }
            assert lifecycle_worker_ids <= definition_worker_ids, (
                "lifecycle events without a matching definition event: "
                f"{lifecycle_worker_ids - definition_worker_ids}"
            )

        run_driver_script_and_wait_for_events(
            script,
            httpserver,
            validate_events,
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
