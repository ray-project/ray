import argparse
import json
import os
import sys
import time
from pathlib import Path

import requests

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import format_web_url
from ray.cluster_utils import AutoscalingCluster

_AUTOSCALER_EVENT_TYPES = (
    "AUTOSCALER_CONFIG_DEFINITION_EVENT,"
    "AUTOSCALER_SCALING_DECISION_EVENT,"
    "AUTOSCALER_NODE_PROVISIONING_EVENT"
)


def _parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Start an autoscaling test cluster and dump dashboard autoscaler events "
            "for manual before/after inspection."
        )
    )
    parser.add_argument(
        "--python-ray-event",
        action="store_true",
        help="Enable autoscaler ONE-event publishing via dashboard head.",
    )
    parser.add_argument(
        "--hold-seconds",
        type=int,
        default=120,
        help="How long to keep the cluster alive after events are dumped.",
    )
    parser.add_argument(
        "--event-timeout",
        type=int,
        default=60,
        help="How long to wait for autoscaler events to reach the dashboard.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory to write captured endpoint payloads into.",
    )
    return parser.parse_args()


def _get_mode_name(enable_python_ray_event: bool) -> str:
    return "python-ray-event" if enable_python_ray_event else "legacy"


def _fetch_json(url: str) -> dict:
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def _extract_dashboard_autoscaler_events(events_response: dict) -> list[dict]:
    all_events = events_response["data"]["events"]
    autoscaler_events = []
    for event_bucket in all_events.values():
        for event in event_bucket:
            if event.get("sourceType") == "AUTOSCALER":
                autoscaler_events.append(event)
    return autoscaler_events


def _extract_cluster_api_autoscaler_events(cluster_events_response: dict) -> list[dict]:
    result = cluster_events_response["data"]["result"]["result"]
    return [event for event in result if event.get("source_type") == "AUTOSCALER"]


def _wait_for_dashboard_autoscaler_events(
    webui_url: str, timeout: int
) -> tuple[list, list]:
    captured = {}

    def _condition():
        events_response = _fetch_json(
            f"{webui_url}/events?job_id=global&t={time.time()}"
        )
        cluster_events_response = _fetch_json(f"{webui_url}/api/v0/cluster_events")
        dashboard_events = _extract_dashboard_autoscaler_events(
            {
                "data": {
                    "events": {
                        "global": events_response["data"]["events"],
                    }
                }
            }
        )
        cluster_api_events = _extract_cluster_api_autoscaler_events(
            cluster_events_response
        )
        if dashboard_events or cluster_api_events:
            captured["dashboard_events"] = dashboard_events
            captured["cluster_api_events"] = cluster_api_events
            captured["events_response"] = events_response
            captured["cluster_events_response"] = cluster_events_response
            return True
        return False

    wait_for_condition(_condition, timeout=timeout)
    return (
        captured["events_response"],
        captured["cluster_events_response"],
    )


def _trigger_autoscaling_workload() -> None:
    @ray.remote(num_cpus=1)
    def block():
        time.sleep(300)
        return "done"

    block.remote()


def main():
    args = _parse_args()
    mode_name = _get_mode_name(args.python_ray_event)
    output_dir = args.output_dir or Path(
        f"/tmp/autoscaler-dashboard-events-{mode_name}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    current_python_bin = Path(sys.executable).resolve().parent
    os.environ["PATH"] = f"{current_python_bin}:{os.environ['PATH']}"

    override_env = {
        "RAY_external_ray_event_allowlist": _AUTOSCALER_EVENT_TYPES,
    }
    if args.python_ray_event:
        override_env["RAY_enable_python_ray_event"] = "true"

    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "type-1": {
                "resources": {"CPU": 1},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 2,
            },
        },
        autoscaler_v2=True,
    )

    try:
        cluster.start(override_env=override_env)
        ray.init("auto")

        dashboard_url = format_web_url(ray._private.worker._global_node.webui_url)
        _trigger_autoscaling_workload()
        (
            events_response,
            cluster_events_response,
        ) = _wait_for_dashboard_autoscaler_events(dashboard_url, args.event_timeout)

        dashboard_events = _extract_dashboard_autoscaler_events(
            {"data": {"events": {"global": events_response["data"]["events"]}}}
        )
        cluster_api_events = _extract_cluster_api_autoscaler_events(
            cluster_events_response
        )

        events_path = output_dir / f"{mode_name}-events.json"
        cluster_events_path = output_dir / f"{mode_name}-cluster-events.json"
        events_path.write_text(json.dumps(events_response, indent=2, sort_keys=True))
        cluster_events_path.write_text(
            json.dumps(cluster_events_response, indent=2, sort_keys=True)
        )

        print(f"Mode: {mode_name}")
        print(f"Dashboard URL: {dashboard_url}/#/overview")
        print(f"Saved /events payload to: {events_path}")
        print(f"Saved /api/v0/cluster_events payload to: {cluster_events_path}")
        print(f"/events AUTOSCALER rows: {len(dashboard_events)}")
        print(f"/api/v0/cluster_events AUTOSCALER rows: {len(cluster_api_events)}")
        if dashboard_events:
            print(
                "Sample /events row:",
                json.dumps(dashboard_events[0], indent=2, sort_keys=True),
            )
        if cluster_api_events:
            print(
                "Sample /api/v0/cluster_events row:",
                json.dumps(cluster_api_events[0], indent=2, sort_keys=True),
            )

        if args.hold_seconds > 0:
            print(
                f"Holding cluster for {args.hold_seconds}s so you can inspect the dashboard."
            )
            time.sleep(args.hold_seconds)
    finally:
        ray.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()
