# isort: skip_file
# ruff: noqa: E402
import json
import os
import sys

import pytest

# RAY_enable_export_api_write env var must be set before importing
# `ray` so the correct value is set for RAY_enable_export_api_write
# even outside a Ray driver.
os.environ["RAY_enable_export_api_write"] = "1"

import ray


@pytest.mark.asyncio
async def test_task_labels(tmp_path):  # noqa: F811
    """
    Test task events are correctly generated and written to file
    """
    ray.init(_temp_dir=str(tmp_path))

    @ray.remote
    def task():
        return 1

    ray.get(task.options(_labels={"hi": "w00t"}).remote())
    # Shutdown the cluster to ensure all events are flushed
    ray.shutdown()

    export_event_path = os.path.join(
        str(tmp_path), "session_latest", "logs", "export_events"
    )
    # Verify export events are written
    hi_w00t_label_found = False
    events = []
    for filename in os.listdir(export_event_path):
        if not filename.startswith("event_EXPORT_TASK"):
            continue
        with open(f"{export_event_path}/{filename}", "r") as f:
            for line in f.readlines():
                events.append(line)
                data = json.loads(line)
                labels = data["event_data"].get("task_info", {}).get("labels", {})
                if labels.get("hi") == "w00t":
                    hi_w00t_label_found = True
                    break

    assert hi_w00t_label_found, f"Label 'hi' with value 'w00t' not found in {events}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
