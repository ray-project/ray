# Copyright 2026 The Ray Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import importlib.util
import json
import pathlib
import sys
import types


class RecordingGcsClient:
    def __init__(self):
        self.batches = []

    def publish_logs(self, data):
        normalized = {
            "IP": data["ip"],
            "PID": "" if data["pid"] is None else str(data["pid"]),
            "JobID": "" if data["job"] is None else data["job"],
            "IsErr": bool(data["is_err"]),
            "Lines": list(data["lines"]),
            "ActorName": "" if data["actor_name"] is None else data["actor_name"],
            "TaskName": "" if data["task_name"] is None else data["task_name"],
        }
        self.batches.append(normalized)


def install_stubs():
    ray_module = types.ModuleType("ray")
    private_module = types.ModuleType("ray._private")
    ray_constants = types.ModuleType("ray._private.ray_constants")
    ray_constants.LOG_MONITOR_MAX_OPEN_FILES = 200
    ray_constants.LOG_MONITOR_NUM_LINES_TO_READ = 1000
    ray_constants.LOG_PREFIX_ACTOR_NAME = ":actor_name:"
    ray_constants.LOG_PREFIX_TASK_NAME = ":task_name:"
    ray_constants.LOG_PREFIX_JOB_ID = ":job_id:"

    services_module = types.ModuleType("ray._private.services")
    utils_module = types.ModuleType("ray._private.utils")
    logging_utils_module = types.ModuleType("ray._private.logging_utils")
    ray_logging_module = types.ModuleType("ray._private.ray_logging")
    ray_logging_module.setup_component_logger = lambda *args, **kwargs: None

    raylet_module = types.ModuleType("ray._raylet")

    class GcsClient:
        pass

    raylet_module.GcsClient = GcsClient

    sys.modules["ray"] = ray_module
    sys.modules["ray._private"] = private_module
    sys.modules["ray._private.ray_constants"] = ray_constants
    sys.modules["ray._private.services"] = services_module
    sys.modules["ray._private.utils"] = utils_module
    sys.modules["ray._private.logging_utils"] = logging_utils_module
    sys.modules["ray._private.ray_logging"] = ray_logging_module
    sys.modules["ray._raylet"] = raylet_module


def load_module(reference_path: str):
    install_stubs()
    spec = importlib.util.spec_from_file_location(
        "ray._private.log_monitor_under_test", reference_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def write_atomically(path: pathlib.Path, contents: str):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(contents, encoding="utf-8")
    tmp.replace(path)


def run_scenario(module, scenario, logs_dir: pathlib.Path):
    client = RecordingGcsClient()
    monitor = module.LogMonitor(
        node_ip_address="127.0.0.1",
        logs_dir=str(logs_dir),
        gcs_client=client,
        is_proc_alive_fn=lambda pid: True,
        max_files_open=8,
        gcs_address=None,
    )

    for initial in scenario["files"]:
        target = logs_dir / initial["path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(initial["contents"], encoding="utf-8")

    for op in scenario["operations"]:
        kind = op["kind"]
        if kind == "scan":
            monitor.update_log_filenames()
        elif kind == "open":
            monitor.open_closed_files()
        elif kind == "drain":
            monitor.check_log_files_and_publish_updates()
        elif kind == "append":
            with open(logs_dir / op["path"], "a", encoding="utf-8") as fh:
                fh.write(op["contents"])
        elif kind == "replace":
            write_atomically(logs_dir / op["path"], op["contents"])
        elif kind == "truncate":
            with open(logs_dir / op["path"], "r+b") as fh:
                fh.truncate(op["size"])
        else:
            raise ValueError(f"unknown op kind: {kind}")

    return client.batches


def main():
    if len(sys.argv) != 4:
        raise SystemExit(
            "usage: python_parity_runner.py <reference_py> <scenario_json> <logs_dir>"
        )

    reference_path, scenario_json_path, logs_dir = sys.argv[1], sys.argv[2], sys.argv[3]
    module = load_module(reference_path)
    scenarios = json.loads(pathlib.Path(scenario_json_path).read_text(encoding="utf-8"))
    results = {}
    for scenario in scenarios:
        scenario_dir = pathlib.Path(logs_dir) / scenario["name"]
        scenario_dir.mkdir(parents=True, exist_ok=True)
        results[scenario["name"]] = run_scenario(module, scenario, scenario_dir)
    print(json.dumps(results, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
