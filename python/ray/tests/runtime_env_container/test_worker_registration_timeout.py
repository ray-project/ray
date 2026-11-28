"""Exercise Raylet's registration timeout with a real Podman worker."""

import argparse
import json
import subprocess
import tempfile
from pathlib import Path

import ray
from ray._private.test_utils import wait_for_condition

SITECUSTOMIZE = """
import json, os, subprocess, sys, time
from pathlib import Path

if sys.argv[0].endswith("default_worker.py"):
    child = subprocess.Popen(["sleep", "600"], start_new_session=True)
    marker = Path(os.environ["RAY_TEST_STARTUP_MARKERS"]) / (
        os.environ["RAY_WORKER_CONTAINER_LAUNCH_ID"] + ".json"
    )
    temporary = marker.with_suffix(".tmp")
    temporary.write_text(json.dumps([os.getpid(), child.pid]))
    temporary.replace(marker)
    time.sleep(600)
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    parser.add_argument("--use-image-uri-api", action="store_true")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="ray-timeout-") as directory:
        shared = Path(directory)
        shared.chmod(0o777)
        (shared / "sitecustomize.py").write_text(SITECUSTOMIZE)
        ray.init(
            num_cpus=1,
            _temp_dir=directory,
            _system_config={"worker_register_timeout_seconds": 10},
        )
        context = ray.get_runtime_context()
        query = [
            "podman",
            "ps",
            "--all",
            "--quiet",
            "--filter",
            "label=ray.worker_container_session=" + context.get_node_id(),
            "--filter",
            "label=ray.job_id=" + context.get_job_id(),
        ]

        def containers():
            return subprocess.check_output(query, timeout=30).decode().split()

        @ray.remote(max_retries=0)
        def ping():
            return "ok"

        task = None
        try:
            assert ray.get(ping.remote(), timeout=30) == "ok"
            runtime_env = {
                "env_vars": {
                    "PYTHONPATH": directory,
                    "RAY_TEST_STARTUP_MARKERS": directory,
                },
            }
            if args.use_image_uri_api:
                runtime_env["image_uri"] = args.image
            else:
                runtime_env["container"] = {"image": args.image}
            task = ping.options(runtime_env=runtime_env).remote()
            wait_for_condition(lambda: bool(list(shared.glob("*.json"))), timeout=60)
            assert containers(), "The blocked worker must still have a container"

            logs = Path(ray._private.worker._global_node.get_logs_dir_path())

            def registration_timed_out():
                return any(
                    "have not registered within the timeout" in path.read_text()
                    for path in logs.glob("raylet.*")
                )

            # Observe Raylet's timeout before cancellation, shutdown, or test cleanup.
            wait_for_condition(registration_timed_out, timeout=60)
            ray.cancel(task, force=True)
            task = None

            def cleaned_up():
                pids = [
                    pid
                    for marker in shared.glob("*.json")
                    for pid in json.loads(marker.read_text())
                ]
                return not containers() and all(
                    not Path(f"/proc/{pid}").exists() for pid in pids
                )

            wait_for_condition(cleaned_up, timeout=60)
            # Cleanup completes while the node can still run ordinary workers.
            assert ray.get(ping.remote(), timeout=30) == "ok"
        finally:
            try:
                if task is not None:
                    ray.cancel(task, force=True)
            finally:
                ray.shutdown()
                remaining = containers()
                if remaining:
                    subprocess.run(
                        [
                            "podman",
                            "rm",
                            "--force",
                            "--ignore",
                            "--volumes",
                            *remaining,
                        ],
                        check=True,
                        timeout=30,
                    )


if __name__ == "__main__":
    main()
