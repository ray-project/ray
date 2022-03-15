import os
import pytest
import sys
import ray
import pathlib
import json
import time
import subprocess

from dataclasses import asdict
from pathlib import Path
from jsonschema import validate

import ray._private.usage.usage_lib as ray_usage_lib
import ray._private.usage.usage_constants as usage_constants

from ray._private.test_utils import wait_for_condition

schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "schema_version": {"type": "string"},
        "source": {"type": "string"},
        "session_id": {"type": "string"},
        "ray_version": {"type": "string"},
        "git_commit": {"type": "string"},
        "os": {"type": "string"},
        "python_version": {"type": "string"},
        "collect_timestamp_ms": {"type": "integer"},
        "session_start_timestamp_ms": {"type": "integer"},
        "total_success": {"type": "integer"},
        "total_failed": {"type": "integer"},
        "seq_number": {"type": "integer"},
    },
}


def file_exists(temp_dir: Path):
    for path in temp_dir.iterdir():
        if usage_constants.USAGE_STATS_FILE in str(path):
            return True
    return False


def read_file(temp_dir: Path, column: str):
    usage_stats_file = temp_dir / usage_constants.USAGE_STATS_FILE
    with usage_stats_file.open() as f:
        result = json.load(f)
        return result[column]


def print_dashboard_log():
    session_dir = ray.worker.global_worker.node.address_info["session_dir"]
    session_path = Path(session_dir)
    log_dir_path = session_path / "logs"

    paths = list(log_dir_path.iterdir())

    contents = None
    for path in paths:
        if "dashboard.log" in str(path):
            with open(str(path), "r") as f:
                contents = f.readlines()
    from pprint import pprint

    pprint(contents)


def test_usage_stats_heads_up_message():
    """
    Test usage stats heads-up message is shown in the proper cases.
    """
    env = os.environ.copy()
    env["RAY_USAGE_STATS_PROMPT_ENABLED"] = "0"
    result = subprocess.run(
        "ray start --head",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    assert result.returncode == 0
    assert usage_constants.USAGE_STATS_HEADS_UP_MESSAGE not in result.stdout.decode(
        "utf-8"
    )
    assert usage_constants.USAGE_STATS_HEADS_UP_MESSAGE not in result.stderr.decode(
        "utf-8"
    )

    subprocess.run("ray stop --force", shell=True)

    result = subprocess.run(
        "ray start --head",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert result.returncode == 0
    assert usage_constants.USAGE_STATS_HEADS_UP_MESSAGE not in result.stdout.decode(
        "utf-8"
    )
    assert usage_constants.USAGE_STATS_HEADS_UP_MESSAGE in result.stderr.decode("utf-8")

    result = subprocess.run(
        'ray start --address="127.0.0.1:6379"',
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert result.returncode == 0
    assert usage_constants.USAGE_STATS_HEADS_UP_MESSAGE not in result.stdout.decode(
        "utf-8"
    )
    assert usage_constants.USAGE_STATS_HEADS_UP_MESSAGE not in result.stderr.decode(
        "utf-8"
    )

    subprocess.run("ray stop --force", shell=True)

    result = subprocess.run(
        "ray up xxx.yml", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    assert usage_constants.USAGE_STATS_HEADS_UP_MESSAGE not in result.stdout.decode(
        "utf-8"
    )
    assert usage_constants.USAGE_STATS_HEADS_UP_MESSAGE in result.stderr.decode("utf-8")

    result = subprocess.run(
        "ray exec xxx.yml ls --start",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert usage_constants.USAGE_STATS_HEADS_UP_MESSAGE not in result.stdout.decode(
        "utf-8"
    )
    assert usage_constants.USAGE_STATS_HEADS_UP_MESSAGE in result.stderr.decode("utf-8")

    result = subprocess.run(
        "ray exec xxx.yml ls",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert usage_constants.USAGE_STATS_HEADS_UP_MESSAGE not in result.stdout.decode(
        "utf-8"
    )
    assert usage_constants.USAGE_STATS_HEADS_UP_MESSAGE not in result.stderr.decode(
        "utf-8"
    )

    result = subprocess.run(
        "ray submit xxx.yml yyy.py --start",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert usage_constants.USAGE_STATS_HEADS_UP_MESSAGE not in result.stdout.decode(
        "utf-8"
    )
    assert usage_constants.USAGE_STATS_HEADS_UP_MESSAGE in result.stderr.decode("utf-8")

    result = subprocess.run(
        "ray submit xxx.yml yyy.py",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert usage_constants.USAGE_STATS_HEADS_UP_MESSAGE not in result.stdout.decode(
        "utf-8"
    )
    assert usage_constants.USAGE_STATS_HEADS_UP_MESSAGE not in result.stderr.decode(
        "utf-8"
    )

    result = subprocess.run(
        'python -c "import ray; ray.init()"',
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert usage_constants.USAGE_STATS_HEADS_UP_MESSAGE not in result.stdout.decode(
        "utf-8"
    )
    assert usage_constants.USAGE_STATS_HEADS_UP_MESSAGE not in result.stderr.decode(
        "utf-8"
    )


def test_usage_lib_cluster_metadata_generation(monkeypatch, shutdown_only):
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        ray.init(num_cpus=0)
        """
        Test metadata stored is equivalent to `_generate_cluster_metadata`.
        """
        meta = ray_usage_lib._generate_cluster_metadata()
        cluster_metadata = ray_usage_lib.get_cluster_metadata(
            ray.experimental.internal_kv.internal_kv_get_gcs_client(), num_retries=20
        )
        # Remove fields that are dynamically changed.
        assert meta.pop("session_id")
        assert meta.pop("session_start_timestamp_ms")
        assert cluster_metadata.pop("session_id")
        assert cluster_metadata.pop("session_start_timestamp_ms")
        assert meta == cluster_metadata

        """
        Make sure put & get works properly.
        """
        cluster_metadata = ray_usage_lib.put_cluster_metadata(
            ray.experimental.internal_kv.internal_kv_get_gcs_client(), num_retries=20
        )
        assert cluster_metadata == ray_usage_lib.get_cluster_metadata(
            ray.experimental.internal_kv.internal_kv_get_gcs_client(), num_retries=20
        )


def test_usage_lib_cluster_metadata_generation_usage_disabled(shutdown_only):
    """
    Make sure only version information is generated when usage stats are not enabled.
    """
    meta = ray_usage_lib._generate_cluster_metadata()
    assert "ray_version" in meta
    assert "python_version" in meta
    assert len(meta) == 2


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Test depends on runtime env feature not supported on Windows.",
)
def test_usage_lib_report_data(monkeypatch, shutdown_only, tmp_path):
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        # Runtime env is required to run this test in minimal installation test.
        ray.init(num_cpus=0, runtime_env={"pip": ["ray[serve]"]})
        """
        Make sure the generated data is following the schema.
        """
        cluster_metadata = ray_usage_lib.get_cluster_metadata(
            ray.experimental.internal_kv.internal_kv_get_gcs_client(), num_retries=20
        )
        d = ray_usage_lib.generate_report_data(cluster_metadata, 2, 2, 2)
        validate(instance=asdict(d), schema=schema)

        """
        Make sure writing to a file works as expected
        """
        client = ray_usage_lib.UsageReportClient()
        temp_dir = Path(tmp_path)
        client._write_usage_data(d, temp_dir)

        wait_for_condition(lambda: file_exists(temp_dir))

        """
        Make sure report usage data works as expected
        """

        @ray.remote(num_cpus=0, runtime_env={"pip": ["ray[serve]"]})
        class ServeInitator:
            def __init__(self):
                # Start the ray serve server to verify requests are sent
                # to the right place.
                from ray import serve

                serve.start()

                @serve.deployment(ray_actor_options={"num_cpus": 0})
                async def usage(request):
                    body = await request.json()
                    if body == asdict(d):
                        return True
                    else:
                        return False

                usage.deploy()

            def ready(self):
                pass

        # We need to start a serve with runtime env to make this test
        # work with minimal installation.
        s = ServeInitator.remote()
        ray.get(s.ready.remote())

        # Query our endpoint over HTTP.
        r = client._report_usage_data("http://127.0.0.1:8000/usage", d)
        r.raise_for_status()
        assert json.loads(r.text) is True


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Test depends on runtime env feature not supported on Windows.",
)
def test_usage_report_e2e(monkeypatch, shutdown_only):
    """
    Test usage report works e2e with env vars.
    """
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv("RAY_USAGE_STATS_REPORT_URL", "http://127.0.0.1:8000/usage")
        m.setenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", "1")
        ray.init(num_cpus=0)

        @ray.remote(num_cpus=0)
        class StatusReporter:
            def __init__(self):
                self.reported = 0
                self.payload = None

            def report_payload(self, payload):
                self.payload = payload

            def reported(self):
                self.reported += 1

            def get(self):
                return self.reported

            def get_payload(self):
                return self.payload

        reporter = StatusReporter.remote()

        @ray.remote(num_cpus=0, runtime_env={"pip": ["ray[serve]"]})
        class ServeInitator:
            def __init__(self):
                from ray import serve

                serve.start()

                # Usage report should be sent to the URL every 1 second.
                @serve.deployment(ray_actor_options={"num_cpus": 0})
                async def usage(request):
                    body = await request.json()
                    reporter.reported.remote()
                    reporter.report_payload.remote(body)
                    return True

                usage.deploy()

            def ready(self):
                pass

        # We need to start a serve with runtime env to make this test
        # work with minimal installation.
        s = ServeInitator.remote()
        ray.get(s.ready.remote())

        """
        Verify the usage stats are reported to the server.
        """
        print("Verifying usage stats report.")
        # Since the interval is 1 second, there must have been
        # more than 5 requests sent within 30 seconds.
        try:
            wait_for_condition(lambda: ray.get(reporter.get.remote()) > 5, timeout=30)
        except Exception:
            print_dashboard_log()
            raise
        validate(instance=ray.get(reporter.get_payload.remote()), schema=schema)
        """
        Verify the usage_stats.json is updated.
        """
        print("Verifying usage stats write.")
        global_node = ray.worker._global_node
        temp_dir = pathlib.Path(global_node.get_session_dir_path())

        wait_for_condition(lambda: file_exists(temp_dir), timeout=30)

        timestamp_old = read_file(temp_dir, "usage_stats")["collect_timestamp_ms"]
        success_old = read_file(temp_dir, "usage_stats")["total_success"]
        # Test if the timestampe has been updated.
        wait_for_condition(
            lambda: timestamp_old
            < read_file(temp_dir, "usage_stats")["collect_timestamp_ms"]
        )
        wait_for_condition(
            lambda: success_old < read_file(temp_dir, "usage_stats")["total_success"]
        )
        assert read_file(temp_dir, "success")


def test_usage_report_disabled(monkeypatch, shutdown_only):
    """
    Make sure usage report module is disabled when the env var is not set.
    It also verifies that the failure message is not printed (note that
    the invalid report url is given as an env var).
    """
    with monkeypatch.context() as m:
        # It is disabled by default.
        # m.setenv("RAY_USAGE_STATS_ENABLED", "0")
        m.setenv("RAY_USAGE_STATS_REPORT_URL", "http://127.0.0.1:8000")
        m.setenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", "1")
        ray.init(num_cpus=0)
        # Wait enough so that usage report should happen.
        time.sleep(5)

        session_dir = ray.worker.global_worker.node.address_info["session_dir"]
        session_path = Path(session_dir)
        log_dir_path = session_path / "logs"

        paths = list(log_dir_path.iterdir())

        contents = None
        for path in paths:
            if "dashboard.log" in str(path):
                with open(str(path), "r") as f:
                    contents = f.readlines()
        assert contents is not None

        keyword_found = False
        for c in contents:
            if "Usage reporting is disabled" in c:
                keyword_found = True

        # Make sure the module was disabled.
        assert keyword_found

        for c in contents:
            assert "Failed to report usage stats" not in c


def test_usage_file_error_message(monkeypatch, shutdown_only):
    """
    Make sure the usage report file is generated with a proper
    error message when the report is failed.
    """
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv("RAY_USAGE_STATS_REPORT_URL", "http://127.0.0.1:8000")
        m.setenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", "1")
        ray.init(num_cpus=0)

        global_node = ray.worker._global_node
        temp_dir = pathlib.Path(global_node.get_session_dir_path())
        try:
            wait_for_condition(lambda: file_exists(temp_dir), timeout=30)
        except Exception:
            print_dashboard_log()
            raise

        error_message = read_file(temp_dir, "error")
        failure_old = read_file(temp_dir, "usage_stats")["total_failed"]
        report_success = read_file(temp_dir, "success")
        # Test if the timestampe has been updated.
        assert (
            "HTTPConnectionPool(host='127.0.0.1', port=8000): "
            "Max retries exceeded with url:"
        ) in error_message
        assert not report_success
        try:
            wait_for_condition(
                lambda: failure_old < read_file(temp_dir, "usage_stats")["total_failed"]
            )
        except Exception:
            print_dashboard_log()
            read_file(temp_dir, "usage_stats")["total_failed"]
            raise
        assert read_file(temp_dir, "usage_stats")["total_success"] == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
