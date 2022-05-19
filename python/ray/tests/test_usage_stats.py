import os
import pytest
import sys
import ray
import pathlib
import json
import time

from dataclasses import asdict
from pathlib import Path
from jsonschema import validate

import ray._private.usage.usage_lib as ray_usage_lib
import ray._private.usage.usage_constants as usage_constants
from ray._private.usage.usage_lib import ClusterConfigToReport
from ray._private.usage.usage_lib import UsageStatsEnabledness
from ray.autoscaler._private.cli_logger import cli_logger

from ray._private.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
)

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
        "cloud_provider": {"type": ["null", "string"]},
        "min_workers": {"type": ["null", "integer"]},
        "max_workers": {"type": ["null", "integer"]},
        "head_node_instance_type": {"type": ["null", "string"]},
        "worker_node_instance_types": {
            "type": ["null", "array"],
            "items": {"type": "string"},
        },
        "total_num_cpus": {"type": ["null", "integer"]},
        "total_num_gpus": {"type": ["null", "integer"]},
        "total_memory_gb": {"type": ["null", "number"]},
        "total_object_store_memory_gb": {"type": ["null", "number"]},
        "library_usages": {
            "type": ["null", "array"],
            "items": {"type": "string"},
        },
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


def test_usage_stats_enabledness(monkeypatch, tmp_path):
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        assert (
            ray_usage_lib._usage_stats_enabledness()
            is UsageStatsEnabledness.ENABLED_EXPLICITLY
        )

    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "0")
        assert (
            ray_usage_lib._usage_stats_enabledness()
            is UsageStatsEnabledness.DISABLED_EXPLICITLY
        )

    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "xxx")
        with pytest.raises(ValueError):
            ray_usage_lib._usage_stats_enabledness()

    with monkeypatch.context() as m:
        tmp_usage_stats_config_path = tmp_path / "config.json"
        monkeypatch.setenv(
            "RAY_USAGE_STATS_CONFIG_PATH", str(tmp_usage_stats_config_path)
        )
        tmp_usage_stats_config_path.write_text('{"usage_stats": true}')
        assert (
            ray_usage_lib._usage_stats_enabledness()
            is UsageStatsEnabledness.ENABLED_EXPLICITLY
        )
        tmp_usage_stats_config_path.write_text('{"usage_stats": false}')
        assert (
            ray_usage_lib._usage_stats_enabledness()
            is UsageStatsEnabledness.DISABLED_EXPLICITLY
        )
        tmp_usage_stats_config_path.write_text('{"usage_stats": "xxx"}')
        with pytest.raises(ValueError):
            ray_usage_lib._usage_stats_enabledness()
        tmp_usage_stats_config_path.write_text("")
        assert (
            ray_usage_lib._usage_stats_enabledness()
            is UsageStatsEnabledness.ENABLED_BY_DEFAULT
        )
        tmp_usage_stats_config_path.unlink()
        assert (
            ray_usage_lib._usage_stats_enabledness()
            is UsageStatsEnabledness.ENABLED_BY_DEFAULT
        )


def test_set_usage_stats_enabled_via_config(monkeypatch, tmp_path):
    tmp_usage_stats_config_path = tmp_path / "config1.json"
    monkeypatch.setenv("RAY_USAGE_STATS_CONFIG_PATH", str(tmp_usage_stats_config_path))
    ray_usage_lib.set_usage_stats_enabled_via_config(True)
    assert '{"usage_stats": true}' == tmp_usage_stats_config_path.read_text()
    ray_usage_lib.set_usage_stats_enabled_via_config(False)
    assert '{"usage_stats": false}' == tmp_usage_stats_config_path.read_text()
    tmp_usage_stats_config_path.write_text('"xxx"')
    ray_usage_lib.set_usage_stats_enabled_via_config(True)
    assert '{"usage_stats": true}' == tmp_usage_stats_config_path.read_text()
    tmp_usage_stats_config_path.unlink()
    os.makedirs(os.path.dirname(tmp_usage_stats_config_path / "xxx.txt"), exist_ok=True)
    with pytest.raises(Exception, match="Failed to enable usage stats.*"):
        ray_usage_lib.set_usage_stats_enabled_via_config(True)


def test_usage_stats_prompt(monkeypatch, capsys, tmp_path):
    """
    Test usage stats prompt is shown in the proper cases.
    """
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv("RAY_USAGE_STATS_PROMPT_ENABLED", "0")
        ray_usage_lib.show_usage_stats_prompt()
        captured = capsys.readouterr()
        assert usage_constants.USAGE_STATS_ENABLED_MESSAGE not in captured.out
        assert usage_constants.USAGE_STATS_ENABLED_MESSAGE not in captured.err

    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "0")
        ray_usage_lib.show_usage_stats_prompt()
        captured = capsys.readouterr()
        assert usage_constants.USAGE_STATS_DISABLED_MESSAGE in captured.out

    with monkeypatch.context() as m:
        m.delenv("RAY_USAGE_STATS_ENABLED", raising=False)
        tmp_usage_stats_config_path = tmp_path / "config1.json"
        monkeypatch.setenv(
            "RAY_USAGE_STATS_CONFIG_PATH", str(tmp_usage_stats_config_path)
        )
        # Usage stats collection is enabled by default.
        ray_usage_lib.show_usage_stats_prompt()
        captured = capsys.readouterr()
        assert usage_constants.USAGE_STATS_ENABLED_BY_DEFAULT_MESSAGE in captured.out

    with monkeypatch.context() as m:
        # Win impl relies on kbhit() instead of select()
        # so the pipe trick won't work.
        if sys.platform != "win32":
            m.delenv("RAY_USAGE_STATS_ENABLED", raising=False)
            saved_interactive = cli_logger.interactive
            saved_stdin = sys.stdin
            tmp_usage_stats_config_path = tmp_path / "config2.json"
            monkeypatch.setenv(
                "RAY_USAGE_STATS_CONFIG_PATH", str(tmp_usage_stats_config_path)
            )
            cli_logger.interactive = True
            (r_pipe, w_pipe) = os.pipe()
            sys.stdin = open(r_pipe)
            os.write(w_pipe, b"y\n")
            ray_usage_lib.show_usage_stats_prompt()
            captured = capsys.readouterr()
            assert usage_constants.USAGE_STATS_CONFIRMATION_MESSAGE in captured.out
            assert usage_constants.USAGE_STATS_ENABLED_MESSAGE in captured.out
            cli_logger.interactive = saved_interactive
            sys.stdin = saved_stdin

    with monkeypatch.context() as m:
        if sys.platform != "win32":
            m.delenv("RAY_USAGE_STATS_ENABLED", raising=False)
            saved_interactive = cli_logger.interactive
            saved_stdin = sys.stdin
            tmp_usage_stats_config_path = tmp_path / "config3.json"
            monkeypatch.setenv(
                "RAY_USAGE_STATS_CONFIG_PATH", str(tmp_usage_stats_config_path)
            )
            cli_logger.interactive = True
            (r_pipe, w_pipe) = os.pipe()
            sys.stdin = open(r_pipe)
            os.write(w_pipe, b"n\n")
            ray_usage_lib.show_usage_stats_prompt()
            captured = capsys.readouterr()
            assert usage_constants.USAGE_STATS_CONFIRMATION_MESSAGE in captured.out
            assert usage_constants.USAGE_STATS_DISABLED_MESSAGE in captured.out
            cli_logger.interactive = saved_interactive
            sys.stdin = saved_stdin

    with monkeypatch.context() as m:
        m.delenv("RAY_USAGE_STATS_ENABLED", raising=False)
        saved_interactive = cli_logger.interactive
        saved_stdin = sys.stdin
        tmp_usage_stats_config_path = tmp_path / "config4.json"
        monkeypatch.setenv(
            "RAY_USAGE_STATS_CONFIG_PATH", str(tmp_usage_stats_config_path)
        )
        cli_logger.interactive = True
        (r_pipe, w_pipe) = os.pipe()
        sys.stdin = open(r_pipe)
        ray_usage_lib.show_usage_stats_prompt()
        captured = capsys.readouterr()
        assert usage_constants.USAGE_STATS_CONFIRMATION_MESSAGE in captured.out
        assert usage_constants.USAGE_STATS_ENABLED_MESSAGE in captured.out
        cli_logger.interactive = saved_interactive
        sys.stdin = saved_stdin

    with monkeypatch.context() as m:
        # Usage stats is not enabled for ray.init()
        ray.init()
        ray.shutdown()
        captured = capsys.readouterr()
        assert (
            usage_constants.USAGE_STATS_ENABLED_BY_DEFAULT_MESSAGE not in captured.out
        )
        assert (
            usage_constants.USAGE_STATS_ENABLED_BY_DEFAULT_MESSAGE not in captured.err
        )


def test_usage_lib_cluster_metadata_generation(monkeypatch, ray_start_cluster):
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv("RAY_USAGE_STATS_REPORT_URL", "http://127.0.0.1:8000")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=0)
        ray.init(address=cluster.address)
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


def test_usage_stats_enabled_endpoint(monkeypatch, ray_start_cluster):
    if os.environ.get("RAY_MINIMAL") == "1":
        # Doesn't work with minimal installation
        # since we need http server.
        return

    import requests

    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "0")
        m.setenv("RAY_USAGE_STATS_PROMPT_ENABLED", "0")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=0)
        context = ray.init(address=cluster.address)
        webui_url = context["webui_url"]
        assert wait_until_server_available(webui_url)
        webui_url = format_web_url(webui_url)
        response = requests.get(f"{webui_url}/usage_stats_enabled")
        assert response.status_code == 200
        assert response.json()["result"] is True
        assert response.json()["data"]["usageStatsEnabled"] is False
        assert response.json()["data"]["usageStatsPromptEnabled"] is False


def test_library_usages():
    if os.environ.get("RAY_MINIMAL") == "1":
        # Doesn't work with minimal installation
        # since we import serve.
        return

    ray_usage_lib._recorded_library_usages.clear()
    ray_usage_lib.record_library_usage("pre_init")
    ray.init()
    ray_usage_lib.record_library_usage("post_init")
    ray.workflow.init()
    ray.data.range(10)
    from ray import serve

    serve.start()
    library_usages = ray_usage_lib.get_library_usages_to_report(
        ray.experimental.internal_kv.internal_kv_get_gcs_client(), num_retries=20
    )
    assert set(library_usages) == {
        "pre_init",
        "post_init",
        "dataset",
        "workflow",
        "serve",
    }
    serve.shutdown()
    ray.shutdown()


def test_usage_lib_cluster_metadata_generation_usage_disabled(
    monkeypatch, shutdown_only
):
    """
    Make sure only version information is generated when usage stats are not enabled.
    """
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "0")
        meta = ray_usage_lib._generate_cluster_metadata()
        assert "ray_version" in meta
        assert "python_version" in meta
        assert len(meta) == 2


def test_usage_lib_get_cluster_status_to_report(shutdown_only):
    ray.init(num_cpus=3, num_gpus=1, object_store_memory=2 ** 30)
    # Wait for monitor.py to update cluster status
    wait_for_condition(
        lambda: ray_usage_lib.get_cluster_status_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client(),
            num_retries=20,
        ).total_num_cpus
        == 3,
        timeout=10,
    )
    cluster_status_to_report = ray_usage_lib.get_cluster_status_to_report(
        ray.experimental.internal_kv.internal_kv_get_gcs_client(),
        num_retries=20,
    )
    assert cluster_status_to_report.total_num_cpus == 3
    assert cluster_status_to_report.total_num_gpus == 1
    assert cluster_status_to_report.total_memory_gb > 0
    assert cluster_status_to_report.total_object_store_memory_gb == 1.0


def test_usage_lib_get_cluster_config_to_report(monkeypatch, tmp_path):
    cluster_config_file_path = tmp_path / "ray_bootstrap_config.yaml"
    """ Test minimal cluster config"""
    cluster_config_file_path.write_text(
        """
cluster_name: minimal
max_workers: 1
provider:
    type: aws
    region: us-west-2
    availability_zone: us-west-2a
"""
    )
    cluster_config_to_report = ray_usage_lib.get_cluster_config_to_report(
        cluster_config_file_path
    )
    assert cluster_config_to_report.cloud_provider == "aws"
    assert cluster_config_to_report.min_workers is None
    assert cluster_config_to_report.max_workers == 1
    assert cluster_config_to_report.head_node_instance_type is None
    assert cluster_config_to_report.worker_node_instance_types is None

    cluster_config_file_path.write_text(
        """
cluster_name: full
min_workers: 1
provider:
    type: gcp
head_node_type: head_node
available_node_types:
    head_node:
        node_config:
            InstanceType: m5.large
        min_workers: 0
        max_workers: 0
    aws_worker_node:
        node_config:
            InstanceType: m3.large
        min_workers: 0
        max_workers: 0
    azure_worker_node:
        node_config:
            azure_arm_parameters:
                vmSize: Standard_D2s_v3
    gcp_worker_node:
        node_config:
            machineType: n1-standard-2
"""
    )
    cluster_config_to_report = ray_usage_lib.get_cluster_config_to_report(
        cluster_config_file_path
    )
    assert cluster_config_to_report.cloud_provider == "gcp"
    assert cluster_config_to_report.min_workers == 1
    assert cluster_config_to_report.max_workers is None
    assert cluster_config_to_report.head_node_instance_type == "m5.large"
    assert cluster_config_to_report.worker_node_instance_types == list(
        {"m3.large", "Standard_D2s_v3", "n1-standard-2"}
    )

    cluster_config_file_path.write_text(
        """
cluster_name: full
head_node_type: head_node
available_node_types:
    worker_node_1:
        node_config:
            ImageId: xyz
    worker_node_2:
        resources: {}
    worker_node_3:
        node_config:
            InstanceType: m5.large
"""
    )
    cluster_config_to_report = ray_usage_lib.get_cluster_config_to_report(
        cluster_config_file_path
    )
    assert cluster_config_to_report.cloud_provider is None
    assert cluster_config_to_report.min_workers is None
    assert cluster_config_to_report.max_workers is None
    assert cluster_config_to_report.head_node_instance_type is None
    assert cluster_config_to_report.worker_node_instance_types == ["m5.large"]

    cluster_config_file_path.write_text("[invalid")
    cluster_config_to_report = ray_usage_lib.get_cluster_config_to_report(
        cluster_config_file_path
    )
    assert cluster_config_to_report == ClusterConfigToReport()

    cluster_config_to_report = ray_usage_lib.get_cluster_config_to_report(
        tmp_path / "does_not_exist.yaml"
    )
    assert cluster_config_to_report == ClusterConfigToReport()

    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "localhost")
    cluster_config_to_report = ray_usage_lib.get_cluster_config_to_report(
        tmp_path / "does_not_exist.yaml"
    )
    assert cluster_config_to_report.cloud_provider == "kubernetes"
    assert cluster_config_to_report.min_workers is None
    assert cluster_config_to_report.max_workers is None
    assert cluster_config_to_report.head_node_instance_type is None
    assert cluster_config_to_report.worker_node_instance_types is None


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Test depends on runtime env feature not supported on Windows.",
)
def test_usage_lib_report_data(monkeypatch, ray_start_cluster, tmp_path):
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv("RAY_USAGE_STATS_REPORT_URL", "http://127.0.0.1:8000")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=0)
        # Runtime env is required to run this test in minimal installation test.
        ray.init(address=cluster.address, runtime_env={"pip": ["ray[serve]"]})
        """
        Make sure the generated data is following the schema.
        """
        cluster_metadata = ray_usage_lib.get_cluster_metadata(
            ray.experimental.internal_kv.internal_kv_get_gcs_client(), num_retries=20
        )
        cluster_config_file_path = tmp_path / "ray_bootstrap_config.yaml"
        cluster_config_file_path.write_text(
            """
cluster_name: minimal
max_workers: 1
provider:
    type: aws
    region: us-west-2
    availability_zone: us-west-2a
"""
        )
        cluster_config_to_report = ray_usage_lib.get_cluster_config_to_report(
            cluster_config_file_path
        )
        d = ray_usage_lib.generate_report_data(
            cluster_metadata, cluster_config_to_report, 2, 2, 2
        )
        validate(instance=asdict(d), schema=schema)

        """
        Make sure writing to a file works as expected
        """
        client = ray_usage_lib.UsageReportClient()
        temp_dir = Path(tmp_path)
        client.write_usage_data(d, temp_dir)

        wait_for_condition(lambda: file_exists(temp_dir))

        """
        Make sure report usage data works as expected
        """

        @ray.remote(num_cpus=0)
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
        r = client.report_usage_data("http://127.0.0.1:8000/usage", d)
        r.raise_for_status()
        assert json.loads(r.text) is True


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Test depends on runtime env feature not supported on Windows.",
)
def test_usage_report_e2e(monkeypatch, ray_start_cluster, tmp_path):
    """
    Test usage report works e2e with env vars.
    """
    cluster_config_file_path = tmp_path / "ray_bootstrap_config.yaml"
    cluster_config_file_path.write_text(
        """
cluster_name: minimal
max_workers: 1
provider:
    type: aws
    region: us-west-2
    availability_zone: us-west-2a
"""
    )
    with monkeypatch.context() as m:
        m.setenv("HOME", str(tmp_path))
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv("RAY_USAGE_STATS_REPORT_URL", "http://127.0.0.1:8000/usage")
        m.setenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", "1")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=3)
        ray_usage_lib._recorded_library_usages.clear()
        if os.environ.get("RAY_MINIMAL") != "1":
            from ray import tune  # noqa: F401
            from ray.rllib.agents.ppo import PPOTrainer  # noqa: F401
            from ray import train  # noqa: F401

        ray.init(address=cluster.address)

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
                # This is used in the worker process
                # so it won't be tracked as library usage.
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
        payload = ray.get(reporter.get_payload.remote())
        ray_version, python_version = ray._private.utils.compute_version_info()
        assert payload["ray_version"] == ray_version
        assert payload["python_version"] == python_version
        assert payload["schema_version"] == "0.1"
        assert payload["os"] == sys.platform
        assert payload["source"] == "OSS"
        assert payload["cloud_provider"] == "aws"
        assert payload["min_workers"] is None
        assert payload["max_workers"] == 1
        assert payload["head_node_instance_type"] is None
        assert payload["worker_node_instance_types"] is None
        assert payload["total_num_cpus"] == 3
        assert payload["total_num_gpus"] is None
        assert payload["total_memory_gb"] > 0
        assert payload["total_object_store_memory_gb"] > 0
        if os.environ.get("RAY_MINIMAL") == "1":
            assert set(payload["library_usages"]) == set()
        else:
            assert set(payload["library_usages"]) == {"rllib", "train", "tune"}
        validate(instance=payload, schema=schema)
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


def test_first_usage_report_delayed(monkeypatch, ray_start_cluster):
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv("RAY_USAGE_STATS_REPORT_URL", "http://127.0.0.1:8000")
        m.setenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", "10")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=0)
        ray.init(address=cluster.address)

        # The first report should be delayed for 10s.
        time.sleep(5)
        session_dir = ray.worker.global_worker.node.address_info["session_dir"]
        session_path = Path(session_dir)
        assert not (session_path / usage_constants.USAGE_STATS_FILE).exists()

        time.sleep(10)
        assert (session_path / usage_constants.USAGE_STATS_FILE).exists()


def test_usage_report_disabled(monkeypatch, ray_start_cluster):
    """
    Make sure usage report module is disabled when the env var is not set.
    It also verifies that the failure message is not printed (note that
    the invalid report url is given as an env var).
    """
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "0")
        m.setenv("RAY_USAGE_STATS_REPORT_URL", "http://127.0.0.1:8000")
        m.setenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", "1")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=0)
        ray.init(address=cluster.address)
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


def test_usage_file_error_message(monkeypatch, ray_start_cluster):
    """
    Make sure the usage report file is generated with a proper
    error message when the report is failed.
    """
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv("RAY_USAGE_STATS_REPORT_URL", "http://127.0.0.1:8000")
        m.setenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", "1")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=0)
        ray.init(address=cluster.address)

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
