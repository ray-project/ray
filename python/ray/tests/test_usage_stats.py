import json
import os
import pathlib
import sys
import time
from dataclasses import asdict
from pathlib import Path

import requests
import pytest
from jsonschema import validate

import ray
import ray._private.usage.usage_constants as usage_constants
import ray._private.usage.usage_lib as ray_usage_lib
from ray._private import gcs_utils
from ray._private.test_utils import (
    format_web_url,
    run_string_as_driver,
    wait_for_condition,
    wait_until_server_available,
)
from ray._private.usage.usage_lib import ClusterConfigToReport, UsageStatsEnabledness
from ray.autoscaler._private.cli_logger import cli_logger
from ray.util.placement_group import (
    placement_group,
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
        "extra_usage_tags": {"type": ["null", "object"]},
        "total_num_nodes": {"type": ["null", "integer"]},
        "total_num_running_jobs": {"type": ["null", "integer"]},
    },
    "additionalProperties": False,
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
    session_dir = ray._private.worker.global_worker.node.address_info["session_dir"]
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


@pytest.fixture
def gcs_storage_type():
    storage = "redis" if os.environ.get("RAY_REDIS_ADDRESS") else "memory"
    yield storage


@pytest.fixture
def reset_usage_stats():
    yield
    # Remove the lib usage so that it will be reset for each test.
    ray_usage_lib.LibUsageRecorder(
        ray._private.utils.get_ray_temp_dir()
    ).delete_lib_usages()
    ray.experimental.internal_kv._internal_kv_reset()
    ray_usage_lib._recorded_library_usages.clear()
    ray_usage_lib._recorded_extra_usage_tags.clear()


@pytest.fixture
def reset_ray_version_commit():
    saved_ray_version = ray.__version__
    saved_ray_commit = ray.__commit__
    yield
    ray.__version__ = saved_ray_version
    ray.__commit__ = saved_ray_commit


@pytest.mark.parametrize("ray_client", [True, False])
def test_get_extra_usage_tags_to_report(
    monkeypatch, call_ray_start, reset_usage_stats, ray_client, gcs_storage_type
):
    with monkeypatch.context() as m:
        # Test a normal case.
        m.setenv("RAY_USAGE_STATS_EXTRA_TAGS", "key=val;key2=val2")
        result = ray_usage_lib.get_extra_usage_tags_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
        assert result["key"] == "val"
        assert result["key2"] == "val2"

        m.setenv("RAY_USAGE_STATS_EXTRA_TAGS", "key=val;key2=val2;")
        result = ray_usage_lib.get_extra_usage_tags_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
        assert result["key"] == "val"
        assert result["key2"] == "val2"

        # Test that the env var is not given.
        m.delenv("RAY_USAGE_STATS_EXTRA_TAGS")
        result = ray_usage_lib.get_extra_usage_tags_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
        assert result == {}

        # Test the parsing failure.
        m.setenv("RAY_USAGE_STATS_EXTRA_TAGS", "key=val,key2=val2")
        result = ray_usage_lib.get_extra_usage_tags_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
        assert result == {}

        # Test differnt types of parsing failures.
        m.setenv("RAY_USAGE_STATS_EXTRA_TAGS", "key=v=al,key2=val2")
        result = ray_usage_lib.get_extra_usage_tags_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
        assert result == {}

        address = call_ray_start
        ray.init(address=address)
        m.setenv("RAY_USAGE_STATS_EXTRA_TAGS", "key=val")
        driver = """
import ray
import ray._private.usage.usage_lib as ray_usage_lib

ray_usage_lib.record_extra_usage_tag(ray_usage_lib.TagKey._TEST1, "val1")
ray.init(address="{}")
ray_usage_lib.record_extra_usage_tag(ray_usage_lib.TagKey._TEST2, "val2")
""".format(
            "ray://127.0.0.1:10001" if ray_client else address
        )
        run_string_as_driver(driver)
        result = ray_usage_lib.get_extra_usage_tags_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
        assert result == {
            "key": "val",
            "_test1": "val1",
            "_test2": "val2",
            "actor_num_created": "0",
            "pg_num_created": "0",
            "gcs_storage": gcs_storage_type,
            "dashboard_used": "False",
        }
        # Make sure the value is overwritten.
        ray_usage_lib.record_extra_usage_tag(ray_usage_lib.TagKey._TEST2, "val3")
        result = ray_usage_lib.get_extra_usage_tags_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
        assert result == {
            "key": "val",
            "_test1": "val1",
            "_test2": "val3",
            "gcs_storage": gcs_storage_type,
            "dashboard_used": "False",
            "actor_num_created": "0",
            "pg_num_created": "0",
        }


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "linux2",
    reason="memory monitor only on linux currently",
)
def test_worker_crash_increment_stats():
    @ray.remote
    def crasher():
        exit(1)

    @ray.remote
    def oomer():
        mem = []
        while True:
            mem.append([0] * 1000000000)

    with ray.init() as ctx:
        with pytest.raises(ray.exceptions.WorkerCrashedError):
            ray.get(crasher.options(max_retries=1).remote())

        with pytest.raises(ray.exceptions.OutOfMemoryError):
            ray.get(oomer.options(max_retries=0).remote())

        gcs_client = gcs_utils.GcsClient(address=ctx.address_info["gcs_address"])
        wait_for_condition(
            lambda: "worker_crash_system_error"
            in ray_usage_lib.get_extra_usage_tags_to_report(gcs_client),
            timeout=4,
        )

        result = ray_usage_lib.get_extra_usage_tags_to_report(gcs_client)

        assert "worker_crash_system_error" in result
        assert result["worker_crash_system_error"] == "2"

        assert "worker_crash_oom" in result
        assert result["worker_crash_oom"] == "1"


def test_actor_stats():
    @ray.remote
    class Actor:
        pass

    with ray.init(
        _system_config={"metrics_report_interval_ms": 1000},
    ) as ctx:
        gcs_client = gcs_utils.GcsClient(address=ctx.address_info["gcs_address"])

        actor = Actor.remote()
        wait_for_condition(
            lambda: ray_usage_lib.get_extra_usage_tags_to_report(gcs_client).get(
                "actor_num_created"
            )
            == "1",
            timeout=5,
        )
        actor = Actor.remote()
        wait_for_condition(
            lambda: ray_usage_lib.get_extra_usage_tags_to_report(gcs_client).get(
                "actor_num_created"
            )
            == "2",
            timeout=5,
        )
        del actor


def test_pg_stats():
    with ray.init(
        num_cpus=3,
        _system_config={"metrics_report_interval_ms": 1000},
    ) as ctx:
        gcs_client = gcs_utils.GcsClient(address=ctx.address_info["gcs_address"])

        pg = placement_group([{"CPU": 1}], strategy="STRICT_PACK")
        ray.get(pg.ready())
        wait_for_condition(
            lambda: ray_usage_lib.get_extra_usage_tags_to_report(gcs_client).get(
                "pg_num_created"
            )
            == "1",
            timeout=5,
        )
        pg1 = placement_group([{"CPU": 1}], strategy="STRICT_PACK")
        ray.get(pg1.ready())
        wait_for_condition(
            lambda: ray_usage_lib.get_extra_usage_tags_to_report(gcs_client).get(
                "pg_num_created"
            )
            == "2",
            timeout=5,
        )


def test_usage_stats_enabledness(monkeypatch, tmp_path, reset_usage_stats):
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
        m.delenv("RAY_USAGE_STATS_ENABLED", raising=False)
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


def test_set_usage_stats_enabled_via_config(monkeypatch, tmp_path, reset_usage_stats):
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


def test_lib_usage_recorder(tmp_path):
    recorder = ray_usage_lib.LibUsageRecorder(tmp_path)
    lib_tune = "tune"
    lib_rllib = "rllib"

    filename = recorder._lib_usage_filename(lib_tune)
    assert recorder._get_lib_usage_from_filename(filename) == lib_tune

    # Write tune.
    assert recorder.read_lib_usages() == []
    recorder.put_lib_usage(lib_tune)
    assert recorder.read_lib_usages() == [lib_tune]
    recorder.put_lib_usage(lib_tune)
    assert recorder.read_lib_usages() == [lib_tune]

    # Test write is idempotent
    for _ in range(5):
        recorder.put_lib_usage(lib_tune)
    assert recorder.read_lib_usages() == [lib_tune]

    # Write rllib.
    recorder.put_lib_usage(lib_rllib)
    assert set(recorder.read_lib_usages()) == {lib_tune, lib_rllib}

    # Test idempotency when there is more than 1 lib.
    recorder.put_lib_usage(lib_rllib)
    recorder.put_lib_usage(lib_rllib)
    recorder.put_lib_usage(lib_tune)
    assert set(recorder.read_lib_usages()) == {lib_tune, lib_rllib}


@pytest.fixture
def clear_loggers():
    """Remove handlers from all loggers"""
    yield
    import logging

    loggers = [logging.getLogger()] + list(logging.Logger.manager.loggerDict.values())
    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)


# NOTE: We are clearing loggers because otherwise, the next test's
# logger will access the capsys buffer that's already closed when this
# test is terminated. It seems like loggers are shared across drivers
# although we call ray.shutdown().
def test_usage_stats_prompt(
    monkeypatch,
    capsys,
    tmp_path,
    reset_usage_stats,
    shutdown_only,
    clear_loggers,
    reset_ray_version_commit,
):
    """
    Test usage stats prompt is shown in the proper cases.
    """
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv("RAY_USAGE_STATS_PROMPT_ENABLED", "0")
        ray_usage_lib.show_usage_stats_prompt(cli=True)
        captured = capsys.readouterr()
        assert usage_constants.USAGE_STATS_ENABLED_FOR_CLI_MESSAGE not in captured.out
        assert usage_constants.USAGE_STATS_ENABLED_FOR_CLI_MESSAGE not in captured.err

    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv("RAY_USAGE_STATS_PROMPT_ENABLED", "0")
        ray_usage_lib.show_usage_stats_prompt(cli=False)
        captured = capsys.readouterr()
        assert (
            usage_constants.USAGE_STATS_ENABLED_FOR_RAY_INIT_MESSAGE not in captured.out
        )
        assert (
            usage_constants.USAGE_STATS_ENABLED_FOR_RAY_INIT_MESSAGE not in captured.err
        )

    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "0")
        ray_usage_lib.show_usage_stats_prompt(cli=True)
        captured = capsys.readouterr()
        assert usage_constants.USAGE_STATS_DISABLED_MESSAGE in captured.out

    with monkeypatch.context() as m:
        m.delenv("RAY_USAGE_STATS_ENABLED", raising=False)
        tmp_usage_stats_config_path = tmp_path / "config1.json"
        m.setenv("RAY_USAGE_STATS_CONFIG_PATH", str(tmp_usage_stats_config_path))
        # Usage stats collection is enabled by default.
        ray_usage_lib.show_usage_stats_prompt(cli=True)
        captured = capsys.readouterr()
        assert (
            usage_constants.USAGE_STATS_ENABLED_BY_DEFAULT_FOR_CLI_MESSAGE
            in captured.out
        )

    with monkeypatch.context() as m:
        # Win impl relies on kbhit() instead of select()
        # so the pipe trick won't work.
        if sys.platform != "win32":
            m.delenv("RAY_USAGE_STATS_ENABLED", raising=False)
            saved_interactive = cli_logger.interactive
            saved_stdin = sys.stdin
            tmp_usage_stats_config_path = tmp_path / "config2.json"
            m.setenv("RAY_USAGE_STATS_CONFIG_PATH", str(tmp_usage_stats_config_path))
            cli_logger.interactive = True
            (r_pipe, w_pipe) = os.pipe()
            sys.stdin = open(r_pipe)
            os.write(w_pipe, b"y\n")
            ray_usage_lib.show_usage_stats_prompt(cli=True)
            captured = capsys.readouterr()
            assert usage_constants.USAGE_STATS_CONFIRMATION_MESSAGE in captured.out
            assert usage_constants.USAGE_STATS_ENABLED_FOR_CLI_MESSAGE in captured.out
            cli_logger.interactive = saved_interactive
            sys.stdin = saved_stdin

    with monkeypatch.context() as m:
        if sys.platform != "win32":
            m.delenv("RAY_USAGE_STATS_ENABLED", raising=False)
            saved_interactive = cli_logger.interactive
            saved_stdin = sys.stdin
            tmp_usage_stats_config_path = tmp_path / "config3.json"
            m.setenv("RAY_USAGE_STATS_CONFIG_PATH", str(tmp_usage_stats_config_path))
            cli_logger.interactive = True
            (r_pipe, w_pipe) = os.pipe()
            sys.stdin = open(r_pipe)
            os.write(w_pipe, b"n\n")
            ray_usage_lib.show_usage_stats_prompt(cli=True)
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
        m.setenv("RAY_USAGE_STATS_CONFIG_PATH", str(tmp_usage_stats_config_path))
        cli_logger.interactive = True
        (r_pipe, w_pipe) = os.pipe()
        sys.stdin = open(r_pipe)
        ray_usage_lib.show_usage_stats_prompt(cli=True)
        captured = capsys.readouterr()
        assert usage_constants.USAGE_STATS_CONFIRMATION_MESSAGE in captured.out
        assert usage_constants.USAGE_STATS_ENABLED_FOR_CLI_MESSAGE in captured.out
        cli_logger.interactive = saved_interactive
        sys.stdin = saved_stdin

    with monkeypatch.context() as m:
        # Usage stats is not enabled for ray.init() unless it's nightly wheel.
        m.delenv("RAY_USAGE_STATS_ENABLED", raising=False)
        tmp_usage_stats_config_path = tmp_path / "config5.json"
        m.setenv("RAY_USAGE_STATS_CONFIG_PATH", str(tmp_usage_stats_config_path))
        ray.__version__ = "2.0.0"
        ray.__commit__ = "xyzf"
        ray.init()
        ray.shutdown()
        captured = capsys.readouterr()
        assert (
            usage_constants.USAGE_STATS_ENABLED_BY_DEFAULT_FOR_RAY_INIT_MESSAGE
            not in captured.out
        )
        assert (
            usage_constants.USAGE_STATS_ENABLED_FOR_RAY_INIT_MESSAGE not in captured.out
        )

    with monkeypatch.context() as m:
        # Usage stats is enabled for ray.init() for nightly wheel.
        m.delenv("RAY_USAGE_STATS_ENABLED", raising=False)
        tmp_usage_stats_config_path = tmp_path / "config6.json"
        m.setenv("RAY_USAGE_STATS_CONFIG_PATH", str(tmp_usage_stats_config_path))
        ray.__version__ = "2.0.0.dev0"
        ray.__commit__ = "xyzf"
        ray.init()
        ray.shutdown()
        captured = capsys.readouterr()
        assert (
            usage_constants.USAGE_STATS_ENABLED_BY_DEFAULT_FOR_RAY_INIT_MESSAGE
            in captured.out
        )

    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "0")
        ray.__version__ = "2.0.0.dev0"
        ray.__commit__ = "xyzf"
        ray.init()
        ray.shutdown()
        captured = capsys.readouterr()
        assert usage_constants.USAGE_STATS_DISABLED_MESSAGE in captured.out

    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        ray.__version__ = "2.0.0.dev0"
        ray.__commit__ = "xyzf"
        ray.init()
        ray.shutdown()
        captured = capsys.readouterr()
        assert usage_constants.USAGE_STATS_ENABLED_FOR_RAY_INIT_MESSAGE in captured.out


def test_is_nightly_wheel(reset_ray_version_commit):
    ray.__version__ = "2.0.0"
    ray.__commit__ = "xyz"
    assert not ray_usage_lib.is_nightly_wheel()

    ray.__version__ = "2.0.0dev0"
    ray.__commit__ = "{{RAY_COMMIT_SHA}}"
    assert not ray_usage_lib.is_nightly_wheel()

    ray.__version__ = "2.0.0dev0"
    ray.__commit__ = "xyz"
    assert ray_usage_lib.is_nightly_wheel()


def test_usage_lib_cluster_metadata_generation(
    monkeypatch, ray_start_cluster, reset_usage_stats
):
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
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
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
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
        assert cluster_metadata == ray_usage_lib.get_cluster_metadata(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
def test_usage_stats_enabled_endpoint(
    monkeypatch, ray_start_cluster, reset_usage_stats
):
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


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation "
    "since we import serve.",
)
@pytest.mark.parametrize("ray_client", [True, False])
def test_library_usages(call_ray_start, reset_usage_stats, ray_client):
    address = call_ray_start
    ray.init(address=address)

    driver = """
import ray
import ray._private.usage.usage_lib as ray_usage_lib

ray_usage_lib.record_library_usage("pre_init")
ray.init(address="{}")

ray_usage_lib.record_library_usage("post_init")
ray.workflow.init()
ray.data.range(10)
from ray import serve

serve.start()
serve.shutdown()

class Actor:
    def get_actor_metadata(self):
        return "metadata"

from ray.util.actor_group import ActorGroup
actor_group = ActorGroup(Actor)

actor_pool = ray.util.actor_pool.ActorPool([])

from ray.util.multiprocessing import Pool
pool = Pool()

from ray.util.queue import Queue
queue = Queue()

import joblib
from ray.util.joblib import register_ray
register_ray()
with joblib.parallel_backend("ray"):
    pass
""".format(
        "ray://127.0.0.1:10001" if ray_client else address
    )
    run_string_as_driver(driver)

    job_submission_client = ray.job_submission.JobSubmissionClient(
        "http://127.0.0.1:8265"
    )
    job_id = job_submission_client.submit_job(entrypoint="ls")
    wait_for_condition(
        lambda: job_submission_client.get_job_status(job_id)
        == ray.job_submission.JobStatus.SUCCEEDED
    )

    library_usages = ray_usage_lib.get_library_usages_to_report(
        ray.experimental.internal_kv.internal_kv_get_gcs_client()
    )
    tmp_path = ray._private.utils.get_ray_temp_dir()
    lib_usages_from_home_folder = ray_usage_lib.LibUsageRecorder(
        tmp_path
    ).read_lib_usages()
    expected = {
        "pre_init",
        "post_init",
        "dataset",
        "workflow",
        "serve",
        "util.ActorGroup",
        "util.ActorPool",
        "util.multiprocessing.Pool",
        "util.Queue",
        "util.joblib",
        "job_submission",
    }
    if ray_client:
        expected.add("client")
    assert set(library_usages) == expected
    if not ray_client:
        assert set(lib_usages_from_home_folder) == expected


def test_usage_lib_cluster_metadata_generation_usage_disabled(
    monkeypatch, shutdown_only, reset_usage_stats
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


def test_usage_lib_get_total_num_running_jobs_to_report(
    ray_start_cluster, reset_usage_stats
):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1)
    gcs_client = gcs_utils.GcsClient(address=cluster.gcs_address)
    assert ray_usage_lib.get_total_num_running_jobs_to_report(gcs_client) == 0

    ray.init(address=cluster.address)
    assert ray_usage_lib.get_total_num_running_jobs_to_report(gcs_client) == 1
    ray.shutdown()

    ray.init(address=cluster.address)
    # Make sure the previously finished job is not counted.
    assert ray_usage_lib.get_total_num_running_jobs_to_report(gcs_client) == 1
    ray.shutdown()


def test_usage_lib_get_total_num_nodes_to_report(ray_start_cluster, reset_usage_stats):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(num_cpus=2)
    assert (
        ray_usage_lib.get_total_num_nodes_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
        == 2
    )
    cluster.remove_node(worker_node)
    # Make sure only alive nodes are counted
    assert (
        ray_usage_lib.get_total_num_nodes_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
        == 1
    )


def test_usage_lib_get_cluster_status_to_report(shutdown_only, reset_usage_stats):
    ray.init(num_cpus=3, num_gpus=1, object_store_memory=2**30)
    # Wait for monitor.py to update cluster status
    wait_for_condition(
        lambda: ray_usage_lib.get_cluster_status_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        ).total_num_cpus
        == 3,
        timeout=10,
    )
    cluster_status_to_report = ray_usage_lib.get_cluster_status_to_report(
        ray.experimental.internal_kv.internal_kv_get_gcs_client()
    )
    assert cluster_status_to_report.total_num_cpus == 3
    assert cluster_status_to_report.total_num_gpus == 1
    assert cluster_status_to_report.total_memory_gb > 0
    assert cluster_status_to_report.total_object_store_memory_gb == 1.0


def test_usage_lib_get_cluster_config_to_report(
    monkeypatch, tmp_path, reset_usage_stats
):
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
    assert set(cluster_config_to_report.worker_node_instance_types) == {
        "m3.large",
        "Standard_D2s_v3",
        "n1-standard-2",
    }

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

    monkeypatch.setenv("RAY_USAGE_STATS_KUBERAY_IN_USE", "1")
    cluster_config_to_report = ray_usage_lib.get_cluster_config_to_report(
        tmp_path / "does_not_exist.yaml"
    )
    assert cluster_config_to_report.cloud_provider == "kuberay"


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Test depends on runtime env feature not supported on Windows.",
)
def test_usage_lib_report_data(
    monkeypatch, ray_start_cluster, tmp_path, reset_usage_stats
):
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
            cluster_config_to_report,
            2,
            2,
            2,
            ray.worker.global_worker.gcs_client.address,
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
def test_usage_report_e2e(
    monkeypatch, ray_start_cluster, tmp_path, reset_usage_stats, gcs_storage_type
):
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
        m.setenv("RAY_USAGE_STATS_EXTRA_TAGS", "extra_k1=extra_v1")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=3)
        if os.environ.get("RAY_MINIMAL") != "1":
            from ray import train  # noqa: F401
            from ray.rllib.algorithms.ppo import PPO  # noqa: F401

        ray_usage_lib.record_extra_usage_tag(ray_usage_lib.TagKey._TEST1, "extra_v2")

        ray.init(address=cluster.address)

        ray_usage_lib.record_extra_usage_tag(ray_usage_lib.TagKey._TEST2, "extra_v3")

        if os.environ.get("RAY_MINIMAL") != "1":
            from ray import tune  # noqa: F401

            def objective(*args):
                pass

            tuner = tune.Tuner(objective)
            tuner.fit()

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
        assert payload["extra_usage_tags"]["actor_num_created"] >= 0
        assert payload["extra_usage_tags"]["pg_num_created"] >= 0
        payload["extra_usage_tags"]["actor_num_created"] = 0
        payload["extra_usage_tags"]["pg_num_created"] = 0
        assert payload["extra_usage_tags"] == {
            "extra_k1": "extra_v1",
            "_test1": "extra_v2",
            "_test2": "extra_v3",
            "dashboard_metrics_grafana_enabled": "False",
            "dashboard_metrics_prometheus_enabled": "False",
            "serve_num_deployments": "1",
            "serve_api_version": "v1",
            "actor_num_created": "0",
            "pg_num_created": "0",
            "gcs_storage": gcs_storage_type,
            "dashboard_used": "False",
        }
        assert payload["total_num_nodes"] == 1
        assert payload["total_num_running_jobs"] == 1
        if os.environ.get("RAY_MINIMAL") == "1":
            # Since we start a serve actor for mocking a server using runtime env.
            assert set(payload["library_usages"]) == {"serve"}
        else:
            # Serve is recorded due to our mock server.
            assert set(payload["library_usages"]) == {"rllib", "train", "tune", "serve"}
        validate(instance=payload, schema=schema)
        """
        Verify the usage_stats.json is updated.
        """
        print("Verifying usage stats write.")
        global_node = ray._private.worker._global_node
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


def test_first_usage_report_delayed(monkeypatch, ray_start_cluster, reset_usage_stats):
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv("RAY_USAGE_STATS_REPORT_URL", "http://127.0.0.1:8000")
        m.setenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", "10")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=0)
        ray.init(address=cluster.address)

        # The first report should be delayed for 10s.
        time.sleep(5)
        session_dir = ray._private.worker.global_worker.node.address_info["session_dir"]
        session_path = Path(session_dir)
        assert not (session_path / usage_constants.USAGE_STATS_FILE).exists()

        time.sleep(10)
        assert (session_path / usage_constants.USAGE_STATS_FILE).exists()


def test_usage_report_disabled(monkeypatch, ray_start_cluster, reset_usage_stats):
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

        session_dir = ray._private.worker.global_worker.node.address_info["session_dir"]
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


def test_usage_file_error_message(monkeypatch, ray_start_cluster, reset_usage_stats):
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

        global_node = ray._private.worker._global_node
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


def test_lib_used_from_driver(monkeypatch, ray_start_cluster, reset_usage_stats):
    """
    Test library usage is correctly reported when they are imported from
    a driver.
    """
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv("RAY_USAGE_STATS_REPORT_URL", "http://127.0.0.1:8000/usage")
        m.setenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", "1")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=3)
        ray.init(address=cluster.address)

        script = """
import ray
import os
if os.environ.get("RAY_MINIMAL") != "1":
    from ray import train  # noqa: F401
    from ray.rllib.algorithms.ppo import PPO  # noqa: F401

ray.init(address="{addr}")

if os.environ.get("RAY_MINIMAL") != "1":
    from ray import tune  # noqa: F401
    def objective(*args):
        pass

    tune.run(objective)
"""
        # Run a script in a separate process. It is a workaround to
        # reimport libraries. Without this, `import train`` will become
        # no-op since we already imported this lib in previous tests.
        run_string_as_driver(script.format(addr=cluster.address))

        """
        Verify the usage_stats.json is updated.
        """
        print("Verifying lib usage report.")
        global_node = ray.worker._global_node
        temp_dir = pathlib.Path(global_node.get_session_dir_path())

        wait_for_condition(lambda: file_exists(temp_dir), timeout=30)

        def verify():
            lib_usages = read_file(temp_dir, "usage_stats")["library_usages"]
            print(lib_usages)
            if os.environ.get("RAY_MINIMAL") == "1":
                return set(lib_usages) == set()
            else:
                return set(lib_usages) == {"rllib", "train", "tune"}

        wait_for_condition(verify)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Test depends on runtime env feature not supported on Windows.",
)
def test_lib_used_from_workers(monkeypatch, ray_start_cluster, reset_usage_stats):
    """
    Test library usage is correctly reported when they are imported from
    workers.
    """
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv("RAY_USAGE_STATS_REPORT_URL", "http://127.0.0.1:8000/usage")
        m.setenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", "1")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=3)
        ray_usage_lib._recorded_library_usages.clear()

        ray.init(address=cluster.address)

        @ray.remote
        class ActorWithLibImport:
            def __init__(self):
                from ray import train  # noqa: F401
                from ray.rllib.algorithms.ppo import PPO  # noqa: F401

            def ready(self):
                from ray import tune  # noqa: F401

                def objective(*args):
                    pass

                tune.run(objective)

        # Use a runtime env to run tests in minimal installation.
        a = ActorWithLibImport.options(
            runtime_env={"pip": ["ray[rllib]", "ray[tune]"]}
        ).remote()
        ray.get(a.ready.remote())

        """
        Verify the usage_stats.json contains the lib usage.
        """
        global_node = ray.worker._global_node
        temp_dir = pathlib.Path(global_node.get_session_dir_path())
        wait_for_condition(lambda: file_exists(temp_dir), timeout=30)

        def verify():
            lib_usages = read_file(temp_dir, "usage_stats")["library_usages"]
            return set(lib_usages) == {"tune", "rllib", "train"}

        wait_for_condition(verify)


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="Test depends on library that's not downloaded from a minimal install.",
)
def test_lib_usage_record_from_init_session(
    monkeypatch, ray_start_cluster, reset_usage_stats
):
    """
    Make sure we store a lib usage to the /tmp/ray folder and report them
    when any instance that has usage stats enabled.
    """

    # Start a driver without usage stats enabled. This will record
    # lib_usage.txt.
    script = """
import ray
import os
from ray import train  # noqa: F401
from ray import tune  # noqa: F401
from ray.rllib.algorithms.ppo import PPO  # noqa: F401

# Start a instance that disables usage stats.
ray.init()

def objective(*args):
    pass

tune.run(objective)
"""

    run_string_as_driver(script)

    # Run the cluster that reports the usage stats. Make sure the lib usage is reported.
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv("RAY_USAGE_STATS_REPORT_URL", "http://127.0.0.1:8000/usage")
        m.setenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", "1")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=3)
        ray.init(address=cluster.address)

        """
        Verify the library usage is recorded to the ray folder.
        """
        lib_usages = ray_usage_lib.LibUsageRecorder(
            ray._private.utils.get_ray_temp_dir()
        ).read_lib_usages()
        assert set(lib_usages) == {"train", "rllib", "tune"}

        """
        Verify the library usage is reported from the current instance.
        """
        print("Verifying lib usage report.")
        global_node = ray.worker._global_node
        temp_dir = pathlib.Path(global_node.get_session_dir_path())

        wait_for_condition(lambda: file_exists(temp_dir), timeout=30)

        def verify():
            lib_usages = read_file(temp_dir, "usage_stats")["library_usages"]
            print(lib_usages)
            return set(lib_usages) == {"rllib", "train", "tune"}

        wait_for_condition(verify)


def test_usage_stats_tags(
    monkeypatch, ray_start_cluster, reset_usage_stats, gcs_storage_type
):
    """
    Test usage tags are correctly reported.
    """
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv("RAY_USAGE_STATS_REPORT_URL", "http://127.0.0.1:8000/usage")
        m.setenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", "1")
        m.setenv("RAY_USAGE_STATS_EXTRA_TAGS", "key=val;key2=val2")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=3)
        cluster.add_node(num_cpus=3)

        context = ray.init(address=cluster.address)

        """
        Verify the usage_stats.json contains the lib usage.
        """
        temp_dir = pathlib.Path(context.address_info["session_dir"])
        wait_for_condition(lambda: file_exists(temp_dir), timeout=30)

        def verify():
            tags = read_file(temp_dir, "usage_stats")["extra_usage_tags"]
            num_nodes = read_file(temp_dir, "usage_stats")["total_num_nodes"]
            assert tags == {
                "key": "val",
                "key2": "val2",
                "dashboard_metrics_grafana_enabled": "False",
                "dashboard_metrics_prometheus_enabled": "False",
                "gcs_storage": gcs_storage_type,
                "dashboard_used": "False",
                "actor_num_created": "0",
                "pg_num_created": "0",
            }
            assert num_nodes == 2
            return True

        wait_for_condition(verify)


def test_usage_stats_gcs_query_failure(
    monkeypatch, ray_start_cluster, reset_usage_stats
):
    """Test None data is reported when the GCS query is failed."""
    with monkeypatch.context() as m:
        m.setenv(
            "RAY_testing_asio_delay_us",
            "NodeInfoGcsService.grpc_server.GetAllNodeInfo=2000000:2000000",
        )
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=3)

        ray.init(address=cluster.address)
        assert (
            ray_usage_lib.get_total_num_nodes_to_report(
                ray.experimental.internal_kv.internal_kv_get_gcs_client(), timeout=1
            )
            is None
        )


def test_usages_stats_available_when_dashboard_not_included(
    monkeypatch, ray_start_cluster, reset_usage_stats
):
    """
    Test library usage is correctly reported when they are imported from
    workers.
    """
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv("RAY_USAGE_STATS_REPORT_URL", "http://127.0.0.1:8000/usage")
        m.setenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", "1")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=1, include_dashboard=False)
        ray.init(address=cluster.address)

        """
        Verify the usage_stats.json contains the lib usage.
        """
        temp_dir = pathlib.Path(cluster.head_node.get_session_dir_path())
        wait_for_condition(lambda: file_exists(temp_dir), timeout=30)

        def verify():
            return read_file(temp_dir, "usage_stats")["seq_number"] > 2

        wait_for_condition(verify)


def test_usages_stats_dashboard(monkeypatch, ray_start_cluster, reset_usage_stats):
    """
    Test dashboard usage metrics are correctly reported.
    This is tested on both minimal / non minimal ray.
    """
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv("RAY_USAGE_STATS_REPORT_URL", "http://127.0.0.1:8000/usage")
        m.setenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", "1")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=0)
        addr = ray.init(address=cluster.address)

        """
        Verify the usage_stats.json contains the lib usage.
        """
        temp_dir = pathlib.Path(ray._private.worker._global_node.get_session_dir_path())
        webui_url = format_web_url(addr["webui_url"])
        wait_for_condition(lambda: file_exists(temp_dir), timeout=30)

        def verify_dashboard_not_used():
            dashboard_used = read_file(temp_dir, "usage_stats")["extra_usage_tags"][
                "dashboard_used"
            ]
            return dashboard_used == "False"

        wait_for_condition(verify_dashboard_not_used)

        if os.environ.get("RAY_MINIMAL") == "1":
            # In the minimal Ray, dashboard is not available.
            return

        # Open the dashboard will set the dashboard_used == "True".
        resp = requests.get(webui_url)
        resp.raise_for_status()

        def verify_dashboard_used():
            dashboard_used = read_file(temp_dir, "usage_stats")["extra_usage_tags"][
                "dashboard_used"
            ]
            if os.environ.get("RAY_MINIMAL") == "1":
                return dashboard_used == "False"
            else:
                return dashboard_used == "True"

        wait_for_condition(verify_dashboard_used)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
