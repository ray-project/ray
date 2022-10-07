import os
import sys
import time
import json
import copy
import logging
import requests
import asyncio
import random
import tempfile
import socket

import pytest
import numpy as np

import ray
from ray._private.utils import binary_to_hex
from ray._private.event.event_logger import get_event_logger
from ray.dashboard.tests.conftest import *  # noqa
from ray.dashboard.modules.event import event_consts
from ray.core.generated import event_pb2
from ray._private.test_utils import (
    format_web_url,
    wait_until_server_available,
    wait_for_condition,
)
from ray.dashboard.modules.event.event_utils import (
    monitor_events,
)

logger = logging.getLogger(__name__)


def _get_event(msg="empty message", job_id=None, source_type=None):
    return {
        "event_id": binary_to_hex(np.random.bytes(18)),
        "source_type": random.choice(event_pb2.Event.SourceType.keys())
        if source_type is None
        else source_type,
        "host_name": "po-dev.inc.alipay.net",
        "pid": random.randint(1, 65536),
        "label": "",
        "message": msg,
        "time_stamp": time.time(),
        "severity": "INFO",
        "custom_fields": {
            "job_id": ray.JobID.from_int(random.randint(1, 100)).hex()
            if job_id is None
            else job_id,
            "node_id": "",
            "task_id": "",
        },
    }


def _test_logger(name, log_file, max_bytes, backup_count):
    handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count
    )
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    return logger


def test_python_global_event_logger(tmp_path):
    logger = get_event_logger(event_pb2.Event.SourceType.GCS, str(tmp_path))
    logger.set_global_context({"test_meta": "1"})
    logger.info("message", a="a", b="b")
    logger.error("message", a="a", b="b")
    logger.warning("message", a="a", b="b")
    logger.fatal("message", a="a", b="b")
    event_dir = tmp_path / "events"
    assert event_dir.exists()
    event_file = event_dir / "event_GCS.log"
    assert event_file.exists()

    line_severities = ["INFO", "ERROR", "WARNING", "FATAL"]

    with event_file.open() as f:
        for line, severity in zip(f.readlines(), line_severities):
            data = json.loads(line)
            assert data["severity"] == severity
            assert data["label"] == ""
            assert "timestamp" in data
            assert len(data["event_id"]) == 36
            assert data["message"] == "message"
            assert data["source_type"] == "GCS"
            assert data["source_hostname"] == socket.gethostname()
            assert data["source_pid"] == os.getpid()
            assert data["custom_fields"]["a"] == "a"
            assert data["custom_fields"]["b"] == "b"


def test_event_basic(disable_aiohttp_cache, ray_start_with_dashboard):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"])
    webui_url = format_web_url(ray_start_with_dashboard["webui_url"])
    session_dir = ray_start_with_dashboard["session_dir"]
    event_dir = os.path.join(session_dir, "logs", "events")
    job_id = ray.JobID.from_int(100).hex()

    source_type_gcs = event_pb2.Event.SourceType.Name(event_pb2.Event.GCS)
    source_type_raylet = event_pb2.Event.SourceType.Name(event_pb2.Event.RAYLET)
    test_count = 20

    for source_type in [source_type_gcs, source_type_raylet]:
        test_log_file = os.path.join(event_dir, f"event_{source_type}.log")
        test_logger = _test_logger(
            __name__ + str(random.random()),
            test_log_file,
            max_bytes=2000,
            backup_count=1000,
        )
        for i in range(test_count):
            sample_event = _get_event(str(i), job_id=job_id, source_type=source_type)
            test_logger.info("%s", json.dumps(sample_event))

    def _check_events():
        try:
            resp = requests.get(f"{webui_url}/events")
            resp.raise_for_status()
            result = resp.json()
            all_events = result["data"]["events"]
            job_events = all_events[job_id]
            assert len(job_events) >= test_count * 2
            source_messages = {}
            for e in job_events:
                source_type = e["sourceType"]
                message = e["message"]
                source_messages.setdefault(source_type, set()).add(message)
            assert len(source_messages[source_type_gcs]) >= test_count
            assert len(source_messages[source_type_raylet]) >= test_count
            data = {str(i) for i in range(test_count)}
            assert data & source_messages[source_type_gcs] == data
            assert data & source_messages[source_type_raylet] == data
            return True
        except Exception as ex:
            logger.exception(ex)
            return False

    wait_for_condition(_check_events, timeout=15)


def test_event_message_limit(
    small_event_line_limit, disable_aiohttp_cache, ray_start_with_dashboard
):
    event_read_line_length_limit = small_event_line_limit
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"])
    webui_url = format_web_url(ray_start_with_dashboard["webui_url"])
    session_dir = ray_start_with_dashboard["session_dir"]
    event_dir = os.path.join(session_dir, "logs", "events")
    job_id = ray.JobID.from_int(100).hex()
    events = []
    # Sample event equals with limit.
    sample_event = _get_event("", job_id=job_id)
    message_len = event_read_line_length_limit - len(json.dumps(sample_event))
    for i in range(10):
        sample_event = copy.deepcopy(sample_event)
        sample_event["event_id"] = binary_to_hex(np.random.bytes(18))
        sample_event["message"] = str(i) * message_len
        assert len(json.dumps(sample_event)) == event_read_line_length_limit
        events.append(sample_event)
    # Sample event longer than limit.
    sample_event = copy.deepcopy(sample_event)
    sample_event["event_id"] = binary_to_hex(np.random.bytes(18))
    sample_event["message"] = "2" * (message_len + 1)
    assert len(json.dumps(sample_event)) > event_read_line_length_limit
    events.append(sample_event)

    for i in range(event_consts.EVENT_READ_LINE_COUNT_LIMIT):
        events.append(_get_event(str(i), job_id=job_id))

    with open(os.path.join(event_dir, "tmp.log"), "w") as f:
        f.writelines([(json.dumps(e) + "\n") for e in events])

    try:
        os.remove(os.path.join(event_dir, "event_GCS.log"))
    except Exception:
        pass
    os.rename(
        os.path.join(event_dir, "tmp.log"), os.path.join(event_dir, "event_GCS.log")
    )

    def _check_events():
        try:
            resp = requests.get(f"{webui_url}/events")
            resp.raise_for_status()
            result = resp.json()
            all_events = result["data"]["events"]
            assert (
                len(all_events[job_id]) >= event_consts.EVENT_READ_LINE_COUNT_LIMIT + 10
            )
            messages = [e["message"] for e in all_events[job_id]]
            for i in range(10):
                assert str(i) * message_len in messages
            assert "2" * (message_len + 1) not in messages
            assert str(event_consts.EVENT_READ_LINE_COUNT_LIMIT - 1) in messages
            return True
        except Exception as ex:
            logger.exception(ex)
            return False

    wait_for_condition(_check_events, timeout=15)


@pytest.mark.asyncio
async def test_monitor_events():
    with tempfile.TemporaryDirectory() as temp_dir:
        common = event_pb2.Event.SourceType.Name(event_pb2.Event.COMMON)
        common_log = os.path.join(temp_dir, f"event_{common}.log")
        test_logger = _test_logger(
            __name__ + str(random.random()), common_log, max_bytes=10, backup_count=10
        )
        test_events1 = []
        monitor_task = monitor_events(
            temp_dir, lambda x: test_events1.extend(x), scan_interval_seconds=0.01
        )
        assert not monitor_task.done()
        count = 10

        async def _writer(*args, read_events, spin=True):
            for x in range(*args):
                test_logger.info("%s", x)
                if spin:
                    while str(x) not in read_events:
                        await asyncio.sleep(0.01)

        async def _check_events(expect_events, read_events, timeout=10):
            start_time = time.time()
            while True:
                sorted_events = sorted(int(i) for i in read_events)
                sorted_events = [str(i) for i in sorted_events]
                if time.time() - start_time > timeout:
                    raise TimeoutError(
                        f"Timeout, read events: {sorted_events}, "
                        f"expect events: {expect_events}"
                    )
                if len(sorted_events) == len(expect_events):
                    if sorted_events == expect_events:
                        break
                await asyncio.sleep(1)

        await asyncio.gather(
            _writer(count, read_events=test_events1),
            _check_events([str(i) for i in range(count)], read_events=test_events1),
        )

        monitor_task.cancel()
        test_events2 = []
        monitor_task = monitor_events(
            temp_dir, lambda x: test_events2.extend(x), scan_interval_seconds=0.1
        )

        await _check_events([str(i) for i in range(count)], read_events=test_events2)

        await _writer(count, count * 2, read_events=test_events2)
        await _check_events(
            [str(i) for i in range(count * 2)], read_events=test_events2
        )

        log_file_count = len(os.listdir(temp_dir))

        test_logger = _test_logger(
            __name__ + str(random.random()), common_log, max_bytes=1000, backup_count=10
        )
        assert len(os.listdir(temp_dir)) == log_file_count

        await _writer(count * 2, count * 3, spin=False, read_events=test_events2)
        await _check_events(
            [str(i) for i in range(count * 3)], read_events=test_events2
        )
        await _writer(count * 3, count * 4, spin=False, read_events=test_events2)
        await _check_events(
            [str(i) for i in range(count * 4)], read_events=test_events2
        )

        # Test cancel monitor task.
        monitor_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await monitor_task
        assert monitor_task.done()

        assert len(os.listdir(temp_dir)) > 1, "Event log should have rollovers."


# TODO(sang): Enable it.
# def test_autoscaler_cluster_events(shutdown_only):
#     ray.init()

#     @ray.remote(num_gpus=1)
#     def f():
#         pass

#     f.remote()

#     wait_for_condition(lambda: len(list_cluster_events()) == 1)
#     infeasible_event = list_cluster_events()[0]
#     assert infeasible_event["source_type"] == "AUTOSCALER"


# def test_jobs_cluster_events(shutdown_only):
#     ray.init()
#     address = ray._private.worker._global_node.webui_url
#     address = format_web_url(address)
#     client = JobSubmissionClient(address)
#     client.submit_job(entrypoint="ls")

#     def verify():
#         assert len(list_cluster_events()) == 3
#         for e in list_cluster_events():
#             e["source_type"] = "JOBS"
#         return True

#     wait_for_condition(verify)
#     print(list_cluster_events())


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
