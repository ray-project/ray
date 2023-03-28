from contextlib import redirect_stderr
import io
import logging
import time
import sys

import requests
import starlette
import pytest

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
import re


def set_logging_config(monkeypatch, max_bytes, backup_count):
    monkeypatch.setenv("RAY_ROTATION_MAX_BYTES", str(max_bytes))
    monkeypatch.setenv("RAY_ROTATION_BACKUP_COUNT", str(backup_count))


def test_log_rotation_config(monkeypatch, ray_shutdown):
    # This test should be executed before any test that uses
    # the shared serve_instance, as environment variables
    # for log rotation need to be set before ray.init
    logger = logging.getLogger("ray.serve")
    max_bytes = 100
    backup_count = 3
    set_logging_config(monkeypatch, max_bytes, backup_count)
    ray.init(num_cpus=1)

    @serve.deployment
    class Handle:
        def __call__(self):
            handlers = logger.handlers
            res = {}
            for handler in handlers:
                if isinstance(handler, logging.handlers.RotatingFileHandler):
                    res["max_bytes"] = handler.maxBytes
                    res["backup_count"] = handler.backupCount
            return res

    handle = serve.run(Handle.bind())
    rotation_config = ray.get(handle.remote())
    assert rotation_config["max_bytes"] == max_bytes
    assert rotation_config["backup_count"] == backup_count


def test_handle_access_log(serve_instance):
    name = "handler"

    @serve.deployment(name=name)
    class Handler:
        def other_method(self, *args):
            return serve.get_replica_context().replica_tag

        def __call__(self, *args):
            return serve.get_replica_context().replica_tag

        def throw(self, *args):
            raise RuntimeError("blah blah blah")

    h = serve.run(Handler.bind())

    f = io.StringIO()
    with redirect_stderr(f):

        def check_log(replica_tag: str, method_name: str, fail: bool = False):
            s = f.getvalue()
            return all(
                [
                    name in s,
                    replica_tag in s,
                    method_name.upper() in s,
                    ("ERROR" if fail else "OK") in s,
                    "ms" in s,
                    ("blah blah blah" in s and "RuntimeError" in s)
                    if fail
                    else True,  # Check for stacktrace.
                ]
            )

        replica_tag = ray.get(h.remote())
        wait_for_condition(check_log, replica_tag=replica_tag, method_name="__call__")

        ray.get(h.other_method.remote())
        wait_for_condition(
            check_log, replica_tag=replica_tag, method_name="other_method"
        )

        with pytest.raises(RuntimeError, match="blah blah blah"):
            ray.get(h.throw.remote())

        wait_for_condition(
            check_log, replica_tag=replica_tag, method_name="throw", fail=True
        )


def test_user_logs(serve_instance):
    logger = logging.getLogger("ray.serve")
    stderr_msg = "user log message"
    log_file_msg = "in file only"
    name = "user_fn"

    @serve.deployment(name=name)
    def fn(*args):
        logger.info(stderr_msg)
        logger.info(log_file_msg, extra={"log_to_stderr": False})
        return serve.get_replica_context().replica_tag, logger.handlers[1].baseFilename

    handle = serve.run(fn.bind())

    f = io.StringIO()
    with redirect_stderr(f):
        replica_tag, log_file_name = ray.get(handle.remote())

        def check_stderr_log(replica_tag: str):
            s = f.getvalue()
            return all(
                [name in s, replica_tag in s, stderr_msg in s, log_file_msg not in s]
            )

        # Only the stderr_msg should be logged to stderr.
        wait_for_condition(check_stderr_log, replica_tag=replica_tag)

        def check_log_file(replica_tag: str):
            with open(log_file_name, "r") as f:
                s = f.read()
                return all(
                    [name in s, replica_tag in s, stderr_msg in s, log_file_msg in s]
                )

        # Both messages should be logged to the file.
        wait_for_condition(check_log_file, replica_tag=replica_tag)


def test_disable_access_log(serve_instance):
    logger = logging.getLogger("ray.serve")

    @serve.deployment
    class A:
        def __init__(self):
            logger.setLevel(logging.ERROR)

        def __call__(self, *args):
            return serve.get_replica_context().replica_tag

    handle = serve.run(A.bind())

    f = io.StringIO()
    with redirect_stderr(f):
        replica_tag = ray.get(handle.remote())

        for _ in range(10):
            time.sleep(0.1)
            assert replica_tag not in f.getvalue()


def test_context_information_in_logging(serve_instance):
    """Make sure all context information exist in the log message"""

    logger = logging.getLogger("ray.serve")

    @serve.deployment
    def fn(*args):
        logger.info("user func")
        request_context = ray.serve.context._serve_request_context.get()
        return {
            "request_id": request_context.request_id,
            "route": request_context.route,
        }

    @serve.deployment
    class Model:
        def __call__(self, req: starlette.requests.Request):
            logger.info("user log message from class method")
            request_context = ray.serve.context._serve_request_context.get()
            return {
                "request_id": request_context.request_id,
                "route": request_context.route,
            }

    serve.run(fn.bind(), name="app1", route_prefix="/fn")
    serve.run(Model.bind(), name="app2", route_prefix="/class_method")

    f = io.StringIO()
    with redirect_stderr(f):
        resp = requests.get("http://127.0.0.1:8000/fn").json()
        resp2 = requests.get("http://127.0.0.1:8000/class_method").json()

        # Check the component log
        expected_log_infos = [
            f"{resp['request_id']} {resp['route']} replica.py",
            f"{resp2['request_id']} {resp2['route']} replica.py",
        ]

        # Check User log
        user_log_regexes = [
            f".*{resp['request_id']} {resp['route']}.* user func.*",
            f".*{resp2['request_id']} {resp2['route']}.* user log "
            "message from class method.*",
        ]

        def check_log():
            logs_content = ""
            for _ in range(20):
                time.sleep(0.1)
                logs_content = f.getvalue()
                if logs_content:
                    break
            for expected_log_info in expected_log_infos:
                assert expected_log_info in logs_content
            for regex in user_log_regexes:
                assert re.findall(regex, logs_content) != []

        check_log()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
