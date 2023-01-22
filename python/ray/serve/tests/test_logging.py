from contextlib import redirect_stderr
import io
import logging
import time
import sys

import requests
import pytest

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition


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
                    method_name in s,
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


@pytest.mark.skip("TODO(edoakes): temporarily unblocking merge.")
def test_http_access_log(serve_instance):
    prefix = "/test"

    @serve.deployment(route_prefix=prefix)
    def fn(req):
        if "throw" in req.query_params:
            raise RuntimeError("blah blah blah")

        return "hi"

    serve.run(fn.bind())

    f = io.StringIO()
    with redirect_stderr(f):

        def check_log(fail: bool = False):
            s = f.getvalue()
            return all(
                [
                    "http_proxy" in s,
                    "127.0.0.1" in s,
                    prefix in s,
                    ("500" if fail else "200") in s,
                    "ms" in s,
                ]
            )

        requests.get(f"http://127.0.0.1:8000{prefix}").raise_for_status()
        wait_for_condition(check_log, timeout=30)

        with pytest.raises(requests.exceptions.RequestException):
            requests.get(f"http://127.0.0.1:8000{prefix}?throw=True").raise_for_status()

        wait_for_condition(check_log, timeout=30, fail=True)


def test_user_logs(serve_instance):
    logger = logging.getLogger("ray.serve")
    msg = "user log message"
    name = "user_fn"

    @serve.deployment(name=name)
    def fn(*args):
        logger.info("user log message")
        return serve.get_replica_context().replica_tag

    handle = serve.run(fn.bind())

    f = io.StringIO()
    with redirect_stderr(f):

        def check_log(replica_tag: str):
            s = f.getvalue()
            return all([name in s, replica_tag in s, msg in s])

        replica_tag = ray.get(handle.remote())
        wait_for_condition(check_log, replica_tag=replica_tag)


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


def test_deprecated_deployment_logger(serve_instance):
    # NOTE(edoakes): using this logger is no longer recommended as of Ray 1.13.
    # The test is maintained for backwards compatibility.
    logger = logging.getLogger("ray")

    @serve.deployment(name="counter")
    class Counter:
        def __init__(self):
            self.count = 0

        def __call__(self, request):
            self.count += 1
            logger.info(f"count: {self.count}")

    serve.run(Counter.bind())
    f = io.StringIO()
    with redirect_stderr(f):
        requests.get("http://127.0.0.1:8000/counter/")

        def counter_log_success():
            s = f.getvalue()
            return "deployment" in s and "replica" in s and "count" in s

        wait_for_condition(counter_log_success)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
