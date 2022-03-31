from contextlib import redirect_stderr
import io
import logging
import time

import requests
import pytest

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition


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

    Handler.deploy()
    h = Handler.get_handle()

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


def test_http_access_log(serve_instance):
    prefix = "/test"

    @serve.deployment(route_prefix=prefix)
    def fn(req):
        if "throw" in req.query_params:
            raise RuntimeError("blah blah blah")

        return "hi"

    fn.deploy()

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
        wait_for_condition(check_log)

        with pytest.raises(requests.exceptions.RequestException):
            requests.get(f"http://127.0.0.1:8000{prefix}?throw=True").raise_for_status()

        wait_for_condition(check_log, fail=True)


def test_user_logs(serve_instance):
    logger = logging.getLogger("ray.serve")
    msg = "user log message"
    name = "user_fn"

    @serve.deployment(name=name)
    def fn(*args):
        logger.info("user log message")
        return serve.get_replica_context().replica_tag

    fn.deploy()
    handle = fn.get_handle()

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

    A.deploy()
    handle = A.get_handle()

    f = io.StringIO()
    with redirect_stderr(f):
        replica_tag = ray.get(handle.remote())

        for _ in range(10):
            time.sleep(0.1)
            assert replica_tag not in f.getvalue()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
