import io
import json
import logging
import os
import re
import string
import sys
import time
from contextlib import redirect_stderr

import pytest
import requests
import starlette

import ray
import ray.util.state as state_api
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve._private.common import ServeComponentType
from ray.serve._private.constants import SERVE_LOG_EXTRA_FIELDS
from ray.serve._private.logging_utils import (
    ServeJSONFormatter,
    get_component_log_file_name,
    get_serve_logs_dir,
)


@pytest.fixture
def serve_and_ray_shutdown():
    serve.shutdown()
    ray.shutdown()
    yield


def set_logging_config(monkeypatch, max_bytes, backup_count):
    monkeypatch.setenv("RAY_ROTATION_MAX_BYTES", str(max_bytes))
    monkeypatch.setenv("RAY_ROTATION_BACKUP_COUNT", str(backup_count))


def _get_expected_replica_log_content(replica_tag: str):
    app_name, deployment_name, replica_id = replica_tag.split("#")
    return f"{app_name}_{deployment_name} {replica_id}"


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
    rotation_config = handle.remote().result()
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
                    _get_expected_replica_log_content(replica_tag) in s,
                    method_name.upper() in s,
                    ("ERROR" if fail else "OK") in s,
                    "ms" in s,
                    ("blah blah blah" in s and "RuntimeError" in s)
                    if fail
                    else True,  # Check for stacktrace.
                ]
            )

        replica_tag = h.remote().result()
        wait_for_condition(check_log, replica_tag=replica_tag, method_name="__call__")

        h.other_method.remote().result()
        wait_for_condition(
            check_log, replica_tag=replica_tag, method_name="other_method"
        )

        with pytest.raises(RuntimeError, match="blah blah blah"):
            h.throw.remote().result()

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
        replica_tag, log_file_name = handle.remote().result()

        def check_stderr_log(replica_tag: str):
            s = f.getvalue()
            app_name, deployment_name, replica_id = replica_tag.split("#")
            return all(
                [
                    name in s,
                    _get_expected_replica_log_content(replica_tag) in s,
                    stderr_msg in s,
                    log_file_msg not in s,
                ]
            )

        # Only the stderr_msg should be logged to stderr.
        wait_for_condition(check_stderr_log, replica_tag=replica_tag)

        def check_log_file(replica_tag: str):
            with open(log_file_name, "r") as f:
                s = f.read()
                return all(
                    [
                        name in s,
                        _get_expected_replica_log_content(replica_tag) in s,
                        stderr_msg in s,
                        log_file_msg in s,
                    ]
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
        replica_tag = handle.remote().result()

        for _ in range(10):
            time.sleep(0.1)
            assert _get_expected_replica_log_content(replica_tag) not in f.getvalue()


def test_log_filenames_contain_only_posix_characters(serve_instance):
    """Assert that all log filenames only consist of POSIX-compliant characters.

    See: https://github.com/ray-project/ray/issues/41615
    """

    @serve.deployment
    class A:
        def __call__(self, *args) -> str:
            return "hi"

    serve.run(A.bind())

    r = requests.get("http://localhost:8000/")
    r.raise_for_status()
    assert r.text == "hi"

    acceptable_chars = string.ascii_letters + string.digits + "_" + "."
    for filename in os.listdir(get_serve_logs_dir()):
        assert all(char in acceptable_chars for char in filename)


@pytest.mark.parametrize("json_log_format", [False, True])
def test_context_information_in_logging(serve_and_ray_shutdown, json_log_format):
    """Make sure all context information exist in the log message"""

    logger = logging.getLogger("ray.serve")

    @serve.deployment(
        logging_config={"encoding": "JSON" if json_log_format else "TEXT"}
    )
    def fn(*args):
        logger.info("user func")
        request_context = ray.serve.context._serve_request_context.get()
        return {
            "request_id": request_context.request_id,
            "route": request_context.route,
            "app_name": request_context.app_name,
            "log_file": logger.handlers[1].baseFilename,
            "replica": serve.get_replica_context().replica_tag,
        }

    @serve.deployment(
        logging_config={"encoding": "JSON" if json_log_format else "TEXT"}
    )
    class Model:
        def __call__(self, req: starlette.requests.Request):
            logger.info("user log message from class method")
            request_context = ray.serve.context._serve_request_context.get()
            return {
                "request_id": request_context.request_id,
                "route": request_context.route,
                "app_name": request_context.app_name,
                "log_file": logger.handlers[1].baseFilename,
                "replica": serve.get_replica_context().replica_tag,
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

        # Check stream log
        check_log()

        # Check user log file
        method_replica_id = resp["replica"].split("#")[-1]
        class_method_replica_id = resp2["replica"].split("#")[-1]
        if json_log_format:
            user_method_log_regex = (
                f'.*"deployment": "{resp["app_name"]}_fn", '
                f'"replica": "{method_replica_id}", '
                f'"request_id": "{resp["request_id"]}", '
                f'"route": "{resp["route"]}", '
                f'"application": "{resp["app_name"]}", "message":.* user func.*'
            )
            user_class_method_log_regex = (
                f'.*"deployment": "{resp2["app_name"]}_Model", '
                f'"replica": "{class_method_replica_id}", '
                f'"request_id": "{resp2["request_id"]}", '
                f'"route": "{resp2["route"]}", '
                f'"application": "{resp2["app_name"]}", "message":.* user log '
                "message from class method.*"
            )
        else:
            user_method_log_regex = (
                f".*{resp['request_id']} {resp['route']}.* user func.*"
            )
            user_class_method_log_regex = (
                f".*{resp2['request_id']} {resp2['route']}.* "
                "user log message from class method.*"
            )

        def check_log_file(log_file: str, expected_regex: list):
            with open(log_file, "r") as f:
                s = f.read()
                assert re.findall(expected_regex, s) != []

        check_log_file(resp["log_file"], user_method_log_regex)
        check_log_file(resp2["log_file"], user_class_method_log_regex)


@pytest.mark.parametrize("raise_error", [True, False])
def test_extra_field(serve_and_ray_shutdown, raise_error):
    """Test ray serve extra logging"""
    logger = logging.getLogger("ray.serve")

    @serve.deployment(logging_config={"encoding": "JSON"})
    def fn(*args):
        if raise_error:
            logger.info("user_func", extra={SERVE_LOG_EXTRA_FIELDS: [123]})
        else:
            logger.info(
                "user_func",
                extra={"k1": "my_v1", SERVE_LOG_EXTRA_FIELDS: {"k2": "my_v2"}},
            )
        return {
            "log_file": logger.handlers[1].baseFilename,
        }

    serve.run(fn.bind(), name="app1", route_prefix="/fn")
    resp = requests.get("http://127.0.0.1:8000/fn")
    if raise_error:
        resp.status_code == 500
    else:
        resp = resp.json()
        with open(resp["log_file"], "r") as f:
            s = f.read()
            assert re.findall(".*my_v1.*", s) == []
            assert re.findall('.*"k2": "my_v2".*', s) != []


def check_log_file(log_file: str, expected_regex: list):
    with open(log_file, "r") as f:
        s = f.read()
        for regex in expected_regex:
            assert re.findall(regex, s) != []


class TestLoggingAPI:
    def test_start_serve_with_logging_config(self, serve_and_ray_shutdown):
        serve.start(logging_config={"log_level": "DEBUG", "encoding": "JSON"})
        serve_log_dir = get_serve_logs_dir()
        # Check controller log
        actors = state_api.list_actors()
        expected_log_regex = [".*logger with logging config.*"]
        for actor in actors:
            print(actor["name"])
            if "SERVE_CONTROLLER_ACTOR" == actor["name"]:
                controller_pid = actor["pid"]
        controller_log_file_name = get_component_log_file_name(
            "controller", controller_pid, component_type=None, suffix=".log"
        )
        controller_log_path = os.path.join(serve_log_dir, controller_log_file_name)
        check_log_file(controller_log_path, expected_log_regex)

        # Check proxy log
        nodes = state_api.list_nodes()
        node_ip_address = nodes[0].node_ip
        proxy_log_file_name = get_component_log_file_name(
            "proxy", node_ip_address, component_type=None, suffix=".log"
        )
        proxy_log_path = os.path.join(serve_log_dir, proxy_log_file_name)
        check_log_file(proxy_log_path, expected_log_regex)

    @pytest.mark.parametrize("encoding_type", ["TEXT", "JSON"])
    def test_encoding(self, serve_and_ray_shutdown, encoding_type):
        """Test serve.run logging API"""
        logging_config = {"encoding": encoding_type}
        logger = logging.getLogger("ray.serve")

        @serve.deployment(logging_config=logging_config)
        class Model:
            def __call__(self, req: starlette.requests.Request):
                return {
                    "log_file": logger.handlers[1].baseFilename,
                    "replica": serve.get_replica_context().replica_tag,
                }

        serve.run(Model.bind())
        resp = requests.get("http://127.0.0.1:8000/").json()

        replica_id = resp["replica"].split("#")[-1]
        if encoding_type == "JSON":
            expected_log_regex = [f'"replica": "{replica_id}", ']
        else:
            expected_log_regex = [f".*{replica_id}.*"]
        check_log_file(resp["log_file"], expected_log_regex)

    def test_log_level(self, serve_and_ray_shutdown):
        logger = logging.getLogger("ray.serve")

        @serve.deployment
        class Model:
            def __call__(self, req: starlette.requests.Request):
                logger.info("model_info_level")
                logger.debug("model_debug_level")
                return {
                    "log_file": logger.handlers[1].baseFilename,
                }

        serve.run(Model.bind())
        resp = requests.get("http://127.0.0.1:8000/").json()
        expected_log_regex = [".*model_info_level.*"]
        check_log_file(resp["log_file"], expected_log_regex)

        # Make sure 'model_debug_level' log content does not exist
        with pytest.raises(AssertionError):
            check_log_file(resp["log_file"], [".*model_debug_level.*"])

        serve.run(Model.options(logging_config={"log_level": "DEBUG"}).bind())
        resp = requests.get("http://127.0.0.1:8000/").json()
        expected_log_regex = [".*model_info_level.*", ".*model_debug_level.*"]
        check_log_file(resp["log_file"], expected_log_regex)

    def test_logs_dir(self, serve_and_ray_shutdown):
        logger = logging.getLogger("ray.serve")

        @serve.deployment
        class Model:
            def __call__(self, req: starlette.requests.Request):
                logger.info("model_info_level")
                return {
                    "logs_path": logger.handlers[1].baseFilename,
                }

        serve.run(Model.bind())
        resp = requests.get("http://127.0.0.1:8000/").json()

        paths = resp["logs_path"].split("/")
        paths[-1] = "new_dir"
        new_log_dir = "/".join(paths)

        serve.run(Model.options(logging_config={"logs_dir": new_log_dir}).bind())
        resp = requests.get("http://127.0.0.1:8000/").json()
        assert "new_dir" in resp["logs_path"]

        check_log_file(resp["logs_path"], [".*model_info_level.*"])

    @pytest.mark.parametrize("enable_access_log", [True, False])
    def test_access_log(self, serve_and_ray_shutdown, enable_access_log):
        logger = logging.getLogger("ray.serve")

        @serve.deployment(logging_config={"enable_access_log": enable_access_log})
        class Model:
            def __call__(self, req: starlette.requests.Request):
                logger.info("model_info_level")
                logger.info("model_not_show", extra={"serve_access_log": True})
                return {
                    "logs_path": logger.handlers[1].baseFilename,
                }

        serve.run(Model.bind())

        resp = requests.get("http://127.0.0.1:8000/")
        assert resp.status_code == 200
        resp = resp.json()
        check_log_file(resp["logs_path"], [".*model_info_level.*"])
        if enable_access_log:
            check_log_file(resp["logs_path"], [".*model_not_show.*"])
        else:
            with pytest.raises(AssertionError):
                check_log_file(resp["logs_path"], [".*model_not_show.*"])

    def test_application_logging_overwrite(self, serve_and_ray_shutdown):
        @serve.deployment
        class Model:
            def __call__(self, req: starlette.requests.Request):
                logger = logging.getLogger("ray.serve")
                logger.info("model_info_level")
                logger.debug("model_debug_level")
                return {
                    "log_file": logger.handlers[1].baseFilename,
                }

        serve.run(Model.bind(), logging_config={"log_level": "DEBUG"})
        resp = requests.get("http://127.0.0.1:8000/").json()
        expected_log_regex = [".*model_info_level.*", ".*model_debug_level.*"]
        check_log_file(resp["log_file"], expected_log_regex)

        # Setting logging config in the deployment level, application
        # config can't override it.

        @serve.deployment(logging_config={"log_level": "INFO"})
        class Model2:
            def __call__(self, req: starlette.requests.Request):
                logger = logging.getLogger("ray.serve")
                logger.info("model_info_level")
                logger.debug("model_debug_level")
                return {
                    "log_file": logger.handlers[1].baseFilename,
                }

        serve.run(
            Model2.bind(),
            logging_config={"log_level": "DEBUG"},
            name="app2",
            route_prefix="/app2",
        )
        resp = requests.get("http://127.0.0.1:8000/app2").json()
        check_log_file(resp["log_file"], [".*model_info_level.*"])
        # Make sure 'model_debug_level' log content does not exist.
        with pytest.raises(AssertionError):
            check_log_file(resp["log_file"], [".*model_debug_level.*"])


@pytest.mark.parametrize("is_replica_type_component", [False, True])
def test_json_log_formatter(is_replica_type_component):
    """Test the json log formatter"""

    if is_replica_type_component:
        component_type = ServeComponentType.REPLICA
        formatter = ServeJSONFormatter("component", "component_id", component_type)
    else:
        formatter = ServeJSONFormatter("component", "component_id")
    init_kwargs = {
        "name": "test_log",
        "level": logging.DEBUG,
        "pathname": "my_path",
        "lineno": 1,
        "msg": "my_message",
        "args": (),
        "exc_info": None,
    }
    record = logging.LogRecord(**init_kwargs)

    def format_and_verify_json_output(record, expected_record: dict):
        formatted_record = formatter.format(record)
        formatted_record_dict = json.loads(formatted_record)
        for key in expected_record:
            assert key in formatted_record_dict
            assert formatted_record_dict[key] == expected_record[key]

    expected_json = {}
    if is_replica_type_component:
        expected_json["deployment"] = "component"
        expected_json["replica"] = "component_id"

    # Ensure message exists in the output.
    # Note that there is no "message" key in the record dict until it has been
    # formatted. This check should go before other fields are set and checked.
    expected_json["message"] = "my_path:1 - my_message"
    format_and_verify_json_output(record, expected_json)

    # Set request id
    record.request_id = "request_id"
    expected_json["request_id"] = "request_id"
    format_and_verify_json_output(record, expected_json)

    # Set route
    record.route = "route"
    expected_json["route"] = "route"
    format_and_verify_json_output(record, expected_json)

    # set application
    record.application = "application"
    expected_json["application"] = "application"
    format_and_verify_json_output(record, expected_json)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
