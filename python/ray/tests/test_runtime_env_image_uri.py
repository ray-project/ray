import logging
import sys

import pytest

from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.image_uri import _modify_context_impl


def test_custom_logs_dir_is_mounted_in_container():
    context = RuntimeEnvContext()
    _modify_context_impl(
        "rayproject/ray:latest",
        "/ray/default_worker.py",
        None,
        context,
        logging.getLogger(__name__),
        "/tmp/ray",
        "/var/log/ray",
    )

    assert "-v /tmp/ray:/tmp/ray" in context.py_executable
    assert "-v /var/log/ray:/var/log/ray" in context.py_executable


def test_logs_dir_under_temp_dir_is_not_mounted_twice():
    context = RuntimeEnvContext()
    _modify_context_impl(
        "rayproject/ray:latest",
        "/ray/default_worker.py",
        None,
        context,
        logging.getLogger(__name__),
        "/tmp/ray",
        "/tmp/ray/session/logs",
    )

    assert context.py_executable.count("-v ") == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
