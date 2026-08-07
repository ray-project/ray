import logging
import sys

import pytest

from ray._private.runtime_env.agent.runtime_env_agent import (
    _create_image_uri_plugin,
)
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.image_uri import ImageURIPlugin, _modify_context_impl


class LegacyImageURIPlugin(ImageURIPlugin):
    def __init__(self, ray_tmp_dir: str):
        super().__init__(ray_tmp_dir)


class KwargsImageURIPlugin(ImageURIPlugin):
    def __init__(self, ray_tmp_dir: str, **kwargs):
        super().__init__(ray_tmp_dir, **kwargs)


class FailingImageURIPlugin(ImageURIPlugin):
    def __init__(self, ray_tmp_dir: str, logs_dir: str):
        raise TypeError("plugin constructor failed")


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


def test_image_uri_plugin_receives_logs_dir():
    plugin = _create_image_uri_plugin(
        ImageURIPlugin,
        "/tmp/ray",
        "/var/log/ray",
        logging.getLogger(__name__),
    )

    assert plugin._logs_dir == "/var/log/ray"


def test_image_uri_plugin_accepting_kwargs_receives_logs_dir():
    plugin = _create_image_uri_plugin(
        KwargsImageURIPlugin,
        "/tmp/ray",
        "/var/log/ray",
        logging.getLogger(__name__),
    )

    assert plugin._logs_dir == "/var/log/ray"


def test_legacy_image_uri_plugin_remains_compatible(caplog):
    with caplog.at_level(logging.WARNING):
        plugin = _create_image_uri_plugin(
            LegacyImageURIPlugin,
            "/tmp/ray",
            "/var/log/ray",
            logging.getLogger(__name__),
        )

    assert plugin._ray_tmp_dir == "/tmp/ray"
    assert plugin._logs_dir is None
    assert "does not accept the logs_dir keyword" in caplog.text


def test_image_uri_plugin_constructor_type_error_is_not_hidden():
    with pytest.raises(TypeError, match="plugin constructor failed"):
        _create_image_uri_plugin(
            FailingImageURIPlugin,
            "/tmp/ray",
            "/var/log/ray",
            logging.getLogger(__name__),
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
