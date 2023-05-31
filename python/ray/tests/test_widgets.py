import logging
from unittest import mock

import pytest
import ray
from ray.widgets.util import repr_with_fallback, _can_display_ipywidgets


@pytest.fixture
def logs(propagate_logs, caplog):
    """A log fixture which captures logs during a test."""
    caplog.set_level(logging.INFO)
    return caplog


@pytest.fixture
def fancy_mimebundle():
    return {
        "fancy/mimetype": "A fancy repr",
        "text/plain": "A simple repr",
    }


@mock.patch.object(ray.widgets.util, "in_notebook")
@mock.patch("importlib.util.find_spec")
@mock.patch("importlib.import_module")
def test_repr_with_fallback_missing(
    mock_import_module,
    mock_find_spec,
    mock_in_notebook,
    logs,
    fancy_mimebundle,
):
    """Test that missing notebook dependencies trigger a log message."""

    def raise_import_error(*args):
        raise ImportError

    mock_import_module.side_effect = raise_import_error
    mock_find_spec.return_value = None
    mock_in_notebook.return_value = True

    class DummyObject:
        def __repr__(self):
            return "dummy repr"

        @repr_with_fallback(["somedep", "8"])
        def _repr_mimebundle_(self, **kwargs):
            return fancy_mimebundle

    result = DummyObject()._repr_mimebundle_()
    assert result == {"text/plain": "dummy repr"}
    assert "Missing packages:" in logs.records[-1].msg


@mock.patch.object(ray.widgets.util, "in_notebook")
@mock.patch("importlib.util.find_spec")
@mock.patch("importlib.import_module")
def test_repr_with_fallback_outdated(
    mock_import_module,
    mock_find_spec,
    mock_in_notebook,
    logs,
    fancy_mimebundle,
):
    """Test that outdated notebook dependencies trigger a log message."""

    class MockDep:
        __version__ = "7.0.0"

    mock_import_module.return_value = MockDep()
    mock_find_spec.return_value = "a valid import spec"
    mock_in_notebook.return_value = True

    class DummyObject:
        def __repr__(self):
            return "dummy repr"

        @repr_with_fallback(["somedep", "8"])
        def _repr_mimebundle_(self, **kwargs):
            return fancy_mimebundle

    result = DummyObject()._repr_mimebundle_()
    assert result == {"text/plain": "dummy repr"}
    assert "Outdated packages:" in logs.records[-1].msg


@mock.patch.object(ray.widgets.util, "_can_display_ipywidgets")
@mock.patch("importlib.util.find_spec")
@mock.patch("importlib.import_module")
def test_repr_with_fallback_valid(
    mock_import_module,
    mock_find_spec,
    mock_can_display_ipywidgets,
    logs,
    fancy_mimebundle,
):
    """Test that valid notebook dependencies don't trigger a log message."""

    class MockDep:
        __version__ = "8.0.0"

    mock_import_module.return_value = MockDep()
    mock_find_spec.return_value = "a valid import spec"
    mock_can_display_ipywidgets.return_value = True

    class DummyObject:
        def __repr__(self):
            return "dummy repr"

        @repr_with_fallback(["somedep", "8"])
        def _repr_mimebundle_(self, **kwargs):
            return fancy_mimebundle

    result = DummyObject()._repr_mimebundle_()
    assert len(logs.records) == 0
    assert result == fancy_mimebundle


@mock.patch.object(ray.widgets.util, "_can_display_ipywidgets")
@mock.patch("importlib.util.find_spec")
@mock.patch("importlib.import_module")
def test_repr_with_fallback_invalid_shell(
    mock_import_module,
    mock_find_spec,
    mock_can_display_ipywidgets,
    logs,
    fancy_mimebundle,
):
    """Test that the mimebundle is correctly stripped if run in an invalid shell."""

    class MockDep:
        __version__ = "8.0.0"

    mock_import_module.return_value = MockDep()
    mock_find_spec.return_value = "a valid import spec"
    mock_can_display_ipywidgets.return_value = False

    class DummyObject:
        def __repr__(self):
            return "dummy repr"

        @repr_with_fallback(["somedep", "8"])
        def _repr_mimebundle_(self, **kwargs):
            return fancy_mimebundle

    result = DummyObject()._repr_mimebundle_()
    assert len(logs.records) == 0
    assert result == {"text/plain": "dummy repr"}


@mock.patch.object(ray.widgets.util, "_get_ipython_shell_name")
@mock.patch("importlib.util.find_spec")
@mock.patch("importlib.import_module")
@pytest.mark.parametrize(
    "shell,can_display",
    [
        ("ZMQInteractiveShell", True),
        ("google.colab.kernel", False),
        ("TerminalInteractiveShell", False),
        ("", False),
    ],
)
def test_can_display_ipywidgets(
    mock_import_module,
    mock_find_spec,
    mock_get_ipython_shell_name,
    shell,
    can_display,
):
    class MockDep:
        __version__ = "8.0.0"

    mock_import_module.return_value = MockDep()
    mock_find_spec.return_value = "a valid import spec"
    mock_get_ipython_shell_name.return_value = shell

    assert _can_display_ipywidgets(["somedep", "8"], message="") == can_display
    mock_get_ipython_shell_name.assert_called()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
