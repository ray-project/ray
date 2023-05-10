from unittest import mock

import pytest
from ray.widgets.util import ensure_notebook_deps, repr_fallback_if_colab


@mock.patch("importlib.import_module")
def test_ensure_notebook_dep_missing(mock_import_module, caplog):
    """Test that missing notebook dependencies trigger a warning."""

    class MockDep:
        __version__ = "8.0.0"

    def raise_import_error(*args):
        raise ImportError

    mock_import_module.return_value = MockDep()
    mock_import_module.side_effect = raise_import_error

    class DummyObject:
        @ensure_notebook_deps(["somedep", "8"])
        def dummy_ipython_display(self):
            return

    DummyObject().dummy_ipython_display()

    assert "Missing packages:" in caplog.records[-1].msg


@mock.patch("importlib.import_module")
def test_ensure_notebook_dep_outdated(mock_import_module, caplog):
    """Test that outdated notebook dependencies trigger a warning."""

    class MockDep:
        __version__ = "7.0.0"

    mock_import_module.return_value = MockDep()

    class DummyObject:
        @ensure_notebook_deps(["somedep", "8"])
        def dummy_ipython_display():
            return

    DummyObject().dummy_ipython_display()

    assert "Outdated packages:" in caplog.records[-1].msg


@mock.patch("importlib.import_module")
def test_ensure_notebook_valid(mock_import_module, caplog):
    """Test that valid notebook dependencies don't trigger a warning."""

    class MockDep:
        __version__ = "8.0.0"

    mock_import_module.return_value = MockDep()

    class DummyObject:
        @ensure_notebook_deps(["somedep", "8"])
        def dummy_ipython_display(self):
            return

    DummyObject().dummy_ipython_display()

    assert len(caplog.records) == 0


@pytest.mark.parametrize(
    "kernel",
    [
        ("google.colab.kernel"),
        ("normal.ipython.kernel"),
    ],
)
def test_repr_fallback_if_colab(kernel):
    """Test that the mimebundle is correctly stripped if run in google colab."""
    pytest.importorskip("IPython", reason="IPython is not installed.")
    with mock.patch("IPython.get_ipython") as mock_get_ipython:
        mock_get_ipython.return_value = kernel

        class DummyObject:
            @repr_fallback_if_colab
            def _repr_mimebundle_(self, **kwargs):
                return {
                    "fancy/mimetype": "A fancy repr",
                    "text/plain": "A simple repr",
                }

        obj = DummyObject()
        result = obj._repr_mimebundle_()

        assert "text/plain" in result
        if "google.colab" in kernel:
            assert len(result) == 1
        else:
            assert len(result) == 2
            assert "fancy/mimetype"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
