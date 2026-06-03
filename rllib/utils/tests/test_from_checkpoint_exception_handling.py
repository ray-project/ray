"""
Tests for exception handling in Checkpointable.from_checkpoint().

When loading the class constructor from a checkpoint's pickle file:
- ValueError, AttributeError, ImportError → caught, fall back to `cls`
  (covers cross-version pickle incompat and renamed/moved classes)
- All other exceptions → propagate to the user, so genuinely broken
  checkpoints fail loudly instead of silently returning an empty module.

See: https://github.com/ray-project/ray/issues/63093
"""

from unittest import mock

import pytest

from ray.rllib.utils.checkpoints import Checkpointable


class _DummyCheckpointable(Checkpointable):
    """Minimal Checkpointable implementation for testing from_checkpoint."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def get_state(self, components=None, *, not_components=None, **kwargs):
        return {}

    def set_state(self, state):
        pass

    def get_ctor_args_and_kwargs(self):
        return ((), {})

    def restore_from_path(self, path, filesystem=None, **kwargs):
        # No-op so the fallback path completes without touching disk.
        pass


# Exceptions raised during pickle.load that SHOULD be caught (fall back to cls).
CAUGHT = [
    pytest.param(ValueError, id="value_error"),
    pytest.param(AttributeError, id="attribute_error"),
    pytest.param(ImportError, id="import_error"),
    pytest.param(ModuleNotFoundError, id="module_not_found_error"),
]

# Exceptions raised during pickle.load that SHOULD propagate to the user.
PROPAGATED = [
    pytest.param(EOFError, id="eof_error"),
    pytest.param(RuntimeError, id="runtime_error"),
    pytest.param(KeyError, id="key_error"),
]


@pytest.fixture
def patched_filesystem():
    """
    Patch the pyarrow filesystem open so from_checkpoint doesn't touch disk.
    pickle.load is patched separately per-test to raise the target exception.
    """
    with mock.patch(
        "ray.rllib.utils.checkpoints.pyarrow.fs.FileSystem.from_uri"
    ) as from_uri:
        fs = mock.MagicMock()
        # open_input_stream is used as a context manager
        fs.open_input_stream.return_value.__enter__.return_value = mock.MagicMock()
        from_uri.return_value = (fs, "tmp/ckpt")
        yield fs


@pytest.mark.parametrize("exc_type", CAUGHT)
def test_caught_exceptions_fall_back_to_cls(patched_filesystem, exc_type):
    """
    ValueError / AttributeError / ImportError / ModuleNotFoundError during
    pickle.load should be caught and the method should fall back to
    constructing `cls`, returning a valid object.
    """
    with mock.patch(
        "ray.rllib.utils.checkpoints.pickle.load", side_effect=exc_type("boom")
    ):
        obj = _DummyCheckpointable.from_checkpoint("gs://tmp/ckpt")

    assert isinstance(obj, _DummyCheckpointable)


@pytest.mark.parametrize("exc_type", PROPAGATED)
def test_other_exceptions_propagate(patched_filesystem, exc_type):
    """
    Exceptions other than the cross-version / renamed-class set should
    propagate, so genuinely broken checkpoints fail loudly.
    """
    with mock.patch(
        "ray.rllib.utils.checkpoints.pickle.load", side_effect=exc_type("boom")
    ):
        with pytest.raises(exc_type):
            _DummyCheckpointable.from_checkpoint("gs://tmp/ckpt")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
