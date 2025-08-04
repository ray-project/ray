from pathlib import Path

import pytest

import ray
from ray.train._internal.utils import construct_path
from ray.train.constants import _get_ray_train_session_dir


@pytest.fixture
def ray_init_custom_tmpdir():
    custom_tmpdir = "/tmp/custom"
    ray.init(_temp_dir=custom_tmpdir)
    yield custom_tmpdir
    ray.shutdown()


def test_construct_path():
    assert construct_path(Path("/a"), Path("/b")) == Path("/a")
    assert construct_path(Path("/a"), Path("~/b")) == Path("/a")
    assert construct_path(Path("/a"), Path("b")) == Path("/a")

    assert construct_path(Path("~/a"), Path("~/b")) == Path("~/a").expanduser()
    assert construct_path(Path("~/a"), Path("/b")) == Path("~/a").expanduser()
    assert construct_path(Path("~/a"), Path("b")) == Path("~/a").expanduser()

    assert construct_path(Path("a"), Path("/b")) == Path("/b/a")
    assert construct_path(Path("a"), Path("~/b")) == Path("~/b/a").expanduser()
    assert construct_path(Path("a"), Path("b")) == Path("b/a").resolve()


def test_customize_local_staging_path(ray_init_custom_tmpdir):
    """Test that the staging directory where driver artifacts are written
    before being persisted to storage path can be customized."""
    assert str(ray_init_custom_tmpdir) in _get_ray_train_session_dir()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
