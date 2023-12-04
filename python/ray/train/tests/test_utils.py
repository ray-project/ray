from pathlib import Path

from ray.train._internal.utils import construct_path


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


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
