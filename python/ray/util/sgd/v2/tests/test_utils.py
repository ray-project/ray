from pathlib import Path

from ray.util.sgd.v2.utils import construct_path


def test_construct_paths():
    assert construct_path(Path("/a"), Path("/b"), Path("c")) == Path("/a")
    assert construct_path(Path("/a"), Path("~/b"), Path("c")) == Path("/a")
    assert construct_path(Path("/a"), Path("b"), Path("c")) == Path("/a")

    assert construct_path(Path("~/a"), Path("~/b"),
                          Path("c")) == Path("~/a").expanduser()
    assert construct_path(Path("~/a"), Path("/b"),
                          Path("c")) == Path("~/a").expanduser()
    assert construct_path(Path("~/a"), Path("b"),
                          Path("c")) == Path("~/a").expanduser()

    assert construct_path(Path("a"), Path("/b"), Path("c")) == Path("/b/a")
    assert construct_path(Path("a"), Path("~/b"),
                          Path("c")) == Path("~/b/a").expanduser()
    assert construct_path(Path("a"), Path("b"),
                          Path("c")) == Path("b/a").resolve()

    assert construct_path(None, Path("/b"), Path("/c")) == Path("/c")
    assert construct_path(None, Path("~/b"), Path("/c")) == Path("/c")
    assert construct_path(None, Path("b"), Path("/c")) == Path("/c")

    assert construct_path(None, Path("~/b"),
                          Path("~/c")) == Path("~/c").expanduser()
    assert construct_path(None, Path("/b"),
                          Path("~/c")) == Path("~/c").expanduser()
    assert construct_path(None, Path("b"),
                          Path("~/c")) == Path("~/c").expanduser()

    assert construct_path(None, Path("/b"), Path("c")) == Path("/b/c")
    assert construct_path(None, Path("~/b"),
                          Path("c")) == Path("~/b/c").expanduser()
    assert construct_path(None, Path("b"), Path("c")) == Path("b/c").resolve()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
