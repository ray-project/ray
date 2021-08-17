from pathlib import Path

import pytest
from ray.util.sgd.v2.utils import construct_path, construct_path_with_default


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


@pytest.mark.parametrize("path", [None, "/a", "~/a", "a"])
@pytest.mark.parametrize("parent_path", ["/b", "~/b", "b"])
@pytest.mark.parametrize("default_path", ["/c", "~/c", "c"])
def test_construct_path_with_default(path, parent_path, default_path):
    path = Path(path) if path is not None else None
    parent_path = Path(parent_path)
    default_path = Path(default_path)
    result = construct_path_with_default(path, parent_path, default_path)
    if path is None:
        assert result == construct_path(default_path, parent_path)
    else:
        assert result == construct_path(path, parent_path)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
