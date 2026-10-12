"""How COPY steps pack local files: the layer key and the archive the node gets.

Needs neither runsc nor a Ray cluster: ``put`` stands in for ``ray.put``.
"""

import io
import os
import sys
import tarfile

import pytest

from ray.experimental.sandbox.modal import _image_build
from ray.experimental.sandbox.modal._image_build import CopySpec, pack_copy


@pytest.fixture(autouse=True)
def _fresh_memos():
    _image_build._HASH_MEMO.clear()
    _image_build._ARCHIVE_REFS.clear()
    yield
    _image_build._HASH_MEMO.clear()
    _image_build._ARCHIVE_REFS.clear()


class _Put:
    """Records what was uploaded, returning the bytes as the handle."""

    def __init__(self):
        self.calls = 0

    def __call__(self, data: bytes):
        self.calls += 1
        return data


def _files(tmp_path, **contents):
    paths = []
    for name, data in contents.items():
        path = tmp_path / name
        path.write_bytes(data)
        paths.append(str(path))
    return paths


def test_the_archive_holds_each_file_at_its_destination(tmp_path):
    a, b = _files(tmp_path, a=b"aa", b=b"bbb")
    os.chmod(b, 0o750)
    step = pack_copy(CopySpec(((b, "/app/z/b"), (a, "/app/a")), "COPY"), put=_Put())
    with tarfile.open(fileobj=io.BytesIO(step.archive)) as tar:
        members = tar.getmembers()
        # Sorted by destination, relative to /, files only: the node resolves
        # and writes the directories against the image.
        assert [m.name for m in members] == ["app/a", "app/z/b"]
        assert all(m.isreg() and m.uid == 0 and m.gid == 0 for m in members)
        assert members[1].mode == 0o750
        assert tar.extractfile(members[0]).read() == b"aa"


def test_the_digest_ignores_timestamps(tmp_path):
    (a,) = _files(tmp_path, a=b"x")
    first = pack_copy(CopySpec(((a, "/a"),)), put=_Put()).digest
    _image_build._HASH_MEMO.clear()
    os.utime(a, (1, 1))
    assert pack_copy(CopySpec(((a, "/a"),)), put=_Put()).digest == first


@pytest.mark.parametrize(
    "change",
    [
        lambda path: open(path, "wb").write(b"changed"),
        lambda path: os.chmod(path, 0o700),
    ],
)
def test_the_digest_follows_content_and_mode(tmp_path, change):
    (a,) = _files(tmp_path, a=b"x")
    first = pack_copy(CopySpec(((a, "/a"),)), put=_Put()).digest
    change(a)
    assert pack_copy(CopySpec(((a, "/a"),)), put=_Put()).digest != first


def test_the_digest_follows_the_destination(tmp_path):
    (a,) = _files(tmp_path, a=b"x")
    first = pack_copy(CopySpec(((a, "/a"),)), put=_Put()).digest
    assert pack_copy(CopySpec(((a, "/b"),)), put=_Put()).digest != first


def test_unchanged_files_are_neither_reread_nor_reuploaded(tmp_path, monkeypatch):
    a, b = _files(tmp_path, a=b"a", b=b"b")
    spec = CopySpec(((a, "/a"), (b, "/b")))
    put = _Put()
    first = pack_copy(spec, put=put)

    def no_reading(*args, **kwargs):
        raise AssertionError("an unchanged file was read again")

    monkeypatch.setattr("builtins.open", no_reading)
    second = pack_copy(spec, put=put)
    assert put.calls == 1
    assert second.digest == first.digest and second.archive is first.archive


def test_a_missing_file_names_itself(tmp_path):
    spec = CopySpec(((str(tmp_path / "gone.txt"), "/gone.txt"),))
    with pytest.raises(FileNotFoundError, match="gone.txt"):
        pack_copy(spec, put=_Put())


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
