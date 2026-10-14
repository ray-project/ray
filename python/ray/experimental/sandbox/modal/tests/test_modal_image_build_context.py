"""How COPY steps pack local files: the layer key and the archive the node gets.

Needs neither runsc nor a Ray cluster: ``put`` stands in for ``ray.put``.
"""

import io
import os
import sys
import tarfile

import pytest

from ray.experimental.sandbox.image_build import COPY_MTIME
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
    """Records what was uploaded, returning a copy of the bytes as the handle.

    It copies, as ``ray.put`` does: ``pack_copy`` hands it a view of the
    archive that is valid only during the call.
    """

    def __init__(self):
        self.calls = 0

    def __call__(self, data):
        self.calls += 1
        return bytes(data)


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


def test_the_archive_ignores_timestamps_too(tmp_path):
    # The key leaves timestamps out, so the content must as well: otherwise
    # one key would stand for different files depending on who sent them.
    (a,) = _files(tmp_path, a=b"x")
    first = pack_copy(CopySpec(((a, "/a"),)), put=_Put())
    _image_build._HASH_MEMO.clear()
    _image_build._ARCHIVE_REFS.clear()
    os.utime(a, (1, 1))
    second = pack_copy(CopySpec(((a, "/a"),)), put=_Put())
    assert second.archive == first.archive
    with tarfile.open(fileobj=io.BytesIO(second.archive)) as tar:
        assert tar.getmembers()[0].mtime == COPY_MTIME


def test_large_files_round_trip(tmp_path):
    # Bigger than one read chunk, so hashing and packing both stream.
    data = os.urandom(3 * (1 << 20) + 17)
    (a,) = _files(tmp_path, a=data)
    step = pack_copy(CopySpec(((a, "/big"),)), put=_Put())
    with tarfile.open(fileobj=io.BytesIO(step.archive)) as tar:
        assert tar.extractfile("big").read() == data


def test_the_upload_memo_is_bounded_by_bytes(tmp_path, monkeypatch):
    # A small file packs into one 10 KiB tar record; room for two of them.
    monkeypatch.setattr(_image_build, "_ARCHIVE_REFS_MAX_BYTES", 25 * 1024)
    for i in range(4):
        (path,) = _files(tmp_path, **{f"f{i}": bytes([i]) * 100})
        pack_copy(CopySpec(((path, f"/f{i}"),)), put=_Put())
    sizes = [size for _, size in _image_build._ARCHIVE_REFS.values()]
    assert sizes == [10240, 10240]


def test_a_missing_file_names_itself(tmp_path):
    spec = CopySpec(((str(tmp_path / "gone.txt"), "/gone.txt"),))
    with pytest.raises(FileNotFoundError, match="gone.txt"):
        pack_copy(spec, put=_Put())


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
