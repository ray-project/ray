"""Integration tests for ``Sandbox.filesystem``.

Parametrized over a busybox image and a GNU coreutils image, because the
filesystem layer is shell scripts and the two toolchains differ. A regression
in portability shows up as one image failing while the other passes.

These need runsc and a Ray cluster, so they run only under ``TEST_SANDBOX=1``.
"""

import asyncio
import hashlib
import sys
import time

import pytest

import ray
from ray.experimental.sandbox import modal
from ray.experimental.sandbox.modal.exception import (
    InvalidError,
    SandboxFilesystemDirectoryNotEmptyError,
    SandboxFilesystemError,
    SandboxFilesystemFileTooLargeError,
    SandboxFilesystemIsADirectoryError,
    SandboxFilesystemNotADirectoryError,
    SandboxFilesystemNotFoundError,
    SandboxFilesystemPathAlreadyExistsError,
)

# busybox applets on one side, GNU coreutils on the other.
IMAGES = ["busybox:latest", "debian:stable-slim"]


@pytest.fixture(scope="module", autouse=True)
def ray_cluster():
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    yield


@pytest.fixture(params=IMAGES, ids=lambda image: image.split(":")[0])
def fs(request):
    """A filesystem namespace on a fresh sandbox, one per image."""
    sandbox = modal.Sandbox.create(image=request.param, timeout=180)
    try:
        yield sandbox.filesystem
    finally:
        sandbox.terminate()


# -- text and bytes --------------------------------------------------------


def test_write_text_then_read_text(fs):
    fs.write_text("Hello, world!\n", "/tmp/hello.txt")
    assert fs.read_text("/tmp/hello.txt") == "Hello, world!\n"


def test_write_bytes_then_read_bytes(fs):
    payload = bytes(range(256))
    fs.write_bytes(payload, "/tmp/blob.bin")
    assert fs.read_bytes("/tmp/blob.bin") == payload


def test_write_creates_missing_parent_directories(fs):
    fs.write_text("nested\n", "/tmp/a/b/c/deep.txt")
    assert fs.read_text("/tmp/a/b/c/deep.txt") == "nested\n"


def test_write_overwrites_existing_content(fs):
    fs.write_text("first version, longer\n", "/tmp/over.txt")
    fs.write_text("second\n", "/tmp/over.txt")
    assert fs.read_text("/tmp/over.txt") == "second\n"


def test_read_text_decodes_utf8(fs):
    fs.write_text("café αβγ 日本語\n", "/tmp/utf8.txt")
    assert fs.read_text("/tmp/utf8.txt") == "café αβγ 日本語\n"


def test_a_large_file_round_trips(fs):
    payload = ("x" * 1023 + "\n") * 512  # 512 KiB
    fs.write_text(payload, "/tmp/large.txt")
    assert fs.read_text("/tmp/large.txt") == payload


@pytest.mark.parametrize("data", [1, None, ["a"]])
def test_write_bytes_rejects_non_bytes(fs, data):
    with pytest.raises(TypeError, match="bytes-like"):
        fs.write_bytes(data, "/tmp/bad.bin")


@pytest.mark.parametrize("data", [b"bytes", 42, None])
def test_write_text_rejects_non_str(fs, data):
    with pytest.raises(TypeError, match="must be a str"):
        fs.write_text(data, "/tmp/bad.txt")


# -- stat and listing ------------------------------------------------------


def test_stat_describes_a_file(fs):
    fs.write_text("123456\n", "/tmp/statme.txt")
    info = fs.stat("/tmp/statme.txt")
    assert info.name == "statme.txt"
    assert info.path == "/tmp/statme.txt"
    assert info.is_file()
    assert info.size == 7
    assert info.permissions == f"{info.mode & 0o7777:04o}"
    assert info.modified_time > 0
    assert info.symlink_target is None


def test_stat_describes_a_directory(fs):
    fs.make_directory("/tmp/adir")
    info = fs.stat("/tmp/adir")
    assert info.is_dir() and not info.is_file()


def _create_symlink(fs, target, link):
    """Create a symlink in the sandbox; the filesystem API has no ``ln``."""
    result = ray.get(
        fs._impl._sandbox._actor.exec_collect.remote(["ln", "-sf", target, link])
    )
    assert result["returncode"] == 0, result["stderr"]


def test_stat_describes_a_symlink_not_its_target(fs):
    fs.write_text("target content\n", "/tmp/real.txt")
    _create_symlink(fs, "/tmp/real.txt", "/tmp/link.txt")
    info = fs.stat("/tmp/link.txt")
    assert info.is_symlink()
    assert info.symlink_target == "/tmp/real.txt"


def test_list_files_reports_every_entry(fs):
    fs.write_text("a", "/tmp/listme/visible.txt")
    fs.write_text("b", "/tmp/listme/.hidden")
    fs.make_directory("/tmp/listme/subdir")

    entries = {entry.name: entry for entry in fs.list_files("/tmp/listme")}
    assert set(entries) == {"visible.txt", ".hidden", "subdir"}
    assert entries["subdir"].is_dir()
    assert entries["visible.txt"].is_file()
    assert entries["visible.txt"].path == "/tmp/listme/visible.txt"


def test_list_files_on_an_empty_directory_is_empty(fs):
    fs.make_directory("/tmp/emptydir")
    assert fs.list_files("/tmp/emptydir") == []


def test_paths_with_spaces_are_handled(fs):
    path = "/tmp/a dir with spaces/a file.txt"
    fs.write_text("spaced\n", path)
    assert fs.read_text(path) == "spaced\n"
    assert fs.stat(path).name == "a file.txt"
    names = [entry.name for entry in fs.list_files("/tmp/a dir with spaces")]
    assert names == ["a file.txt"]


# -- directories and removal ----------------------------------------------


def test_make_directory_creates_parents_by_default(fs):
    fs.make_directory("/tmp/x/y/z")
    assert fs.stat("/tmp/x/y/z").is_dir()


def test_make_directory_with_parents_is_idempotent(fs):
    fs.make_directory("/tmp/idem")
    fs.make_directory("/tmp/idem")


def test_make_directory_without_parents_rejects_an_existing_path(fs):
    fs.make_directory("/tmp/exists")
    with pytest.raises(SandboxFilesystemPathAlreadyExistsError):
        fs.make_directory("/tmp/exists", create_parents=False)


def test_make_directory_without_parents_requires_the_parent(fs):
    with pytest.raises(SandboxFilesystemNotFoundError):
        fs.make_directory("/tmp/absent/child", create_parents=False)


def test_remove_deletes_a_file(fs):
    fs.write_text("x", "/tmp/gone.txt")
    fs.remove("/tmp/gone.txt")
    with pytest.raises(SandboxFilesystemNotFoundError):
        fs.stat("/tmp/gone.txt")


def test_remove_deletes_an_empty_directory(fs):
    fs.make_directory("/tmp/emptied")
    fs.remove("/tmp/emptied")
    with pytest.raises(SandboxFilesystemNotFoundError):
        fs.stat("/tmp/emptied")


def test_remove_refuses_a_non_empty_directory(fs):
    fs.write_text("x", "/tmp/full/child.txt")
    with pytest.raises(SandboxFilesystemDirectoryNotEmptyError):
        fs.remove("/tmp/full")
    assert fs.stat("/tmp/full").is_dir()


def test_remove_recursive_deletes_a_tree(fs):
    fs.write_text("x", "/tmp/tree/nested/child.txt")
    fs.remove("/tmp/tree", recursive=True)
    with pytest.raises(SandboxFilesystemNotFoundError):
        fs.stat("/tmp/tree")


# -- error classification --------------------------------------------------


def test_reading_a_missing_path_raises_not_found(fs):
    with pytest.raises(SandboxFilesystemNotFoundError):
        fs.read_text("/tmp/definitely-absent")


def test_reading_a_directory_raises_is_a_directory(fs):
    fs.make_directory("/tmp/readdir")
    with pytest.raises(SandboxFilesystemIsADirectoryError):
        fs.read_bytes("/tmp/readdir")


def test_writing_onto_a_directory_raises_is_a_directory(fs):
    fs.make_directory("/tmp/writedir")
    with pytest.raises(SandboxFilesystemIsADirectoryError):
        fs.write_text("x", "/tmp/writedir")


def test_writing_under_a_file_raises_not_a_directory(fs):
    fs.write_text("x", "/tmp/afile.txt")
    with pytest.raises(SandboxFilesystemNotADirectoryError):
        fs.write_text("y", "/tmp/afile.txt/child")


def test_listing_a_file_raises_not_a_directory(fs):
    fs.write_text("x", "/tmp/notadir.txt")
    with pytest.raises(SandboxFilesystemNotADirectoryError):
        fs.list_files("/tmp/notadir.txt")


def test_listing_a_missing_path_raises_not_found(fs):
    with pytest.raises(SandboxFilesystemNotFoundError):
        fs.list_files("/tmp/absent-dir")


def test_removing_a_missing_path_raises_not_found(fs):
    with pytest.raises(SandboxFilesystemNotFoundError):
        fs.remove("/tmp/absent-file")


@pytest.mark.parametrize(
    "call",
    [
        lambda fs: fs.read_text("relative.txt"),
        lambda fs: fs.read_bytes("relative.txt"),
        lambda fs: fs.write_text("x", "relative.txt"),
        lambda fs: fs.write_bytes(b"x", "relative.txt"),
        lambda fs: fs.stat("relative.txt"),
        lambda fs: fs.list_files("relative"),
        lambda fs: fs.make_directory("relative"),
        lambda fs: fs.remove("relative"),
    ],
)
def test_relative_paths_are_refused(fs, call):
    with pytest.raises(InvalidError, match="absolute remote_path"):
        call(fs)


def test_watch_is_not_supported(fs):
    with pytest.raises(NotImplementedError, match="watch"):
        fs.watch("/tmp")


# -- local transfers -------------------------------------------------------


def test_copy_from_local_then_back(fs, tmp_path):
    source = tmp_path / "upload.bin"
    payload = bytes(range(256)) * 512  # 128 KiB, several stream chunks
    source.write_bytes(payload)

    fs.copy_from_local(source, "/tmp/uploaded.bin")
    assert fs.read_bytes("/tmp/uploaded.bin") == payload

    destination = tmp_path / "downloaded" / "out.bin"
    fs.copy_to_local("/tmp/uploaded.bin", destination)
    assert destination.read_bytes() == payload


def test_copy_to_local_overwrites_atomically(fs, tmp_path):
    destination = tmp_path / "out.txt"
    destination.write_text("stale content that is longer")
    fs.write_text("fresh\n", "/tmp/fresh.txt")
    fs.copy_to_local("/tmp/fresh.txt", destination)
    assert destination.read_text() == "fresh\n"


def test_copy_to_local_leaves_no_file_when_the_source_is_missing(fs, tmp_path):
    destination = tmp_path / "never.txt"
    with pytest.raises(SandboxFilesystemNotFoundError):
        fs.copy_to_local("/tmp/absent-source", destination)
    assert not destination.exists()
    assert list(tmp_path.iterdir()) == []


def test_copy_from_local_reports_a_missing_local_file(fs, tmp_path):
    """A local error stays a local error, not a Sandbox error."""
    with pytest.raises(FileNotFoundError):
        fs.copy_from_local(tmp_path / "absent", "/tmp/whatever")


# -- measured against Modal --------------------------------------------------


@pytest.fixture(params=IMAGES, ids=lambda image: image.split(":")[0])
def sandbox(request):
    """A fresh sandbox, for tests that set things up with exec first."""
    sb = modal.Sandbox.create(image=request.param, timeout=180)
    try:
        yield sb
    finally:
        sb.terminate()


def sh(sandbox, script: str) -> str:
    process = sandbox.exec("sh", "-c", script)
    output = process.stdout.read()
    assert process.wait() == 0, process.stderr.read()
    return output.strip()


def test_list_files_is_sorted_by_name_hidden_entries_included(fs):
    names = [".h", "B", "Zed", "_x", "a", "~t"]
    for name in reversed(names):
        fs.write_text("", f"/tmp/order/{name}")
    assert [e.name for e in fs.list_files("/tmp/order")] == names


def test_listed_paths_join_the_directory_with_one_slash(fs):
    fs.write_text("", "/tmp/joined/f")
    assert [e.path for e in fs.list_files("/tmp/joined/")] == ["/tmp/joined/f"]
    root = [e.path for e in fs.list_files("/")]
    assert "/tmp" in root
    assert not any(path.startswith("//") for path in root)


@pytest.mark.parametrize(
    "call,expected",
    [
        (lambda fs: fs.stat("/tmp/blocker/x/y"), SandboxFilesystemNotADirectoryError),
        (
            lambda fs: fs.list_files("/tmp/blocker/x"),
            SandboxFilesystemNotADirectoryError,
        ),
        (
            lambda fs: fs.make_directory("/tmp/blocker/x/y", create_parents=False),
            SandboxFilesystemNotADirectoryError,
        ),
        (
            lambda fs: fs.write_text("x", "/tmp/blocker/x/y"),
            SandboxFilesystemNotADirectoryError,
        ),
        # Modal's generic error for these two, not a more specific one.
        (lambda fs: fs.read_text("/tmp/blocker/x"), SandboxFilesystemError),
        (lambda fs: fs.remove("/tmp/blocker/x"), SandboxFilesystemError),
    ],
)
def test_a_file_anywhere_up_the_path_is_reported_as_modal_reports_it(
    fs, call, expected
):
    fs.write_text("x", "/tmp/blocker")
    with pytest.raises(SandboxFilesystemError) as excinfo:
        call(fs)
    assert type(excinfo.value) is expected


def test_make_directory_without_parents_takes_a_trailing_slash(fs):
    fs.make_directory("/tmp/slashed/", create_parents=False)
    assert fs.stat("/tmp/slashed").is_dir()


def test_a_symlink_target_is_reported_exactly(sandbox):
    sh(sandbox, 'ln -s "it\'s -> café" /tmp/awkward')
    assert sandbox.filesystem.stat("/tmp/awkward").symlink_target == "it's -> café"
    (entry,) = [e for e in sandbox.filesystem.list_files("/tmp") if e.name == "awkward"]
    assert entry.symlink_target == "it's -> café"


def test_an_owner_with_no_name_is_reported_by_number(sandbox):
    sh(sandbox, "touch /tmp/owned && chown 12345:23456 /tmp/owned")
    info = sandbox.filesystem.stat("/tmp/owned")
    assert (info.owner, info.group) == ("12345", "23456")


def test_a_file_over_modals_limit_is_refused_without_reading_it(sandbox):
    sh(sandbox, "truncate -s 6G /tmp/sparse")
    started = time.monotonic()
    with pytest.raises(SandboxFilesystemFileTooLargeError, match="6442450944 bytes"):
        sandbox.filesystem.read_bytes("/tmp/sparse")
    assert time.monotonic() - started < 10


def test_a_file_past_one_reply_is_streamed_whole(sandbox):
    """Past 16 MiB a read streams; Ray's own limit used to stop at 128 MiB."""
    digest = sh(
        sandbox,
        "head -c 41943040 /dev/urandom > /tmp/big && md5sum /tmp/big | cut -d' ' -f1",
    )
    data = sandbox.filesystem.read_bytes("/tmp/big")
    assert len(data) == 40 * 1024 * 1024
    assert hashlib.md5(data).hexdigest() == digest


def test_a_large_copy_to_local_arrives_whole(sandbox, tmp_path):
    """The copy is paced by this side. Unpaced, it depended on the actor
    spilling what had not been read yet, and on a disk too full to spill a
    256 MiB copy failed within a second."""
    digest = sh(
        sandbox,
        "head -c 67108864 /dev/urandom > /tmp/big && md5sum /tmp/big | cut -d' ' -f1",
    )
    destination = tmp_path / "big.bin"
    sandbox.filesystem.copy_to_local("/tmp/big", destination)
    assert hashlib.md5(destination.read_bytes()).hexdigest() == digest


def test_an_aborted_upload_leaves_the_destination_and_nothing_running(sandbox):
    """Killing the runsc client alone left the upload's `cat` waiting inside the
    sandbox, ready to move a partial file into place once its stdin closed."""
    fs = sandbox.filesystem
    fs.write_text("old contents\n", "/tmp/dest")

    async def failing_chunks():
        for _ in range(3):
            yield b"n" * 65536
        raise OSError("local read failed")

    with pytest.raises(OSError, match="local read failed"):
        asyncio.run(fs._impl._upload(failing_chunks(), "/tmp/dest"))

    assert fs.read_text("/tmp/dest") == "old contents\n"
    # "tm[p]" so this check's own sh and grep, whose command lines carry the
    # pattern, do not count themselves.
    leftovers = sh(
        sandbox,
        "ls -a /tmp | grep -c 'ray-sandbox-tm[p]' || true; "
        "grep -l 'ray-sandbox-tm[p]' /proc/[0-9]*/cmdline 2>/dev/null | wc -l",
    )
    assert leftovers.split() == ["0", "0"], "a temp file or the upload survived"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
