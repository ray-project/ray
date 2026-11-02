"""Integration tests for ``Sandbox.filesystem``.

Parametrized over a busybox image and a GNU coreutils image, because the
filesystem layer is shell scripts and the two toolchains differ. A regression
in portability shows up as one image failing while the other passes.

These need runsc and a Ray cluster, so they run only under ``TEST_SANDBOX=1``.
"""

import asyncio
import hashlib
import os
import sys
import time

import pytest

import ray
from ray.experimental.sandbox import modal
from ray.experimental.sandbox.modal.exception import (
    SandboxFilesystemDirectoryNotEmptyError,
    SandboxFilesystemError,
    SandboxFilesystemFileTooLargeError,
    SandboxFilesystemIsADirectoryError,
    SandboxFilesystemNotADirectoryError,
    SandboxFilesystemNotFoundError,
    SandboxFilesystemPathAlreadyExistsError,
)
from ray.experimental.sandbox.modal.types import FileType

# Sandboxes here run on Modal's default network, network="public", which
# needs slirp4netns and a host that allows a per-sandbox network namespace.
pytestmark = pytest.mark.usefixtures("ensure_slirp4netns")

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


# -- one round trip per family -----------------------------------------------
#
# What each script does, case by case, is test_modal_fs_commands.py's job: it
# runs the same scripts against the local shell. Here each family is driven
# once through the real actor, on both toolchains.


def test_text_and_bytes_round_trip(fs):
    """Into directories that do not exist yet, then over what is there."""
    payload = bytes(range(256))
    fs.write_bytes(payload, "/tmp/a/b/c/blob.bin")
    assert fs.read_bytes("/tmp/a/b/c/blob.bin") == payload
    fs.write_text("café αβγ 日本語\n", "/tmp/a/b/c/blob.bin")
    assert fs.read_text("/tmp/a/b/c/blob.bin") == "café αβγ 日本語\n"


def _create_symlink(fs, target, link):
    """Create a symlink in the sandbox; the filesystem API has no ``ln``."""
    result = ray.get(
        fs._impl._sandbox._actor.exec_collect.remote(["ln", "-sf", target, link])
    )
    assert result["returncode"] == 0, result["stderr"]


def test_stat_describes_files_directories_and_symlinks(fs):
    fs.write_text("123456\n", "/tmp/statme.txt")
    info = fs.stat("/tmp/statme.txt")
    assert info.name == "statme.txt"
    assert info.path == "/tmp/statme.txt"
    assert info.is_file()
    assert info.size == 7
    assert info.permissions == f"{info.mode & 0o7777:04o}"
    assert info.modified_time > 0
    assert info.symlink_target is None

    fs.make_directory("/tmp/adir")
    assert fs.stat("/tmp/adir").is_dir()

    # The link itself, not its target.
    _create_symlink(fs, "/tmp/statme.txt", "/tmp/link.txt")
    info = fs.stat("/tmp/link.txt")
    assert info.is_symlink()
    assert info.symlink_target == "/tmp/statme.txt"


def test_list_files_reports_every_entry_sorted_by_name(fs):
    """Hidden entries included, each typed and with its full path."""
    files = [".h", "B", "Zed", "_x", "a b", "~t"]
    for name in reversed(files):
        fs.write_text("", f"/tmp/list me/{name}")
    fs.make_directory("/tmp/list me/sub")

    entries = fs.list_files("/tmp/list me")
    assert [e.name for e in entries] == [".h", "B", "Zed", "_x", "a b", "sub", "~t"]
    by_name = {e.name: e for e in entries}
    assert by_name["sub"].is_dir()
    assert by_name["a b"].is_file()
    assert by_name["a b"].path == "/tmp/list me/a b"
    assert fs.stat("/tmp/list me/a b").name == "a b"

    fs.make_directory("/tmp/empty")
    assert fs.list_files("/tmp/empty") == []


def test_make_directory(fs):
    fs.make_directory("/tmp/x/y/z")
    assert fs.stat("/tmp/x/y/z").is_dir(), "parents are created by default"
    fs.make_directory("/tmp/x/y/z")
    fs.make_directory("/tmp/slashed/", create_parents=False)
    assert fs.stat("/tmp/slashed").is_dir()
    with pytest.raises(SandboxFilesystemPathAlreadyExistsError):
        fs.make_directory("/tmp/x/y/z", create_parents=False)
    with pytest.raises(SandboxFilesystemNotFoundError):
        fs.make_directory("/tmp/absent/child", create_parents=False)


def test_remove(fs):
    fs.write_text("x", "/tmp/gone.txt")
    fs.make_directory("/tmp/emptied")
    fs.write_text("x", "/tmp/full/child.txt")
    fs.write_text("x", "/tmp/tree/nested/child.txt")

    fs.remove("/tmp/gone.txt")
    fs.remove("/tmp/emptied")
    with pytest.raises(SandboxFilesystemDirectoryNotEmptyError):
        fs.remove("/tmp/full")
    assert fs.stat("/tmp/full").is_dir()
    fs.remove("/tmp/tree", recursive=True)
    for path in ("/tmp/gone.txt", "/tmp/emptied", "/tmp/tree"):
        with pytest.raises(SandboxFilesystemNotFoundError):
            fs.stat(path)


def test_errors_are_classified_as_modal_classifies_them(fs):
    fs.make_directory("/tmp/adir")
    fs.write_text("x", "/tmp/afile")
    cases = [
        (lambda: fs.read_text("/tmp/absent"), SandboxFilesystemNotFoundError),
        (lambda: fs.read_bytes("/tmp/adir"), SandboxFilesystemIsADirectoryError),
        (lambda: fs.write_text("x", "/tmp/adir"), SandboxFilesystemIsADirectoryError),
        (
            lambda: fs.write_text("y", "/tmp/afile/child"),
            SandboxFilesystemNotADirectoryError,
        ),
        (lambda: fs.list_files("/tmp/afile"), SandboxFilesystemNotADirectoryError),
        (lambda: fs.list_files("/tmp/absent"), SandboxFilesystemNotFoundError),
        (lambda: fs.remove("/tmp/absent"), SandboxFilesystemNotFoundError),
    ]
    for call, expected in cases:
        with pytest.raises(expected):
            call()


def test_copy_from_local_then_back(fs, tmp_path):
    source = tmp_path / "upload.bin"
    payload = bytes(range(256)) * 512  # 128 KiB, several stream chunks
    source.write_bytes(payload)

    fs.copy_from_local(source, "/tmp/uploaded.bin")
    assert fs.read_bytes("/tmp/uploaded.bin") == payload

    destination = tmp_path / "downloaded" / "out.bin"
    fs.copy_to_local("/tmp/uploaded.bin", destination)
    assert destination.read_bytes() == payload

    # Over a file already there: replaced whole, not written into.
    fs.write_text("fresh\n", "/tmp/fresh.txt")
    fs.copy_to_local("/tmp/fresh.txt", destination)
    assert destination.read_text() == "fresh\n"

    # A failed copy leaves nothing behind, not even a temporary file.
    with pytest.raises(SandboxFilesystemNotFoundError):
        fs.copy_to_local("/tmp/absent-source", tmp_path / "never.txt")
    assert sorted(p.name for p in tmp_path.iterdir()) == ["downloaded", "upload.bin"]
    assert [p.name for p in destination.parent.iterdir()] == ["out.bin"]


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


def test_a_file_anywhere_up_the_path_is_reported_as_modal_reports_it(fs):
    fs.write_text("x", "/tmp/blocker")
    cases = [
        (lambda: fs.stat("/tmp/blocker/x/y"), SandboxFilesystemNotADirectoryError),
        (lambda: fs.list_files("/tmp/blocker/x"), SandboxFilesystemNotADirectoryError),
        (
            lambda: fs.make_directory("/tmp/blocker/x/y", create_parents=False),
            SandboxFilesystemNotADirectoryError,
        ),
        (
            lambda: fs.write_text("x", "/tmp/blocker/x/y"),
            SandboxFilesystemNotADirectoryError,
        ),
        # Modal's generic error for these two, not a more specific one.
        (lambda: fs.read_text("/tmp/blocker/x"), SandboxFilesystemError),
        (lambda: fs.remove("/tmp/blocker/x"), SandboxFilesystemError),
    ]
    for call, expected in cases:
        with pytest.raises(SandboxFilesystemError) as excinfo:
            call()
        assert type(excinfo.value) is expected


def test_a_symlink_target_is_reported_exactly(sandbox):
    sh(sandbox, 'ln -s "it\'s -> café" /tmp/awkward')
    assert sandbox.filesystem.stat("/tmp/awkward").symlink_target == "it's -> café"
    (entry,) = [e for e in sandbox.filesystem.list_files("/tmp") if e.name == "awkward"]
    assert entry.symlink_target == "it's -> café"


def test_an_owner_with_no_name_is_reported_by_number(sandbox):
    sh(sandbox, "touch /tmp/owned && chown 12345:23456 /tmp/owned")
    info = sandbox.filesystem.stat("/tmp/owned")
    assert (info.owner, info.group) == ("12345", "23456")


def test_a_file_over_modals_limit_is_refused_without_reading_it(sandbox, tmp_path):
    """By a read, and by a copy: Modal's copy_to_local reads through the same
    command as read_bytes."""
    sh(sandbox, "truncate -s 6G /tmp/sparse")
    for read in (
        lambda: sandbox.filesystem.read_bytes("/tmp/sparse"),
        lambda: sandbox.filesystem.copy_to_local("/tmp/sparse", tmp_path / "sparse"),
    ):
        started = time.monotonic()
        with pytest.raises(SandboxFilesystemFileTooLargeError, match="6442450944"):
            read()
        assert time.monotonic() - started < 10
    assert list(tmp_path.iterdir()) == [], "nothing left behind locally"


def test_a_large_file_is_read_and_copied_whole(sandbox, tmp_path):
    """Past 16 MiB a read streams; Ray's own limit used to stop at 128 MiB.

    A copy is paced by this side, so the actor holds at most a bounded stretch
    of it however far the reader lags. Unpaced, a reader behind by more than
    the actor held lost bytes, and the copy failed.
    """
    digest = sh(
        sandbox,
        "head -c 67108864 /dev/urandom > /tmp/big && md5sum /tmp/big | cut -d' ' -f1",
    )
    data = sandbox.filesystem.read_bytes("/tmp/big")
    assert len(data) == 64 * 1024 * 1024
    assert hashlib.md5(data).hexdigest() == digest
    del data
    destination = tmp_path / "big.bin"
    sandbox.filesystem.copy_to_local("/tmp/big", destination)
    assert hashlib.md5(destination.read_bytes()).hexdigest() == digest


def _sandbox_sha256(sandbox, path: str) -> str:
    return sh(sandbox, f"sha256sum {path} | cut -d' ' -f1")


def test_a_large_random_upload_lands_byte_for_byte(sandbox, tmp_path):
    """Checked inside the sandbox, on content that never repeats. The round
    trips above use periodic payloads, which read back identical even with
    their chunks out of order -- which is how uploads scrambled by Ray's
    unordered actor calls passed this suite."""
    payload = os.urandom(64 * 1024 * 1024)
    sandbox.filesystem.write_bytes(payload, "/tmp/written.bin")
    assert (
        _sandbox_sha256(sandbox, "/tmp/written.bin")
        == hashlib.sha256(payload).hexdigest()
    )

    payload = payload[: 40 * 1024 * 1024]
    source = tmp_path / "upload.bin"
    source.write_bytes(payload)
    sandbox.filesystem.copy_from_local(source, "/tmp/copied.bin")
    assert (
        _sandbox_sha256(sandbox, "/tmp/copied.bin")
        == hashlib.sha256(payload).hexdigest()
    )


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


# -- edge cases, each measured on Modal --------------------------------------


def test_dangling_links_are_reported_as_modal_reports_them(sandbox):
    """Creating through one was "permission denied"; reaching through one is
    still "not found"."""
    sh(sandbox, "mkdir -p /tmp/t && ln -s /nowhere /tmp/t/dangling")
    fs = sandbox.filesystem
    with pytest.raises(SandboxFilesystemNotADirectoryError):
        fs.write_text("x", "/tmp/t/dangling/f.txt")
    with pytest.raises(SandboxFilesystemNotADirectoryError):
        fs.write_text("x", "/tmp/t/dangling/a/b/f.txt")
    with pytest.raises(SandboxFilesystemPathAlreadyExistsError):
        fs.make_directory("/tmp/t/dangling/sub")
    with pytest.raises(SandboxFilesystemNotFoundError):
        fs.make_directory("/tmp/t/dangling/sub", create_parents=False)
    with pytest.raises(SandboxFilesystemNotFoundError):
        fs.read_text("/tmp/t/dangling/f.txt")
    info = fs.stat("/tmp/t/dangling")
    assert (info.type, info.symlink_target) == (FileType.SYMLINK, "/nowhere")


def test_bind_mounted_files_are_written_in_place(sandbox):
    """/etc/hosts is a per-sandbox bind mount: a rename onto it fails, which
    made every write of it a permission error. Modal writes it."""
    fs = sandbox.filesystem
    fs.write_text("127.0.0.1 localhost\n10.0.0.9 other\n", "/etc/hosts")
    assert fs.read_text("/etc/hosts") == "127.0.0.1 localhost\n10.0.0.9 other\n"
    # Mounted read-only: still refused, with the base error on both images,
    # as Modal reports it.
    before = fs.read_text("/etc/resolv.conf")
    with pytest.raises(SandboxFilesystemError) as info:
        fs.write_text("nameserver 1.1.1.1\n", "/etc/resolv.conf")
    assert type(info.value) is SandboxFilesystemError
    assert fs.read_text("/etc/resolv.conf") == before
    assert not [
        e for e in fs.list_files("/etc") if e.name.startswith(".ray-sandbox-tmp")
    ]


def test_names_and_listed_paths_follow_modal(sandbox):
    sh(sandbox, "mkdir -p /tmp/t && : > /tmp/t/f")
    fs = sandbox.filesystem
    assert (fs.stat("/tmp/.").name, fs.stat("/tmp/.").path) == ("tmp", "/tmp/.")
    assert fs.stat("/tmp/t/..").name == ""
    assert [e.path for e in fs.list_files("/tmp/t/")] == ["/tmp/t/f"]
    assert [e.path for e in fs.list_files("/tmp/t//")] == ["/tmp/t//f"]
    assert [e.path for e in fs.list_files("/tmp/./t")] == ["/tmp/./t/f"]
    root = [e.path for e in fs.list_files("/")]
    assert "/tmp" in root
    assert not any(path.startswith("//") for path in root)


def test_odd_names_links_and_proc(sandbox):
    """Names with a newline were dropped from listings without a word; Modal
    lists them."""
    sh(
        sandbox,
        "mkdir -p /tmp/odd /tmp/outside /tmp/rm1 && cd /tmp/odd && "
        ": > '*' && : > '-dash' && : > 'a b' && : > '[x]' && : > '.*' && "
        ": > \"$(printf 'new\\nline')\" && "
        ": > /tmp/outside/keep && ln -s /tmp/outside /tmp/rm1/link && "
        "ln -s /tmp/outside /tmp/out_link",
    )
    fs = sandbox.filesystem
    names = sorted(e.name for e in fs.list_files("/tmp/odd"))
    assert names == ["*", "-dash", ".*", "[x]", "a b", "new\nline"]
    assert fs.stat("/tmp/odd/new\nline").name == "new\nline"
    # rm -r never follows a link out of the tree it removes.
    fs.remove("/tmp/rm1", recursive=True)
    fs.remove("/tmp/out_link")
    assert [e.name for e in fs.list_files("/tmp/outside")] == ["keep"]
    assert fs.read_text("/proc/self/status").startswith("Name:")
    assert len(fs.list_files("/proc")) > 5


def test_unclassified_failures_are_the_base_error_with_their_reason(sandbox):
    """What Modal reports as the base error -- a name too long, a mount point
    that cannot be removed, the root, a symlink loop -- was "permission denied"
    or "not found" here. busybox's rm -rf / wiped the sandbox, where GNU's
    refused; Modal refuses it, and its sandbox keeps running."""
    sh(sandbox, "ln -s /tmp/loop /tmp/loop")
    fs = sandbox.filesystem
    cases = [
        (lambda: fs.write_text("x", "/tmp/" + "n" * 300), "File name too long"),
        (lambda: fs.read_text("/tmp/" + "n" * 300), "File name too long"),
        (lambda: fs.remove("/etc/resolv.conf"), ""),
        (lambda: fs.remove("/etc/hosts"), ""),
        (lambda: fs.remove("/"), "root directory"),
        (lambda: fs.remove("/", recursive=True), "root directory"),
        (lambda: fs.remove("//", recursive=True), "root directory"),
        (lambda: fs.remove("/tmp/..", recursive=True), "root directory"),
        (lambda: fs.read_text("/tmp/loop"), "ymbolic link"),
        (lambda: fs.list_files("/tmp/loop"), "ymbolic link"),
    ]
    for call, reason in cases:
        with pytest.raises(SandboxFilesystemError, match=reason) as info:
            call()
        assert type(info.value) is SandboxFilesystemError, info.value
    # Still usable, and a loop is still described and replaced as Modal does.
    assert fs.stat("/tmp/loop").type == FileType.SYMLINK
    fs.write_text("replaced\n", "/tmp/loop")
    assert fs.read_text("/tmp/loop") == "replaced\n"
    assert fs.list_files("/bin")
    assert sandbox.exec("echo", "alive").stdout.read() == "alive\n"


@pytest.fixture(params=IMAGES, ids=lambda image: image.split(":")[0])
def sandbox_in_app(request):
    """A sandbox whose working directory is one a test may remove."""
    sb = modal.Sandbox.create(image=request.param, timeout=180, workdir="/app")
    try:
        yield sb
    finally:
        sb.terminate()


def test_the_filesystem_outlives_the_working_directory(sandbox_in_app):
    """Every command ran in the WORKDIR, and once it was removed runsc failed
    them all before their scripts ran -- each error the whole script."""
    fs = sandbox_in_app.filesystem
    fs.write_text("x", "/app/f.txt")
    fs.remove("/app", recursive=True)
    assert fs.stat("/").path == "/"
    fs.write_text("still here", "/tmp/after.txt")
    assert "after.txt" in [entry.name for entry in fs.list_files("/tmp")]
    # Past the inline size, so both directions stream.
    payload = os.urandom(20 * 1024 * 1024)
    fs.write_bytes(payload, "/tmp/big.bin")
    assert fs.read_bytes("/tmp/big.bin") == payload


def test_separators_in_names_and_targets_hide_no_entry(fs):
    """One name holding the record separator dropped every entry listed after
    it, and a symlink target holding one did the same."""
    for name in ("a", "b\x1fweird", "c", "d"):
        fs.write_text(name, f"/tmp/odd/{name}")
    _create_symlink(fs, "x\x1fy", "/tmp/odd/link")
    entries = {entry.name: entry for entry in fs.list_files("/tmp/odd")}
    assert sorted(entries) == ["a", "b\x1fweird", "c", "d", "link"]
    assert entries["b\x1fweird"].path == "/tmp/odd/b\x1fweird"
    assert entries["link"].symlink_target == "x\x1fy"
    info = fs.stat("/tmp/odd/b\x1fweird")
    assert (info.name, info.path, info.size) == ("b\x1fweird", "/tmp/odd/b\x1fweird", 7)


def test_root_searches_a_directory_with_no_execute_bit(sandbox):
    """busybox's test -x is false for root here, though the kernel lets root
    in: listing the directory, or a missing path below it, was a permission
    denial."""
    sh(
        sandbox,
        "mkdir /tmp/closed && touch /tmp/closed/inside && chmod 600 /tmp/closed",
    )
    fs = sandbox.filesystem
    assert [entry.name for entry in fs.list_files("/tmp/closed")] == ["inside"]
    with pytest.raises(SandboxFilesystemNotFoundError):
        fs.stat("/tmp/closed/missing")


def test_a_write_never_lands_inside_a_directory(fs):
    """ "/tmp/fresh/.." created /tmp/fresh and moved the payload into /tmp as a
    hidden file, reporting success."""
    with pytest.raises(SandboxFilesystemIsADirectoryError):
        fs.write_text("x", "/tmp/fresh/..")
    with pytest.raises(SandboxFilesystemNotFoundError):
        fs.stat("/tmp/fresh")
    assert not [
        e for e in fs.list_files("/tmp") if e.name.startswith(".ray-sandbox-tmp")
    ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
