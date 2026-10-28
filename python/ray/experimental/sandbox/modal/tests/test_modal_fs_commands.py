"""Unit tests for the shell commands behind ``Sandbox.filesystem``.

Two layers are covered without any sandbox:

* the exit-code to exception mapping and the stat record parser, everywhere;
* the shell snippets themselves, run against the local ``/bin/sh`` on POSIX
  hosts. That is the same script the sandbox runs, so a mistake in the
  classification prologue fails here rather than only in privileged CI.
"""

import logging
import os
import re
import shutil
import subprocess
import sys
import time

import pytest

import ray
from ray.experimental.sandbox.modal import _fs_commands as fs
from ray.experimental.sandbox.modal.exception import (
    InvalidError,
    NotFoundError,
    SandboxFilesystemDirectoryNotEmptyError,
    SandboxFilesystemError,
    SandboxFilesystemIsADirectoryError,
    SandboxFilesystemNotADirectoryError,
    SandboxFilesystemNotFoundError,
    SandboxFilesystemPathAlreadyExistsError,
    SandboxFilesystemPermissionError,
)
from ray.experimental.sandbox.modal.types import FileType

requires_posix_shell = pytest.mark.skipif(
    sys.platform == "win32" or not os.path.exists("/bin/sh"),
    reason="The filesystem commands are POSIX shell scripts.",
)


def run_script(argv, stdin=None, env=None):
    """Run a built command against the local filesystem."""
    return subprocess.run(argv, input=stdin, capture_output=True, env=env)


# -- path validation -------------------------------------------------------


@pytest.mark.parametrize(
    "path,accepted",
    [
        ("relative/path", False),
        ("", False),
        ("./x", False),
        ("~/x", False),
        ("/tmp/file", True),
        # The longest name Linux allows; a byte more is refused further down.
        ("/tmp/" + "n" * 255, True),
    ],
)
def test_only_absolute_paths_are_accepted(path, accepted):
    if accepted:
        fs.validate_absolute_remote_path(path, "stat")
        return
    with pytest.raises(
        InvalidError, match=r"Sandbox\.filesystem\.stat\(\).*absolute remote_path"
    ):
        fs.validate_absolute_remote_path(path, "stat")


# -- error mapping ---------------------------------------------------------


@pytest.mark.parametrize(
    "raiser,exit_code,expected",
    [
        (fs.raise_stat_error, fs.EXIT_NOT_FOUND, SandboxFilesystemNotFoundError),
        (
            fs.raise_stat_error,
            fs.EXIT_NOT_A_DIRECTORY,
            SandboxFilesystemNotADirectoryError,
        ),
        (
            fs.raise_stat_error,
            fs.EXIT_PERMISSION_DENIED,
            SandboxFilesystemPermissionError,
        ),
        (fs.raise_list_files_error, fs.EXIT_NOT_FOUND, SandboxFilesystemNotFoundError),
        (
            fs.raise_list_files_error,
            fs.EXIT_NOT_A_DIRECTORY,
            SandboxFilesystemNotADirectoryError,
        ),
        (fs.raise_read_file_error, fs.EXIT_NOT_FOUND, SandboxFilesystemNotFoundError),
        (
            fs.raise_read_file_error,
            fs.EXIT_IS_A_DIRECTORY,
            SandboxFilesystemIsADirectoryError,
        ),
        (
            fs.raise_write_file_error,
            fs.EXIT_IS_A_DIRECTORY,
            SandboxFilesystemIsADirectoryError,
        ),
        (
            fs.raise_write_file_error,
            fs.EXIT_NOT_A_DIRECTORY,
            SandboxFilesystemNotADirectoryError,
        ),
        (
            fs.raise_make_directory_error,
            fs.EXIT_ALREADY_EXISTS,
            SandboxFilesystemPathAlreadyExistsError,
        ),
        (
            fs.raise_make_directory_error,
            fs.EXIT_NOT_FOUND,
            SandboxFilesystemNotFoundError,
        ),
        (
            fs.raise_remove_error,
            fs.EXIT_DIRECTORY_NOT_EMPTY,
            SandboxFilesystemDirectoryNotEmptyError,
        ),
        (fs.raise_remove_error, fs.EXIT_NOT_FOUND, SandboxFilesystemNotFoundError),
    ],
)
def test_exit_codes_map_to_typed_exceptions(raiser, exit_code, expected):
    with pytest.raises(expected, match="/tmp/target"):
        raiser(exit_code, b"", "/tmp/target")


@pytest.mark.parametrize(
    "raiser",
    [
        fs.raise_stat_error,
        fs.raise_list_files_error,
        fs.raise_read_file_error,
        fs.raise_write_file_error,
        fs.raise_make_directory_error,
        fs.raise_remove_error,
    ],
)
def test_unclassified_exit_codes_fall_back_to_the_base_error(raiser):
    """Without a message from the command, Modal's own text, word for word."""
    with pytest.raises(SandboxFilesystemError) as excinfo:
        raiser(77, b"", "/tmp/target")
    assert type(excinfo.value) is SandboxFilesystemError
    assert str(excinfo.value) == "Operation on '/tmp/target' failed with exit code 77"


def test_an_unclassified_failure_carries_the_commands_message(caplog):
    """A full disk, say. Modal's helper puts its own message in the base
    error; the command's last line of stderr is the equivalent here, where a
    fixed "exit code 1" told the caller nothing."""
    stderr = b"some preamble\ncat: write error: No space left on device\n"
    with caplog.at_level(logging.DEBUG, logger=fs.__name__):
        with pytest.raises(SandboxFilesystemError) as excinfo:
            fs.raise_write_file_error(1, stderr, "/tmp/target")

    assert type(excinfo.value) is SandboxFilesystemError
    assert str(excinfo.value) == "cat: write error: No space left on device"
    assert "some preamble" in caplog.text


@pytest.mark.parametrize("raiser", [fs.raise_read_file_error, fs.raise_remove_error])
def test_a_file_in_the_way_is_the_base_error_for_reads_and_removals(raiser):
    """Measured on Modal: ENOTDIR there is not SandboxFilesystemNotADirectoryError."""
    with pytest.raises(SandboxFilesystemError) as excinfo:
        raiser(fs.EXIT_NOT_A_DIRECTORY, b"", "/etc/passwd/x")
    assert type(excinfo.value) is SandboxFilesystemError
    assert str(excinfo.value) == "a component of the path is not a directory"


def test_a_file_too_large_is_reported_in_modals_words():
    from ray.experimental.sandbox.modal.exception import (
        SandboxFilesystemFileTooLargeError,
    )

    with pytest.raises(SandboxFilesystemFileTooLargeError) as excinfo:
        fs.raise_read_file_error(fs.EXIT_FILE_TOO_LARGE, b"6442450944\n", "/tmp/big")
    assert str(excinfo.value) == (
        "file is 6442450944 bytes, which exceeds the 5368709120 byte limit: /tmp/big"
    )


def _sandbox_not_found():
    from ray.experimental.sandbox.exceptions import SandboxNotFoundError

    return SandboxNotFoundError("gone")


@pytest.mark.parametrize(
    "error",
    # A dead actor is how a sandbox usually turns out to be gone. RayActorError
    # is not a SandboxError, so it used to fall through to the catch-all and
    # surface as a generic filesystem error.
    [_sandbox_not_found, lambda: ray.exceptions.RayActorError()],
    ids=["sandbox-not-found", "actor-died"],
)
def test_translate_exec_errors_reports_a_gone_sandbox_as_modal_does(error):
    with pytest.raises(NotFoundError, match="Sandbox is unavailable"):
        with fs.translate_exec_errors("read_text", "/tmp/x"):
            raise error()


@pytest.mark.parametrize(
    "error",
    [
        SandboxFilesystemNotFoundError,
        # copy_to_local opens its destination inside the block. Modal documents
        # these as reaching the caller unchanged; wrapping them lost both the
        # class and the errno.
        IsADirectoryError,
        NotADirectoryError,
        PermissionError,
        FileNotFoundError,
    ],
)
def test_translate_exec_errors_lets_filesystem_errors_through(error):
    """Ahead of the catch-all below, whichever side of the call raised them."""
    with pytest.raises(error):
        with fs.translate_exec_errors("copy_to_local", "/tmp/x"):
            raise error("local disk problem")


def test_translate_exec_errors_hides_unexpected_failures():
    """A caller should never see a raw 'exec failed' message."""
    with pytest.raises(SandboxFilesystemError, match="unexpected error"):
        with fs.translate_exec_errors("read_text", "/tmp/x"):
            raise ValueError("call to exec() failed")


# -- record parsing --------------------------------------------------------


def _record(*fields):
    return fs._US.join(fields)


# A raw st_mode for each kind of entry, permissions 0644 or 0755 or 0777.
_MODES = {
    "regular file": "81a4",
    "directory": "41ed",
    "symbolic link": "a1ff",
    "fifo": "11a4",
    "socket": "c1ed",
}


def _entry(
    path,
    kind="regular file",
    name_n=None,
    mode=None,
    owner="ray",
    group="users",
    uid="1000",
    gid="100",
):
    """One record, in the field order of fs._STAT_FORMAT. The kind of entry
    is carried by the mode bits alone, as stat's %f reports them."""
    return _record(
        mode or _MODES[kind],
        "12",
        uid,
        gid,
        owner,
        group,
        "1700000000",
        name_n or path,
        path,
    )


def test_parse_stat_records_reads_every_field():
    (info,) = fs.parse_stat_records(_entry("/tmp/hello.txt").encode())
    assert info.name == "hello.txt"
    assert info.path == "/tmp/hello.txt"
    assert info.type == FileType.FILE
    assert info.size == 12
    assert info.mode == 0x81A4
    # Octal, as Modal reports it -- not the symbolic "-rw-r--r--" stat prints.
    assert info.permissions == "0644"
    assert info.owner == "ray"
    assert info.group == "users"
    assert info.modified_time == 1700000000.0
    assert info.symlink_target is None
    assert info.is_file() and not info.is_dir() and not info.is_symlink()


def test_an_owner_without_a_name_is_reported_by_number():
    """Both stats print UNKNOWN; Modal reports the id, measured."""
    line = _entry("/tmp/f", owner="UNKNOWN", group="UNKNOWN", uid="12345", gid="23456")
    (info,) = fs.parse_stat_records(line.encode())
    assert (info.owner, info.group) == ("12345", "23456")


def test_the_root_is_named_as_modal_names_it():
    (info,) = fs.parse_stat_records(_entry("/", kind="directory").encode())
    assert (info.name, info.path) == ("", "/")


def test_records_split_on_newlines_only():
    """splitlines() would also split a name on \\x1c-\\x1e or \\u2028."""
    odd = "/tmp/a\x1cb c"
    (info,) = fs.parse_stat_records(_entry(odd).encode())
    assert info.path == odd


@pytest.mark.parametrize(
    "kind,expected",
    [
        ("regular file", FileType.FILE),
        ("directory", FileType.DIRECTORY),
        ("symbolic link", FileType.SYMLINK),
        # stat reports these too; FileType has no case for them.
        ("fifo", FileType.FILE),
        ("socket", FileType.FILE),
    ],
)
def test_stat_file_types_are_mapped(kind, expected):
    (info,) = fs.parse_stat_records(_entry("/tmp/entry", kind=kind).encode())
    assert info.type == expected


def test_the_type_comes_from_the_mode_not_from_text_a_locale_translates():
    """GNU translates stat's %F ("Verzeichnis" under a German LANG), which made
    a directory a FILE. The record no longer carries that text at all."""
    assert "%F" not in fs._STAT_FORMAT
    (info,) = fs.parse_stat_records(_entry("/tmp/d", kind="directory").encode())
    assert info.is_dir()


@requires_posix_shell
def test_a_localized_stat_still_reports_types(tmp_path):
    """The real command, under a non-English locale."""
    (tmp_path / "d").mkdir()
    (tmp_path / "l").symlink_to(tmp_path / "d")
    env = dict(os.environ, LANG="de_DE.UTF-8", LC_ALL="de_DE.UTF-8", LANGUAGE="de")
    result = subprocess.run(
        fs.make_list_files_command(str(tmp_path)), capture_output=True, env=env
    )
    assert result.returncode == 0, result.stderr
    types = {i.name: i.type for i in fs.parse_stat_records(result.stdout)}
    assert types == {"d": FileType.DIRECTORY, "l": FileType.SYMLINK}


@pytest.mark.parametrize(
    "kind,target", [("symbolic link", "/tmp/real"), ("regular file", None)]
)
def test_only_symlinks_carry_a_target(kind, target):
    line = _entry("/tmp/link", kind=kind, name_n="/tmp/link -> /tmp/real")
    (info,) = fs.parse_stat_records(line.encode())
    assert info.is_symlink() is (target is not None)
    assert info.symlink_target == target


@pytest.mark.parametrize(
    "name_n,path,expected",
    [
        # GNU stat under QUOTING_STYLE=literal.
        ("/tmp/l -> /tmp/real", "/tmp/l", "/tmp/real"),
        ("/tmp/l -> it's -> café", "/tmp/l", "it's -> café"),
        # A link whose own name contains the arrow.
        ("/tmp/a -> b -> target", "/tmp/a -> b", "target"),
        # busybox, which quotes plainly whatever the environment.
        ("'/tmp/l' -> '/tmp/real'", "/tmp/l", "/tmp/real"),
        ("'/tmp/l' -> 'it's -> café'", "/tmp/l", "it's -> café"),
        ("'/tmp/it's' -> 'x'", "/tmp/it's", "x"),
        # Neither: the old best effort.
        ("'/elsewhere' -> 'x'", "/tmp/l", "x"),
    ],
)
def test_symlink_targets_are_read_exactly(name_n, path, expected):
    """Split on the known path, not the arrow, and nothing gets mangled.

    GNU's default quoting printed `'it'\\''s -> caf'$'\\303\\251'` for a
    target Modal reports as "it's -> café".
    """
    assert fs._symlink_target(name_n, path) == expected


def test_a_directory_listing_stats_in_batches_not_per_entry():
    """Two process spawns per entry is what this command exists to avoid."""
    script = fs.make_list_files_command("/tmp")[2]
    assert "readlink" not in script
    # One stat call per batch: the mid-loop flush and the trailing partial
    # batch -- plus the one for a path holding the field separator, which is
    # stat'd alone.
    assert script.count("stat -c") == 3
    assert f"-ge {fs._STAT_BATCH}" in script


def test_parse_stat_records_skips_blank_and_short_lines():
    good = _entry("/tmp/a")
    stdout = ("\n" + good + "\ntruncated\x1frecord\n").encode()
    assert [info.path for info in fs.parse_stat_records(stdout)] == ["/tmp/a"]


# -- the shell snippets, against a real shell ------------------------------


@requires_posix_shell
def test_write_then_read_round_trips_and_a_second_write_replaces(tmp_path):
    target = str(tmp_path / "nested" / "blob.bin")
    for payload in (bytes(range(256)), b"second"):
        written = run_script(fs.make_write_file_command(target), stdin=payload)
        assert written.returncode == 0, written.stderr
        read = run_script(fs.make_read_file_command(target))
        assert (read.returncode, read.stdout) == (0, payload)


def _remove(recursive):
    return lambda path: fs.make_remove_command(path, recursive)


def _mkdir(create_parents):
    return lambda path: fs.make_make_directory_command(path, create_parents)


@requires_posix_shell
@pytest.mark.parametrize(
    "build,below",
    [
        (fs.make_stat_command, "absent"),
        (fs.make_stat_command, "a/b/c"),
        (fs.make_list_files_command, "absent"),
        (fs.make_list_files_command, "a/b/c"),
        (fs.make_read_file_command, "absent"),
        (_remove(False), "absent"),
        (_mkdir(False), "missing/child"),
    ],
    ids=["stat", "stat-deep", "list", "list-deep", "read", "remove", "mkdir"],
)
def test_a_missing_path_is_not_found(tmp_path, build, below):
    result = run_script(build(f"{tmp_path}/{below}"))
    assert result.returncode == fs.EXIT_NOT_FOUND, result.stderr


@requires_posix_shell
@pytest.mark.parametrize(
    "build,below",
    [
        (fs.make_read_file_command, ""),
        (fs.make_write_file_command, ""),
        # A trailing slash demands a directory, so writing a file there is
        # EISDIR. It used to fall through to the parent check -- which stripped
        # the wrong component -- and surface as a permission denial.
        (fs.make_write_file_command, "newthing/"),
    ],
    ids=["read", "write", "write-trailing-slash"],
)
def test_a_directory_where_a_file_is_wanted_is_a_directory_error(
    tmp_path, build, below
):
    result = run_script(build(f"{tmp_path}/{below}"), stdin=b"y")
    assert result.returncode == fs.EXIT_IS_A_DIRECTORY, result.stderr


@requires_posix_shell
@pytest.mark.parametrize("below", ["child", "a/b/child"])
def test_write_classifies_a_file_up_the_path(tmp_path, below):
    """Several components up, `mkdir -p` is what fails. Classifying that
    failure means walking up to the nearest component that exists; before it
    did, every `mkdir -p` failure was reported as "not a directory" whatever
    the real cause."""
    blocker = tmp_path / "afile"
    blocker.write_text("x")
    result = run_script(fs.make_write_file_command(f"{blocker}/{below}"), stdin=b"y")
    assert result.returncode == fs.EXIT_NOT_A_DIRECTORY


@requires_posix_shell
@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores directory permissions.")
def test_write_classifies_an_unwritable_parent_as_permission_denied(tmp_path):
    """The counterpart of the test above, and the far more common case.

    A `mkdir -p` that fails on EACCES used to be reported as
    SandboxFilesystemNotADirectoryError, where Modal reports a permission
    denial.
    """
    locked = tmp_path / "locked"
    locked.mkdir()
    locked.chmod(0o500)
    try:
        target = str(locked / "sub" / "child")
        result = run_script(fs.make_write_file_command(target), stdin=b"y")
        # Classified from mkdir's own "Permission denied".
        with pytest.raises(SandboxFilesystemPermissionError):
            fs.raise_write_file_error(result.returncode, result.stderr, target)
    finally:
        locked.chmod(0o700)


@requires_posix_shell
def test_stat_reports_a_file(tmp_path):
    target = tmp_path / "hello.txt"
    target.write_text("hello\n")
    result = run_script(fs.make_stat_command(str(target)))
    assert result.returncode == 0, result.stderr
    (info,) = fs.parse_stat_records(result.stdout)
    assert info.name == "hello.txt"
    assert info.type == FileType.FILE
    assert info.size == 6
    assert info.permissions == f"{info.mode & 0o7777:04o}"
    assert info.modified_time > 0


@requires_posix_shell
def test_stat_describes_the_symlink_not_its_target(tmp_path):
    target = tmp_path / "real.txt"
    target.write_text("content")
    link = tmp_path / "link.txt"
    try:
        link.symlink_to(target)
    except (OSError, NotImplementedError):
        pytest.skip("This host cannot create symlinks.")
    result = run_script(fs.make_stat_command(str(link)))
    (info,) = fs.parse_stat_records(result.stdout)
    assert info.is_symlink()
    assert info.symlink_target == str(target)


@requires_posix_shell
def test_list_files_includes_hidden_entries(tmp_path):
    (tmp_path / "visible.txt").write_text("a")
    (tmp_path / ".hidden").write_text("b")
    (tmp_path / "subdir").mkdir()
    result = run_script(fs.make_list_files_command(str(tmp_path)))
    assert result.returncode == 0, result.stderr
    entries = {info.name: info for info in fs.parse_stat_records(result.stdout)}
    assert set(entries) == {"visible.txt", ".hidden", "subdir"}
    assert entries["subdir"].is_dir()
    assert entries["visible.txt"].is_file()


@requires_posix_shell
def test_list_files_on_an_empty_directory_returns_nothing(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    result = run_script(fs.make_list_files_command(str(empty)))
    assert result.returncode == 0
    assert fs.parse_stat_records(result.stdout) == []


@requires_posix_shell
def test_list_files_classifies_a_file(tmp_path):
    target = tmp_path / "f.txt"
    target.write_text("x")
    assert (
        run_script(fs.make_list_files_command(str(target))).returncode
        == fs.EXIT_EXPECTED_A_DIRECTORY
    )


@requires_posix_shell
@pytest.mark.parametrize(
    "create_parents,below",
    # Measured on Modal: /tmp/n/ creates /tmp/n. The parent used to be computed
    # from the path with its slash still on, and was reported absent.
    [(True, "new"), (False, "new"), (True, "a/b/c"), (False, "new/")],
)
def test_make_directory_creates_the_directory(tmp_path, create_parents, below):
    result = run_script(_mkdir(create_parents)(f"{tmp_path}/{below}"))
    assert result.returncode == 0, result.stderr
    assert (tmp_path / below).is_dir()


@requires_posix_shell
@pytest.mark.parametrize("through_a_link", [False, True])
def test_make_directory_with_parents_is_idempotent(tmp_path, through_a_link):
    """Still when the existing directory is reached through a link."""
    target = tmp_path / "a"
    target.mkdir()
    if through_a_link:
        (tmp_path / "link").symlink_to(target)
        target = tmp_path / "link"
    assert run_script(_mkdir(True)(str(target))).returncode == 0


@requires_posix_shell
@pytest.mark.parametrize(
    "create_parents,existing",
    [
        (False, "directory"),
        # Measured on Modal: PathAlreadyExists in both modes, not NotADirectory.
        (True, "file"),
        (False, "file"),
        # Neither -d nor -e, but mkdir still fails EEXIST. With create_parents
        # the path used to reach `mkdir -p` and be misreported as a permission
        # denial; both spellings now agree with each other and with Modal.
        (True, "dangling link"),
        (False, "dangling link"),
    ],
)
def test_make_directory_over_an_existing_path_reports_it_exists(
    tmp_path, create_parents, existing
):
    target = tmp_path / "existing"
    if existing == "directory":
        target.mkdir()
    elif existing == "file":
        target.write_text("x")
    else:
        target.symlink_to(tmp_path / "nothing-here")
    result = run_script(_mkdir(create_parents)(str(target)))
    assert result.returncode == fs.EXIT_ALREADY_EXISTS, result.stderr


@requires_posix_shell
@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores directory permissions.")
def test_make_directory_with_parents_reports_permission_denied(tmp_path):
    locked = tmp_path / "locked"
    locked.mkdir()
    locked.chmod(0o500)
    try:
        target = str(locked / "a" / "b")
        result = run_script(fs.make_make_directory_command(target, True))
        with pytest.raises(SandboxFilesystemPermissionError):
            fs.raise_make_directory_error(result.returncode, result.stderr, target)
    finally:
        locked.chmod(0o700)


@requires_posix_shell
def test_overwriting_a_file_resets_its_mode(tmp_path):
    """Measured on Modal: an overwrite comes back 0644, not the old mode."""
    target = tmp_path / "script.sh"
    target.write_text("old")
    target.chmod(0o750)
    umask = os.umask(0)
    os.umask(umask)
    result = run_script(fs.make_write_file_command(str(target)), stdin=b"new")
    assert result.returncode == 0, result.stderr
    assert target.read_bytes() == b"new"
    assert target.stat().st_mode & 0o7777 == 0o666 & ~umask


@requires_posix_shell
@pytest.mark.parametrize(
    "contents,recursive,suffix",
    [
        (None, False, ""),
        ([], False, ""),
        (["nested/child.txt"], True, ""),
        (["child.txt"], True, "/"),
    ],
    ids=["file", "empty-directory", "tree", "tree-trailing-slash"],
)
def test_remove_deletes_the_path(tmp_path, contents, recursive, suffix):
    target = tmp_path / "doomed"
    if contents is None:
        target.write_text("x")
    else:
        target.mkdir()
        for name in contents:
            (target / name).parent.mkdir(parents=True, exist_ok=True)
            (target / name).write_text("x")
    result = run_script(_remove(recursive)(f"{target}{suffix}"))
    assert result.returncode == 0, result.stderr
    assert not target.exists()


@requires_posix_shell
@pytest.mark.parametrize("suffix", ["", "/"])
def test_remove_refuses_a_non_empty_directory_without_recursive(tmp_path, suffix):
    """With a trailing slash, the parent is found past the slash, not taken to
    be the path itself."""
    target = tmp_path / "full"
    target.mkdir()
    (target / "child.txt").write_text("x")
    assert (
        run_script(fs.make_remove_command(f"{target}{suffix}", False)).returncode
        == fs.EXIT_DIRECTORY_NOT_EMPTY
    )
    assert target.exists()


@requires_posix_shell
@pytest.mark.skipif(
    hasattr(os, "geteuid") and os.geteuid() == 0,
    reason="root can write to any directory",
)
@pytest.mark.parametrize("suffix", ["", "/"])
def test_a_non_empty_directory_under_a_read_only_parent_is_a_permission_error(
    tmp_path, suffix
):
    """The kernel checks write permission on the parent before it looks at the
    directory's contents, so rmdir there fails EACCES rather than ENOTEMPTY --
    and Modal's helper reports the errno it gets."""
    parent = tmp_path / "parent"
    target = parent / "full"
    target.mkdir(parents=True)
    (target / "child.txt").write_text("x")
    parent.chmod(0o555)
    try:
        result = run_script(fs.make_remove_command(f"{target}{suffix}", False))
    finally:
        parent.chmod(0o755)
    assert result.returncode == fs.EXIT_PERMISSION_DENIED
    assert target.exists()


@requires_posix_shell
@pytest.mark.parametrize("recursive", [True, False])
def test_remove_with_a_trailing_slash_removes_a_symlink_not_its_target(
    tmp_path, recursive
):
    """`link/` resolves to the link's target. GNU `rm -rf link/` then emptied
    the directory the link points to, left the link in place, and exited 0."""
    target = tmp_path / "target"
    target.mkdir()
    (target / "keep.txt").write_text("x")
    link = tmp_path / "link"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("This host cannot create symlinks.")
    result = run_script(fs.make_remove_command(f"{link}/", recursive))
    assert result.returncode == 0, result.stderr
    assert not os.path.lexists(link)
    assert (target / "keep.txt").exists(), "the link's target was emptied"


@requires_posix_shell
def test_remove_of_a_file_with_a_trailing_slash_is_still_refused(tmp_path):
    target = tmp_path / "f.txt"
    target.write_text("x")
    result = run_script(fs.make_remove_command(f"{target}/", False))
    assert result.returncode == fs.EXIT_NOT_A_DIRECTORY
    assert target.exists()


def test_temp_paths_do_not_embed_the_destination_name():
    """The name added 24 bytes, so a valid name past 231 bytes overflowed
    NAME_MAX in the temporary file and the write failed."""
    temp = fs.make_temp_path("/dir/" + "n" * 255)
    directory, _, name = temp.rpartition("/")
    assert directory == "/dir"
    assert re.fullmatch(r"\.ray-sandbox-tmp-[A-Za-z0-9]{6}", name)


@requires_posix_shell
def test_a_write_to_a_name_at_the_length_limit_round_trips(tmp_path):
    target = str(tmp_path / ("n" * 255))
    written = run_script(fs.make_write_file_command(target), stdin=b"ok")
    assert written.returncode == 0, written.stderr
    assert run_script(fs.make_read_file_command(target)).stdout == b"ok"
    assert os.listdir(tmp_path) == ["n" * 255], "a temporary file was left behind"


@requires_posix_shell
def test_paths_with_spaces_and_quotes_are_handled(tmp_path):
    target = str(tmp_path / "a dir with spaces" / "it's a file.txt")
    assert run_script(fs.make_write_file_command(target), stdin=b"ok").returncode == 0
    assert run_script(fs.make_read_file_command(target)).stdout == b"ok"
    (info,) = fs.parse_stat_records(run_script(fs.make_stat_command(target)).stdout)
    assert info.name == "it's a file.txt"


# -- measured against Modal ------------------------------------------------


@requires_posix_shell
@pytest.mark.parametrize(
    "build",
    [
        fs.make_stat_command,
        fs.make_list_files_command,
        fs.make_read_file_command,
        lambda p: fs.make_make_directory_command(p, create_parents=False),
        lambda p: fs.make_make_directory_command(p, create_parents=True),
        lambda p: fs.make_remove_command(p, recursive=False),
    ],
)
@pytest.mark.parametrize("below", ["child", "x/y", "x/y/"])
def test_a_file_anywhere_up_the_path_is_not_a_directory(tmp_path, build, below):
    """Modal reports ENOTDIR at any depth; the parent alone used to be checked,
    so /file/x/y came back as "does not exist"."""
    blocker = tmp_path / "afile"
    blocker.write_text("x")
    result = run_script(build(f"{blocker}/{below}"))
    assert result.returncode == fs.EXIT_NOT_A_DIRECTORY, result.stderr


@requires_posix_shell
@pytest.mark.parametrize(
    "suffix,joined",
    [("", "/f"), ("/", "/f"), ("//", "//f"), ("/.", "/./f")],
)
def test_listed_paths_join_as_modal_joins_them(tmp_path, suffix, joined):
    """Measured on Modal, which joins as Rust's PathBuf::push does: a slash
    only where the path does not already end in one -- so "/" lists /bin,
    and "/tmp/t//" keeps its //."""
    (tmp_path / "f").write_text("x")
    result = run_script(fs.make_list_files_command(f"{tmp_path}{suffix}"))
    assert result.returncode == 0, result.stderr
    (info,) = fs.parse_stat_records(result.stdout)
    assert (info.name, info.path) == ("f", f"{tmp_path}{joined}")


@pytest.mark.parametrize(
    "path,name",
    [
        ("/", ""),
        ("/tmp/.", "tmp"),
        ("/tmp/./", "tmp"),
        ("/tmp/t/..", ""),
        ("/tmp/t/../", ""),
        ("//tmp", "tmp"),
        ("/tmp//t", "t"),
        ("/tmp/t/", "t"),
        ("/tmp/./t", "t"),
        ("/.", ""),
        ("/..", ""),
        ("/tmp/t/./f", "f"),
    ],
)
def test_names_follow_modal(path, name):
    """Every pair measured on Modal: the name is Rust's Path::file_name(), and
    the path is reported exactly as given."""
    kind = "regular file" if name == "f" else "directory"
    (info,) = fs.parse_stat_records(_entry(path, kind=kind).encode())
    assert (info.name, info.path) == (name, path)


@requires_posix_shell
def test_listing_the_root_has_no_double_slash():
    result = run_script(fs.make_list_files_command("/"))
    assert result.returncode == 0, result.stderr
    paths = [i.path for i in fs.parse_stat_records(result.stdout)]
    assert paths and not any(p.startswith("//") for p in paths)


@requires_posix_shell
def test_a_symlink_target_comes_back_byte_for_byte(tmp_path):
    """GNU stat's default %N quoting mangled a quote or a non-ASCII byte."""
    link = tmp_path / "link"
    os.symlink("it's -> café", link)
    env = {**os.environ, "LC_ALL": "C"}
    for build in (fs.make_stat_command, fs.make_list_files_command):
        path = str(link) if build is fs.make_stat_command else str(tmp_path)
        result = subprocess.run(build(path), capture_output=True, env=env)
        assert result.returncode == 0, result.stderr
        (info,) = fs.parse_stat_records(result.stdout)
        assert info.symlink_target == "it's -> café"


@requires_posix_shell
def test_a_read_refuses_a_file_over_the_limit_before_reading(tmp_path):
    target = tmp_path / "big"
    target.write_bytes(b"x" * 10)
    result = run_script(fs.make_read_file_command(str(target), limit=9))
    assert result.returncode == fs.EXIT_FILE_TOO_LARGE
    assert result.stdout == b""
    assert result.stderr.strip() == b"10"


@requires_posix_shell
def test_an_inline_read_hands_a_larger_file_back_unread(tmp_path):
    target = tmp_path / "medium"
    target.write_bytes(b"x" * 10)
    result = run_script(fs.make_read_file_command(str(target), limit=100, inline=9))
    assert result.returncode == fs.EXIT_NOT_INLINE
    assert result.stdout == b""

    fits = run_script(fs.make_read_file_command(str(target), limit=100, inline=10))
    assert (fits.returncode, fits.stdout) == (0, b"x" * 10)


@requires_posix_shell
@pytest.mark.skipif(not os.path.exists("/proc/self/status"), reason="needs /proc")
def test_an_inline_read_of_a_file_stat_calls_empty_overruns_visibly():
    """/proc files report size 0; the one byte past the cap is the tell."""
    result = run_script(
        fs.make_read_file_command("/proc/self/status", limit=1 << 20, inline=4)
    )
    assert result.returncode == 0
    assert len(result.stdout) == 5


@requires_posix_shell
def test_a_write_that_fails_part_way_is_not_a_permission_error(tmp_path):
    """A full disk used to come back as SandboxFilesystemPermissionError.

    `ulimit -f` stands in for the disk running out: the directory is writable
    and the file is created, then writing it fails.
    """
    target = tmp_path / "dest"
    target.write_bytes(b"old")
    argv = ["/bin/sh", "-c", 'ulimit -f 1; exec "$@"', "sh"]
    argv += fs.make_write_file_command(str(target))
    result = subprocess.run(argv, input=b"x" * 100_000, capture_output=True)
    assert result.returncode not in (0, fs.EXIT_PERMISSION_DENIED)
    with pytest.raises(SandboxFilesystemError) as excinfo:
        fs.raise_write_file_error(result.returncode, result.stderr, str(target))
    assert type(excinfo.value) is SandboxFilesystemError
    # The destination is untouched and no temporary file is left behind.
    assert target.read_bytes() == b"old"
    assert os.listdir(tmp_path) == ["dest"]


# -- dangling links, the root, mounted files (measured on Modal) -------------


def _dangling(tmp_path):
    (tmp_path / "dangling").symlink_to(tmp_path / "nowhere")
    return tmp_path / "dangling"


@requires_posix_shell
@pytest.mark.parametrize("below", ["f.txt", "a/b/f.txt"])
def test_a_write_through_a_dangling_link_is_not_a_directory(tmp_path, below):
    """It was "permission denied": mkdir -p failed on the link, and the walk
    stepped past it to a writable directory. Modal: NotADirectoryError."""
    target = f"{_dangling(tmp_path)}/{below}"
    result = run_script(fs.make_write_file_command(target), stdin=b"x")
    assert result.returncode == fs.EXIT_NOT_A_DIRECTORY, result.stderr
    with pytest.raises(SandboxFilesystemNotADirectoryError):
        fs.raise_write_file_error(result.returncode, result.stderr, target)


@requires_posix_shell
def test_making_directories_through_a_dangling_link_says_it_exists(tmp_path):
    """Modal: PathAlreadyExistsError -- its mkdir meets the link itself."""
    target = f"{_dangling(tmp_path)}/sub"
    result = run_script(fs.make_make_directory_command(target, create_parents=True))
    assert result.returncode == fs.EXIT_ALREADY_EXISTS, result.stderr
    with pytest.raises(SandboxFilesystemPathAlreadyExistsError):
        fs.raise_make_directory_error(result.returncode, result.stderr, target)


@requires_posix_shell
@pytest.mark.parametrize(
    "build",
    [
        lambda p: fs.make_make_directory_command(p, create_parents=False),
        lambda p: fs.make_read_file_command(p),
        lambda p: fs.make_stat_command(p),
        lambda p: fs.make_list_files_command(p),
    ],
    ids=["mkdir-no-parents", "read", "stat", "list"],
)
def test_reaching_through_a_dangling_link_is_still_not_found(tmp_path, build):
    """What the kernel reports, and Modal too: only creating through the link
    changed."""
    result = run_script(build(f"{_dangling(tmp_path)}/sub"))
    assert result.returncode == fs.EXIT_NOT_FOUND, result.stderr


@pytest.fixture
def fake_root(tmp_path, monkeypatch):
    """A scratch directory standing in for "/". The remove script is never run
    against the real root: if its guard were broken, that would delete
    everything this user can write."""
    root = tmp_path / "root"
    (root / "bin").mkdir(parents=True)
    (root / "bin" / "sh").write_text("keep")
    monkeypatch.setattr(fs, "_ROOT", str(root))
    return root


@requires_posix_shell
@pytest.mark.parametrize("recursive", [True, False])
@pytest.mark.parametrize("spelling", ["", "/", "//", "/bin/..", "/."])
def test_a_remove_of_the_root_is_refused(fake_root, spelling, recursive):
    """busybox's rm -rf / wiped the sandbox where GNU's refused, and both were
    reported as a permission denial; without -r it said "not empty". Modal
    refuses with the base error."""
    result = run_script(fs.make_remove_command(f"{fake_root}{spelling}", recursive))
    assert result.returncode == 1
    assert (fake_root / "bin" / "sh").read_text() == "keep"
    with pytest.raises(SandboxFilesystemError) as info:
        fs.raise_remove_error(result.returncode, result.stderr, "/")
    assert type(info.value) is SandboxFilesystemError
    assert "root directory" in str(info.value)


@requires_posix_shell
def test_a_link_to_the_root_is_only_unlinked(fake_root, tmp_path):
    link = tmp_path / "to_root"
    link.symlink_to(fake_root)
    for target in (str(link), f"{link}/"):
        link.unlink(missing_ok=True)
        link.symlink_to(fake_root)
        result = run_script(fs.make_remove_command(target, True))
        assert result.returncode == 0, result.stderr
        assert not os.path.lexists(link)
        assert (fake_root / "bin" / "sh").read_text() == "keep"


@requires_posix_shell
def test_other_directories_are_still_removed_recursively(fake_root):
    doomed = fake_root / "bin"
    result = run_script(fs.make_remove_command(str(doomed), True))
    assert result.returncode == 0, result.stderr
    assert not doomed.exists()


@pytest.fixture
def failing_mv(tmp_path):
    """A PATH whose mv always fails, as a rename onto a mount point does."""
    bin_dir = tmp_path / "fakebin"
    bin_dir.mkdir()
    mv = bin_dir / "mv"
    mv.write_text("#!/bin/sh\nexit 1\n")
    mv.chmod(0o755)
    return dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}")


@requires_posix_shell
def test_a_file_that_cannot_be_renamed_onto_is_rewritten_in_place(tmp_path, failing_mv):
    """/etc/hosts is a bind mount: rename onto it fails, writing it does not.
    Modal's write of /etc/hosts succeeds."""
    target = tmp_path / "hosts"
    target.write_text("old contents that are longer\n")
    temp = str(tmp_path / ".ray-sandbox-tmp-abcdef")
    result = run_script(
        fs.make_write_file_command(str(target), temp), stdin=b"new\n", env=failing_mv
    )
    assert result.returncode == 0, result.stderr
    assert target.read_text() == "new\n"
    assert not os.path.exists(temp)


@requires_posix_shell
@pytest.mark.skipif(os.geteuid() == 0, reason="root writes read-only files.")
def test_a_file_that_can_be_neither_renamed_onto_nor_written_is_refused(
    tmp_path, failing_mv
):
    """Unwritable to this user: EACCES, a permission denial as on Modal. (A
    read-only mount -- EROFS -- is the base error instead; see test_modal_fs.)"""
    target = tmp_path / "resolv.conf"
    target.write_text("nameserver 8.8.8.8\n")
    target.chmod(0o444)
    temp = str(tmp_path / ".ray-sandbox-tmp-abcdef")
    result = run_script(
        fs.make_write_file_command(str(target), temp), stdin=b"x", env=failing_mv
    )
    assert result.returncode == 1
    with pytest.raises(SandboxFilesystemPermissionError):
        fs.raise_write_file_error(result.returncode, result.stderr, str(target))
    assert target.read_text() == "nameserver 8.8.8.8\n"
    assert not os.path.exists(temp)


@requires_posix_shell
def test_a_new_file_that_cannot_be_renamed_into_place_is_refused(tmp_path, failing_mv):
    temp = str(tmp_path / ".ray-sandbox-tmp-abcdef")
    target = tmp_path / "new.txt"
    result = run_script(
        fs.make_write_file_command(str(target), temp), stdin=b"x", env=failing_mv
    )
    # Not a permission problem: whatever mv said is the message.
    assert result.returncode == 1
    with pytest.raises(SandboxFilesystemError) as info:
        fs.raise_write_file_error(result.returncode, result.stderr, str(target))
    assert type(info.value) is SandboxFilesystemError
    assert not target.exists() and not os.path.exists(temp)


# -- names, loops, and errors by their own message (measured on Modal) ------


def test_a_name_with_a_newline_is_parsed_not_dropped():
    """Splitting stat's output on lines dropped such an entry from a listing
    without a word. Modal lists it."""
    odd = "/tmp/new\nline"
    link = "/tmp/link"
    stdout = (
        _entry(odd)
        + "\n"
        + _entry(link, kind="symbolic link", name_n=f"{link} -> /tmp/a\nb")
        + "\n"
        + _entry("/tmp/ends\n")
        + "\n"
    ).encode()
    infos = fs.parse_stat_records(stdout)
    assert [(i.name, i.path) for i in infos] == [
        ("new\nline", odd),
        ("link", link),
        ("ends\n", "/tmp/ends\n"),
    ]
    assert infos[1].symlink_target == "/tmp/a\nb"


@requires_posix_shell
def test_a_directory_holding_a_name_with_a_newline_is_listed_whole(tmp_path):
    (tmp_path / "new\nline").write_text("x")
    (tmp_path / "plain").write_text("x")
    result = run_script(fs.make_list_files_command(str(tmp_path)))
    assert result.returncode == 0, result.stderr
    names = sorted(i.name for i in fs.parse_stat_records(result.stdout))
    assert names == ["new\nline", "plain"]
    single = run_script(fs.make_stat_command(str(tmp_path / "new\nline")))
    (info,) = fs.parse_stat_records(single.stdout)
    assert info.name == "new\nline"


@pytest.mark.parametrize(
    "path",
    ["/tmp/" + "n" * 256, "/tmp/" + "\u00e9" * 128, "/" + "a/" * 2048],
    ids=["component-256", "component-256-bytes-of-utf8", "path-4096"],
)
def test_a_name_longer_than_linux_allows_is_the_base_error(path):
    """ENAMETOOLONG, which Modal reports as the base error. The scripts saw
    only a path that did not exist, and called it missing or forbidden."""
    with pytest.raises(SandboxFilesystemError, match="File name too long") as info:
        fs.validate_absolute_remote_path(path, "write_text")
    assert type(info.value) is SandboxFilesystemError


@pytest.mark.parametrize(
    "line,expected",
    [
        (
            "mkdir: cannot create directory '/x': Permission denied",
            SandboxFilesystemPermissionError,
        ),
        (
            "rm: can't remove '/x': Operation not permitted",
            SandboxFilesystemPermissionError,
        ),
        ("sh: 1: cannot create /x: Read-only file system", SandboxFilesystemError),
        (
            "rm: cannot remove '/etc/hosts': Device or resource busy",
            SandboxFilesystemError,
        ),
        ("cat: write error: No space left on device", SandboxFilesystemError),
    ],
)
def test_an_unclassified_failure_is_reported_by_its_own_message(line, expected):
    """A permission denial only when the command says so, as Modal decides
    from the errno; everything else is the base error, carrying the reason."""
    with pytest.raises(SandboxFilesystemError) as info:
        fs.raise_remove_error(1, line.encode(), "/x")
    assert type(info.value) is expected
    if expected is SandboxFilesystemPermissionError:
        assert str(info.value) == "permission denied: /x"
    else:
        assert str(info.value) == line


def test_every_script_runs_in_the_c_locale():
    """So the messages above are English whatever the image's LANG."""
    for argv in (
        fs.make_stat_command("/x"),
        fs.make_list_files_command("/x"),
        fs.make_read_file_command("/x"),
        fs.make_write_file_command("/x"),
        fs.make_make_directory_command("/x", True),
        fs.make_remove_command("/x", False),
        fs.make_remove_temp_command("/x"),
    ):
        assert argv[2].startswith("LC_ALL=C; export LC_ALL; ")


@requires_posix_shell
@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores directory permissions.")
def test_removing_from_an_unwritable_directory_is_a_permission_denial(tmp_path):
    locked = tmp_path / "locked"
    locked.mkdir()
    (locked / "f").write_text("x")
    locked.chmod(0o555)
    try:
        result = run_script(fs.make_remove_command(str(locked / "f"), False))
    finally:
        locked.chmod(0o755)
    with pytest.raises(SandboxFilesystemPermissionError):
        fs.raise_remove_error(result.returncode, result.stderr, str(locked / "f"))
    assert (locked / "f").exists()


def _loop(tmp_path):
    (tmp_path / "loop").symlink_to(tmp_path / "loop")
    return tmp_path / "loop"


@requires_posix_shell
@pytest.mark.parametrize(
    "build,raiser",
    [
        (fs.make_read_file_command, fs.raise_read_file_error),
        (fs.make_list_files_command, fs.raise_list_files_error),
    ],
    ids=["read", "list"],
)
@pytest.mark.parametrize("below", ["", "/x"])
def test_a_symlink_loop_is_the_base_error(tmp_path, build, raiser, below):
    """ELOOP, which Modal reports as the base error; it was "not found"."""
    target = f"{_loop(tmp_path)}{below}"
    result = run_script(build(target))
    assert result.returncode == 1, result.stderr
    with pytest.raises(SandboxFilesystemError, match="ymbolic link") as info:
        raiser(result.returncode, result.stderr, target)
    assert type(info.value) is SandboxFilesystemError


@requires_posix_shell
def test_a_symlink_loop_is_still_described_and_replaced(tmp_path):
    loop = _loop(tmp_path)
    (info,) = fs.parse_stat_records(run_script(fs.make_stat_command(str(loop))).stdout)
    assert info.type == FileType.SYMLINK
    # busybox's mv, and GNU's before coreutils 9 (this host's, perhaps), fail
    # ELOOP on the destination; the script removes the link and retries.
    result = run_script(fs.make_write_file_command(str(loop)), stdin=b"data")
    assert result.returncode == 0, result.stderr
    assert not loop.is_symlink() and loop.read_text() == "data"


@requires_posix_shell
def test_a_symlink_loop_message_keeps_a_backslash_in_its_path(tmp_path):
    """dash's echo expanded the "\\n" in this name into a newline, and the
    error reported only what followed it: "op"."""
    loop = tmp_path / "lo\\nop"
    loop.symlink_to(loop)
    result = run_script(fs.make_read_file_command(f"{loop}/x"))
    assert result.returncode == 1, result.stderr
    with pytest.raises(SandboxFilesystemError) as info:
        fs.raise_read_file_error(result.returncode, result.stderr, str(loop))
    assert str(info.value).endswith(f": {loop}")


# -- the field separator in a path ------------------------------------------


@requires_posix_shell
@pytest.mark.parametrize("directory", ["plain", "with\x1fseparator"])
def test_a_name_holding_the_separator_hides_no_other_entry(tmp_path, directory):
    """One such name threw the parser out of step, and every entry after it
    was dropped without a word. In the directory's own path, it is in every
    entry."""
    listed = tmp_path / directory
    listed.mkdir()
    names = ["a", "b\x1fweird", "c", "d"]
    for name in names:
        (listed / name).write_text(name)
    result = run_script(fs.make_list_files_command(str(listed)))
    assert result.returncode == 0, result.stderr
    infos = fs.parse_stat_records(result.stdout)
    assert sorted((i.name, i.path, i.size) for i in infos) == [
        (name, f"{listed}/{name}", len(name)) for name in names
    ]


@requires_posix_shell
def test_a_symlink_target_holding_the_separator_hides_no_other_entry(tmp_path):
    """The target is in %N, so it split a batched record just the same."""
    os.symlink("x\x1fy -> z", tmp_path / "link")
    for name in ("m", "n"):
        (tmp_path / name).write_text(name)
    result = run_script(fs.make_list_files_command(str(tmp_path)))
    assert result.returncode == 0, result.stderr
    infos = {i.name: i for i in fs.parse_stat_records(result.stdout)}
    assert sorted(infos) == ["link", "m", "n"]
    assert infos["link"].symlink_target == "x\x1fy -> z"


@requires_posix_shell
@pytest.mark.parametrize("target", [None, "t\x1fu\nv"])
def test_stat_of_a_name_holding_the_separator_reports_its_path(tmp_path, target):
    path = tmp_path / "b\x1fweird\nname"
    if target is None:
        path.write_text("12345")
    else:
        os.symlink(target, path)
    result = run_script(fs.make_stat_command(str(path)))
    assert result.returncode == 0, result.stderr
    info = fs.parse_stat_record_for(result.stdout, str(path))
    assert (info.name, info.path) == ("b\x1fweird\nname", str(path))
    assert info.symlink_target == target


def test_framed_records_are_read_between_batches():
    """The listing's output for a path stat'd alone: NUL, path, NUL, record,
    NUL -- with batch output on either side."""
    odd = "/d/b\x1fodd"
    stdout = (
        _entry("/d/a")
        + "\n\0"
        + odd
        + "\0"
        + _entry(odd)
        + "\n\0"
        + _entry("/d/c")
        + "\n"
    ).encode()
    assert [i.path for i in fs.parse_stat_records(stdout)] == ["/d/a", odd, "/d/c"]


# -- writes ----------------------------------------------------------------


@requires_posix_shell
@pytest.mark.parametrize("ending", ["..", "."])
def test_a_write_to_a_dot_path_is_a_directory_error(tmp_path, ending):
    """ "/tmp/new/.." created /tmp/new, then moved the payload *into* /tmp as a
    hidden file and reported success."""
    target = f"{tmp_path}/new/{ending}"
    result = run_script(fs.make_write_file_command(target), stdin=b"payload")
    assert result.returncode == fs.EXIT_IS_A_DIRECTORY, result.stderr
    assert os.listdir(tmp_path) == []


@requires_posix_shell
def test_a_directory_appearing_during_an_upload_is_not_written_into(tmp_path):
    """The directory check ran once, before an upload that can take minutes;
    mv then moved the payload into the new directory and exited 0."""
    target = tmp_path / "dest"
    temp = tmp_path / ".ray-sandbox-tmp-test00"
    process = subprocess.Popen(
        fs.make_write_file_command(str(target), str(temp)),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    process.stdin.write(b"first half, ")
    process.stdin.flush()
    # The temporary file exists once the script is past its up-front checks;
    # a directory made any sooner is caught by those.
    deadline = time.monotonic() + 10
    while not temp.exists():
        assert time.monotonic() < deadline, "the write never started"
        time.sleep(0.01)
    target.mkdir()
    _, stderr = process.communicate(b"second half")
    assert process.returncode == fs.EXIT_IS_A_DIRECTORY, stderr
    assert os.listdir(target) == [], "the payload was moved into the directory"
    assert os.listdir(tmp_path) == ["dest"], "the temporary file was left behind"


@pytest.fixture
def no_stat(tmp_path):
    """A PATH whose stat always fails, so the size comes from wc."""
    bin_dir = tmp_path / "nostat"
    bin_dir.mkdir()
    stat = bin_dir / "stat"
    stat.write_text("#!/bin/sh\nexit 1\n")
    stat.chmod(0o755)
    return dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}")


@requires_posix_shell
@pytest.mark.parametrize("counted_by", ["stat", "wc"])
def test_a_short_upload_is_refused_and_the_destination_kept(
    tmp_path, no_stat, counted_by
):
    """A stdin that closed early -- its runsc client gone first -- looked like
    the end of the payload, and the partial file was moved into place."""
    env = no_stat if counted_by == "wc" else None
    target = tmp_path / "dest"
    target.write_text("previous")
    short = run_script(
        fs.make_write_file_command(str(target), size=10), stdin=b"12345", env=env
    )
    assert short.returncode == 1
    with pytest.raises(SandboxFilesystemError, match="upload truncated: 5 of 10"):
        fs.raise_write_file_error(short.returncode, short.stderr, str(target))
    assert target.read_text() == "previous"
    whole = run_script(
        fs.make_write_file_command(str(target), size=10), stdin=b"0123456789", env=env
    )
    assert whole.returncode == 0, whole.stderr
    assert target.read_text() == "0123456789"
    assert sorted(os.listdir(tmp_path)) == ["dest", "nostat"]


@requires_posix_shell
def test_concurrent_calls_creating_one_directory_all_succeed(tmp_path):
    """ "Exists" was tested apart from "is a directory", so a directory that a
    concurrent call created between the two tests was refused as not one:
    a write into it failed as "not a directory", a mkdir as "already
    exists". Timing-dependent -- under gVisor it failed one write in twelve
    -- so this runs many rounds."""
    for round_ in range(10):
        base = tmp_path / str(round_)
        base.mkdir()
        writes = [
            subprocess.Popen(
                fs.make_write_file_command(f"{base}/new/{i}.txt"),
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            for i in range(12)
        ]
        mkdirs = [
            subprocess.Popen(
                fs.make_make_directory_command(f"{base}/made/deep", True),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            for _ in range(12)
        ]
        for i, process in enumerate(writes):
            _, stderr = process.communicate(f"payload {i}".encode())
            assert process.returncode == 0, stderr
        for process in mkdirs:
            _, stderr = process.communicate()
            assert process.returncode == 0, stderr
        assert len(os.listdir(base / "new")) == 12


def test_temp_names_ignore_a_seeded_random_module():
    """Two processes that seeded `random` alike drew the same name and shared
    one temporary file."""
    import random

    random.seed(1234)
    first = fs.temp_name()
    random.seed(1234)
    assert fs.temp_name() != first


# -- removes ---------------------------------------------------------------


@requires_posix_shell
def test_a_non_empty_directory_is_reported_without_ls(tmp_path):
    """`ls -A` decided "not empty", so an image without ls got the base
    error. rmdir's own message decides now."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for tool in ("rmdir", "rm", "stat"):
        os.symlink(shutil.which(tool), bin_dir / tool)
    target = tmp_path / "full"
    target.mkdir()
    (target / "child").write_text("x")
    env = dict(os.environ, PATH=str(bin_dir))
    result = run_script(fs.make_remove_command(str(target), False), env=env)
    assert result.returncode == fs.EXIT_DIRECTORY_NOT_EMPTY, result.stderr
    assert target.exists()


@requires_posix_shell
def test_a_root_check_that_cannot_answer_refuses(tmp_path):
    """A shell whose test could not answer "is this the root?" -- exit 2 --
    skipped the guard and ran rm -rf. An unknown operator stands in for a
    shell without -ef."""
    doomed = tmp_path / "dir"
    doomed.mkdir()
    (doomed / "keep").write_text("x")
    argv = fs.make_remove_command(str(doomed), True)
    argv[2] = argv[2].replace("-ef", "-no-such-test")
    result = run_script(argv)
    assert result.returncode == 1
    assert b"could not check" in result.stderr
    assert (doomed / "keep").exists()


# -- error translation -----------------------------------------------------


def test_an_os_error_inside_the_actor_is_not_a_local_one():
    """Ray's task error is also an instance of what it carries, so a fork
    refused in the actor reached copy_from_local's caller as a bare OSError,
    as if their own file had failed."""
    cause = BlockingIOError(11, "Resource temporarily unavailable")
    remote = ray.exceptions.RayTaskError("exec_collect", "tb", cause)
    remote = remote.as_instanceof_cause()
    assert isinstance(remote, OSError)
    with pytest.raises(SandboxFilesystemError, match="Resource temporarily") as info:
        with fs.translate_exec_errors("read_text", "/tmp/x"):
            raise remote
    assert not isinstance(info.value, OSError)


def test_a_local_os_error_still_reaches_the_caller_unchanged():
    with pytest.raises(IsADirectoryError):
        with fs.translate_exec_errors("copy_to_local", "/tmp/x"):
            raise IsADirectoryError(21, "Is a directory", "/local/dir")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
