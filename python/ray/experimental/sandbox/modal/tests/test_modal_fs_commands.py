"""Unit tests for the shell commands behind ``Sandbox.filesystem``.

Two layers are covered without any sandbox:

* the exit-code to exception mapping and the stat record parser, everywhere;
* the shell snippets themselves, run against the local ``/bin/sh`` on POSIX
  hosts. That is the same script the sandbox runs, so a mistake in the
  classification prologue fails here rather than only in privileged CI.
"""

import logging
import os
import subprocess
import sys

import pytest

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


def run_script(argv, stdin=None):
    """Run a built command against the local filesystem."""
    return subprocess.run(argv, input=stdin, capture_output=True)


# -- path validation -------------------------------------------------------


@pytest.mark.parametrize("path", ["relative/path", "", "./x", "~/x"])
def test_relative_paths_are_rejected(path):
    with pytest.raises(InvalidError, match="absolute remote_path"):
        fs.validate_absolute_remote_path(path, "read_text")


def test_validation_message_names_the_operation():
    with pytest.raises(InvalidError, match=r"Sandbox\.filesystem\.stat\(\)"):
        fs.validate_absolute_remote_path("nope", "stat")


def test_absolute_paths_are_accepted():
    fs.validate_absolute_remote_path("/tmp/file", "read_text")


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


def test_translate_exec_errors_passes_filesystem_errors_through():
    with pytest.raises(SandboxFilesystemNotFoundError):
        with fs.translate_exec_errors("read_text", "/tmp/x"):
            raise SandboxFilesystemNotFoundError("missing")


def test_translate_exec_errors_reports_a_dead_sandbox_clearly():
    from ray.experimental.sandbox.exceptions import SandboxNotFoundError

    with pytest.raises(NotFoundError, match="Sandbox is unavailable"):
        with fs.translate_exec_errors("read_text", "/tmp/x"):
            raise SandboxNotFoundError("gone")


def test_translate_exec_errors_reports_a_dead_actor_as_an_absent_sandbox():
    """A dead actor is how a sandbox usually turns out to be gone.

    RayActorError is not a SandboxError, so it used to fall through to the
    catch-all and surface as a generic filesystem error rather than Modal's
    "the Sandbox is unavailable".
    """
    import ray

    with pytest.raises(NotFoundError, match="Sandbox is unavailable"):
        with fs.translate_exec_errors("read_text", "/tmp/x"):
            raise ray.exceptions.RayActorError()


@pytest.mark.parametrize(
    "error",
    [IsADirectoryError, NotADirectoryError, PermissionError, FileNotFoundError],
)
def test_translate_exec_errors_lets_local_file_errors_through(error):
    """copy_to_local opens its destination inside the block.

    Modal documents these as reaching the caller unchanged; wrapping them lost
    both the class and the errno.
    """
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


def _entry(
    path,
    kind="regular file",
    name_n=None,
    mode="81a4",
    owner="ray",
    group="users",
    uid="1000",
    gid="100",
):
    """One record, in the field order of fs._STAT_FORMAT."""
    return _record(
        mode, "12", uid, gid, owner, group, "1700000000", kind, name_n or path, path
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
        ("regular empty file", FileType.FILE),
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


def test_parse_stat_records_captures_symlink_targets():
    line = _entry(
        "/tmp/link",
        kind="symbolic link",
        mode="a1ff",
        name_n="/tmp/link -> /tmp/real",
    )
    (info,) = fs.parse_stat_records(line.encode())
    assert info.is_symlink()
    assert info.symlink_target == "/tmp/real"


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


def test_only_symlinks_carry_a_target():
    line = _entry("/tmp/f", name_n="/tmp/f -> looks like a target")
    (info,) = fs.parse_stat_records(line.encode())
    assert info.symlink_target is None


def test_a_directory_listing_stats_in_batches_not_per_entry():
    """Two process spawns per entry is what this command exists to avoid."""
    script = fs.make_list_files_command("/tmp")[2]
    assert "readlink" not in script
    # One stat call per batch: two occurrences, the mid-loop flush and the
    # trailing partial batch.
    assert script.count("stat -c") == 2
    assert f"-ge {fs._STAT_BATCH}" in script


def test_parse_stat_records_skips_blank_and_short_lines():
    good = _entry("/tmp/a")
    stdout = ("\n" + good + "\ntruncated\x1frecord\n").encode()
    assert [info.path for info in fs.parse_stat_records(stdout)] == ["/tmp/a"]


def test_parse_stat_records_handles_several_entries():
    lines = "\n".join(_entry(f"/tmp/{n}") for n in ("a", "b", "c"))
    assert len(fs.parse_stat_records(lines.encode())) == 3


# -- the shell snippets, against a real shell ------------------------------


@requires_posix_shell
def test_write_then_read_round_trips(tmp_path):
    target = str(tmp_path / "nested" / "hello.txt")
    written = run_script(fs.make_write_file_command(target), stdin=b"hello\n")
    assert written.returncode == 0, written.stderr

    read = run_script(fs.make_read_file_command(target))
    assert read.returncode == 0
    assert read.stdout == b"hello\n"


@requires_posix_shell
def test_write_replaces_existing_content(tmp_path):
    target = str(tmp_path / "f.txt")
    run_script(fs.make_write_file_command(target), stdin=b"first pass")
    run_script(fs.make_write_file_command(target), stdin=b"second")
    assert run_script(fs.make_read_file_command(target)).stdout == b"second"


@requires_posix_shell
def test_write_handles_binary_content(tmp_path):
    target = str(tmp_path / "blob.bin")
    payload = bytes(range(256))
    run_script(fs.make_write_file_command(target), stdin=payload)
    assert run_script(fs.make_read_file_command(target)).stdout == payload


@requires_posix_shell
def test_read_classifies_a_missing_path(tmp_path):
    result = run_script(fs.make_read_file_command(str(tmp_path / "absent")))
    assert result.returncode == fs.EXIT_NOT_FOUND


@requires_posix_shell
def test_read_classifies_a_directory(tmp_path):
    assert (
        run_script(fs.make_read_file_command(str(tmp_path))).returncode
        == fs.EXIT_IS_A_DIRECTORY
    )


@requires_posix_shell
def test_write_classifies_a_directory_target(tmp_path):
    assert (
        run_script(fs.make_write_file_command(str(tmp_path)), stdin=b"x").returncode
        == fs.EXIT_IS_A_DIRECTORY
    )


@requires_posix_shell
def test_write_classifies_a_file_used_as_a_parent(tmp_path):
    blocker = tmp_path / "afile"
    blocker.write_text("x")
    result = run_script(fs.make_write_file_command(str(blocker / "child")), stdin=b"y")
    assert result.returncode == fs.EXIT_NOT_A_DIRECTORY


@requires_posix_shell
def test_write_classifies_a_file_used_as_a_grandparent(tmp_path):
    """The non-directory is several components up, so `mkdir -p` is what fails.

    Classifying that failure means walking up to the nearest component that
    exists; before it did, every `mkdir -p` failure was reported as "not a
    directory" whatever the real cause.
    """
    blocker = tmp_path / "afile"
    blocker.write_text("x")
    result = run_script(
        fs.make_write_file_command(str(blocker / "a" / "b" / "child")), stdin=b"y"
    )
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
        result = run_script(
            fs.make_write_file_command(str(locked / "sub" / "child")), stdin=b"y"
        )
        assert result.returncode == fs.EXIT_PERMISSION_DENIED
    finally:
        locked.chmod(0o700)


@requires_posix_shell
def test_write_to_a_path_with_a_trailing_slash_is_a_directory_error(tmp_path):
    """A trailing slash demands a directory, so writing a file there is EISDIR.

    This used to fall through to the parent check -- which stripped the wrong
    component -- and surface as a permission denial.
    """
    target = str(tmp_path / "newthing") + "/"
    result = run_script(fs.make_write_file_command(target), stdin=b"y")
    assert result.returncode == fs.EXIT_IS_A_DIRECTORY


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
def test_stat_reports_a_directory(tmp_path):
    result = run_script(fs.make_stat_command(str(tmp_path)))
    (info,) = fs.parse_stat_records(result.stdout)
    assert info.is_dir()


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
def test_stat_classifies_a_missing_path(tmp_path):
    assert (
        run_script(fs.make_stat_command(str(tmp_path / "absent"))).returncode
        == fs.EXIT_NOT_FOUND
    )


@requires_posix_shell
def test_stat_classifies_a_file_used_as_a_parent(tmp_path):
    blocker = tmp_path / "afile"
    blocker.write_text("x")
    assert (
        run_script(fs.make_stat_command(str(blocker / "child"))).returncode
        == fs.EXIT_NOT_A_DIRECTORY
    )


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
def test_list_files_classifies_a_missing_path(tmp_path):
    assert (
        run_script(fs.make_list_files_command(str(tmp_path / "absent"))).returncode
        == fs.EXIT_NOT_FOUND
    )


@requires_posix_shell
@pytest.mark.parametrize("create_parents", [True, False])
def test_make_directory_creates_a_directory(tmp_path, create_parents):
    target = tmp_path / "new"
    result = run_script(fs.make_make_directory_command(str(target), create_parents))
    assert result.returncode == 0, result.stderr
    assert target.is_dir()


@requires_posix_shell
def test_make_directory_creates_missing_parents(tmp_path):
    target = tmp_path / "a" / "b" / "c"
    assert run_script(fs.make_make_directory_command(str(target), True)).returncode == 0
    assert target.is_dir()


@requires_posix_shell
def test_make_directory_with_parents_is_idempotent(tmp_path):
    target = tmp_path / "a"
    target.mkdir()
    assert run_script(fs.make_make_directory_command(str(target), True)).returncode == 0


@requires_posix_shell
def test_make_directory_without_parents_rejects_an_existing_path(tmp_path):
    target = tmp_path / "a"
    target.mkdir()
    assert (
        run_script(fs.make_make_directory_command(str(target), False)).returncode
        == fs.EXIT_ALREADY_EXISTS
    )


@requires_posix_shell
def test_make_directory_without_parents_requires_the_parent(tmp_path):
    target = tmp_path / "missing" / "child"
    assert (
        run_script(fs.make_make_directory_command(str(target), False)).returncode
        == fs.EXIT_NOT_FOUND
    )


@requires_posix_shell
@pytest.mark.parametrize("create_parents", [True, False])
def test_make_directory_reports_a_dangling_symlink_as_existing(
    tmp_path, create_parents
):
    """A dangling symlink is neither -d nor -e, but mkdir still fails EEXIST.

    With create_parents the path used to reach `mkdir -p` and be misreported as
    a permission denial; both spellings now agree with each other and with
    Modal.
    """
    target = tmp_path / "dangling"
    target.symlink_to(tmp_path / "nothing-here")
    result = run_script(fs.make_make_directory_command(str(target), create_parents))
    assert result.returncode == fs.EXIT_ALREADY_EXISTS


@requires_posix_shell
def test_make_directory_with_parents_follows_a_symlink_to_a_directory(tmp_path):
    """Still idempotent when the existing directory is reached through a link."""
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real)
    assert run_script(fs.make_make_directory_command(str(link), True)).returncode == 0


@requires_posix_shell
@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores directory permissions.")
def test_make_directory_with_parents_reports_permission_denied(tmp_path):
    locked = tmp_path / "locked"
    locked.mkdir()
    locked.chmod(0o500)
    try:
        result = run_script(
            fs.make_make_directory_command(str(locked / "a" / "b"), True)
        )
        assert result.returncode == fs.EXIT_PERMISSION_DENIED
    finally:
        locked.chmod(0o700)


@requires_posix_shell
@pytest.mark.parametrize("create_parents", [True, False])
def test_make_directory_over_an_existing_file_reports_it_exists(
    tmp_path, create_parents
):
    """Measured on Modal: PathAlreadyExists in both modes, not NotADirectory."""
    existing = tmp_path / "afile"
    existing.write_text("x")
    result = run_script(fs.make_make_directory_command(str(existing), create_parents))
    assert result.returncode == fs.EXIT_ALREADY_EXISTS


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
def test_make_directory_with_parents_reports_a_file_in_the_way(tmp_path):
    blocker = tmp_path / "afile"
    blocker.write_text("x")
    result = run_script(fs.make_make_directory_command(str(blocker / "a" / "b"), True))
    assert result.returncode == fs.EXIT_NOT_A_DIRECTORY


@requires_posix_shell
def test_remove_deletes_a_file(tmp_path):
    target = tmp_path / "f.txt"
    target.write_text("x")
    assert run_script(fs.make_remove_command(str(target), False)).returncode == 0
    assert not target.exists()


@requires_posix_shell
def test_remove_deletes_an_empty_directory(tmp_path):
    target = tmp_path / "empty"
    target.mkdir()
    assert run_script(fs.make_remove_command(str(target), False)).returncode == 0
    assert not target.exists()


@requires_posix_shell
def test_remove_refuses_a_non_empty_directory_without_recursive(tmp_path):
    target = tmp_path / "full"
    target.mkdir()
    (target / "child.txt").write_text("x")
    assert (
        run_script(fs.make_remove_command(str(target), False)).returncode
        == fs.EXIT_DIRECTORY_NOT_EMPTY
    )
    assert target.exists()


@requires_posix_shell
def test_remove_recursive_deletes_a_tree(tmp_path):
    target = tmp_path / "full"
    (target / "nested").mkdir(parents=True)
    (target / "nested" / "child.txt").write_text("x")
    assert run_script(fs.make_remove_command(str(target), True)).returncode == 0
    assert not target.exists()


@requires_posix_shell
def test_remove_classifies_a_missing_path(tmp_path):
    assert (
        run_script(fs.make_remove_command(str(tmp_path / "absent"), False)).returncode
        == fs.EXIT_NOT_FOUND
    )


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
def test_a_missing_path_several_levels_down_is_not_found(tmp_path):
    for build in (fs.make_stat_command, fs.make_list_files_command):
        result = run_script(build(str(tmp_path / "a" / "b" / "c")))
        assert result.returncode == fs.EXIT_NOT_FOUND


@requires_posix_shell
def test_make_directory_without_parents_takes_a_trailing_slash(tmp_path):
    """Measured on Modal: /tmp/n/ creates /tmp/n. The parent used to be
    computed from the path with its slash still on, and was reported absent."""
    result = run_script(
        fs.make_make_directory_command(f"{tmp_path}/new/", create_parents=False)
    )
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "new").is_dir()


@requires_posix_shell
@pytest.mark.parametrize("suffix", ["", "/", "//"])
def test_listed_paths_join_the_directory_with_one_slash(tmp_path, suffix):
    """Modal lists "/" as /bin, not //bin."""
    (tmp_path / "f").write_text("x")
    result = run_script(fs.make_list_files_command(f"{tmp_path}{suffix}"))
    assert result.returncode == 0, result.stderr
    assert [i.path for i in fs.parse_stat_records(result.stdout)] == [f"{tmp_path}/f"]


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
