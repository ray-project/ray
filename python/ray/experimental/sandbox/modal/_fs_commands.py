"""Shell commands and error translation behind ``Sandbox.filesystem``.

Modal implements its filesystem API by exec'ing a purpose-built helper binary
that emits JSON. No such binary exists in an arbitrary container image, so the
equivalent here is a small POSIX ``/bin/sh`` script per operation: a prologue
that classifies the failure with a distinct exit code, then the work itself.

Keying on exit codes rather than parsing stderr keeps the mapping independent
of locale and of which ``coreutils`` the image ships (GNU or busybox).

Known limitation: records are newline-separated, so paths containing a literal
newline cannot be listed or stat'ed.
"""

import contextlib
import logging
import posixpath
import random
import string
from typing import List, Optional

import ray
from ray.experimental.sandbox.exceptions import SandboxError
from ray.experimental.sandbox.modal.exception import (
    ConnectionError,
    Error,
    InvalidError,
    NotFoundError,
    SandboxFilesystemDirectoryNotEmptyError,
    SandboxFilesystemError,
    SandboxFilesystemFileTooLargeError,
    SandboxFilesystemIsADirectoryError,
    SandboxFilesystemNotADirectoryError,
    SandboxFilesystemNotFoundError,
    SandboxFilesystemPathAlreadyExistsError,
    SandboxFilesystemPermissionError,
    ServiceError,
)
from ray.experimental.sandbox.modal.types import FileInfo, FileType

logger = logging.getLogger(__name__)

# Every filesystem command runs under /bin/sh, which even minimal images have.
SHELL = "/bin/sh"

# Field separator inside one stat record.
_US = "\x1f"

# Exit codes the prologues use to report a classified failure.
EXIT_NOT_FOUND = 10
EXIT_NOT_A_DIRECTORY = 11
EXIT_IS_A_DIRECTORY = 12
EXIT_PERMISSION_DENIED = 13
EXIT_DIRECTORY_NOT_EMPTY = 14
EXIT_ALREADY_EXISTS = 15
EXIT_FILE_TOO_LARGE = 16
# Not a failure: the file is larger than the caller will take in one reply,
# so it should be streamed instead.
EXIT_NOT_INLINE = 17
# list_files on a path that is a file, which Modal words differently from a
# file further up the path.
EXIT_EXPECTED_A_DIRECTORY = 18

# The largest file a single read returns, Modal's: its helper refuses anything
# bigger, measured with a 6 GiB file.
MAX_READ_FILE_BYTES = 5 * 1024 * 1024 * 1024

# One record per path: mode, size, uid, gid, owner, group, mtime, type, the
# name with any symlink target, path. stat does not dereference symlinks by
# default, so the record describes the link itself, as Modal documents.
#
# %N rather than a `readlink` subshell, and one stat invocation covering many
# operands rather than one per entry: those were two process spawns per
# directory entry, and inside gVisor a spawn costs enough that they dominated
# the cost of listing a directory. Both forms are busybox-compatible.
_STAT_FORMAT = f"%f{_US}%s{_US}%u{_US}%g{_US}%U{_US}%G{_US}%Y{_US}%F{_US}%N{_US}%n"

# GNU stat quotes and escapes %N -- a target with a quote or a non-ASCII byte
# came back mangled -- unless told not to. busybox ignores this and quotes
# plainly, which the parser also reads.
_STAT = f'QUOTING_STYLE=literal stat -c "{_STAT_FORMAT}"'

# How many paths go into a single stat call. Bounded because the operands
# become one argv, and an unbounded directory would build one past ARG_MAX and
# fail outright where the per-entry form merely ran slowly.
_STAT_BATCH = 200

_NUM_STAT_FIELDS = 10

# What %N puts between the name and the target of a symlink.
_LINK_ARROW = " -> "

# What both stats print for an owner with no passwd or group entry. Modal
# reports the numeric id instead.
_UNKNOWN_OWNER = "UNKNOWN"

# stat's %F values that are not files, directories, or symlinks (sockets,
# fifos, device nodes) collapse to FILE, since FileType has no case for them.
_FILE_TYPES = {
    "directory": FileType.DIRECTORY,
    "symbolic link": FileType.SYMLINK,
}


def _sh(script: str, *args: str) -> List[str]:
    """Build the argv for running ``script`` with positional arguments."""
    return [SHELL, "-c", script, "sh", *args]


def _classify_missing(var: str, otherwise: int) -> str:
    """Why ``$var`` cannot be reached, as an exit code.

    Walks up to the nearest ancestor that exists -- the only way a shell can
    tell the errno apart after the fact. A file there means a component is not
    a directory (ENOTDIR), and one that cannot be searched means a permission
    denial; anything else exits ``otherwise``: 10 for a path that is simply
    missing, 13 for a mkdir that failed under a searchable directory. Modal
    raises a different exception for each, at any depth -- a file three levels
    up is as much "not a directory" as the parent.

    ``-e`` follows symlinks, as the kernel does: a dangling link on the way is
    a missing directory, not a file in the way.
    """
    return (
        f'{{ a="${var}"; '
        'while [ -n "$a" ] && [ ! -e "$a" ]; do a=${a%/*}; done; '
        '[ -n "$a" ] || a=/; '
        '[ -d "$a" ] || exit 11; '
        '[ -x "$a" ] || exit 13; '
        f"exit {otherwise}; }}"
    )


# Strip trailing slashes from $p, leaving "/" alone. A trailing slash only
# demands a directory, so "/tmp/new/" names /tmp/new -- and the parent has to
# be computed from that, or it came out as the path itself.
_STRIP_TRAILING_SLASHES = 'case "$p" in */) q=${p%"${p##*[!/]}"}; p=${q:-/};; esac; '


# -- command builders ------------------------------------------------------


def make_stat_command(remote_path: str) -> List[str]:
    """Build the command that stats a single path."""
    script = (
        'p="$1"; '
        'if [ ! -e "$p" ] && [ ! -L "$p" ]; then '
        f"  {_classify_missing('p', EXIT_NOT_FOUND)}; "
        "fi; "
        # No `|| exit 13` here. The path exists and its parent was searchable
        # -- `test -e` above did its own stat -- so a failure now is the tool
        # itself failing, not a permission problem. Letting stat's own code
        # through lands it in the generic fallback, which is what Modal reports
        # for a failure it cannot classify.
        f'{_STAT} "$p"'
    )
    return _sh(script, remote_path)


def make_list_files_command(remote_path: str) -> List[str]:
    """Build the command that lists the entries of a directory.

    Entries are collected into the positional parameters and stat'd in batches,
    so the cost is one process per _STAT_BATCH entries rather than two per
    entry.
    """
    # stat exits 1 when an operand has vanished since the glob, and prints the
    # rest anyway. The entry is skipped, as Modal skips it; anything else --
    # 127 for an image without stat -- is a real failure and propagates.
    batch = f'{_STAT} "$@"; rc=$?; [ "$rc" -le 1 ] || exit "$rc"; '
    script = (
        'p="$1"; '
        f'[ -e "$p" ] || {_classify_missing("p", EXIT_NOT_FOUND)}; '
        f'[ -d "$p" ] || exit {EXIT_EXPECTED_A_DIRECTORY}; '
        '[ -r "$p" ] && [ -x "$p" ] || exit 13; '
        # Joined to each name with a single slash, as Modal does: listing "/"
        # used to report "//bin".
        'p=${p%"${p##*[!/]}"}; '
        # $1 is saved in $p, so the positional list is free to use as the
        # accumulator -- the one list a POSIX shell can append to.
        "shift; n=0; "
        'for f in "$p"/* "$p"/.*; do '
        "  b=${f##*/}; "
        '  [ "$b" = "." ] && continue; '
        '  [ "$b" = ".." ] && continue; '
        '  { [ -e "$f" ] || [ -L "$f" ]; } || continue; '
        '  set -- "$@" "$f"; '
        "  n=$((n+1)); "
        f'  if [ "$n" -ge {_STAT_BATCH} ]; then {batch}set --; n=0; fi; '
        "done; "
        f'[ "$n" -eq 0 ] || {{ {batch}}}'
    )
    return _sh(script, remote_path)


def make_read_file_command(
    remote_path: str, limit: Optional[int] = None, inline: Optional[int] = None
) -> List[str]:
    """Build the command that writes a file's bytes to stdout.

    Args:
        remote_path: Absolute path to read.
        limit: Refuse a file larger than this many bytes, exiting
            ``EXIT_FILE_TOO_LARGE`` with the size on stderr before reading
            any of it -- Modal refuses a 6 GiB file in milliseconds. A file
            whose size stat under-reports is not caught here; a streaming
            caller enforces the limit on what it receives. None reads the
            whole file.
        inline: Exit ``EXIT_NOT_INLINE``, again before reading anything, for
            a file larger than this. For a caller that takes small files in
            one reply and streams the rest.

    Returns:
        The argv that runs the read.
    """
    size_checks = ""
    if limit is not None or inline is not None:
        # The size stat reports, which is 0 for /proc files and devices that
        # have plenty to say -- so the checks below are not the whole story.
        size_checks = 's=$(stat -L -c %s "$p" 2>/dev/null) || s=0; '
    if limit is not None:
        size_checks += f'[ "$s" -le {limit} ] || {{ echo "$s" >&2; exit {EXIT_FILE_TOO_LARGE}; }}; '
    if inline is not None:
        size_checks += f'[ "$s" -le {inline} ] || exit {EXIT_NOT_INLINE}; '
    # A one-reply read is capped here, one byte past the inline size, so a file
    # stat under-reports cannot grow the reply without bound; the caller sees
    # the extra byte and streams instead. A streamed read is not: its reader
    # counts what arrives against ``limit`` and stops the command itself. `cat`
    # rather than `head -c` for it, because busybox's head reads in small
    # blocks and is syscall-bound under gVisor: a 64 MiB read ran at 18 MiB/s
    # through head and 78 MiB/s through cat, measured.
    body = 'cat "$p"' if inline is None else f'head -c {inline + 1} "$p"'
    script = (
        'p="$1"; '
        # A file in the way is ENOTDIR, which Modal reports for a read as the
        # generic error rather than as the path being absent.
        f'[ -e "$p" ] || {_classify_missing("p", EXIT_NOT_FOUND)}; '
        '[ -d "$p" ] && exit 12; '
        '[ -r "$p" ] || exit 13; '
        f"{size_checks}{body}"
    )
    return _sh(script, remote_path)


def make_write_file_command(
    remote_path: str, temp_path: Optional[str] = None
) -> List[str]:
    """Build the command that writes stdin to a file, creating parents.

    The payload lands in ``temp_path`` and is moved onto ``remote_path`` only
    once it has all arrived, so an interrupted transfer -- a client that dies
    mid-upload, a sandbox that hits its timeout -- leaves the destination as it
    was. Writing straight to it truncated the file on the first line of the
    script and left it partial, destroying the previous contents before a single
    byte of the new ones was known to be coming.

    Consequences of installing a new inode rather than rewriting in place,
    each measured to match Modal: an overwrite leaves the file owned by the
    writer with default permissions (0644 under the usual umask) rather than
    keeping the old ones, and a ``remote_path`` that is a symlink is replaced
    rather than written through.

    Args:
        remote_path: Absolute destination path.
        temp_path: Scratch path in the same directory, so the move is a rename
            rather than a copy across filesystems. Defaults to one derived from
            ``remote_path``; pass it only to make a test deterministic.

    Returns:
        The argv that runs the write, reading the payload from its stdin.
    """
    if temp_path is None:
        temp_path = make_temp_path(remote_path)
    script = (
        'p="$1"; t="$2"; '
        # A trailing slash demands a directory, so writing a file there is
        # EISDIR -- which is what open(2) reports and what Modal surfaces.
        # Without this the path fell through to the parent check below and was
        # reported as a permission denial.
        'case "$p" in */) exit 12;; esac; '
        '[ -d "$p" ] && exit 12; '
        'd=${p%/*}; [ -n "$d" ] || d=/; '
        'if [ ! -d "$d" ]; then '
        '  [ -e "$d" ] && exit 11; '
        f'  mkdir -p "$d" 2>/dev/null || {_classify_missing("d", 13)}; '
        "fi; "
        '[ -w "$d" ] || exit 13; '
        # With the directory known to be writable, a failure to create or fill
        # the file is something else -- a full disk, a quota, EFBIG -- and
        # reporting it as a permission denial sent people to chmod. The
        # command's own message says what it was.
        ': > "$t" || exit 1; '
        'cat >> "$t" || { rm -f "$t"; exit 1; }; '
        'mv -f "$t" "$p" 2>/dev/null || { rm -f "$t"; exit 13; }'
    )
    return _sh(script, remote_path, temp_path)


def make_remove_temp_command(temp_path: str) -> List[str]:
    """Build the command that deletes a write's temporary file, if present."""
    return _sh('rm -f "$1"', temp_path)


def make_temp_path(remote_path: str) -> str:
    """A scratch path beside ``remote_path`` for :func:`make_write_file_command`.

    A random suffix, as ``copy_to_local`` uses locally: a fixed name would let
    two concurrent writes to one destination clobber each other's partial file
    before either was moved into place.
    """
    directory, _, name = remote_path.rpartition("/")
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return f"{directory}/.{name}.ray-sandbox-tmp-{suffix}"


def make_make_directory_command(remote_path: str, create_parents: bool) -> List[str]:
    """Build the command that creates a directory."""
    script = (
        'p="$1"; parents="$2"; '
        f"{_STRIP_TRAILING_SLASHES}"
        'if [ "$parents" = "1" ]; then '
        '  [ -d "$p" ] && exit 0; '
        # A file already at the path: "already exists" in both modes, as
        # measured on Modal. A file further up the path is still "not a
        # directory", via the classifier below.
        '  [ -e "$p" ] && exit 15; '
        # A dangling symlink is neither -d nor -e, but mkdir still fails EEXIST
        # on it. Reported as "already exists", matching both the create_parents
        # =False branch below and what Modal reports; it used to fall through
        # to the mkdir and be misreported as a permission denial.
        '  [ -L "$p" ] && exit 15; '
        f'  mkdir -p "$p" 2>/dev/null || {_classify_missing("p", 13)}; '
        "  exit 0; "
        "fi; "
        '{ [ -e "$p" ] || [ -L "$p" ]; } && exit 15; '
        'd=${p%/*}; [ -n "$d" ] || d=/; '
        f'[ -e "$d" ] || {_classify_missing("d", EXIT_NOT_FOUND)}; '
        '[ -d "$d" ] || exit 11; '
        'mkdir "$p" 2>/dev/null || exit 13'
    )
    return _sh(script, remote_path, "1" if create_parents else "0")


def make_remove_command(remote_path: str, recursive: bool) -> List[str]:
    """Build the command that removes a file or directory."""
    script = (
        'p="$1"; recursive="$2"; '
        '{ [ -e "$p" ] || [ -L "$p" ]; } || '
        f"{_classify_missing('p', EXIT_NOT_FOUND)}; "
        'if [ -d "$p" ] && [ ! -L "$p" ]; then '
        '  if [ "$recursive" = "1" ]; then '
        '    rm -rf "$p" 2>/dev/null || exit 13; '
        "  else "
        # rmdir first, and only classify the failure if it fails: `ls -A`
        # forks a subshell and captures the whole directory listing into a
        # variable to answer a boolean, on a path that is usually about to
        # succeed anyway.
        '    rmdir "$p" 2>/dev/null && exit 0; '
        '    if [ -n "$(ls -A "$p" 2>/dev/null)" ]; then exit 14; fi; '
        "    exit 13; "
        "  fi; "
        "else "
        '  rm -f "$p" 2>/dev/null || exit 13; '
        "fi"
    )
    return _sh(script, remote_path, "1" if recursive else "0")


# -- output parsing --------------------------------------------------------


def _symlink_target(name_n: str, path: str) -> Optional[str]:
    """Pull the target out of stat's %N rendering of the symlink at ``path``.

    GNU stat under ``QUOTING_STYLE=literal`` prints ``path -> target``, and
    busybox ``'path' -> 'target'`` whatever the environment. The path is known
    exactly -- it is ``%n`` -- so the target is whatever follows that prefix,
    byte for byte: splitting on the arrow instead cut a target that contained
    one, and a quote inside it defeated the unquoting.
    """
    for prefix, suffix in (
        (f"{path}{_LINK_ARROW}", ""),
        (f"'{path}'{_LINK_ARROW}'", "'"),
    ):
        if name_n.startswith(prefix) and name_n.endswith(suffix):
            return name_n[len(prefix) : len(name_n) - len(suffix)]
    # A stat quoting some other way: take the arrow, and strip plain quotes.
    _, arrow, target = name_n.partition(_LINK_ARROW)
    if not arrow:
        return None
    if len(target) >= 2 and target[0] == target[-1] == "'":
        target = target[1:-1]
    return target


def parse_stat_records(stdout: bytes) -> List[FileInfo]:
    """Parse the records emitted by the stat calls above."""
    entries = []
    # "\n" exactly: stat ends each record with one, while splitlines() also
    # splits on \x1c-\x1e, \x85 and  , all legal in a file name.
    for line in stdout.decode("utf-8", errors="replace").split("\n"):
        if not line.strip():
            continue
        fields = line.split(_US)
        if len(fields) < _NUM_STAT_FIELDS:
            continue
        (
            mode_hex,
            size,
            uid,
            gid,
            owner,
            group,
            mtime,
            kind,
            name_n,
            path,
        ) = fields[:_NUM_STAT_FIELDS]
        file_type = _FILE_TYPES.get(kind.strip().lower(), FileType.FILE)
        entries.append(
            FileInfo(
                # "" for "/", as Modal names it.
                name=posixpath.basename(path.rstrip("/")),
                path=path,
                type=file_type,
                size=_to_int(size),
                mode=_to_int(mode_hex, base=16),
                permissions=_octal_permissions(mode_hex),
                owner=uid if owner == _UNKNOWN_OWNER else owner,
                group=gid if group == _UNKNOWN_OWNER else group,
                modified_time=float(_to_int(mtime)),
                symlink_target=(
                    _symlink_target(name_n, path)
                    if file_type == FileType.SYMLINK
                    else None
                ),
            )
        )
    return entries


def _octal_permissions(mode_hex: str) -> str:
    """The permission bits as four octal digits -- "0644", "1777" -- which is
    the form Modal's FileInfo carries, rather than stat's symbolic %A."""
    return f"{_to_int(mode_hex, base=16) & 0o7777:04o}"


def _to_int(value: str, base: int = 10) -> int:
    try:
        return int(value.strip(), base)
    except ValueError:
        return 0


# -- error translation -----------------------------------------------------


def _fallback(returncode: int, stderr: bytes, remote_path: str):
    # A failure the script did not classify -- a full disk, an image without
    # stat. Modal's helper reports those as its own message in the base error,
    # so the command's last line of stderr is the equivalent; with nothing on
    # stderr the message is Modal's own for an unexplained exit.
    text = stderr.decode("utf-8", errors="replace").strip() if stderr else ""
    if text:
        logger.debug("sandbox filesystem command stderr: %s", text)
        return SandboxFilesystemError(text.splitlines()[-1].strip())
    return SandboxFilesystemError(
        f"Operation on '{remote_path}' failed with exit code {returncode}"
    )


def _raise_mapped(returncode, stderr, remote_path, mapping):
    exc_class = mapping.get(returncode)
    if exc_class is SandboxFilesystemFileTooLargeError:
        raise exc_class(_too_large_message(stderr, remote_path))
    if exc_class is SandboxFilesystemError:
        # Modal's generic error carries its message alone, without the path.
        raise exc_class(_MESSAGES[returncode])
    if exc_class is not None:
        raise exc_class(f"{_MESSAGES[returncode]}: {remote_path}")
    raise _fallback(returncode, stderr, remote_path)


def _too_large_message(stderr: bytes, remote_path: str) -> str:
    """Modal's wording, with the size the read command reported on stderr."""
    size = stderr.decode("utf-8", errors="replace").strip() if stderr else ""
    if size.isdigit():
        return (
            f"file is {size} bytes, which exceeds the {MAX_READ_FILE_BYTES} "
            f"byte limit: {remote_path}"
        )
    return f"file exceeds the {MAX_READ_FILE_BYTES} byte limit: {remote_path}"


# Modal's wording for each, as its helper reports them.
_MESSAGES = {
    EXIT_NOT_FOUND: "path does not exist",
    EXIT_NOT_A_DIRECTORY: "a component of the path is not a directory",
    EXIT_IS_A_DIRECTORY: "expected a file path",
    EXIT_PERMISSION_DENIED: "permission denied",
    EXIT_DIRECTORY_NOT_EMPTY: "directory is not empty",
    EXIT_ALREADY_EXISTS: "path already exists",
    EXIT_EXPECTED_A_DIRECTORY: "expected a directory path",
}

_STAT_MAP = {
    EXIT_NOT_FOUND: SandboxFilesystemNotFoundError,
    EXIT_NOT_A_DIRECTORY: SandboxFilesystemNotADirectoryError,
    EXIT_PERMISSION_DENIED: SandboxFilesystemPermissionError,
}

_LIST_MAP = {
    EXIT_NOT_FOUND: SandboxFilesystemNotFoundError,
    EXIT_NOT_A_DIRECTORY: SandboxFilesystemNotADirectoryError,
    EXIT_EXPECTED_A_DIRECTORY: SandboxFilesystemNotADirectoryError,
    EXIT_PERMISSION_DENIED: SandboxFilesystemPermissionError,
}

# A file in the way is the base error for a read and a removal: that is how
# Modal reports ENOTDIR for those two, measured.
_READ_MAP = {
    EXIT_NOT_FOUND: SandboxFilesystemNotFoundError,
    EXIT_NOT_A_DIRECTORY: SandboxFilesystemError,
    EXIT_IS_A_DIRECTORY: SandboxFilesystemIsADirectoryError,
    EXIT_PERMISSION_DENIED: SandboxFilesystemPermissionError,
    EXIT_FILE_TOO_LARGE: SandboxFilesystemFileTooLargeError,
}

_WRITE_MAP = {
    EXIT_NOT_A_DIRECTORY: SandboxFilesystemNotADirectoryError,
    EXIT_IS_A_DIRECTORY: SandboxFilesystemIsADirectoryError,
    EXIT_PERMISSION_DENIED: SandboxFilesystemPermissionError,
}

_MKDIR_MAP = {
    EXIT_NOT_FOUND: SandboxFilesystemNotFoundError,
    EXIT_NOT_A_DIRECTORY: SandboxFilesystemNotADirectoryError,
    EXIT_PERMISSION_DENIED: SandboxFilesystemPermissionError,
    EXIT_ALREADY_EXISTS: SandboxFilesystemPathAlreadyExistsError,
}

_REMOVE_MAP = {
    EXIT_NOT_FOUND: SandboxFilesystemNotFoundError,
    EXIT_NOT_A_DIRECTORY: SandboxFilesystemError,
    EXIT_DIRECTORY_NOT_EMPTY: SandboxFilesystemDirectoryNotEmptyError,
    EXIT_PERMISSION_DENIED: SandboxFilesystemPermissionError,
}


def raise_stat_error(returncode: int, stderr: bytes, remote_path: str):
    """Raise the exception matching a failed stat."""
    _raise_mapped(returncode, stderr, remote_path, _STAT_MAP)


def raise_list_files_error(returncode: int, stderr: bytes, remote_path: str):
    """Raise the exception matching a failed directory listing."""
    _raise_mapped(returncode, stderr, remote_path, _LIST_MAP)


def raise_read_file_error(returncode: int, stderr: bytes, remote_path: str):
    """Raise the exception matching a failed read."""
    _raise_mapped(returncode, stderr, remote_path, _READ_MAP)


def raise_write_file_error(returncode: int, stderr: bytes, remote_path: str):
    """Raise the exception matching a failed write."""
    _raise_mapped(returncode, stderr, remote_path, _WRITE_MAP)


def raise_make_directory_error(returncode: int, stderr: bytes, remote_path: str):
    """Raise the exception matching a failed directory creation."""
    _raise_mapped(returncode, stderr, remote_path, _MKDIR_MAP)


def raise_remove_error(returncode: int, stderr: bytes, remote_path: str):
    """Raise the exception matching a failed removal."""
    _raise_mapped(returncode, stderr, remote_path, _REMOVE_MAP)


def validate_absolute_remote_path(remote_path: str, operation: str) -> None:
    """Reject a relative path, the way Modal does.

    Args:
        remote_path: The path the caller passed.
        operation: The filesystem method's name, for the message.

    Raises:
        InvalidError: ``remote_path`` is not absolute.
    """
    if not isinstance(remote_path, str) or not remote_path.startswith("/"):
        raise InvalidError(
            f"Sandbox.filesystem.{operation}() currently only supports "
            f"absolute remote_path values"
        )


_SANDBOX_UNAVAILABLE = (
    "The Sandbox is unavailable. This Sandbox may have already shut down."
)


@contextlib.contextmanager
def translate_exec_errors(operation: str, remote_path: str):
    """Keep sandbox-level failures from surfacing as ``exec`` errors.

    A caller of ``filesystem.read_text()`` should never see a message about a
    failed command; they should see either a filesystem error or a clear
    statement that the Sandbox is gone.
    """
    try:
        yield
    except (SandboxFilesystemError, InvalidError):
        raise
    except (
        NotFoundError,
        ServiceError,
        ConnectionError,
        ray.exceptions.RayActorError,
    ) as exc:
        # The sandbox is gone: ended (NotFoundError from the actor, wrapped in
        # Ray's task error, whose message is the remote traceback) or its actor
        # dead. Modal says this for its equivalents, whatever the cause.
        raise NotFoundError(_SANDBOX_UNAVAILABLE) from exc
    except OSError:
        # A failure of the *local* file, not the sandbox: copy_to_local opens
        # and writes its destination inside this block. Modal documents
        # IsADirectoryError, NotADirectoryError and PermissionError as reaching
        # the caller unchanged, and wrapping them lost both the class and the
        # errno.
        raise
    except Error as exc:
        # Any other Modal error -- ClientClosed from a detached handle, say.
        # Every one of them is also a SandboxError, so this has to come first:
        # the clause below used to turn them all into "unavailable".
        raise SandboxFilesystemError(
            f"An unexpected error occurred during "
            f"Sandbox.filesystem.{operation}('{remote_path}'): {exc}"
        ) from exc
    except SandboxError as exc:
        # From the backend itself, which raises these once the container is
        # gone.
        raise NotFoundError(_SANDBOX_UNAVAILABLE) from exc
    except Exception as exc:
        raise SandboxFilesystemError(
            f"An unexpected error occurred during "
            f"Sandbox.filesystem.{operation}('{remote_path}'): {exc}"
        ) from exc


def exit_code_message(returncode: int) -> Optional[str]:
    """Human-readable text for a classified exit code, if it is one."""
    return _MESSAGES.get(returncode)
