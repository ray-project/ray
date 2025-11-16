import pathlib
import sys
import urllib
from typing import TYPE_CHECKING, List, Optional, Tuple, Union

from ray.data._internal.util import RetryingPyFileSystem, _resolve_custom_scheme

if TYPE_CHECKING:
    import pyarrow


def _get_fsspec_http_filesystem() -> "pyarrow.fs.PyFileSystem":
    """Get fsspec HTTPFileSystem wrapped in PyArrow PyFileSystem.

    Returns:
        PyFileSystem wrapping fsspec HTTPFileSystem.

    Raises:
        ImportError: If fsspec is not installed.
    """
    try:
        import fsspec  # noqa: F401
        from fsspec.implementations.http import HTTPFileSystem
    except ModuleNotFoundError:
        raise ImportError(
            "Please install fsspec to read files from HTTP."
        ) from None

    from pyarrow.fs import FSSpecHandler, PyFileSystem
    return PyFileSystem(FSSpecHandler(HTTPFileSystem()))


def _wrap_fsspec_filesystem(
    filesystem: "pyarrow.fs.FileSystem",
) -> "pyarrow.fs.PyFileSystem":
    """Validate and wrap an fsspec filesystem in PyArrow PyFileSystem.

    Args:
        filesystem: Filesystem to validate and wrap.

    Returns:
        PyFileSystem wrapping the fsspec filesystem.

    Raises:
        TypeError: If filesystem is not a valid pyarrow or fsspec filesystem.
    """
    from pyarrow.fs import FSSpecHandler, PyFileSystem

    err_msg = (
        f"The filesystem passed must either conform to "
        f"pyarrow.fs.FileSystem, or "
        f"fsspec.spec.AbstractFileSystem. The provided "
        f"filesystem was: {filesystem}"
    )
    try:
        import fsspec  # noqa: F401
    except ModuleNotFoundError:
        # If filesystem is not a pyarrow filesystem and fsspec isn't
        # installed, then filesystem is neither a pyarrow filesystem nor
        # an fsspec filesystem, so we raise a TypeError.
        raise TypeError(err_msg) from None

    if not isinstance(filesystem, fsspec.spec.AbstractFileSystem):
        raise TypeError(err_msg)

    return PyFileSystem(FSSpecHandler(filesystem))


def _try_resolve_with_encoding(
    path: str,
    filesystem: Optional["pyarrow.fs.FileSystem"],
) -> Tuple["pyarrow.fs.FileSystem", str]:
    """Try resolving a path with URL encoding for special characters.

    This handles paths with special characters like ';', '?', '#' that
    may cause URI parsing errors.

    Args:
        path: The path to resolve.
        filesystem: Optional filesystem to validate against.

    Returns:
        Tuple of (resolved_filesystem, resolved_path).

    Raises:
        Exception: Re-raises any errors that aren't URI parsing related.
    """
    from pyarrow.fs import _resolve_filesystem_and_path

    resolved_filesystem, resolved_path = _resolve_filesystem_and_path(
        _encode_url(path), filesystem
    )
    resolved_path = _decode_url(resolved_path)
    return resolved_filesystem, resolved_path


def _try_resolve_with_http_fsspec(
    path: str,
) -> Tuple["pyarrow.fs.PyFileSystem", str]:
    """Try resolving an HTTP(S) path using fsspec HTTPFileSystem.

    This is a fallback for when PyArrow doesn't recognize the HTTP(S) scheme.

    Args:
        path: The HTTP(S) path to resolve.

    Returns:
        Tuple of (http_filesystem, path).

    Raises:
        ImportError: If fsspec is not installed.
    """
    http_filesystem = _get_fsspec_http_filesystem()
    return http_filesystem, path


def _has_file_extension(path: str, extensions: Optional[List[str]]) -> bool:
    """Check if a path has a file extension in the provided list.

    Examples:
        >>> _has_file_extension("foo.csv", ["csv"])
        True
        >>> _has_file_extension("foo.CSV", ["csv"])
        True
        >>> _has_file_extension("foo.CSV", [".csv"])
        True
        >>> _has_file_extension("foo.csv", ["json", "jsonl"])
        False
        >>> _has_file_extension("foo.csv", None)
        True

    Args:
        path: The path to check.
        extensions: A list of extensions to check against. If `None`, any extension is
            considered valid.
    """
    assert extensions is None or isinstance(extensions, list), type(extensions)

    if extensions is None:
        return True

    # If the user-specified extensions don't contain a leading dot, we add it here
    extensions = [
        f".{ext.lower()}" if not ext.startswith(".") else ext.lower()
        for ext in extensions
    ]
    return any(path.lower().endswith(ext) for ext in extensions)


def _resolve_paths_and_filesystem(
    paths: Union[str, List[str]],
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
) -> Tuple[List[str], "pyarrow.fs.FileSystem"]:
    """
    Resolves and normalizes all provided paths, infers a filesystem from the
    paths and ensures that all paths use the same filesystem.

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation that should be used for
            reading these files. If None, a filesystem will be inferred. If not
            None, the provided filesystem will still be validated against all
            filesystems inferred from the provided paths to ensure
            compatibility.
    """
    import pyarrow as pa
    from pyarrow.fs import FileSystem, _resolve_filesystem_and_path

    if isinstance(paths, str):
        paths = [paths]
    if isinstance(paths, pathlib.Path):
        paths = [str(paths)]
    elif not isinstance(paths, list) or any(not isinstance(p, str) for p in paths):
        raise ValueError(
            "Expected `paths` to be a `str`, `pathlib.Path`, or `list[str]`, but got "
            f"`{paths}`"
        )
    elif len(paths) == 0:
        raise ValueError("Must provide at least one path.")

    if filesystem and not isinstance(filesystem, FileSystem):
        filesystem = _wrap_fsspec_filesystem(filesystem)

    resolved_paths = []
    for path in paths:
        path = _resolve_custom_scheme(path)
        try:
            resolved_filesystem, resolved_path = _resolve_filesystem_and_path(
                path, filesystem
            )
        except pa.lib.ArrowInvalid as e:
            error_str = str(e)
            if "Cannot parse URI" in error_str:
                # Try URL encoding for special characters
                try:
                    resolved_filesystem, resolved_path = _try_resolve_with_encoding(
                        path, filesystem
                    )
                except Exception as encoding_error:
                    raise ValueError(
                        f"Failed to resolve path '{path}': {encoding_error}"
                    ) from encoding_error
            elif "Unrecognized filesystem type in URI" in error_str:
                scheme = urllib.parse.urlparse(path, allow_fragments=False).scheme
                if scheme in ["http", "https"]:
                    # Try fsspec HTTP filesystem
                    try:
                        resolved_filesystem, resolved_path = _try_resolve_with_http_fsspec(
                            path
                        )
                    except ImportError as import_error:
                        raise ImportError(
                            f"Cannot resolve HTTP path '{path}': {import_error}"
                        ) from import_error
                else:
                    raise ValueError(
                        f"Unrecognized filesystem type in URI '{path}': {error_str}"
                    ) from e
            else:
                raise ValueError(
                    f"Failed to resolve path '{path}': {error_str}"
                ) from e

        if filesystem is None:
            filesystem = resolved_filesystem

        # If the PyArrow filesystem is handled by a fsspec HTTPFileSystem, the protocol/
        # scheme of paths should not be unwrapped/removed, because HTTPFileSystem
        # expects full file paths including protocol/scheme. This is different behavior
        # compared to other file system implementation in pyarrow.fs.FileSystem.
        if not _is_http_filesystem(filesystem):
            resolved_path = _unwrap_protocol(resolved_path)

        resolved_path = filesystem.normalize_path(resolved_path)
        resolved_paths.append(resolved_path)

    return resolved_paths, filesystem


def _is_http_filesystem(fs: "pyarrow.fs.FileSystem") -> bool:
    """Return whether ``fs`` is a PyFileSystem handled by a fsspec HTTPFileSystem."""
    from pyarrow.fs import FSSpecHandler, PyFileSystem

    # Try to import HTTPFileSystem
    try:
        from fsspec.implementations.http import HTTPFileSystem
    except ModuleNotFoundError:
        return False

    if isinstance(fs, RetryingPyFileSystem):
        fs = fs.unwrap()

    if not isinstance(fs, PyFileSystem):
        return False

    return isinstance(fs.handler, FSSpecHandler) and isinstance(
        fs.handler.fs, HTTPFileSystem
    )


def _unwrap_protocol(path):
    """
    Slice off any protocol prefixes on path.
    """
    if sys.platform == "win32" and _is_local_windows_path(path):
        # Represent as posix path such that downstream functions properly handle it.
        # This is executed when 'file://' is NOT included in the path.
        return pathlib.Path(path).as_posix()

    parsed = urllib.parse.urlparse(path, allow_fragments=False)  # support '#' in path
    params = ";" + parsed.params if parsed.params else ""  # support ';' in path
    query = "?" + parsed.query if parsed.query else ""  # support '?' in path
    netloc = parsed.netloc
    if parsed.scheme == "s3" and "@" in parsed.netloc:
        # If the path contains an @, it is assumed to be an anonymous
        # credentialed path, and we need to strip off the credentials.
        netloc = parsed.netloc.split("@")[-1]

    parsed_path = parsed.path
    # urlparse prepends the path with a '/'. This does not work on Windows
    # so if this is the case strip the leading slash.
    if (
        sys.platform == "win32"
        and not netloc
        and len(parsed_path) >= 3
        and parsed_path[0] == "/"  # The problematic leading slash
        and parsed_path[1].isalpha()  # Ensure it is a drive letter.
        and parsed_path[2:4] in (":", ":/")
    ):
        parsed_path = parsed_path[1:]

    return netloc + parsed_path + params + query


def _is_url(path) -> bool:
    return urllib.parse.urlparse(path).scheme != ""


def _is_http_url(path) -> bool:
    parsed = urllib.parse.urlparse(path)
    return parsed.scheme in ("http", "https")


def _is_local_windows_path(path: str) -> bool:
    """Determines if path is a Windows file-system location."""
    if sys.platform != "win32":
        return False

    if len(path) >= 1 and path[0] == "\\":
        return True
    if (
        len(path) >= 3
        and path[1] == ":"
        and (path[2] == "/" or path[2] == "\\")
        and path[0].isalpha()
    ):
        return True
    return False


def _encode_url(path):
    return urllib.parse.quote(path, safe="/:")


def _decode_url(path):
    return urllib.parse.unquote(path)
