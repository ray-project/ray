import logging
import pathlib
import sys
from typing import TYPE_CHECKING, List, Optional, Tuple, Union
from urllib.parse import quote, unquote, urlparse

from ray.data._internal.util import RetryingPyFileSystem, _resolve_custom_scheme

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import fsspec.spec
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
        raise ImportError("Please install fsspec to read files from HTTP.") from None

    from pyarrow.fs import FSSpecHandler, PyFileSystem

    return PyFileSystem(FSSpecHandler(HTTPFileSystem()))


def _validate_and_wrap_filesystem(
    filesystem: Optional[
        Union["pyarrow.fs.FileSystem", "fsspec.spec.AbstractFileSystem"]
    ],
) -> Optional["pyarrow.fs.FileSystem"]:
    """Validate filesystem and wrap fsspec filesystems in PyArrow.

    Args:
        filesystem: Filesystem to validate and potentially wrap. Can be None,
            a pyarrow.fs.FileSystem, or an fsspec.spec.AbstractFileSystem.

    Returns:
        None if filesystem is None, otherwise a pyarrow.fs.FileSystem
        (either the original if already PyArrow, or wrapped if fsspec).

    Raises:
        TypeError: If filesystem is not None and not a valid pyarrow or fsspec filesystem.
    """
    if filesystem is None:
        return None

    from pyarrow.fs import FileSystem

    if isinstance(filesystem, FileSystem):
        return filesystem

    try:
        import fsspec  # noqa: F401
    except ModuleNotFoundError:
        raise TypeError("fsspec is not installed") from None

    if not isinstance(filesystem, fsspec.spec.AbstractFileSystem):
        raise TypeError(
            f"Filesystem must conform to pyarrow.fs.FileSystem or "
            f"fsspec.spec.AbstractFileSystem, got: {type(filesystem).__name__}"
        )

    from pyarrow.fs import FSSpecHandler, PyFileSystem

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
    """
    from pyarrow.fs import _resolve_filesystem_and_path

    encoded_path = quote(path, safe="/:", errors="ignore")
    resolved_filesystem, resolved_path = _resolve_filesystem_and_path(
        encoded_path, filesystem
    )
    return resolved_filesystem, unquote(resolved_path, errors="ignore")


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


# Mapping from URI schemes to compatible filesystem type_name values.
# Used to validate that a cached filesystem is compatible with a given URI scheme
# before attempting to use it, avoiding silent failures from PyArrow when the
# wrong filesystem type is passed to _resolve_filesystem_and_path.
_SCHEME_TO_FS_TYPE_NAMES = {
    "": ("local",),  # No scheme = local filesystem
    "file": ("local",),  # file:// = local filesystem
    "s3": ("s3",),  # s3:// = S3 filesystem
    "s3a": ("s3",),  # s3a:// = S3 filesystem (Hadoop compat)
    "gs": ("gcs",),  # gs:// = GCS filesystem
    "gcs": ("gcs",),  # gcs:// = GCS filesystem
    "hdfs": ("hdfs",),  # hdfs:// = Hadoop filesystem
    "viewfs": ("hdfs",),  # viewfs:// = Hadoop filesystem
    "abfs": ("abfs",),  # abfs:// = Azure Blob FileSystem
    "abfss": ("abfs",),  # abfss:// = Azure Blob FileSystem (TLS)
    "http": ("py",),  # http:// = fsspec HTTP (wrapped in PyFileSystem)
    "https": ("py",),  # https:// = fsspec HTTP (wrapped in PyFileSystem)
}


def _is_filesystem_compatible_with_scheme(
    filesystem: "pyarrow.fs.FileSystem",
    scheme: str,
) -> bool:
    """Check if a filesystem is compatible with a URI scheme.

    Uses PyArrow's `type_name` property for reliable filesystem type detection.
    This prevents silently using the wrong filesystem for a URI, which can result
    in malformed paths or incorrect behavior.

    Args:
        filesystem: The PyArrow filesystem to check.
        scheme: The URI scheme (e.g., 's3', 'gs', 'http', 'file', '').

    Returns:
        True if the filesystem can handle the scheme, False otherwise.
    """
    # Get expected type names for this scheme
    expected_types = _SCHEME_TO_FS_TYPE_NAMES.get(scheme.lower())
    if expected_types is None:
        # Unknown scheme (e.g., abfs://, az://, custom protocols) - trust user's filesystem
        # This preserves backward compatibility for custom filesystems
        return True

    # Get the actual filesystem type
    fs_type = filesystem.type_name

    # For PyFileSystem (fsspec wrappers), also check if it's HTTP
    if fs_type == "py" and scheme in ("http", "https"):
        return _is_http_filesystem(filesystem)

    return fs_type in expected_types


def _resolve_single_path_with_fallback(
    path: str,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
) -> Tuple["pyarrow.fs.FileSystem", str]:
    """Resolve a single path with filesystem, with fallback to re-resolution on error.

    This is a helper for lazy filesystem resolution. If a filesystem is provided,
    it first validates that the filesystem type is compatible with the URI scheme,
    then attempts to resolve the path. If the filesystem is incompatible or
    resolution fails, it re-resolves without the cached filesystem.

    Args:
        path: A single file/directory path.
        filesystem: Optional cached filesystem from previous resolution.

    Returns:
        Tuple of (resolved_filesystem, resolved_path).

    Raises:
        ValueError: If path resolution fails.
        ImportError: If required dependencies are missing.
    """
    import pyarrow as pa
    from pyarrow.fs import _resolve_filesystem_and_path

    path = _resolve_custom_scheme(path)

    # Validate/wrap filesystem if needed
    try:
        filesystem = _validate_and_wrap_filesystem(filesystem)
    except TypeError as e:
        raise ValueError(f"Invalid filesystem provided: {e}") from e

    # Parse scheme to validate filesystem compatibility
    parsed = urlparse(path, allow_fragments=False)
    scheme = parsed.scheme.lower() if parsed.scheme else ""

    # Check HTTP scheme FIRST - PyArrow doesn't support HTTP/HTTPS natively
    if scheme in ("http", "https"):
        # If we have a compatible cached HTTP filesystem, use it
        if filesystem is not None and _is_filesystem_compatible_with_scheme(
            filesystem, scheme
        ):
            return filesystem, path
        # Otherwise create a new HTTP filesystem
        try:
            resolved_filesystem = _get_fsspec_http_filesystem()
            resolved_path = path
            return resolved_filesystem, resolved_path
        except ImportError as import_error:
            raise ImportError(
                f"Cannot resolve HTTP path '{path}': {import_error}"
            ) from import_error

    # Try with provided filesystem only if scheme is compatible (fast path for cached FS)
    if filesystem is not None and _is_filesystem_compatible_with_scheme(
        filesystem, scheme
    ):
        try:
            _, resolved_path = _resolve_filesystem_and_path(path, filesystem)
            # Return the wrapped filesystem we passed in.
            return filesystem, resolved_path
        except Exception:
            # Fall through to full resolution without cached filesystem
            pass

    # Full resolution without cached filesystem
    try:
        resolved_filesystem, resolved_path = _resolve_filesystem_and_path(path, None)
    except (pa.lib.ArrowInvalid, ValueError) as original_error:
        # Try URL encoding for paths with special characters that may cause parsing issues
        try:
            resolved_filesystem, resolved_path = _try_resolve_with_encoding(path, None)
        except (pa.lib.ArrowInvalid, ValueError, TypeError) as encoding_error:
            # If encoding doesn't help, raise with both errors for full context
            raise ValueError(
                f"Failed to resolve path '{path}'. Initial error: {original_error}. "
                f"URL encoding fallback also failed: {encoding_error}"
            ) from original_error
    except TypeError as e:
        raise ValueError(f"The path: '{path}' has an invalid type {e}") from e

    return resolved_filesystem, resolved_path


def _resolve_paths_and_filesystem(
    paths: Union[str, List[str]],
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
) -> Tuple[List[str], "pyarrow.fs.FileSystem"]:
    """
    Resolves and normalizes all provided paths, infers a filesystem from the
    paths and assumes that all paths use the same filesystem.

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation that should be used for
            reading these files. If None, a filesystem will be inferred. If not
            None, the provided filesystem will still be validated against all
            filesystems inferred from the provided paths to ensure
            compatibility.
    """
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

    # Validate/wrap filesystem upfront so we return a proper PyArrow filesystem
    filesystem = _validate_and_wrap_filesystem(filesystem)

    resolved_paths = []
    for path in paths:
        try:
            resolved_filesystem, resolved_path = _resolve_single_path_with_fallback(
                path, filesystem
            )
        except (ValueError, ImportError) as e:
            logger.warning(f"Failed to resolve path '{path}': {e}, skipping")
            continue

        if filesystem is None:
            filesystem = resolved_filesystem

        # If the PyArrow filesystem is handled by a fsspec HTTPFileSystem, the protocol/
        # scheme of paths should not be unwrapped/removed, because HTTPFileSystem
        # expects full file paths including protocol/scheme. This is different behavior
        # compared to other file system implementation in pyarrow.fs.FileSystem.
        if not _is_http_filesystem(resolved_filesystem):
            resolved_path = _unwrap_protocol(resolved_path)

        resolved_path = resolved_filesystem.normalize_path(resolved_path)
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

    parsed = urlparse(path, allow_fragments=False)  # support '#' in path
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
    return urlparse(path).scheme != ""


def _is_http_url(path) -> bool:
    parsed = urlparse(path)
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
