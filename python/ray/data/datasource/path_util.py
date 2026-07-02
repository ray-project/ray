import fnmatch
import logging
import os
import pathlib
import re
import sys
from typing import TYPE_CHECKING, List, Optional, Tuple, Union
from urllib.parse import quote, unquote, urlparse

from ray.data._internal.util import (
    RetryingPyFileSystem,
    _normalize_paths_to_strings,
    _resolve_custom_scheme,
)

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

    Returns:
        ``True`` if ``path`` ends with one of the provided extensions (or
        ``extensions`` is ``None``), otherwise ``False``.
    """
    assert extensions is None or isinstance(extensions, list), type(extensions)

    if extensions is None:
        return True

    # If the user-specified extensions don't contain a leading dot, we add it here
    extensions = [
        f".{ext.lower()}" if not ext.startswith(".") else ext.lower()
        for ext in extensions
    ]
    # Ignore query components when checking extensions (for example,
    # versioned object-store paths like `...parquet?versionId=...`).
    # Keep `#` untouched because it can be part of object keys.
    parsed_path = path.split("?", 1)[0]
    return any(parsed_path.lower().endswith(ext) for ext in extensions)


def _is_local_filesystem(filesystem: Optional["pyarrow.fs.FileSystem"]) -> bool:
    """Check if *filesystem* is a local filesystem.

    Unwraps ``RetryingPyFileSystem`` (and similar wrappers) before
    checking, so that a wrapped ``LocalFileSystem`` is correctly
    identified as local.
    """
    import pyarrow.fs

    if filesystem is None:
        return True
    # Unwrap RetryingPyFileSystem to get the underlying filesystem.
    try:
        from ray.data._internal.util import RetryingPyFileSystem

        if isinstance(filesystem, RetryingPyFileSystem):
            filesystem = filesystem.unwrap()
    except ImportError:
        pass
    return isinstance(filesystem, pyarrow.fs.LocalFileSystem)


def _has_glob_chars(path: str) -> bool:
    """Check if ``path`` contains glob metacharacters.

    For URI paths with a scheme (e.g. ``s3://``), both the netloc
    (bucket/host) and path component are checked, but the query string
    is not — ``?`` in a query parameter is not treated as a glob char.

    ``[`` is treated as a glob char only when a matching ``]`` follows
    (forming a bracket expression ``[...]``) AND the ``[`` is not
    preceded by a space.  This avoids false positives on literal
    filenames like ``file [1].parquet``.
    """
    parsed = urlparse(path)
    # For URIs, check both the netloc (e.g. bucket-*) and the path component.
    check_target = (parsed.netloc + parsed.path) if parsed.scheme else path
    if "*" in check_target:
        return True
    # Only treat '?' as a glob char for cloud paths (with a scheme) and
    # only when it appears in the path component, not in a query string.
    # Local filenames may contain literal '?' (rare but legal on most
    # filesystems), and prior to expand_globs these paths worked unchanged.
    if parsed.scheme and "?" in parsed.path:
        return True
    # Bracket expression: [ followed by ], not preceded by a space.
    # Require at least one character between brackets to avoid matching empty [].
    # Only treat as glob for cloud paths (with a scheme).  Local directory
    # names may contain literal brackets (e.g. "data[backup]/") and prior
    # to expand_globs these paths worked unchanged.
    if parsed.scheme:
        return bool(re.search(r"(?<! )\[[^\]]+\]", parsed.path))
    return False


def _split_glob_base(pattern: str) -> Tuple[str, str]:
    """Split a glob pattern into ``(base_dir, glob_pattern)``.

    Finds the last path separator before the first glob character and
    splits there, so glob characters inside directory names (e.g.
    ``sub[12]``) belong to the pattern, not the base.

    For URI paths with a scheme (e.g. ``s3://``), only the path component
    is split — the scheme and authority are preserved in the base.

    Examples::

        _split_glob_base("s3://b/prefix/*.parquet")
        # -> ("s3://b/prefix", "*.parquet")

        _split_glob_base("s3://bucket/sub[12]/file.parquet")
        # -> ("s3://bucket", "sub[12]/file.parquet")

        _split_glob_base("*.parquet")
        # -> ("", "*.parquet")
    """
    # Only split on path component for URIs with scheme.
    parsed = urlparse(pattern)
    if parsed.scheme:
        # For URIs, split on the path component only.
        path = parsed.path
        glob_chars = ("*", "?", "[")
        first_glob = min(
            (path.index(c) for c in glob_chars if c in path),
            default=len(path),
        )
        prefix = path[:first_glob]
        # URI path components use "/" only — "\" is a literal key character.
        last_sep = prefix.rfind("/")
        if last_sep >= 0:
            base_path = path[:last_sep]
            glob_part = path[last_sep + 1 :]
            # Reconstruct full URI base.
            base = f"{parsed.scheme}://{parsed.netloc}{base_path}"
            return base, glob_part
        # Bucket-level glob (e.g. s3://bucket-*/file) — defensive fallback.
        # Note: _has_glob_chars currently checks netloc, but this branch
        # handles the case where a glob char appears only in the netloc.
        return f"{parsed.scheme}://{parsed.netloc}", path

    # Local path: split on last separator before first glob char.
    glob_chars = ("*", "?", "[")
    first_glob = min(
        (pattern.index(c) for c in glob_chars if c in pattern),
        default=len(pattern),
    )
    prefix = pattern[:first_glob]
    last_sep = prefix.rfind("/")
    # On Windows, backslash is also a path separator.
    if sys.platform == "win32":
        last_sep = max(last_sep, prefix.rfind("\\"))
    if last_sep >= 0:
        return prefix[:last_sep], pattern[last_sep + 1 :]
    return "", pattern


def _glob_match_path(pattern: str, relative: str) -> bool:
    """Segment-aware glob matching for cloud paths.

    Unlike :func:`fnmatch.fnmatch`, this function treats ``/`` as a path
    separator, so ``*`` matches only within a single path segment while
    ``**`` matches zero or more segments.

    Args:
        pattern: Glob pattern (e.g. ``*.parquet``, ``**/*.parquet``,
            ``sub[12]/file.parquet``).
        relative: Relative path to test (e.g. ``data-0.parquet``,
            ``sub-a/data-0.parquet``).

    Returns:
        ``True`` if *relative* matches *pattern*.
    """
    pat_segs = pattern.split("/")
    rel_segs = relative.split("/")
    return _glob_match_segs(pat_segs, rel_segs)


def _glob_match_segs(pat_segs: List[str], rel_segs: List[str]) -> bool:
    """Match pattern segments against path segments using dynamic programming.

    ``*`` matches one segment (no ``/``).  ``**`` matches zero or more
    segments.  Other glob chars (``?``, ``[...]``) work within a segment
    via :mod:`fnmatch`.

    Args:
        pat_segs: Pattern split by ``/`` into segments.
        rel_segs: Relative path split by ``/`` into segments.

    Returns:
        ``True`` if *rel_segs* matches *pat_segs*.
    """
    n_pat, n_rel = len(pat_segs), len(rel_segs)
    # dp[i][j] == True  <=>  pat_segs[:i] matches rel_segs[:j]
    dp = [[False] * (n_rel + 1) for _ in range(n_pat + 1)]
    dp[0][0] = True

    for i in range(1, n_pat + 1):
        seg = pat_segs[i - 1]
        if seg == "**":
            # ** matches zero or more segments: propagate from the same row.
            dp[i][0] = dp[i - 1][0]
            for j in range(1, n_rel + 1):
                dp[i][j] = dp[i - 1][j] or dp[i][j - 1]
        else:
            for j in range(1, n_rel + 1):
                if fnmatch.fnmatchcase(rel_segs[j - 1], seg):
                    dp[i][j] = dp[i - 1][j - 1]

    return dp[n_pat][n_rel]


def _expand_glob(
    path: str,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ignore_missing_paths: bool = False,
) -> List[str]:
    """Expand a glob pattern to concrete file paths.

    For local paths uses :func:`pathlib.Path.glob`.  For cloud paths
    (S3, GCS, etc.) uses ``FileSelector`` listing + segment-aware glob
    matching (``*`` stays within one path segment, ``**`` spans multiple).

    Args:
        path: Glob pattern to expand.
        filesystem: Optional filesystem for cloud paths. If ``None``, uses
            the local filesystem.
        ignore_missing_paths: If ``True``, returns an empty list when no
            files match the pattern. If ``False``, raises ``ValueError``
            when no files match. Defaults to ``False``.

    Returns:
        Sorted list of matched file paths, or empty list if
        *ignore_missing_paths* is ``True`` and no files matched.

    Raises:
        ValueError: If no files match and *ignore_missing_paths* is
            ``False``.
    """
    import pyarrow.fs
    from pyarrow.fs import FileType

    base, pattern = _split_glob_base(path)
    is_local = _is_local_filesystem(filesystem)

    if is_local:
        # Strip scheme (e.g. local://, file://) so pathlib can parse the path.
        local_base = _unwrap_protocol(base) if base else ""
        if local_base:
            base_path = pathlib.Path(local_base).resolve()
        else:
            base_path = pathlib.Path(os.getcwd())
        # Return absolute paths for consistency with cloud paths
        matched = [str(p.resolve()) for p in base_path.glob(pattern) if p.is_file()]
        if not matched:
            if ignore_missing_paths:
                return []
            raise ValueError(f"Glob pattern '{path}' matched no files.")
        return sorted(matched)

    # Cloud: list base directory, filter with segment-aware glob matching.
    if not base:
        raise ValueError(
            f"Glob pattern {path!r} expands to a bucket-level glob, "
            "which is not supported. Provide a concrete bucket path."
        )
    # Strip scheme so FileSelector and relative-path calculation use the
    # bare bucket/key form that PyArrow returns internally (e.g.
    # "bucket/prefix" instead of "s3://bucket/prefix").
    base_stripped = _unwrap_protocol(base)
    # Check the original base (with scheme) so that '?' and '[...]' in the
    # bucket/host name are detected — _has_glob_chars only treats these as
    # metacharacters for paths with a URI scheme.
    if _has_glob_chars(base):
        raise ValueError(
            f"Glob pattern {path!r} contains wildcards in the bucket/host "
            "name, which is not supported. Use wildcards in the path only."
        )
    # Use recursive listing when the pattern requires traversing subdirectories:
    # - "**" explicitly requests recursive matching across directory boundaries
    # - glob characters in a non-terminal directory segment (e.g. sub*/*.parquet)
    #   require listing into matching subdirectories
    # NOTE: This may list more files than needed. For example, "sub*/file.parquet"
    # triggers recursive listing even though only one level is needed. Optimizing
    # this would require iterative expansion (list matching dirs, then descend),
    # which adds complexity. Current approach favors simplicity over efficiency.
    pat_segs = pattern.split("/")
    # Recursive listing is needed when the pattern traverses subdirectories:
    # - '**' explicitly requests recursive matching
    # - '*' in a non-terminal segment (e.g. sub*/file.parquet)
    # - '[...]' or '?' in a non-terminal segment (e.g. sub[12]/file.parquet)
    # We check segments directly (not via _has_glob_chars) because individual
    # segments lack a URI scheme and _has_glob_chars only treats '?' and
    # '[...]' as metacharacters for cloud URIs.
    needs_recursive = "**" in pattern or any(
        "*" in seg or "?" in seg or re.search(r"\[[^\]]+\]", seg)
        for seg in pat_segs[:-1]
    )
    try:
        selector = pyarrow.fs.FileSelector(base_stripped, recursive=needs_recursive)
        file_infos = filesystem.get_file_info(selector)
    except (FileNotFoundError, OSError) as e:
        # Only catch file-not-found and OS errors (e.g. missing bucket/prefix).
        # Permission, network, and configuration errors propagate to the caller
        # even when ignore_missing_paths=True so they are not silently swallowed.
        if ignore_missing_paths:
            return []
        raise ValueError(f"Glob pattern '{path}' failed to list: {e}") from e

    matched = []
    for fi in file_infos:
        if fi.type == FileType.File:
            # Ensure the file is actually under the base directory,
            # not just a string-prefix match (e.g. "bucket/data" should
            # not match "bucket/dataextra.parquet").
            if not (
                fi.path == base_stripped or fi.path.startswith(base_stripped + "/")
            ):
                continue
            # S3, GCS, HDFS, and ABFS all use "/" as path separator.
            relative = fi.path[len(base_stripped) :].lstrip("/")
            if _glob_match_path(pattern, relative):
                # PyArrow filesystems expect paths without scheme when the
                # filesystem object already knows the scheme (e.g. S3FileSystem
                # expects "bucket/key", not "s3://bucket/key").
                matched.append(fi.path)
    if not matched:
        if ignore_missing_paths:
            return []
        raise ValueError(f"Glob pattern '{path}' matched no files.")
    return sorted(matched)


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

    # Unwrap RetryingPyFileSystem to get the underlying filesystem's type
    from ray.data._internal.util import RetryingPyFileSystem

    unwrapped = (
        filesystem.unwrap()
        if isinstance(filesystem, RetryingPyFileSystem)
        else filesystem
    )

    # Get the actual filesystem type
    fs_type = unwrapped.type_name

    # For PyFileSystem (fsspec wrappers), check the inner fsspec protocol
    # rather than relying on type_name alone, since all fsspec wrappers
    # share type_name "py" regardless of the underlying protocol.
    if fs_type == "py" or fs_type.startswith("py::"):
        from pyarrow.fs import FSSpecHandler, PyFileSystem

        actual_fs = filesystem
        if isinstance(actual_fs, RetryingPyFileSystem):
            actual_fs = actual_fs.unwrap()

        # After unwrapping, the inner filesystem may be a native PyArrow
        # filesystem (e.g., S3FileSystem) rather than a PyFileSystem wrapper.
        # Fall back to direct type_name matching in that case.
        if not isinstance(actual_fs, PyFileSystem):
            return actual_fs.type_name in expected_types

        if isinstance(actual_fs.handler, FSSpecHandler):
            inner_fs = actual_fs.handler.fs
            protocol = getattr(inner_fs, "protocol", None)
            if protocol is not None:
                if isinstance(protocol, str):
                    protocol = (protocol,)
                # Match scheme against fsspec protocol(s)
                if scheme in protocol:
                    return True
                # For bare paths (empty scheme), trust user-provided filesystem
                if scheme == "":
                    return True

        # Fallback: check HTTP
        if scheme in ("http", "https"):
            return _is_http_filesystem(filesystem)

        return False

    # Direct match for native PyArrow filesystems (s3, gcs, local, hdfs, etc.)
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
    ignore_missing_paths: bool = False,
    expand_globs: bool = False,
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
        ignore_missing_paths: If True, ignores any glob patterns that match no
            files instead of raising ValueError. Defaults to False.
        expand_globs: If True, detects glob metacharacters (``*``, ``?``,
            ``[...]``) in paths and expands them to concrete file paths
            before resolution. Only callers that intend to read files should
            enable this; write, download, and partition callers must leave
            this as ``False`` to avoid unintended expansion. Defaults to
            False.

    Returns:
        A pair ``(resolved_paths, filesystem)``. *resolved_paths* lists the
        normalized paths for each input path that resolved successfully, in
        order.

        If *filesystem* was ``None``, the returned *filesystem* is set from
        ``resolved_filesystem`` on the first successful path and is left
        unchanged on later iterations whenever it is already non-``None``.

        If *filesystem* was not ``None``, the returned value is always that
        same validated instance, even when ``_resolve_single_path_with_fallback``
        inferred a different filesystem for a given path. Callers should pass
        ``None`` or a filesystem compatible with the path URIs so returned paths
        and filesystem stay consistent.

        All paths are assumed to use one storage backend; mixing unrelated URI
        schemes in a single call is unsupported and may fail when reading.
    """
    paths = _normalize_paths_to_strings(paths)

    # Validate/wrap filesystem upfront so we return a proper PyArrow filesystem
    filesystem = _validate_and_wrap_filesystem(filesystem)

    # Track the inferred filesystem separately from the input parameter so
    # that resolving one glob path does not overwrite the shared ``filesystem``
    # variable for subsequent iterations.
    resolved_filesystem = filesystem

    resolved_paths = []
    for path in paths:
        # Expand glob patterns before resolving individual paths.
        # Only enabled when callers explicitly opt in (expand_globs=True),
        # since this function is also used by write, download, and
        # partition paths where glob expansion is undesirable.
        if expand_globs and _has_glob_chars(path):
            # Resolve relative paths to absolute before glob expansion
            # to ensure consistency across processes (e.g., Ray workers).
            # Only for local paths — cloud URIs (s3://, gs://, etc.) are
            # already absolute; os.path.isabs misidentifies them as relative.
            parsed = urlparse(path)
            is_cloud_fs = not _is_local_filesystem(resolved_filesystem)
            if not parsed.scheme and not os.path.isabs(path) and not is_cloud_fs:
                path = os.path.abspath(path)

            glob_fs = resolved_filesystem
            if glob_fs is None:
                try:
                    glob_fs, _ = _resolve_single_path_with_fallback(
                        path, resolved_filesystem
                    )
                except (ValueError, ImportError) as e:
                    # For local paths (non-cloud), still attempt glob expansion
                    # even when filesystem resolution fails — pathlib can
                    # expand globs without a PyArrow filesystem.
                    if not parsed.scheme:
                        expanded = _expand_glob(path, None, ignore_missing_paths)
                        if expanded:
                            resolved_paths.extend(expanded)
                            # pathlib fallback succeeded — ensure a
                            # filesystem is set so downstream metadata
                            # expansion does not receive None.
                            if resolved_filesystem is None:
                                import pyarrow.fs

                                resolved_filesystem = pyarrow.fs.LocalFileSystem()
                            continue
                    if ignore_missing_paths:
                        logger.debug(
                            f"Failed to resolve filesystem for glob "
                            f"'{path}': {e}, skipping"
                        )
                        continue
                    raise ValueError(
                        f"Failed to resolve filesystem for glob pattern '{path}': {e}"
                    ) from e
            expanded = _expand_glob(path, glob_fs, ignore_missing_paths)
            resolved_paths.extend(expanded)
            # Update the inferred filesystem from the first successful glob
            # resolution so that subsequent paths use the same filesystem.
            if resolved_filesystem is None and glob_fs is not None:
                resolved_filesystem = glob_fs
            continue

        try:
            iter_resolved_fs, resolved_path = _resolve_single_path_with_fallback(
                path, resolved_filesystem
            )
        except (ValueError, ImportError) as e:
            logger.warning(f"Failed to resolve path '{path}': {e}, skipping")
            continue

        if resolved_filesystem is None:
            resolved_filesystem = iter_resolved_fs

        # If the PyArrow filesystem is handled by a fsspec HTTPFileSystem, the protocol/
        # scheme of paths should not be unwrapped/removed, because HTTPFileSystem
        # expects full file paths including protocol/scheme. This is different behavior
        # compared to other file system implementation in pyarrow.fs.FileSystem.
        if not _is_http_filesystem(iter_resolved_fs):
            resolved_path = _unwrap_protocol(resolved_path)

        resolved_path = iter_resolved_fs.normalize_path(resolved_path)
        resolved_paths.append(resolved_path)

    return resolved_paths, resolved_filesystem


def _split_uri(uri: str):
    """Split a URI into (store_url, path) for use with obstore.

    e.g. "s3://my-bucket/a/b/c.jpg"               -> ("s3://my-bucket", "a/b/c.jpg")
         "https://host.com/a/b?X-Amz-Signature=x" -> ("https://host.com", "a/b?X-Amz-Signature=x")

    The query string is preserved so signed URLs (e.g. pre-signed S3 HTTPS)
    reach obstore intact. Semicolons in object keys normally appear in
    ``parsed.path`` (not ``parsed.params``) for typical ``urlparse`` output.

    Only the first leading ``/`` after the authority (as reported in
    ``parsed.path``) is removed. Extra leading slashes belong to the object
    key (e.g. ``s3://bucket//abs/key`` -> key ``/abs/key``), so
    ``str.lstrip("/")`` is not used.
    """
    parsed = urlparse(uri, allow_fragments=False)
    store_url = f"{parsed.scheme}://{parsed.netloc}"
    raw_path = parsed.path
    path = raw_path[1:] if raw_path.startswith("/") else raw_path
    if parsed.query:
        path = f"{path}?{parsed.query}"
    return store_url, path


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
