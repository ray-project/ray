import importlib
import os
import re
import urllib
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

from ray._private.client_mode_hook import client_mode_hook
from ray._private.utils import _add_creatable_buckets_param_if_s3_uri
from ray._private.auto_init_hook import wrap_auto_init

if TYPE_CHECKING:
    import pyarrow.fs


# The full storage argument specified, e.g., in ``ray.init(storage="s3://foo/bar")``
# This is set immediately on Ray worker init.
_storage_uri = None

# The storage prefix, e.g., "foo/bar" under which files should be written.
# This is set lazily the first time storage is accessed on a worker.
_storage_prefix = None

# The pyarrow.fs.FileSystem instantiated for the storage.
# This is set lazily the first time storage is accessed on a worker.
_filesystem = None


@wrap_auto_init
@client_mode_hook
def get_filesystem() -> ("pyarrow.fs.FileSystem", str):
    """Initialize and get the configured storage filesystem, if possible.

    This method can be called from any Ray worker to get a reference to the configured
    storage filesystem.

    Examples:
        # Assume ray.init(storage="s3:/bucket/cluster_1/storage")
        >>> fs, path = storage.get_filesystem()
        >>> print(fs)
        <pyarrow._fs.LocalFileSystem object at 0x7fd745dd9830>
        >>> print(path)
        cluster_1/storage

    Returns:
        Tuple of pyarrow filesystem instance and the path under which files should
        be created for this cluster.

    Raises:
        RuntimeError if storage has not been configured or init failed.
    """
    return _get_filesystem_internal()


# TODO(suquark): There is no implementation of 'get_client' in client hook.
@wrap_auto_init
@client_mode_hook
def get_client(prefix: str) -> "KVClient":
    """Returns a KV-client (convenience wrapper around underlying filesystem).

    Args:
        prefix: Path prefix (e.g., "foo", "foo/bar") that defines the sub-directory
            data will be stored under. All writes will be scoped to this sub-dir.

    Examples:
        # Assume ray.init(storage="s3:/bucket/cluster_1/storage")
        >>> client = storage.get_client("foo")
        >>> client.put("foo", b"bar")

    Returns:
        KVClient.
    """
    if not prefix:
        raise ValueError("A directory prefix must be specified.")
    fs, base_prefix = get_filesystem()
    combined_prefix = os.path.join(base_prefix, prefix)
    return KVClient(fs, combined_prefix)


def _is_os_error_file_not_found(err: OSError) -> bool:
    """Instead of "FileNotFoundError", pyarrow S3 filesystem raises
    OSError starts with "Path does not exist" for some of its APIs.

    # TODO(suquark): Delete this function after pyarrow handles missing files
    in a consistent way.
    """
    return (
        len(err.args) > 0
        and isinstance(err.args[0], str)
        and err.args[0].startswith("Path does not exist")
    )


class KVClient:
    """Simple KV API built on the underlying filesystem.

    This is a convenience wrapper around get_filesystem() and working with files.
    Slashes in the path are interpreted as directory delimiters.
    """

    def __init__(self, fs: "pyarrow.fs.FileSystem", prefix: str):
        """Use storage.get_client() to construct KVClient."""
        self.fs = fs
        self.root = Path(prefix)

    def put(self, path: str, value: bytes) -> None:
        """Save a blob in persistent storage at the given path, if possible.

        Examples:
            # Writes "bar" to <storage_prefix>/my_app/path/foo.txt
            >>> client = storage.get_client("my_app")
            >>> client.put("path/foo.txt", b"bar")

        Args:
            path: Relative directory of the blobs.
            value: String value to save.
        """
        full_path = self._resolve_path(path)
        parent_dir = os.path.dirname(full_path)
        try:
            with self.fs.open_output_stream(full_path) as f:
                f.write(value)
        except FileNotFoundError:
            # Directory likely doesn't exist; retry after creating it.
            self.fs.create_dir(parent_dir)
            with self.fs.open_output_stream(full_path) as f:
                f.write(value)

    def get(self, path: str) -> bytes:
        """Load a blob from persistent storage at the given path, if possible.

        Examples:
            # Loads value from <storage_prefix>/my_app/path/foo.txt
            >>> client = storage.get_client("my_app")
            >>> client.get("path/foo.txt")
            b"bar"
            >>> client.get("invalid")
            None

        Args:
            path: Relative directory of the blobs.

        Returns:
            String content of the blob, or None if not found.
        """
        full_path = self._resolve_path(path)
        try:
            with self.fs.open_input_stream(full_path) as f:
                return f.read()
        except FileNotFoundError:
            return None
        except OSError as e:
            if _is_os_error_file_not_found(e):
                return None
            raise e

    def delete(self, path: str) -> bool:
        """Load the blob from persistent storage at the given path, if possible.

        Examples:
            # Deletes blob at <storage_prefix>/my_app/path/foo.txt
            >>> client = storage.get_client("my_app")
            >>> client.delete("path/foo.txt")
            True

        Args:
            path: Relative directory of the blob.

        Returns:
            Whether the blob was deleted.
        """
        full_path = self._resolve_path(path)
        try:
            self.fs.delete_file(full_path)
            return True
        except FileNotFoundError:
            return False
        except OSError as e:
            if _is_os_error_file_not_found(e):
                return False
            raise e

    def delete_dir(self, path: str) -> bool:
        """Delete a directory and its contents, recursively.

        Examples:
            # Deletes dir at <storage_prefix>/my_app/path/
            >>> client = storage.get_client("my_app")
            >>> client.delete_dir("path")
            True

        Args:
            path: Relative directory of the blob.

        Returns:
            Whether the dir was deleted.
        """
        full_path = self._resolve_path(path)
        try:
            self.fs.delete_dir(full_path)
            return True
        except FileNotFoundError:
            return False
        except OSError as e:
            if _is_os_error_file_not_found(e):
                return False
            raise e

    def get_info(self, path: str) -> Optional["pyarrow.fs.FileInfo"]:
        """Get info about the persistent blob at the given path, if possible.

        Examples:
            # Inspect blob at <storage_prefix>/my_app/path/foo.txt
            >>> client = storage.get_client("my_app")
            >>> client.get_info("path/foo.txt")
            <FileInfo for '/tmp/storage/my_app/path/foo.txt': type=FileType.File>

            # Non-existent blob.
            >>> client.get_info("path/does_not_exist.txt")
            None
            <FileInfo for '/tmp/storage/my_app/path/foo.txt': type=FileType.NotFound>

        Args:
            path: Relative directory of the blob.

        Returns:
            Info about the blob, or None if it doesn't exist.
        """
        import pyarrow.fs

        full_path = self._resolve_path(path)
        info = self.fs.get_file_info([full_path])[0]
        if info.type == pyarrow.fs.FileType.NotFound:
            return None
        return info

    def list(
        self,
        path: str,
    ) -> List["pyarrow.fs.FileInfo"]:
        """List blobs and sub-dirs in the given path, if possible.

        Examples:
            # List created blobs and dirs at <storage_prefix>/my_app/path
            >>> client = storage.get_client("my_app")
            >>> client.list("path")
            [<FileInfo for '/tmp/storage/my_app/path/foo.txt' type=FileType.File>,
             <FileInfo for '/tmp/storage/my_app/path/subdir' type=FileType.Directory>]

            # Non-existent path.
            >>> client.get_info("does_not_exist")
            FileNotFoundError: ...

            # Not a directory.
            >>> storage.get_info("path/foo.txt")
            NotADirectoryError: ...

        Args:
            path: Relative directory to list from.

        Returns:
            List of file-info objects for the directory contents.

        Raises:
            FileNotFoundError if the given path is not found.
            NotADirectoryError if the given path isn't a valid directory.
        """
        from pyarrow.fs import FileSelector, FileType, LocalFileSystem

        full_path = self._resolve_path(path)
        selector = FileSelector(full_path, recursive=False)
        try:
            files = self.fs.get_file_info(selector)
        except FileNotFoundError as e:
            raise e
        except OSError as e:
            if _is_os_error_file_not_found(e):
                raise FileNotFoundError(*e.args)
            raise e
        if self.fs is not LocalFileSystem and not files:
            # TODO(suquark): pyarrow does not raise "NotADirectoryError"
            # for non-local filesystems like S3. Check and raise it here.
            info = self.fs.get_file_info([full_path])[0]
            if info.type == FileType.File:
                raise NotADirectoryError(
                    f"Cannot list directory '{full_path}'. "
                    f"Detail: [errno 20] Not a directory"
                )
        return files

    def _resolve_path(self, path: str) -> str:
        from pyarrow.fs import LocalFileSystem

        if isinstance(self.fs, LocalFileSystem):
            joined = self.root.joinpath(path).resolve()
            # Raises an error if the path is above the root (e.g., "../data" attack).
            joined.relative_to(self.root.resolve())
            return str(joined)

        # In this case, we are not a local file system. However, pathlib would
        # still add prefix to the path as if it is a local path when resolving
        # the path, even when the path does not exist at all. If the path exists
        # locally and is a symlink, then pathlib resolves it to the unwanted
        # physical path. This could leak to an attack. Third, if the path was
        # under Windows, "/" becomes "\", which is invalid for non-local stores.
        # So we decide to resolve it mannually.
        def _normalize_path(p: str) -> str:
            # "////bucket//go/./foo///..//.././/bar/./" becomes "bucket/bar"
            segments = []
            for s in p.replace("\\", "/").split("/"):
                if s == "..":
                    if not segments:
                        raise ValueError("Path goes beyond root.")
                    segments.pop()
                elif s not in (".", ""):
                    segments.append(s)
            return "/".join(segments)

        root = _normalize_path(str(self.root))
        joined = _normalize_path(str(self.root.joinpath(path)))
        if not joined.startswith(root):
            raise ValueError(f"{joined!r} does not start with {root!r}")
        return joined


def _init_storage(storage_uri: str, is_head: bool):
    """Init global storage.

    On the head (ray start) process, this also creates a _valid file under the given
    storage path to validate the storage is writable. This file is also checked on each
    worker process to validate the storage is readable. This catches common errors
    like using a non-NFS filesystem path on a multi-node cluster.

    On worker nodes, the actual filesystem is lazily initialized on first use.
    """
    global _storage_uri

    if storage_uri:
        _storage_uri = storage_uri
        if is_head:
            _init_filesystem(create_valid_file=True)


def _get_storage_uri() -> Optional[str]:
    """Get storage API, if configured."""
    global _storage_uri
    return _storage_uri


def _get_filesystem_internal() -> ("pyarrow.fs.FileSystem", str):
    """Internal version of get_filesystem() that doesn't hit Ray client hooks.

    This forces full (non-lazy) init of the filesystem.
    """
    global _filesystem, _storage_prefix
    if _filesystem is None:
        _init_filesystem()
    return _filesystem, _storage_prefix


def _init_filesystem(create_valid_file: bool = False, check_valid_file: bool = True):
    """Fully initialize the filesystem at the given storage URI."""
    global _filesystem, _storage_prefix, _storage_uri
    assert _filesystem is None, "Init can only be called once."

    if not _storage_uri:
        raise RuntimeError(
            "No storage URI has been configured for the cluster. "
            "Specify a storage URI via `ray.init(storage=<uri>)` or "
            "`ray start --head --storage=<uri>`"
        )

    import pyarrow.fs

    # TODO(suquark): This is a temporary patch for windows - the backslash
    # could not be understood by pyarrow. We replace it with slash here.
    parsed_uri = urllib.parse.urlparse(_storage_uri.replace("\\", "/"))
    if parsed_uri.scheme == "custom":
        fs_creator = _load_class(parsed_uri.netloc)
        _filesystem, _storage_prefix = fs_creator(parsed_uri.path)
    else:
        # Arrow's S3FileSystem doesn't allow creating buckets by default, so we add a
        # query arg enabling bucket creation if an S3 URI is provided.
        _storage_uri = _add_creatable_buckets_param_if_s3_uri(_storage_uri)
        _filesystem, _storage_prefix = pyarrow.fs.FileSystem.from_uri(_storage_uri)

    if os.name == "nt":
        # Special care for windows. "//C/windows/system32" is a valid network
        # name many applications support, but unfortunately not by pyarrow.
        # This formats "//C/windows/system32" to "C:/windows/system32".
        if re.match("^//[A-Za-z]/.*", _storage_prefix):
            _storage_prefix = _storage_prefix[2] + ":" + _storage_prefix[4:]

    # enforce use of "/"
    valid_file = _storage_prefix + "/_valid"
    if create_valid_file:
        _filesystem.create_dir(_storage_prefix)
        with _filesystem.open_output_stream(valid_file):
            pass
    if check_valid_file:
        valid = _filesystem.get_file_info([valid_file])[0]
        if valid.type == pyarrow.fs.FileType.NotFound:
            raise RuntimeError(
                "Unable to initialize storage: {} file created during init not found. "
                "Check that configured cluster storage path is readable from all "
                "worker nodes of the cluster.".format(valid_file)
            )

    return _filesystem, _storage_prefix


def _reset() -> None:
    """Resets all initialized state to None."""
    global _storage_uri, _filesystem, _storage_prefix
    _storage_uri = _filesystem = _storage_prefix = None


def _load_class(path):
    """Load a class at runtime given a full path.

    Example of the path: mypkg.mysubpkg.myclass
    """
    class_data = path.split(".")
    if len(class_data) < 2:
        raise ValueError("You need to pass a valid path like mymodule.provider_class")
    module_path = ".".join(class_data[:-1])
    class_str = class_data[-1]
    module = importlib.import_module(module_path)
    return getattr(module, class_str)
