from typing import List, TYPE_CHECKING
import os

from ray.util.annotations import DeveloperAPI
from ray._private.client_mode_hook import client_mode_hook

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


@DeveloperAPI
@client_mode_hook(auto_init=True)
def get_filesystem() -> ("pyarrow.fs.FileSystem", str):
    """Initialize and get the configured storage filesystem, if possible.

    This method can be called from any Ray worker to get a reference to the configured
    storage filesystem.

    Examples:
        # Assume ray.init(storage="s3:/bucket/cluster_1/storage")
        >>> fs, path = ray.storage.get_filesystem()
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
    return _get_or_init_filesystem()


@DeveloperAPI
@client_mode_hook(auto_init=True)
def put(namespace: str, path: str, value: str) -> None:
    """Save a blob in persistent storage at the given path, if possible.

    This is a convenience wrapper around get_filesystem() and working with files.
    Slashes in the path are interpreted as directory delimiters.

    Examples:
        # Writes "bar" to <storage_prefix>/my_app/path/foo.txt
        >>> ray.storage.put("my_app", "path/foo.txt", "bar")

    Args:
        namespace: Namespace used to isolate blobs from different applications.
        path: Relative directory of the blobs.
        value: String value to save.
    """
    fs, prefix = get_filesystem()
    full_path = os.path.join(os.path.join(prefix, namespace), path)
    fs.create_dir(os.path.dirname(full_path))
    with fs.open_output_stream(full_path) as f:
        f.write(value)


@DeveloperAPI
@client_mode_hook(auto_init=True)
def load(namespace: str, path: str) -> str:
    """Load a blob from persistent storage at the given path, if possible.

    This is a convenience wrapper around get_filesystem() and working with files.
    Slashes in the path are interpreted as directory delimiters.

    Examples:
        # Loads value from <storage_prefix>/my_app/path/foo.txt
        >>> ray.storage.load("my_app", "path/foo.txt")
        "bar"

    Args:
        namespace: Namespace used to isolate blobs from different applications.
        path: Relative directory of the blobs.

    Returns:
        String content of the blob.
    """
    fs, prefix = get_filesystem()
    full_path = os.path.join(os.path.join(prefix, namespace), path)
    with fs.open_input_stream(full_path) as f:
        return f.read()


@DeveloperAPI
@client_mode_hook(auto_init=True)
def delete(namespace: str, path: str) -> bool:
    """Load the blob from persistent storage at the given path, if possible.

    This is a convenience wrapper around get_filesystem() and working with files.
    Slashes in the path are interpreted as directory delimiters.

    Examples:
        # Deletes blob at <storage_prefix>/my_app/path/foo.txt
        >>> ray.storage.delete("my_app", "path/foo.txt")
        True

    Args:
        namespace: Namespace used to isolate blobs from different applications.
        path: Relative directory of the blob.

    Returns:
        Whether the blob was deleted.
    """
    fs, prefix = get_filesystem()
    full_path = os.path.join(os.path.join(prefix, namespace), path)
    try:
        fs.delete_file(full_path)
        return True
    except FileNotFoundError:
        return False


@DeveloperAPI
@client_mode_hook(auto_init=True)
def get_info(namespace: str, path: str) -> "pyarrow.fs.FileInfo":
    """Get info about the persistent blob at the given path, if possible.

    This is a convenience wrapper around get_filesystem() and working with files.
    Slashes in the path are interpreted as directory delimiters.

    Examples:
        # Inspect blob at <storage_prefix>/my_app/path/foo.txt
        >>> ray.storage.get_info("my_app", "path/foo.txt")
        <FileInfo for '/tmp/storage/my_app/path/foo.txt': type=FileType.File, size=4>

        # Non-existent blob.
        >>> ray.storage.get_info("my_app", "path/does_not_exist.txt")
        <FileInfo for '/tmp/storage/my_app/path/foo.txt': type=FileType.NotFound>

    Args:
        namespace: Namespace used to isolate blobs from different applications.
        path: Relative directory of the blob.

    Returns:
        Info about the blob.
    """
    fs, prefix = get_filesystem()
    full_path = os.path.join(os.path.join(prefix, namespace), path)
    return fs.get_file_info([full_path])[0]


@DeveloperAPI
@client_mode_hook(auto_init=True)
def list(
    namespace: str,
    path: str,
) -> List["pyarrow.fs.FileInfo"]:
    """List blobs and sub-dirs in the given path, if possible.

    This is a convenience wrapper around get_filesystem() and working with files.
    Slashes in the path are interpreted as directory delimiters.

    Examples:
        # List created blobs and dirs at <storage_prefix>/my_app/path
        >>> ray.storage.list("my_app", "path")
        [<FileInfo for '/tmp/storage/my_app/path/foo.txt' type=FileType.File>,
         <FileInfo for '/tmp/storage/my_app/path/subdir' type=FileType.Directory>]

        # Non-existent path.
        >>> ray.storage.get_info("my_app", "does_not_exist")
        FileNotFoundError: ...

        # Not a directory.
        >>> ray.storage.get_info("my_app", "path/foo.txt")
        NotADirectoryError: ...

    Args:
        namespace: Namespace used to isolate blobs from different applications.
        path: Relative directory to list from.

    Returns:
        List of file-info objects for the directory contents.

    Raises:
        FileNotFoundError if the given path is not found.
        NotADirectoryError if the given path isn't a valid directory.
    """
    fs, prefix = get_filesystem()
    from pyarrow.fs import FileSelector

    full_path = os.path.join(os.path.join(prefix, namespace), path)
    selector = FileSelector(full_path, recursive=False)
    files = fs.get_file_info(selector)
    return files


def _init_storage(storage_uri: str, is_head: bool):
    global _storage_uri
    _storage_uri = storage_uri

    if is_head:
        _init_filesystem(storage_uri, create_valid_file=True)


def _init_filesystem(storage_uri: str, create_valid_file: bool = False):
    global _filesystem, _storage_prefix
    if not storage_uri:
        raise RuntimeError(
            "No storage URI has been configured for the cluster. "
            "Specify a storage URI via `ray.init(storage=<uri>)`"
        )

    import pyarrow.fs

    _filesystem, _storage_prefix = pyarrow.fs.FileSystem.from_uri(storage_uri)

    valid_file = os.path.join(_storage_prefix, "_valid")
    if create_valid_file:
        _filesystem.create_dir(_storage_prefix)
        with _filesystem.open_output_stream(valid_file):
            pass
    else:
        valid = _filesystem.get_file_info([valid_file])[0]
        if valid.type == pyarrow.fs.FileType.NotFound:
            raise RuntimeError(
                "Unable to initialize storage: {} file created during init not found. "
                "Check that configured cluster storage path is readable from all "
                "worker nodes of the cluster.".format(valid_file)
            )

    return _filesystem, _storage_prefix


def _get_or_init_filesystem() -> ("pyarrow.fs.FileSystem", str):
    global _filesystem, _storage_prefix
    if _filesystem is None:
        _init_filesystem(_storage_uri)
    return _filesystem, _storage_prefix
