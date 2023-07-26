import os
import sys
import time
import urllib.parse
from pathlib import Path
from pkg_resources import packaging
import psutil
from typing import Any, Dict, List, Optional, Tuple

try:
    import fsspec

except ImportError:
    fsspec = None

try:
    import pyarrow
    import pyarrow.fs

except (ImportError, ModuleNotFoundError):
    pyarrow = None


# Re-create fs objects after this amount of seconds
_CACHE_VALIDITY_S = 300


def _assert_pyarrow_installed():
    if pyarrow is None:
        raise RuntimeError(
            "Uploading, downloading, and deleting from cloud storage "
            "requires pyarrow to be installed. Install with: "
            "`pip install pyarrow`. Subsequent calls to cloud operations "
            "will be ignored."
        )


def fs_hint(uri: str) -> str:
    """Return a hint how to install required filesystem package"""
    if pyarrow is None:
        return "Please make sure PyArrow is installed: `pip install pyarrow`."
    if fsspec is None:
        return "Try installing fsspec: `pip install fsspec`."

    from fsspec.registry import known_implementations

    protocol = urllib.parse.urlparse(uri).scheme

    if protocol in known_implementations:
        return known_implementations[protocol]["err"]

    return "Make sure to install and register your fsspec-compatible filesystem."


def is_non_local_path_uri(uri: str) -> bool:
    """Check if target URI points to a non-local location"""
    parsed = urllib.parse.urlparse(uri)
    if parsed.scheme == "file" or not parsed.scheme:
        return False

    if bool(get_fs_and_path(uri)[0]):
        return True

    return False


# Cache fs objects. Map from cache_key --> timestamp, fs
_cached_fs: Dict[tuple, Tuple[float, "pyarrow.fs.FileSystem"]] = {}


def _get_cache(cache_key: tuple) -> Optional["pyarrow.fs.FileSystem"]:
    ts, fs = _cached_fs.get(cache_key, (0, None))
    if not fs:
        return None

    now = time.monotonic()
    if now - ts >= _CACHE_VALIDITY_S:
        _cached_fs.pop(cache_key)
        return None

    return fs


def _put_cache(cache_key: tuple, fs: "pyarrow.fs.FileSystem"):
    now = time.monotonic()
    _cached_fs[cache_key] = (now, fs)


def _get_network_mounts() -> List[str]:
    """Get mounted network filesystems on the current node.

    Network file system (NFS), server message block (SMB) and
    common internet file system (CIFS) are all file access storage protocols,
    used to access files on remote servers and storage servers (such as NAS storage)
    as if they were local files.
    """
    partitions = psutil.disk_partitions(all=True)
    network_fstypes = ("nfs", "smbfs", "cifs")
    return [p.mountpoint for p in partitions if p.fstype in network_fstypes]


def _is_network_mount(path: str) -> bool:
    """Checks if a path is within a mounted network filesystem."""
    resolved_path = Path(path).expanduser().resolve()
    network_mounts = {Path(mount) for mount in _get_network_mounts()}

    # Check if any of the network mounts are one of the path's parents.
    return bool(set(resolved_path.parents).intersection(network_mounts))


def is_local_path(path: str) -> bool:
    """Check if a given path is a local path or a remote URI."""
    if sys.platform == "win32":
        return _is_local_windows_path(path)

    scheme = urllib.parse.urlparse(path).scheme
    return scheme in ("", "file")


def _is_local_windows_path(path: str) -> bool:
    """Determines if path is a Windows file-system location."""
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


def _translate_options(
    option_map: Dict[str, str], options: Dict[str, List[str]]
) -> Dict[str, str]:
    """Given mapping of old_name -> new_name in option_map, rename keys."""
    translated = {}
    for opt, target in option_map.items():
        if opt in options:
            translated[target] = options[opt][0]

    return translated


def _translate_s3_options(options: Dict[str, List[str]]) -> Dict[str, Any]:
    """Translate pyarrow s3 query options into s3fs ``storage_kwargs``.

    ``storage_kwargs`` are passed to ``s3fs.S3Filesystem``. They accept
    ``client_kwargs``, which are passed to ``botocore.session.Session.Client``.

    In this function, we translate query string parameters from an s3 URI
    (e.g. ``s3://bucket/folder?endpoint_override=somewhere``) into the respective
    query parameters for the botocore clent.

    S3Filesystem API ref: https://s3fs.readthedocs.io/en/latest/api.html

    Botocore Client API ref: https://boto3.amazonaws.com/v1/documentation/api/latest/
    reference/core/session.html#boto3.session.Session.client

    """
    # Map from s3 query keys --> botocore client arguments
    # client_kwargs
    option_map = {
        "endpoint_override": "endpoint_url",
        "region": "region_name",
        "access_key": "aws_access_key_id",
        "secret_key": "aws_secret_access_key",
    }
    client_kwargs = _translate_options(option_map, options)

    # config_kwargs
    option_map = {
        "signature_version": "signature_version",
    }
    config_kwargs = _translate_options(option_map, options)

    # s3_additional_kwargs
    option_map = {
        "ServerSideEncryption": "ServerSideEncryption",
        "SSEKMSKeyId": "SSEKMSKeyId",
        "GrantFullControl": "GrantFullControl",
    }
    s3_additional_kwargs = _translate_options(option_map, options)

    # s3fs directory cache does not work correctly, so we pass
    # `use_listings_cache` to disable it. See https://github.com/fsspec/s3fs/issues/657
    # We should keep this for s3fs versions <= 2023.4.0.
    return {
        "use_listings_cache": False,
        "client_kwargs": client_kwargs,
        "config_kwargs": config_kwargs,
        "s3_additional_kwargs": s3_additional_kwargs,
    }


def _translate_gcs_options(options: Dict[str, List[str]]) -> Dict[str, Any]:
    """Translate pyarrow s3 query options into s3fs ``storage_kwargs``.

    ``storage_kwargs`` are passed to ``gcsfs.GCSFileSystem``.

    In this function, we translate query string parameters from an s3 URI
    (e.g. ``s3://bucket/folder?endpoint_override=somewhere``) into the respective
    arguments for the gcs filesystem.

    GCSFileSystem API ref: https://gcsfs.readthedocs.io/en/latest/api.html

    """
    # Map from gcs query keys --> gcsfs kwarg names
    option_map = {
        "endpoint_override": "endpoint_url",
    }

    storage_kwargs = _translate_options(option_map, options)

    return storage_kwargs


def _has_compatible_gcsfs_version() -> bool:
    """GCSFS does not work for versions > 2022.7.1 and < 2022.10.0.

    See https://github.com/fsspec/gcsfs/issues/498.

    In that case, and if we can't fallback to native PyArrow's GCS handler,
    we raise an error.
    """
    try:
        import gcsfs

        # For minimal install that only needs python3-setuptools
        if packaging.version.parse(gcsfs.__version__) > packaging.version.parse(
            "2022.7.1"
        ) and packaging.version.parse(gcsfs.__version__) < packaging.version.parse(
            "2022.10.0"
        ):
            # PyArrow's GcsFileSystem was introduced in 9.0.0.
            if packaging.version.parse(pyarrow.__version__) < packaging.version.parse(
                "9.0.0"
            ):
                raise RuntimeError(
                    "`gcsfs` versions between '2022.7.1' and '2022.10.0' are not "
                    f"compatible with pyarrow. You have gcsfs version "
                    f"{gcsfs.__version__}. Please downgrade or upgrade your gcsfs "
                    f"version or upgrade PyArrow. See more details in "
                    f"https://github.com/fsspec/gcsfs/issues/498."
                )
            # Returning False here means we fall back to pyarrow.
            return False
    except ImportError:
        return False
    return True


def _get_fsspec_fs_and_path(uri: str) -> Optional["pyarrow.fs.FileSystem"]:
    parsed = urllib.parse.urlparse(uri)

    storage_kwargs = {}
    if parsed.scheme in ["s3", "s3a"]:
        storage_kwargs = _translate_s3_options(urllib.parse.parse_qs(parsed.query))
    elif parsed.scheme in ["gs", "gcs"]:
        if not _has_compatible_gcsfs_version():
            # If gcsfs is incompatible, fallback to pyarrow.fs.
            return None
        storage_kwargs = _translate_gcs_options(urllib.parse.parse_qs(parsed.query))

    try:
        fsspec_fs = fsspec.filesystem(parsed.scheme, **storage_kwargs)
    except Exception:
        # ValueError when protocol is not known.
        # ImportError when protocol is known but package not installed.
        # Other errors can be raised if args/kwargs are incompatible.
        # Thus we should except broadly here.
        return None

    fsspec_handler = pyarrow.fs.FSSpecHandler
    fs = pyarrow.fs.PyFileSystem(fsspec_handler(fsspec_fs))
    return fs


def get_fs_and_path(
    uri: str,
) -> Tuple[Optional["pyarrow.fs.FileSystem"], Optional[str]]:
    if not pyarrow:
        return None, None

    scheme = urllib.parse.urlparse(uri).scheme
    if is_local_path(uri) and not scheme:
        # Append local filesys scheme such that the downstream operations work
        # properly on Linux and Windows.
        uri = "file://" + Path(uri).as_posix()

    parsed = urllib.parse.urlparse(uri)
    # for uri="hdfs://48bb8ca83706:8020/test":
    # netloc="48bb8ca83706:8020/"
    # netloc's information is taken into account from the pyarrow client.
    # so path should not include netloc.
    # On the other hand, for uri="s3://my_bucket/test":
    # netloc="my_bucket/" and path="test/"
    # netloc's information is not part of the pyarrow client.
    # so path should include netloc information hence the concatenation.
    if uri.startswith("hdfs://"):
        path = parsed.path
    else:
        path = parsed.netloc + parsed.path

    cache_key = (parsed.scheme, parsed.netloc, parsed.query)

    fs = _get_cache(cache_key)
    if fs:
        return fs, path

    # Prefer fsspec over native pyarrow.
    if fsspec:
        fs = _get_fsspec_fs_and_path(uri)
        if fs:
            _put_cache(cache_key, fs)
            return fs, path

    # In case of hdfs filesystem, if uri does not have the netloc part below, it will
    # fail with hdfs access error. For example 'hdfs:///user_folder/...' will
    # fail, while only 'hdfs://namenode_server/user_foler/...' will work.
    # Thus, if fsspec didn't return a filesystem, we return None.
    hdfs_uri = parsed.scheme == "hdfs"
    short_hdfs_uri = hdfs_uri and parsed.netloc == ""

    if short_hdfs_uri:
        return None, None

    # If no fsspec filesystem was found, use pyarrow native filesystem.
    try:
        fs, path = pyarrow.fs.FileSystem.from_uri(uri)
        _put_cache(cache_key, fs)
        return fs, path
    except (pyarrow.lib.ArrowInvalid, pyarrow.lib.ArrowNotImplementedError):
        # Raised when URI not recognized
        pass

    return None, None


def read_file_from_uri(uri: str) -> bytes:
    _assert_pyarrow_installed()

    fs, file_path = get_fs_and_path(uri)
    if not fs:
        raise ValueError(
            f"Could not download from URI: "
            f"URI `{uri}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(uri)}"
        )

    with fs.open_input_file(file_path) as file:
        return file.read()


def download_from_uri(
    uri: str,
    local_path: str,
    filelock: bool = True,
):
    source_filesystem, source_path = get_fs_and_path(uri)
    if not source_filesystem:
        raise ValueError(
            f"Could not upload to URI: "
            f"URI `{uri}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(uri)}"
        )

    from ray.train._internal.fs_utils import _download_from_fs_path

    _download_from_fs_path(
        fs=source_filesystem,
        fs_path=source_path,
        local_path=local_path,
        filelock=filelock,
    )


def upload_to_uri(
    local_path: str,
    uri: str,
    exclude: Optional[List[str]] = None,
) -> None:
    destination_filesystem, destination_path = get_fs_and_path(uri)
    if not destination_filesystem:
        raise ValueError(
            f"Could not upload to URI: "
            f"URI `{uri}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(uri)}"
        )

    from ray.train._internal.fs_utils import _upload_to_fs_path

    _upload_to_fs_path(
        local_path=local_path,
        fs=destination_filesystem,
        fs_path=destination_path,
        exclude=exclude,
    )


def delete_at_uri(uri: str):
    fs, fs_path = get_fs_and_path(uri)
    if not fs:
        raise ValueError(
            f"Could not clear URI contents: "
            f"URI `{uri}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(uri)}"
        )

    from ray.train._internal.fs_utils import _delete_fs_path

    _delete_fs_path(fs, fs_path)


def list_at_uri(uri: str) -> List[str]:
    fs, fs_path = get_fs_and_path(uri)
    if not fs:
        raise ValueError(
            f"Could not list at URI: "
            f"URI `{uri}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(uri)}"
        )

    if not is_non_local_path_uri(uri):
        # Make sure local paths get expanded fully
        fs_path = os.path.abspath(os.path.expanduser(fs_path))

    from ray.train._internal.fs_utils import _list_at_fs_path

    return _list_at_fs_path(fs, fs_path)


def is_directory(uri: str) -> bool:
    fs, fs_path = get_fs_and_path(uri)
    if not fs:
        raise ValueError(
            f"URI `{uri}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(uri)}"
        )

    from ray.train._internal.fs_utils import _is_directory

    return _is_directory(fs=fs, fs_path=fs_path)


def _ensure_directory(uri: str, fs: Optional["pyarrow.fs.FileSystem"] = None):
    fs, fs_path = get_fs_and_path(uri)

    from ray.train._internal.fs_utils import _ensure_directory

    _ensure_directory(fs=fs, fs_path=fs_path)
