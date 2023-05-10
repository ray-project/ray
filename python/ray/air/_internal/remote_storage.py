import fnmatch
import os
import urllib.parse
from pkg_resources import packaging
from typing import List, Optional, Tuple

from ray.air._internal.filelock import TempFileLock

try:
    import fsspec

except ImportError:
    fsspec = None

try:
    import pyarrow
    import pyarrow.fs

    # TODO(krfricke): Remove this once gcsfs > 2022.3.0 is released
    # (and make sure to pin)
    class _CustomGCSHandler(pyarrow.fs.FSSpecHandler):
        """Custom FSSpecHandler that avoids a bug in gcsfs <= 2022.3.0."""

        def create_dir(self, path, recursive):
            try:
                # GCSFS doesn't expose `create_parents` argument,
                # so it is omitted here
                self.fs.mkdir(path)
            except FileExistsError:
                pass

except (ImportError, ModuleNotFoundError):
    pyarrow = None
    _CustomGCSHandler = None

from ray import logger
import time
FILE_READING_RETRY = 5

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


# Cache fs objects
_cached_fs = {}


def get_fs_and_path(
    uri: str,
) -> Tuple[Optional["pyarrow.fs.FileSystem"], Optional[str]]:

    if not pyarrow:
        return None, None

    parsed = urllib.parse.urlparse(uri)
    path = parsed.netloc + parsed.path

    cache_key = (parsed.scheme, parsed.netloc)
    if cache_key in _cached_fs:
        fs = _cached_fs[cache_key]
        print(f'get_fs_and_path 1 {time.time()}  uri:{uri} parsed:{parsed} path:{path} cache_key:{cache_key} _cached_fs:{_cached_fs} fs:{fs}')
        return fs, path

    # In case of hdfs filesystem, if uri does not have the netloc part below will
    # failed with hdfs access error. For example 'hdfs:///user_folder/...' will
    # fail, while only 'hdfs://namenode_server/user_foler/...' will work
    if parsed.scheme == "hdfs" and parsed.netloc == "" and not fsspec:
        return None, None
    try:
        if not (parsed.scheme == "hdfs" and parsed.netloc == ""):
            fs, path = pyarrow.fs.FileSystem.from_uri(uri)
            _cached_fs[cache_key] = fs
            print(f'get_fs_and_path 2 {time.time()} uri:{uri} parsed:{parsed} path:{path} cache_key:{cache_key} _cached_fs:{_cached_fs} fs:{fs}')
            return fs, path
    except (pyarrow.lib.ArrowInvalid, pyarrow.lib.ArrowNotImplementedError):
        # Raised when URI not recognized
        if not fsspec:
            # Only return if fsspec is not installed
            return None, None

    # Else, try to resolve protocol via fsspec
    try:
        fsspec_fs = fsspec.filesystem(parsed.scheme)
    except ValueError:
        # Raised when protocol not known
        return None, None

    fsspec_handler = pyarrow.fs.FSSpecHandler
    if parsed.scheme in ["gs", "gcs"]:

        # TODO(amogkam): Remove after https://github.com/fsspec/gcsfs/issues/498 is
        #  resolved.
        try:
            import gcsfs

            # For minimal install that only needs python3-setuptools
            if packaging.version.parse(gcsfs.__version__) > packaging.version.parse(
                "2022.7.1"
            ):
                raise RuntimeError(
                    "`gcsfs` versions greater than '2022.7.1' are not "
                    f"compatible with pyarrow. You have gcsfs version "
                    f"{gcsfs.__version__}. Please downgrade your gcsfs "
                    f"version. See more details in "
                    f"https://github.com/fsspec/gcsfs/issues/498."
                )
        except ImportError:
            pass

        # GS doesn't support `create_parents` arg in `create_dir()`
        fsspec_handler = _CustomGCSHandler

    fs = pyarrow.fs.PyFileSystem(fsspec_handler(fsspec_fs))
    _cached_fs[cache_key] = fs
    print(f'get_fs_and_path 3 {time.time()} uri:{uri} parsed:{parsed} path:{path} cache_key:{cache_key} _cached_fs:{_cached_fs} fs:{fs}')
    return fs, path


def delete_at_uri(uri: str):
    _assert_pyarrow_installed()

    fs, bucket_path = get_fs_and_path(uri)
    if not fs:
        raise ValueError(
            f"Could not clear URI contents: "
            f"URI `{uri}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(uri)}"
        )

    try:
        fs.delete_dir(bucket_path)
    except Exception as e:
        logger.warning(f"Caught exception when clearing URI `{uri}`: {e}")


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


# Visible for test mocking.
def _copy_files(src, des, source_filesystem=None, destination_filesystem=None):
    pyarrow.fs.copy_files(
        src,
        des,
        source_filesystem=source_filesystem,
        destination_filesystem=destination_filesystem,
    )


# This retry helps when the upstream datasource is not able to handle
# overloaded write request or failed with some retriable failures.
# For example when copying data from HA hdfs service to local disk or vice versa,
# hdfs might lose connection for some unknown reason expecially when
# simutaneously running many ray tuning jobs
# Such failure can be restored with some waiting and retry.
def _copy_files_with_retry(src, des, source_filesystem=None, destination_filesystem=None):
    min_interval = 0
    final_exception = None
    for i in range(FILE_READING_RETRY):
        try:
            return _copy_files(
                src,
                des,
                source_filesystem=source_filesystem,
                destination_filesystem=destination_filesystem,
            )
        except Exception as e:
            import random
            import time
            retry_timing = (
                ""
                if i == FILE_READING_RETRY - 1
                else (f"Retry after {min_interval} sec. ")
            )
            log_only_show_in_1st_retry = (
                ""
                if i
                else (
                    f"If earlier read attempt threw certain Exception"
                    f", it may or may not be an issue depends on these retries "
                    f"succeed or not."
                )
            )
            logger.exception(
                f"{i + 1}th attempt to pyarrow.fs.copy_files failed "
                f"{retry_timing}"
                f"{log_only_show_in_1st_retry}"
            )
            if not min_interval:
                # to make retries of different process hit hdfs server
                # at slightly different time
                min_interval = (1 + random.random()) * 10
            # exponential backoff at
            # 10, 20, 40, 80, 160 seconds
            time.sleep(min_interval)
            min_interval = min_interval * 2
            final_exception = e
    raise final_exception


def download_from_uri(uri: str, local_path: str, filelock: bool = True):
    _assert_pyarrow_installed()

    fs, bucket_path = get_fs_and_path(uri)
    if not fs:
        raise ValueError(
            f"Could not download from URI: "
            f"URI `{uri}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(uri)}"
        )
    file_infos = fs.get_file_info(pyarrow.fs.FileSelector(bucket_path, allow_not_found=True, recursive=True))
    print(f'download_from_uri get_file_info remote location  {time.time()} file_infos:{file_infos} bucket_path:{bucket_path}')
    if filelock:
        with TempFileLock(f"{os.path.normpath(local_path)}.lock"):
            # pyarrow.fs.copy_files(bucket_path, local_path, source_filesystem=fs)
            _copy_files_with_retry(bucket_path, local_path, source_filesystem=fs)
    else:
        # pyarrow.fs.copy_files(bucket_path, local_path, source_filesystem=fs)
        _copy_files_with_retry(bucket_path, local_path, source_filesystem=fs)


def upload_to_uri(
    local_path: str, uri: str, exclude: Optional[List[str]] = None
) -> None:
    import os
    _assert_pyarrow_installed()

    fs, bucket_path = get_fs_and_path(uri)
    if not fs:
        raise ValueError(
            f"Could not upload to URI: "
            f"URI `{uri}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(uri)}"
        )

    file_infos2 = fs.get_file_info(pyarrow.fs.FileSelector(bucket_path, allow_not_found=True, recursive=True))
    print(f'upload_to_uri before copy file_infos2:{file_infos2} uri:{uri} local_path:{local_path} exclude:{exclude} fs:{fs} bucket_path:{bucket_path}')

    if not exclude:
        # pyarrow.fs.copy_files(local_path, bucket_path, destination_filesystem=fs)
        _copy_files_with_retry(local_path, bucket_path, destination_filesystem=fs)
    else:
        # Else, walk and upload
        _upload_to_uri_with_exclude(
            local_path=local_path, fs=fs, bucket_path=bucket_path, exclude=exclude
        )
    file_infos3 = fs.get_file_info(pyarrow.fs.FileSelector(bucket_path, allow_not_found=True, recursive=True))
    print(f'upload_to_uri after copy file_infos3:{file_infos3} uri:{uri} local_path:{local_path} exclude:{exclude} fs:{fs} bucket_path:{bucket_path}')
    return


def _upload_to_uri_with_exclude(
    local_path: str, fs: "pyarrow.fs", bucket_path: str, exclude: Optional[List[str]]
) -> None:
    def _should_exclude(candidate: str) -> bool:
        for excl in exclude:
            if fnmatch.fnmatch(candidate, excl):
                return True
        return False

    for root, dirs, files in os.walk(local_path):
        print(f'_upload_to_uri_with_exclude doing root:{root}  dirs:{dirs} files:{files}')

        rel_root = os.path.relpath(root, local_path)
        for file in files:
            candidate = os.path.join(rel_root, file)

            if _should_exclude(candidate):
                continue

            full_source_path = os.path.normpath(os.path.join(local_path, candidate))
            full_target_path = os.path.normpath(os.path.join(bucket_path, candidate))
            try:
                print(f'_upload_to_uri_with_exclude copy full_source_path:{full_source_path} full_target_path:{full_target_path} candidate:{candidate} ')
                # pyarrow.fs.copy_files(
                #     full_source_path, full_target_path, destination_filesystem=fs
                # )
                _copy_files_with_retry(full_source_path, full_target_path, destination_filesystem=fs)
            except Exception as e:
                print(f'candidate:{candidate} failed with e:{e}')


def list_at_uri(uri: str) -> List[str]:
    _assert_pyarrow_installed()

    fs, bucket_path = get_fs_and_path(uri)
    if not fs:
        raise ValueError(
            f"Could not upload to URI: "
            f"URI `{uri}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(uri)}"
        )

    selector = pyarrow.fs.FileSelector(
        bucket_path, allow_not_found=True, recursive=False
    )
    return [
        os.path.relpath(file_info.path.lstrip("/"), start=bucket_path.lstrip("/"))
        for file_info in fs.get_file_info(selector)
    ]


def _ensure_directory(uri: str):
    """Create directory at remote URI.

    Some external filesystems require directories to already exist, or at least
    the `netloc` to be created (e.g. PyArrows ``mock://`` filesystem).

    Generally this should be done before and outside of Ray applications. This
    utility is thus primarily used in testing, e.g. of ``mock://` URIs.
    """
    fs, path = get_fs_and_path(uri)
    try:
        fs.create_dir(path)
    except Exception:
        pass

