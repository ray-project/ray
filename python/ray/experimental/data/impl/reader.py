import logging
from typing import Any, List, Union, Optional, Tuple, TYPE_CHECKING
import urllib

from ray.experimental.data.datasource import _S3FileSystemWrapper

if TYPE_CHECKING:
    import pyarrow

logger = logging.getLogger(__name__)


def read_file(path: str,
              include_paths: bool = False,
              filesystem: Optional["pyarrow.fs.FileSystem"] = None
              ) -> Union[Any, Tuple[str, Any]]:
    """A helper function which takes in the path to a single file, and the params
    from `dataset.from_binary_files`.

    Returns The contents of the file. If `include_paths` is True, a tuple of
      the path and the contents of the file.
    """
    if filesystem:
        if isinstance(filesystem, _S3FileSystemWrapper):
            filesystem = filesystem.unwrap()
        contents = filesystem.open_input_stream(path).readall()
    else:
        parsed = urllib.parse.urlparse(path)
        if parsed.scheme == "s3":
            contents = download_single_s3_file(parsed.netloc,
                                               parsed.path.strip("/"))
        else:
            contents = open(path, "rb").read()

    if include_paths:
        return path, contents
    else:
        return contents


def list_objects(path: str) -> List[str]:
    # S3 specific dependencies should only be imported if we're downloading a
    # file from s3.

    parsed = urllib.parse.urlparse(path)
    if parsed.scheme != "s3":
        # TODO (Alex): Implement this after porting it to the datasource.
        raise NotImplementedError(
            "Binary files can only be enumerated for S3 buckets.")

    import boto3
    path = parsed.path.strip("/")

    split = path.split("/", 1)
    bucket = split[0]
    key = split[1] if len(split) > 1 else ""

    s3 = boto3.resource("s3")
    s3_objects = s3.Bucket(bucket).objects.filter(Prefix=key).all()
    paths = [f"s3://{obj.bucket_name}/{obj.key}" for obj in s3_objects]
    return paths


download_initialized = False
http_session = None


def download_single_s3_file(bucket: str, key: str) -> bytes:
    import requests
    global download_initialized, http_session
    if not download_initialized:
        http_session = requests.Session()
        download_initialized = True

    url = f"https://{bucket}.s3.amazonaws.com/{key}"

    # Retry download if it fails.
    for _ in range(3):
        result = http_session.get(url)
        if result.status_code == 200:
            break
        logger.warning(
            f"Failed to download {url} with error: {result.content}. Retrying."
        )
    else:
        raise ValueError(
            f"({result.status_code}) {url} is not a valid s3 url. "
            f"{result.content}")

    return result.content
