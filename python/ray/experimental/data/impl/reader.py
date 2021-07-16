from typing import Any, Union, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow  # noqa: F401


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
        contents = filesystem.open_input_stream(path).readall()
    else:
        if path.startswith("s3://"):
            contents = s3_downloader(path)
        else:
            contents = open(path, "rb").read()

    if include_paths:
        return path, contents
    else:
        return contents


download_initialized = False
s3_client = None
http_session = None


def s3_downloader(path: str) -> bytes:
    # S3 specific dependencies should only be imported if we're downloading a
    # file from s3.
    import boto3
    import requests

    global download_initialized, s3_client, http_session
    if download_initialized is False:
        s3_client = boto3.client("s3")
        http_session = requests.Session()
        download_initialized = True

    without_prefix = path[len("s3://"):]
    bucket, key = without_prefix.split("/", 1)

    # TODO (Alex): We should consider generating a presigned url once we port
    # this to the file system abstraction.
    # url = s3_client.generate_presigned_url(
    #     "get_object",
    #     Params={"Bucket": bucket, "Key": key},
    #     ExpiresIn=3600
    # )
    url = f"https://{bucket}.s3.amazonaws.com/{key}"

    # Retry download if it fails.
    for _ in range(3):
        result = http_session.get(url)
        if result.status_code == 200:
            break
        print(
            f"Failed to download {url} with error: {result.content}. Retrying."
        )

    return result.content
