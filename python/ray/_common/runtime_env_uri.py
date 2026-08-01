import enum
import hashlib
import pathlib
import urllib.parse
from typing import Tuple
from urllib.parse import urlparse

_REMOTE_PROTOCOLS = ("http", "https", "s3", "gs", "azure", "abfss", "file")


class Protocol(enum.Enum):
    # For packages dynamically uploaded and managed by the GCS.
    GCS = "gcs"
    # For conda environments installed locally on each node.
    CONDA = "conda"
    # For pip environments installed locally on each node.
    PIP = "pip"
    # For uv environments installed locally on each node.
    UV = "uv"
    # Remote http path, assumes everything packed in one zip file.
    HTTP = "http"
    # Remote https path, assumes everything packed in one zip file.
    HTTPS = "https"
    # Remote s3 path, assumes everything packed in one zip file.
    S3 = "s3"
    # Remote google storage path, assumes everything packed in one zip file.
    GS = "gs"
    # Remote azure blob storage path, assumes everything packed in one zip file.
    AZURE = "azure"
    # Remote Azure Blob File System Secure path, assumes everything packed in one zip file.
    ABFSS = "abfss"
    # File storage path, assumes everything packed in one zip file.
    FILE = "file"

    @classmethod
    def remote_protocols(cls):
        # Returns a list of protocols that support remote storage.
        # These protocols should only be used with paths that end in
        # ".zip", ".whl", ".tar.gz", or ".tgz".
        return [cls[protocol.upper()] for protocol in _REMOTE_PROTOCOLS]


def _is_path(path_or_uri: str) -> bool:
    """Returns True if path_or_uri is a path and False otherwise."""
    if not isinstance(path_or_uri, str):
        raise TypeError(f" path_or_uri must be a string, got {type(path_or_uri)}.")

    parsed_path = pathlib.Path(path_or_uri)
    parsed_uri = urllib.parse.urlparse(path_or_uri)

    if isinstance(parsed_path, pathlib.PurePosixPath):
        return not parsed_uri.scheme
    elif isinstance(parsed_path, pathlib.PureWindowsPath):
        return parsed_uri.scheme == parsed_path.drive.strip(":").lower()
    else:
        # this should never happen.
        raise TypeError(f"Unsupported path type: {type(parsed_path).__name__}")


def parse_uri(pkg_uri: str) -> Tuple[Protocol, str]:
    """
    Parse package uri into protocol and package name based on its format.
    Note that the output of this function is not for handling actual IO, it's
    only for setting up local directory folders by using package name as path.

    >>> parse_uri("https://test.com/file.zip")  # doctest: +ELLIPSIS
    (<Protocol.HTTPS: 'https'>, 'https_...zip')

    >>> parse_uri("https://test.com/file.whl")
    (<Protocol.HTTPS: 'https'>, 'file.whl')

    """
    if _is_path(pkg_uri):
        raise ValueError(f"Expected URI but received path {pkg_uri}")

    uri = urlparse(pkg_uri)
    try:
        protocol = Protocol(uri.scheme)
    except ValueError as e:
        raise ValueError(
            f'Invalid protocol for runtime_env URI "{pkg_uri}". '
            f"Supported protocols: {Protocol._member_names_}. Original error: {e}"
        )

    if protocol in Protocol.remote_protocols():
        if uri.path.endswith(".whl"):
            # Don't modify the .whl filename. See
            # https://peps.python.org/pep-0427/#file-name-convention
            # for more information.
            package_name = uri.path.split("/")[-1]
        else:
            # Hash the URI to produce a stable, NAME_MAX-safe local filename
            # regardless of how long or deeply nested the URI is. The extension
            # is preserved so is_zip_uri / is_jar_uri keep working. Compound
            # extensions (.tar.gz, .tar.bz2) are kept intact so archive-type
            # detection downstream still works.
            # netloc + path covers URIs where the filename has no path
            # component (e.g., s3://package.zip puts "package.zip" in netloc).
            raw = uri.netloc + uri.path
            if raw.endswith(".tar.gz"):
                suffix = ".tar.gz"
            elif raw.endswith(".tar.bz2"):
                suffix = ".tar.bz2"
            else:
                suffix = pathlib.Path(raw).suffix
            digest = hashlib.sha1(pkg_uri.encode("utf-8")).hexdigest()
            package_name = f"{protocol.value}_{digest}{suffix}"
    else:
        package_name = uri.netloc
    return (protocol, package_name)
