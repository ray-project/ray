import enum
from typing import Tuple
from urllib.parse import urlparse

from ray._common.path_utils import is_path

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


def parse_uri(pkg_uri: str) -> Tuple[Protocol, str]:
    """
    Parse package uri into protocol and package name based on its format.
    Note that the output of this function is not for handling actual IO, it's
    only for setting up local directory folders by using package name as path.

    >>> parse_uri("https://test.com/file.zip")
    (<Protocol.HTTPS: 'https'>, 'https_test_com_file.zip')

    >>> parse_uri("https://test.com/file.whl")
    (<Protocol.HTTPS: 'https'>, 'file.whl')

    """
    if is_path(pkg_uri):
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
            package_name = f"{protocol.value}_{uri.netloc}{uri.path}"

            disallowed_chars = ["/", ":", "@", "+", " ", "(", ")"]
            for disallowed_char in disallowed_chars:
                package_name = package_name.replace(disallowed_char, "_")

            # Preserve compound extensions like .tar.gz before replacing dots.
            compound_ext = None
            if package_name.endswith(".tar.gz"):
                compound_ext = ".tar.gz"
                package_name = package_name[: -len(".tar.gz")]
            elif package_name.endswith(".tar.bz2"):
                compound_ext = ".tar.bz2"
                package_name = package_name[: -len(".tar.bz2")]

            if compound_ext:
                package_name = package_name.replace(".", "_")
                package_name += compound_ext
            else:
                # Remove all periods except the last, which is part of the
                # file extension.
                package_name = package_name.replace(
                    ".", "_", package_name.count(".") - 1
                )
    else:
        package_name = uri.netloc
    return (protocol, package_name)
