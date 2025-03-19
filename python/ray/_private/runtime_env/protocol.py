import enum
from ray._private.runtime_env.default_impl import get_protocols_provider


class ProtocolsProvider:
    _MISSING_DEPENDENCIES_WARNING = (
        "Note that these must be preinstalled "
        "on all nodes in the Ray cluster; it is not "
        "sufficient to install them in the runtime_env."
    )

    @classmethod
    def get_protocols(cls):
        return {
            # For packages dynamically uploaded and managed by the GCS.
            "gcs",
            # For conda environments installed locally on each node.
            "conda",
            # For pip environments installed locally on each node.
            "pip",
            # For uv environments install locally on each node.
            "uv",
            # Remote https path, assumes everything packed in one zip file.
            "https",
            # Remote s3 path, assumes everything packed in one zip file.
            "s3",
            # Remote google storage path, assumes everything packed in one zip file.
            "gs",
            # File storage path, assumes everything packed in one zip file.
            "file",
            "http",
        }

    @classmethod
    def get_remote_protocols(cls):
        return {"https", "s3", "gs", "file", "http"}

    @classmethod
    def download_remote_uri(cls, protocol: str, source_uri: str, dest_file: str):
        """Download file from remote URI to dest file"""
        assert protocol in cls.get_remote_protocols()

        tp = None

        if protocol == "file":
            source_uri = source_uri[len("file://") :]

            def open_file(uri, mode, *, transport_params=None):
                return open(uri, mode)

        elif protocol == "s3":
            try:
                import boto3
                from smart_open import open as open_file
            except ImportError:
                raise ImportError(
                    "You must `pip install smart_open[s3]` "
                    "to fetch URIs in s3 bucket. " + cls._MISSING_DEPENDENCIES_WARNING
                )
            tp = {"client": boto3.client("s3")}
        elif protocol == "gs":
            try:
                from google.cloud import storage  # noqa: F401
                from smart_open import open as open_file
            except ImportError:
                raise ImportError(
                    "You must `pip install smart_open[gcs]` "
                    "to fetch URIs in Google Cloud Storage bucket."
                    + cls._MISSING_DEPENDENCIES_WARNING
                )
        else:
            try:
                from smart_open import open as open_file
            except ImportError:
                raise ImportError(
                    "You must `pip install smart_open` "
                    f"to fetch {protocol.upper()} URIs. "
                    + cls._MISSING_DEPENDENCIES_WARNING
                )

        with open_file(source_uri, "rb", transport_params=tp) as fin:
            with open_file(dest_file, "wb") as fout:
                fout.write(fin.read())


_protocols_provider = get_protocols_provider()

Protocol = enum.Enum(
    "Protocol",
    {protocol.upper(): protocol for protocol in _protocols_provider.get_protocols()},
)


@classmethod
def _remote_protocols(cls):
    # Returns a list of protocols that support remote storage
    # These protocols should only be used with paths that end in ".zip" or ".whl"
    return [
        cls[protocol.upper()] for protocol in _protocols_provider.get_remote_protocols()
    ]


Protocol.remote_protocols = _remote_protocols


def _download_remote_uri(self, source_uri, dest_file):
    return _protocols_provider.download_remote_uri(self.value, source_uri, dest_file)


Protocol.download_remote_uri = _download_remote_uri
