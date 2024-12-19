import enum
from ray._private.runtime_env.default_impl import get_protocols_provider


class ProtocolsProvider:
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
        }

    @classmethod
    def get_remote_protocols(cls):
        return {"https", "s3", "gs", "file"}

    @classmethod
    def get_smart_open_transport_params(cls, protocol):
        install_warning = (
            "Note that these must be preinstalled "
            "on all nodes in the Ray cluster; it is not "
            "sufficient to install them in the runtime_env."
        )

        if protocol == "s3":
            try:
                import boto3
                from smart_open import open as open_file  # noqa: F401
            except ImportError:
                raise ImportError(
                    "You must `pip install smart_open` and "
                    "`pip install boto3` to fetch URIs in s3 "
                    "bucket. " + install_warning
                )
            return {"client": boto3.client("s3")}
        elif protocol == "gs":
            try:
                from google.cloud import storage  # noqa: F401
                from smart_open import open as open_file  # noqa: F401
            except ImportError:
                raise ImportError(
                    "You must `pip install smart_open` and "
                    "`pip install google-cloud-storage` "
                    "to fetch URIs in Google Cloud Storage bucket." + install_warning
                )
            return None
        else:
            try:
                from smart_open import open as open_file  # noqa: F401
            except ImportError:
                raise ImportError(
                    "You must `pip install smart_open` "
                    f"to fetch {protocol.upper()} URIs. " + install_warning
                )
            return None


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


def _get_smart_open_transport_params(self):
    return _protocols_provider.get_smart_open_transport_params(self.value)


Protocol.get_smart_open_transport_params = _get_smart_open_transport_params
