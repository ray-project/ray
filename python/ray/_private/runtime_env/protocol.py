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


_protocols_provider = get_protocols_provider()

Protocol = enum.Enum(
    "Protocol",
    {protocol.upper(): protocol for protocol in _protocols_provider.get_protocols()},
)


@classmethod
def remote_protocols(cls):
    # Returns a list of protocols that support remote storage
    # These protocols should only be used with paths that end in ".zip" or ".whl"
    return [
        cls[protocol.upper()] for protocol in _protocols_provider.get_remote_protocols()
    ]


Protocol.remote_protocols = remote_protocols
