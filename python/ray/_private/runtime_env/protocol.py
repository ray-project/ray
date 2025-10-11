import enum
import os
from urllib.parse import urlparse


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
            # Remote azure blob storage path, assumes everything packed in one zip file.
            "azure",
            # Remote Azure Blob File System Secure path, assumes everything packed in one zip file.
            "abfss",
            # File storage path, assumes everything packed in one zip file.
            "file",
        }

    @classmethod
    def get_remote_protocols(cls):
        return {"https", "s3", "gs", "azure", "abfss", "file"}

    @classmethod
    def _handle_s3_protocol(cls):
        """Set up S3 protocol handling.

        Returns:
            tuple: (open_file function, transport_params)

        Raises:
            ImportError: If required dependencies are not installed.
        """
        try:
            import boto3
            from smart_open import open as open_file
        except ImportError:
            raise ImportError(
                "You must `pip install smart_open[s3]` "
                "to fetch URIs in s3 bucket. " + cls._MISSING_DEPENDENCIES_WARNING
            )

        # Create S3 client, falling back to unsigned for public buckets
        session = boto3.Session()
        # session.get_credentials() will return None if no credentials can be found.
        if session.get_credentials():
            # If credentials are found, use a standard signed client.
            s3_client = session.client("s3")
        else:
            # No credentials found, fall back to an unsigned client for public buckets.
            from botocore import UNSIGNED
            from botocore.config import Config

            s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

        transport_params = {"client": s3_client}
        return open_file, transport_params

    @classmethod
    def _handle_gs_protocol(cls):
        """Set up Google Cloud Storage protocol handling.

        Returns:
            tuple: (open_file function, transport_params)

        Raises:
            ImportError: If required dependencies are not installed.
        """
        try:
            from google.cloud import storage  # noqa: F401
            from smart_open import open as open_file
        except ImportError:
            raise ImportError(
                "You must `pip install smart_open[gcs]` "
                "to fetch URIs in Google Cloud Storage bucket."
                + cls._MISSING_DEPENDENCIES_WARNING
            )

        return open_file, None

    @classmethod
    def _handle_azure_protocol(cls):
        """Set up Azure blob storage protocol handling.

        Returns:
            tuple: (open_file function, transport_params)

        Raises:
            ImportError: If required dependencies are not installed.
            ValueError: If required environment variables are not set.
        """
        try:
            from azure.identity import DefaultAzureCredential
            from azure.storage.blob import BlobServiceClient  # noqa: F401
            from smart_open import open as open_file
        except ImportError:
            raise ImportError(
                "You must `pip install azure-storage-blob azure-identity smart_open[azure]` "
                "to fetch URIs in Azure Blob Storage. "
                + cls._MISSING_DEPENDENCIES_WARNING
            )

        # Define authentication variable
        azure_storage_account_name = os.getenv("AZURE_STORAGE_ACCOUNT")

        if not azure_storage_account_name:
            raise ValueError(
                "Azure Blob Storage authentication requires "
                "AZURE_STORAGE_ACCOUNT environment variable to be set."
            )

        account_url = f"https://{azure_storage_account_name}.blob.core.windows.net/"
        transport_params = {
            "client": BlobServiceClient(
                account_url=account_url, credential=DefaultAzureCredential()
            )
        }

        return open_file, transport_params

    @classmethod
    def _handle_abfss_protocol(cls):
        """Set up Azure Blob File System Secure (ABFSS) protocol handling.

        Returns:
            tuple: (open_file function, transport_params)

        Raises:
            ImportError: If required dependencies are not installed.
            ValueError: If the ABFSS URI format is invalid.
        """
        try:
            import adlfs
            from azure.identity import DefaultAzureCredential
        except ImportError:
            raise ImportError(
                "You must `pip install adlfs azure-identity` "
                "to fetch URIs in Azure Blob File System Secure. "
                + cls._MISSING_DEPENDENCIES_WARNING
            )

        def open_file(uri, mode, *, transport_params=None):
            # Parse and validate the ABFSS URI
            parsed = urlparse(uri)

            # Validate ABFSS URI format: abfss://container@account.dfs.core.windows.net/path
            if not parsed.netloc or "@" not in parsed.netloc:
                raise ValueError(
                    f"Invalid ABFSS URI format - missing container@account: {uri}"
                )

            container_part, hostname_part = parsed.netloc.split("@", 1)

            # Validate container name (must be non-empty)
            if not container_part:
                raise ValueError(
                    f"Invalid ABFSS URI format - empty container name: {uri}"
                )

            # Validate hostname format
            if not hostname_part or not hostname_part.endswith(".dfs.core.windows.net"):
                raise ValueError(
                    f"Invalid ABFSS URI format - invalid hostname (must end with .dfs.core.windows.net): {uri}"
                )

            # Extract and validate account name
            azure_storage_account_name = hostname_part.split(".")[0]
            if not azure_storage_account_name:
                raise ValueError(
                    f"Invalid ABFSS URI format - empty account name: {uri}"
                )

            # Handle ABFSS URI with adlfs
            filesystem = adlfs.AzureBlobFileSystem(
                account_name=azure_storage_account_name,
                credential=DefaultAzureCredential(),
            )
            return filesystem.open(uri, mode)

        return open_file, None

    @classmethod
    def download_remote_uri(cls, protocol: str, source_uri: str, dest_file: str):
        """Download file from remote URI to destination file.

        Args:
            protocol: The protocol to use for downloading (e.g., 's3', 'https').
            source_uri: The source URI to download from.
            dest_file: The destination file path to save to.

        Raises:
            ImportError: If required dependencies for the protocol are not installed.
        """
        assert protocol in cls.get_remote_protocols()

        tp = None
        open_file = None

        if protocol == "file":
            source_uri = source_uri[len("file://") :]

            def open_file(uri, mode, *, transport_params=None):
                return open(uri, mode)

        elif protocol == "s3":
            open_file, tp = cls._handle_s3_protocol()
        elif protocol == "gs":
            open_file, tp = cls._handle_gs_protocol()
        elif protocol == "azure":
            open_file, tp = cls._handle_azure_protocol()
        elif protocol == "abfss":
            open_file, tp = cls._handle_abfss_protocol()
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
            with open(dest_file, "wb") as fout:
                fout.write(fin.read())


Protocol = enum.Enum(
    "Protocol",
    {protocol.upper(): protocol for protocol in ProtocolsProvider.get_protocols()},
)


@classmethod
def _remote_protocols(cls):
    # Returns a list of protocols that support remote storage
    # These protocols should only be used with paths that end in ".zip" or ".whl"
    return [
        cls[protocol.upper()] for protocol in ProtocolsProvider.get_remote_protocols()
    ]


Protocol.remote_protocols = _remote_protocols


def _download_remote_uri(self, source_uri, dest_file):
    return ProtocolsProvider.download_remote_uri(self.value, source_uri, dest_file)


Protocol.download_remote_uri = _download_remote_uri
