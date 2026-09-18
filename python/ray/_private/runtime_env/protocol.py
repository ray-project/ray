import os
import re
import ssl
from contextlib import contextmanager
from inspect import signature
from ipaddress import ip_address
from urllib.parse import urlparse

from ray._common.runtime_env_uri import Protocol

RAY_RUNTIME_ENV_HTTP_USER_AGENT_ENV_VAR = "RAY_RUNTIME_ENV_HTTP_USER_AGENT"
RAY_RUNTIME_ENV_BEARER_TOKEN_ENV_VAR = "RAY_RUNTIME_ENV_BEARER_TOKEN"
RAY_RUNTIME_ENV_HTTP_KERBEROS_HOSTS_ENV_VAR = "RAY_RUNTIME_ENV_HTTP_KERBEROS_HOSTS"
_DEFAULT_HTTP_USER_AGENT = "ray-runtime-env-curl/1.0"


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
            # Remote http path, assumes everything packed in one zip file.
            "http",
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
            # A directory that is already present on every node. Assumes absolute path.
            "local",
        }

    @classmethod
    def get_remote_protocols(cls):
        return {"http", "https", "s3", "gs", "azure", "abfss", "file"}

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
    def _http_headers(cls) -> dict:
        headers = {
            "User-Agent": os.environ.get(
                RAY_RUNTIME_ENV_HTTP_USER_AGENT_ENV_VAR, _DEFAULT_HTTP_USER_AGENT
            ),
            "Accept": "*/*",
        }

        bearer_token = os.environ.get(RAY_RUNTIME_ENV_BEARER_TOKEN_ENV_VAR)
        if bearer_token:
            headers["Authorization"] = f"Bearer {bearer_token}"

        return headers

    @classmethod
    def _http_kerberos_hosts(cls):
        hosts = {
            host.strip().lower()
            for host in os.environ.get(
                RAY_RUNTIME_ENV_HTTP_KERBEROS_HOSTS_ENV_VAR, ""
            ).split(",")
            if host.strip()
        }
        if any(
            not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", host)
            for host in hosts
        ):
            raise ValueError(
                f"{RAY_RUNTIME_ENV_HTTP_KERBEROS_HOSTS_ENV_VAR} must contain "
                "comma-separated hostnames, without schemes, ports or wildcards."
            )
        for host in hosts:
            try:
                ip_address(host)
            except ValueError:
                continue
            raise ValueError(
                f"{RAY_RUNTIME_ENV_HTTP_KERBEROS_HOSTS_ENV_VAR} requires DNS "
                "hostnames, not IP addresses. Use the server's DNS name in "
                "both the HTTPS URL and the host list."
            )
        return hosts

    @classmethod
    def _http_kerberos_session(cls, hosts):
        try:
            import requests
            import requests_kerberos
            from smart_open import http
        except ImportError as exc:
            raise ImportError(
                "You must `pip install 'smart_open[http]>=7.1.0' requests-kerberos` "
                "to fetch Kerberos-protected HTTPS URIs. "
                + cls._MISSING_DEPENDENCIES_WARNING
            ) from exc

        # Require session injection and per-request TLS configuration. Older
        # smart_open versions would silently ignore our redirect policy.
        if "session" not in signature(http.open).parameters or not hasattr(
            requests.adapters.HTTPAdapter, "build_connection_pool_key_attributes"
        ):
            raise ImportError(
                "Kerberos downloads require `pip install 'smart_open[http]>=7.1.0' "
                "'requests>=2.32.3'` for redirect and TLS validation. "
                + cls._MISSING_DEPENDENCIES_WARNING
            )

        class HTTPSAdapter(requests.adapters.HTTPAdapter):
            def __init__(self):
                # Require SANs even on older urllib3; Requests supplies the CAs.
                self._ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                self._ssl_context.hostname_checks_common_name = False
                super().__init__()

            def build_connection_pool_key_attributes(self, request, verify, cert=None):
                host_params, pool_kwargs = super().build_connection_pool_key_attributes(
                    request, verify, cert
                )
                pool_kwargs["ssl_context"] = self._ssl_context
                if verify is True:
                    # Preserve Requests' default CAs when replacing its context.
                    pool_kwargs["ca_certs"] = requests.utils.DEFAULT_CA_BUNDLE_PATH
                return host_params, pool_kwargs

        class KerberosSession(requests.Session):
            def send(self, request, **kwargs):
                parsed = urlparse(request.url)
                if (
                    parsed.scheme != "https"
                    or parsed.hostname not in hosts
                    or parsed.username is not None
                    or parsed.password is not None
                ):
                    raise ValueError(
                        "Kerberos downloads and redirects require HTTPS URLs "
                        "without embedded credentials and hosts listed in "
                        f"{RAY_RUNTIME_ENV_HTTP_KERBEROS_HOSTS_ENV_VAR}."
                    )
                return super().send(request, **kwargs)

            def rebuild_auth(self, prepared_request, response):
                # Never forward a previous hop's token. Use a fresh Kerberos
                # context (including mutual authentication) for each redirect.
                prepared_request.headers.pop("Authorization", None)
                prepared_request.hooks = {"response": []}
                prepared_request.prepare_auth(requests_kerberos.HTTPKerberosAuth())

        session = KerberosSession()
        session.mount("https://", HTTPSAdapter())
        return session

    @classmethod
    def _handle_http_protocol(cls):
        """Set up HTTP/HTTPS protocol handling with curl-like headers."""

        try:
            from smart_open import open as smart_open_open
        except ImportError:
            raise ImportError(
                "You must `pip install smart_open` to fetch HTTP/HTTPS URIs. "
                + cls._MISSING_DEPENDENCIES_WARNING
            )

        @contextmanager
        def open_file(uri, mode, *, transport_params=None):
            params = {
                "headers": cls._http_headers(),
                "timeout": 60,
            }
            if transport_params:
                params.update(transport_params)
            hosts = cls._http_kerberos_hosts()
            parsed = urlparse(uri)
            if parsed.scheme == "https" and parsed.hostname in hosts:
                if os.environ.get(RAY_RUNTIME_ENV_BEARER_TOKEN_ENV_VAR):
                    raise ValueError(
                        "Kerberos and Bearer Token authentication cannot be used "
                        "together for the same download. Unset "
                        f"{RAY_RUNTIME_ENV_BEARER_TOKEN_ENV_VAR} or remove the host "
                        f"from {RAY_RUNTIME_ENV_HTTP_KERBEROS_HOSTS_ENV_VAR}."
                    )
                with cls._http_kerberos_session(hosts) as session:
                    params.update(kerberos=True, session=session)
                    with smart_open_open(uri, mode, transport_params=params) as stream:
                        yield stream
            else:
                with smart_open_open(uri, mode, transport_params=params) as stream:
                    yield stream

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

        elif protocol in ("http", "https"):
            open_file, tp = cls._handle_http_protocol()
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


def _download_remote_uri(self, source_uri, dest_file):
    return ProtocolsProvider.download_remote_uri(self.value, source_uri, dest_file)


Protocol.download_remote_uri = _download_remote_uri
