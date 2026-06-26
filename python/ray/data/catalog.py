"""Catalog abstraction for Ray Data read/write APIs.

A :class:`Catalog` resolves a *catalog table identifier* (e.g.
``"main.sales.orders"``) to the physical information needed to read or write
the table: the storage URL, the data format, and short-lived cloud storage
credentials ("credential vending").

The read/write APIs accept an optional ``catalog=`` argument. When provided,
the first positional argument is treated as a catalog table identifier and the
catalog resolves it::

    import ray
    from ray.data import UnityCatalog

    ds = ray.data.read_delta(
        "main.sales.orders",
        catalog=UnityCatalog(url="https://...", token="...", region="us-west-2"),
    )
    ds.write_delta("main.sales.orders", catalog=UnityCatalog(url=..., token=...))

Resolution happens entirely on the driver. Only the resolved
``storage_options`` dict (plain strings) travels to workers — the ``Catalog``
object itself is never pickled or sent to workers.

This is the generic replacement for the dedicated ``read_unity_catalog`` API.
``UnityCatalog`` is the first concrete catalog; other catalogs (Iceberg REST,
Glue, ...) can subclass :class:`Catalog` later.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Optional
from urllib.parse import urlparse

from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    import pyarrow.fs as pa_fs

    from ray.data._internal.datasource.databricks_credentials import (
        DatabricksCredentialProvider,
    )

logger = logging.getLogger(__name__)

# Operations a catalog can vend credentials for.
READ = "READ"
READ_WRITE = "READ_WRITE"


@DeveloperAPI
@dataclass(frozen=True)
class ResolvedTable:
    """Driver-side resolution of a catalog identifier to physical access info.

    Attributes:
        url: Physical storage URI (e.g. ``s3://bucket/path/``,
            ``abfss://...``, ``gs://...``).
        data_format: Lowercased table format, e.g. ``"delta"`` or
            ``"parquet"``.
        storage_options: Vended cloud-storage credentials, in the same key
            vocabulary the Delta datasource/datasink understand
            (``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` /
            ``AWS_SESSION_TOKEN`` / ``AWS_REGION``; ``AZURE_STORAGE_*``;
            ``GOOGLE_*``). Always picklable (plain strings).
        filesystem: Optional prebuilt PyArrow filesystem, used only when the
            credentials cannot be expressed via ``storage_options`` alone.
    """

    url: str
    data_format: str
    storage_options: Dict[str, str] = field(default_factory=dict)
    filesystem: Optional["pa_fs.FileSystem"] = None


@PublicAPI(stability="alpha")
class Catalog(ABC):
    """Resolves a catalog table identifier to physical access information.

    Subclasses implement :meth:`resolve`, which runs on the driver and may
    perform network calls (catalog lookup, credential vending). The read/write
    APIs call ``resolve`` once on the driver and ship only the resulting
    ``storage_options`` to workers.
    """

    @abstractmethod
    def resolve(self, identifier: str, *, operation: str = READ) -> ResolvedTable:
        """Resolve a table identifier to a :class:`ResolvedTable`.

        Args:
            identifier: Catalog table identifier (e.g. ``"catalog.schema.table"``).
            operation: ``"READ"`` or ``"READ_WRITE"``. Controls the scope of
                vended credentials.

        Returns:
            A :class:`ResolvedTable` with the physical URL, format, and vended
            ``storage_options``.
        """
        ...


@PublicAPI(stability="alpha")
class UnityCatalog(Catalog):
    """Databricks Unity Catalog with automatic credential vending.

    Resolves a Unity Catalog table to its physical storage location and vends
    short-lived cloud credentials (AWS / Azure / GCP) via the Databricks
    ``temporary-table-credentials`` API, so external engines can read/write the
    underlying files directly and in parallel.

    Args:
        url: Databricks workspace URL (e.g.
            ``"https://dbc-XXXX.cloud.databricks.com"``). Required unless
            ``credential_provider`` is given.
        token: Databricks personal access token with ``EXTERNAL USE SCHEMA``
            permission. Required unless ``credential_provider`` is given.
        credential_provider: Optional custom
            :class:`~ray.data._internal.datasource.databricks_credentials.DatabricksCredentialProvider`.
            When provided, ``url`` / ``token`` are ignored.
        region: Cloud region (e.g. ``"us-west-2"``). Required for AWS S3
            access; not needed for Azure / GCP.
    """

    def __init__(
        self,
        url: Optional[str] = None,
        token: Optional[str] = None,
        *,
        credential_provider: Optional["DatabricksCredentialProvider"] = None,
        region: Optional[str] = None,
    ):
        from ray.data._internal.datasource.databricks_credentials import (
            UnityCatalogCredentialConfig,
            resolve_credential_provider,
        )

        self._provider = resolve_credential_provider(
            UnityCatalogCredentialConfig(
                credential_provider=credential_provider, url=url, token=token
            )
        )
        self._region = region

        base = self._provider.get_host().rstrip("/")
        if not base.startswith(("http://", "https://")):
            base = f"https://{base}"
        self._base_url = base

    # ------------------------------------------------------------------
    # Catalog interface.
    # ------------------------------------------------------------------
    def resolve(self, identifier: str, *, operation: str = READ) -> ResolvedTable:
        import requests

        from ray.data._internal.datasource.databricks_credentials import (
            request_with_401_retry,
        )

        # 1. Table metadata (table_id + declared format).
        info_url = f"{self._base_url}/api/2.1/unity-catalog/tables/{identifier}"
        info = request_with_401_retry(requests.get, info_url, self._provider).json()
        table_id = info["table_id"]

        # 2. Vend temporary cloud credentials for the requested operation.
        creds_url = (
            f"{self._base_url}/api/2.1/unity-catalog/temporary-table-credentials"
        )
        creds = request_with_401_retry(
            requests.post,
            creds_url,
            self._provider,
            json={"table_id": table_id, "operation": operation},
        ).json()

        physical_url = creds["url"]
        data_format = self._infer_format(info, physical_url)
        storage_options = self._creds_to_storage_options(creds, physical_url)

        return ResolvedTable(
            url=physical_url,
            data_format=data_format,
            storage_options=storage_options,
        )

    # ------------------------------------------------------------------
    # Helpers.
    # ------------------------------------------------------------------
    @staticmethod
    def _infer_format(info: Dict[str, Any], physical_url: str) -> str:
        fmt = info.get("data_source_format")
        if fmt:
            return fmt.lower()
        # Fall back to the storage-location extension.
        ext = physical_url.rsplit(".", 1)[-1].lower() if "." in physical_url else ""
        if ext in ("delta", "parquet"):
            return ext
        raise ValueError(
            f"Could not infer data format for Unity Catalog table; "
            f"data_source_format missing and URL '{physical_url}' has no "
            f"recognizable extension."
        )

    def _creds_to_storage_options(
        self, creds: Dict[str, Any], physical_url: str
    ) -> Dict[str, str]:
        """Translate vended UC credentials into a ``storage_options`` dict.

        Produces keys in the vocabulary understood by
        ``delta.utils.create_filesystem_from_storage_options`` /
        ``get_storage_options``. Pure function: does NOT mutate ``os.environ``
        or initialize Ray (unlike the legacy ``UnityCatalogConnector``).
        """
        options: Dict[str, str] = {}

        if "aws_temp_credentials" in creds:
            aws = creds["aws_temp_credentials"]
            if not self._region:
                raise ValueError(
                    "The 'region' parameter is required for AWS S3 access via "
                    "Unity Catalog. Pass region=... to UnityCatalog(...)."
                )
            options["AWS_ACCESS_KEY_ID"] = aws["access_key_id"]
            options["AWS_SECRET_ACCESS_KEY"] = aws["secret_access_key"]
            options["AWS_SESSION_TOKEN"] = aws["session_token"]
            options["AWS_REGION"] = self._region
        elif "azuresasuri" in creds:
            options["AZURE_STORAGE_SAS_TOKEN"] = creds["azuresasuri"]
            account = self._azure_account_from_url(physical_url)
            if account:
                options["AZURE_STORAGE_ACCOUNT_NAME"] = account
        elif "azure_user_delegation_sas" in creds:
            azure = creds["azure_user_delegation_sas"] or {}
            sas_token = (
                azure.get("sas_token")
                or azure.get("sas")
                or azure.get("token")
                or azure.get("sasToken")
            )
            if sas_token and sas_token.startswith("?"):
                sas_token = sas_token[1:]
            if not sas_token:
                raise ValueError(
                    "Azure UC credentials missing SAS token in "
                    f"azure_user_delegation_sas. Available keys: "
                    f"{', '.join(azure.keys())}"
                )
            options["AZURE_STORAGE_SAS_TOKEN"] = sas_token
            # Prefer the account UC returns; otherwise derive from the URL so a
            # worker can rebuild the Azure filesystem.
            account = azure.get("storage_account") or self._azure_account_from_url(
                physical_url
            )
            if account:
                options["AZURE_STORAGE_ACCOUNT_NAME"] = account
        elif "gcp_service_account" in creds:
            # GCP vending writes a service-account JSON to a temp file + env var
            # in the legacy connector; that does not survive to workers. GCP via
            # UnityCatalog is handled at the call site (read can build a driver
            # filesystem; write raises NotImplementedError until a worker-safe
            # path exists). Surface the raw JSON so callers can decide.
            options["GOOGLE_SERVICE_ACCOUNT_JSON"] = creds["gcp_service_account"]
        else:
            raise ValueError(
                "No known credential type in Unity Catalog response. "
                f"Available keys: {', '.join(creds.keys())}"
            )

        return options

    @staticmethod
    def _azure_account_from_url(physical_url: str) -> Optional[str]:
        """Extract the storage account from an ``abfss://container@acct...`` URL."""
        try:
            host = urlparse(physical_url).hostname or ""
        except Exception:
            return None
        # host looks like ``<account>.dfs.core.windows.net`` or
        # ``<account>.blob.core.windows.net``.
        if host.endswith(("dfs.core.windows.net", "blob.core.windows.net")):
            return host.split(".", 1)[0] or None
        return None
