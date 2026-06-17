"""Catalog connectors for Ray Data readers.

A :class:`Catalog` resolves a table name into a readable source (location +
credentials) for a reader such as :func:`ray.data.read_delta`,
:func:`ray.data.read_parquet`, or :func:`ray.data.read_iceberg`.

This inverts the previous design (``read_unity_catalog``), where the
authentication layer owned the readers. Now the reader is primary and a catalog
is passed in only when authentication is required.
"""

import atexit
import logging
import os
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

import requests

from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow.fs

    from ray.data._internal.datasource.databricks_credentials import (
        DatabricksCredentialProvider,
    )

logger = logging.getLogger(__name__)


def _normalize_host(host: str) -> str:
    host = host.rstrip("/")
    if not host.startswith(("http://", "https://")):
        host = f"https://{host}"
    return host


@PublicAPI(stability="alpha")
class ReaderFormat(str, Enum):
    """Which reader is asking the catalog to resolve a table.

    ``str``-based so it stays ergonomic (``ReaderFormat.DELTA == "delta"``) and
    serializes cleanly.
    """

    DELTA = "delta"
    PARQUET = "parquet"
    ICEBERG = "iceberg"


@PublicAPI(stability="alpha")
@dataclass
class ResolvedSource:
    """The output of :meth:`Catalog.resolve` — location/credentials for a reader.

    A reader consumes only the fields it understands:

    * ``read_delta``:   ``path`` + (``storage_options`` and/or ``filesystem``)
    * ``read_parquet``: ``path`` + ``filesystem``
    * ``read_iceberg``: ``catalog_kwargs``

    Unused fields are ``None``.
    """

    path: Optional[str] = None
    filesystem: Optional["pyarrow.fs.FileSystem"] = None
    storage_options: Optional[Dict[str, Any]] = None
    catalog_kwargs: Optional[Dict[str, Any]] = None
    data_format: Optional[ReaderFormat] = None  # hint, e.g. ReaderFormat.DELTA


@PublicAPI(stability="alpha")
class Catalog(ABC):
    """A directory service that resolves a table name to a readable source."""

    @abstractmethod
    def resolve(self, table: str, *, reader: ReaderFormat) -> ResolvedSource:
        """Resolve ``table`` for the given ``reader``."""
        ...


@PublicAPI(stability="alpha")
class UnityCatalog(Catalog):
    """Databricks Unity Catalog connector.

    For Delta and Parquet tables this performs Unity Catalog credential vending
    (temporary, least-privilege cloud credentials). For Iceberg tables it
    returns configuration pointing PyIceberg at Unity Catalog's Iceberg REST
    catalog endpoint.

    Args:
        url: Databricks workspace URL (e.g.
            ``"https://dbc-XXXX.cloud.databricks.com"``). Required unless
            ``credential_provider`` is given.
        token: Databricks Personal Access Token with ``EXTERNAL USE SCHEMA``
            permission. Required unless ``credential_provider`` is given.
        credential_provider: A custom
            :class:`~ray.data._internal.datasource.databricks_credentials.DatabricksCredentialProvider`.
            If provided, ``url``/``token`` are ignored.
        region: AWS region for S3 access (e.g. ``"us-west-2"``). Required for
            AWS-backed tables; not needed for Azure/GCP.

    Example:
        >>> import ray
        >>> catalog = ray.data.UnityCatalog(  # doctest: +SKIP
        ...     url="https://dbc-XXXX.cloud.databricks.com",
        ...     token="dapi...",
        ...     region="us-west-2",
        ... )
        >>> ds = ray.data.read_delta(  # doctest: +SKIP
        ...     "main.sales.transactions", catalog=catalog
        ... )
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
        self._base_url = _normalize_host(self._provider.get_host())

    # ---- Catalog interface -------------------------------------------------
    def resolve(self, table: str, *, reader: ReaderFormat) -> ResolvedSource:
        if reader is ReaderFormat.ICEBERG:
            return self._resolve_iceberg(table)
        if reader in (ReaderFormat.DELTA, ReaderFormat.PARQUET):
            return self._resolve_storage(table, reader)
        raise ValueError(f"UnityCatalog does not support reader={reader!r}")

    # ---- storage-credential vending (delta / parquet) ----------------------
    def _resolve_storage(self, table: str, reader: ReaderFormat) -> ResolvedSource:
        table_info = self._get_table_info(table)
        creds, table_url = self._get_creds(table_info["table_id"])

        # Deliver vended credentials via environment variables. This is the
        # mechanism the underlying libraries read uniformly: pyarrow (Parquet
        # data, and S3/Azure/GCS auto-filesystems) and deltalake's object_store
        # (the Delta transaction *log* read in `DeltaTable(...)`, which neither
        # a pyarrow `filesystem` nor `storage_options` keyed for pyarrow would
        # satisfy). See `_apply_env` for the worker-propagation note.
        #
        # TODO: remove the env-var + ray.init mechanism once credential vending
        # is performed inside the read tasks themselves (worker-side).
        self._apply_env(self._creds_to_env(creds))

        # For AWS Delta, also hand the data scan an explicit S3FileSystem: the
        # vended session token isn't reliably propagated through
        # `DeltaTable.to_pyarrow_dataset`'s auto-built filesystem.
        filesystem = None
        if "aws_temp_credentials" in creds and reader is ReaderFormat.DELTA:
            filesystem = self._build_s3_filesystem(creds["aws_temp_credentials"])

        return ResolvedSource(
            path=table_url,
            filesystem=filesystem,
            data_format=self._infer_format(table_info, table_url),
        )

    # ---- iceberg REST catalog ---------------------------------------------
    def _resolve_iceberg(self, table: str) -> ResolvedSource:
        # PyIceberg speaks the Iceberg REST protocol; Unity Catalog implements
        # it and vends data-file credentials via the access-delegation header.
        # No manual S3/ADLS/GCS keys are needed here.
        return ResolvedSource(
            catalog_kwargs={
                "type": "rest",
                "uri": f"{self._base_url}/api/2.1/unity-catalog/iceberg",
                "token": self._provider.get_token(),
                "header.X-Iceberg-Access-Delegation": "vended-credentials",
            },
            data_format=ReaderFormat.ICEBERG,
        )

    # ---- Unity Catalog REST helpers ---------------------------------------
    def _get_table_info(self, table: str) -> dict:
        from ray.data._internal.datasource.databricks_credentials import (
            request_with_401_retry,
        )

        url = f"{self._base_url}/api/2.1/unity-catalog/tables/{table}"
        resp = request_with_401_retry(requests.get, url, self._provider)
        return resp.json()

    def _get_creds(self, table_id: str) -> Tuple[dict, str]:
        from ray.data._internal.datasource.databricks_credentials import (
            request_with_401_retry,
        )

        url = f"{self._base_url}/api/2.1/unity-catalog/temporary-table-credentials"
        payload = {"table_id": table_id, "operation": "READ"}
        resp = request_with_401_retry(requests.post, url, self._provider, json=payload)
        creds = resp.json()
        return creds, creds["url"]

    @staticmethod
    def _infer_format(table_info: dict, table_url: str) -> Optional[ReaderFormat]:
        """Best-effort format hint from table metadata or file extension."""
        fmt = (table_info.get("data_source_format") or "").lower()
        if fmt in (ReaderFormat.DELTA.value, ReaderFormat.PARQUET.value):
            return ReaderFormat(fmt)

        storage_loc = table_info.get("storage_location") or table_url
        if storage_loc:
            ext = os.path.splitext(storage_loc)[-1].replace(".", "").lower()
            if ext in (ReaderFormat.DELTA.value, ReaderFormat.PARQUET.value):
                return ReaderFormat(ext)
        return None

    def infer_format(self, table: str) -> Optional[ReaderFormat]:
        """Best-effort format hint from table metadata or file extension.

        Calling this function will query UnityCatalog to get the relevant
        information."""
        info = self._get_table_info(table)
        _, table_url = self._get_creds(info["table_id"])
        return self._infer_format(info, table_url)

    def _creds_to_env(self, creds: dict) -> Dict[str, str]:
        """Translate vended credentials into environment variables."""
        if "aws_temp_credentials" in creds:
            aws = creds["aws_temp_credentials"]
            env = {
                "AWS_ACCESS_KEY_ID": aws["access_key_id"],
                "AWS_SECRET_ACCESS_KEY": aws["secret_access_key"],
                "AWS_SESSION_TOKEN": aws["session_token"],
            }
            if self._region:
                env["AWS_REGION"] = self._region
                env["AWS_DEFAULT_REGION"] = self._region
            return env

        if "azuresasuri" in creds or "azure_user_delegation_sas" in creds:
            return self._parse_azure_creds(creds)

        if "gcp_service_account" in creds:
            return {
                "GOOGLE_APPLICATION_CREDENTIALS": self._write_gcp_creds(
                    creds["gcp_service_account"]
                )
            }

        raise ValueError(
            "No known credential type found in Databricks UC response. "
            f"Available keys: {', '.join(creds.keys())}"
        )

    @staticmethod
    def _apply_env(env_vars: Dict[str, str]) -> None:
        """Set vended credentials in the environment and propagate to workers.

        Credentials are set on the driver's ``os.environ`` and, if Ray has not
        been initialized yet, into the cluster ``runtime_env`` so read tasks on
        workers inherit them. If Ray is already running we cannot retroactively
        amend its ``runtime_env``; driver-side env still covers driver reads
        (e.g. the Delta log) and single-node execution.

        TODO: remove once credential vending happens inside the read tasks.
        """
        import ray

        for k, v in env_vars.items():
            os.environ[k] = v
        if not ray.is_initialized():
            ray.init(runtime_env={"env_vars": dict(env_vars)})

    def _build_s3_filesystem(self, aws: dict) -> "pyarrow.fs.FileSystem":
        if not self._region:
            raise ValueError(
                "The 'region' parameter is required for AWS S3 access. "
                "Please specify the AWS region (e.g., region='us-west-2')."
            )
        import pyarrow.fs as pafs

        return pafs.S3FileSystem(
            access_key=aws["access_key_id"],
            secret_key=aws["secret_access_key"],
            session_token=aws["session_token"],
            region=self._region,
        )

    @staticmethod
    def _parse_azure_creds(creds: dict) -> Dict[str, str]:
        if "azuresasuri" in creds:
            return {"AZURE_STORAGE_SAS_TOKEN": creds["azuresasuri"]}

        # Azure UC returns a user delegation SAS; see
        # https://docs.databricks.com/en/data-governance/unity-catalog/credential-vending.html
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
                "azure_user_delegation_sas. "
                f"Available keys: {', '.join(azure.keys())}"
            )
        env: Dict[str, str] = {"AZURE_STORAGE_SAS_TOKEN": sas_token}
        storage_account = azure.get("storage_account")
        if storage_account:
            env["AZURE_STORAGE_ACCOUNT"] = storage_account
            env["AZURE_STORAGE_ACCOUNT_NAME"] = storage_account
        return env

    @staticmethod
    def _write_gcp_creds(gcp_json: str) -> str:
        """Write the GCP service-account JSON to a temp file; return its path.

        Registers an ``atexit`` handler to remove the file on interpreter exit.
        """
        temp_file = tempfile.NamedTemporaryFile(
            mode="w", prefix="gcp_sa_", suffix=".json", delete=False
        )
        temp_file.write(gcp_json)
        temp_file.close()
        atexit.register(UnityCatalog._cleanup_gcp_temp_file, temp_file.name)
        return temp_file.name

    @staticmethod
    def _cleanup_gcp_temp_file(temp_file_path: str) -> None:
        """Clean up temporary GCP service account file."""
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except OSError:
                pass
