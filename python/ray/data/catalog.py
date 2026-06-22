"""Catalog connectors for Ray Data readers.

A :class:`Catalog` resolves a table name into a readable source (location +
credentials) for a reader such as :func:`ray.data.read_delta`,
:func:`ray.data.read_parquet`, or :func:`ray.data.read_iceberg`.
"""

import atexit
import logging
import os
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple
from urllib.parse import urljoin

import requests

from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow.fs

    from ray.data._internal.datasource.databricks_credentials import (
        DatabricksCredentialProvider,
    )

logger = logging.getLogger(__name__)

# Keys returned by the Unity Catalog REST API (table metadata + credential vending).
_UNIFORM_FORMATS_PROPERTY = "delta.universalFormat.enabledFormats"
_AWS_CREDS_KEY = "aws_temp_credentials"
_AZURE_SAS_URI_KEY = "azuresasuri"
_AZURE_USER_DELEGATION_SAS_KEY = "azure_user_delegation_sas"
_GCP_SERVICE_ACCOUNT_KEY = "gcp_service_account"


def _normalize_host(host: str) -> str:
    host = host.rstrip("/")
    if not host.startswith(("http://", "https://")):
        host = f"https://{host}"
    return host


@PublicAPI(stability="alpha")
class ReaderFormat(str, Enum):
    """Which reader is asking the catalog to resolve a table."""

    DELTA = "delta"
    PARQUET = "parquet"
    ICEBERG = "iceberg"


@dataclass
class ResolvedSource:
    """The output of :meth:`Catalog.resolve` — location/credentials for a reader.

    A reader consumes only the fields it understands:

    * ``read_delta``:   ``path`` + (``storage_options`` and/or ``filesystem``)
    * ``read_parquet``: ``path`` + ``filesystem``
    * ``read_iceberg``: ``catalog_kwargs`` + ``table_identifier``

    Unused fields are ``None``.
    """

    path: Optional[str] = None
    filesystem: Optional["pyarrow.fs.FileSystem"] = None
    storage_options: Optional[Dict[str, Any]] = None
    catalog_kwargs: Optional[Dict[str, Any]] = None
    # Identifier the reader should address the table by, if the catalog rewrites
    # it (e.g. Iceberg REST scopes the warehouse to the catalog, so the table is
    # addressed as ``schema.table`` rather than ``catalog.schema.table``).
    table_identifier: Optional[str] = None
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
        assert reader is not None and isinstance(reader, ReaderFormat)
        if reader is ReaderFormat.ICEBERG:
            return self._resolve_iceberg(table)
        if reader in (ReaderFormat.DELTA, ReaderFormat.PARQUET):
            return self._resolve_storage(table, reader)
        # Reached only if a new ReaderFormat is added without handling here.
        raise ValueError(f"UnityCatalog does not support format={reader!r}")

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
        if _AWS_CREDS_KEY in creds and reader is ReaderFormat.DELTA:
            filesystem = self._build_s3_filesystem(creds[_AWS_CREDS_KEY])

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
        #
        # The REST catalog is scoped to a single UC catalog via `warehouse`, so
        # the table is addressed by `schema.table` (the catalog prefix would
        # otherwise be double-applied, e.g. `tmp.tmp.schema.table`).
        catalog_name, _, namespace_table = table.partition(".")
        return ResolvedSource(
            table_identifier=namespace_table,
            catalog_kwargs={
                "type": "rest",
                "uri": urljoin(self._base_url, "/api/2.1/unity-catalog/iceberg-rest"),
                "warehouse": catalog_name,
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

        url = urljoin(self._base_url, f"/api/2.1/unity-catalog/tables/{table}")
        resp = request_with_401_retry(requests.get, url, self._provider)
        return resp.json()

    def _get_creds(self, table_id: str) -> Tuple[dict, str]:
        from ray.data._internal.datasource.databricks_credentials import (
            request_with_401_retry,
        )

        url = urljoin(
            self._base_url, "/api/2.1/unity-catalog/temporary-table-credentials"
        )
        payload = {"table_id": table_id, "operation": "READ"}
        resp = request_with_401_retry(requests.post, url, self._provider, json=payload)
        creds = resp.json()
        return creds, creds["url"]

    @staticmethod
    def _infer_format(table_info: dict, table_url: str) -> Optional[ReaderFormat]:
        """Best-effort format hint from table metadata or file extension."""
        fmt = (table_info.get("data_source_format") or "").lower()
        if fmt in {f.value for f in ReaderFormat}:
            # A UniForm Delta table reports DELTA but also exposes Iceberg
            # metadata. Prefer Iceberg: these tables require columnMapping, which
            # deltalake's pyarrow reader can't read, so the Delta path would fail.
            if fmt == ReaderFormat.DELTA.value:
                uniform = (table_info.get("properties") or {}).get(
                    _UNIFORM_FORMATS_PROPERTY, ""
                )
                if "iceberg" in uniform.lower():
                    return ReaderFormat.ICEBERG
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
        if _AWS_CREDS_KEY in creds:
            aws = creds[_AWS_CREDS_KEY]
            env = {
                "AWS_ACCESS_KEY_ID": aws["access_key_id"],
                "AWS_SECRET_ACCESS_KEY": aws["secret_access_key"],
                "AWS_SESSION_TOKEN": aws["session_token"],
            }
            if self._region:
                env["AWS_REGION"] = self._region
                env["AWS_DEFAULT_REGION"] = self._region
            return env

        if _AZURE_SAS_URI_KEY in creds or _AZURE_USER_DELEGATION_SAS_KEY in creds:
            return self._parse_azure_creds(creds)

        if _GCP_SERVICE_ACCOUNT_KEY in creds:
            return {
                "GOOGLE_APPLICATION_CREDENTIALS": self._write_gcp_creds(
                    creds[_GCP_SERVICE_ACCOUNT_KEY]
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
        if _AZURE_SAS_URI_KEY in creds:
            return {"AZURE_STORAGE_SAS_TOKEN": creds[_AZURE_SAS_URI_KEY]}

        # Azure UC returns a user delegation SAS; see
        # https://docs.databricks.com/en/data-governance/unity-catalog/credential-vending.html
        azure = creds[_AZURE_USER_DELEGATION_SAS_KEY] or {}
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
                f"{_AZURE_USER_DELEGATION_SAS_KEY}. "
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
