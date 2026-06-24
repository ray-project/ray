"""Catalog connectors for Ray Data readers.

A :class:`Catalog` resolves a table name into a readable source (location +
credentials) for a reader such as :func:`ray.data.read_delta`,
:func:`ray.data.read_parquet`, or :func:`ray.data.read_iceberg`.
"""

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple
from urllib.parse import urljoin

from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    import pyarrow.fs
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import (
        AwsCredentials,
        AzureUserDelegationSas,
        GcpOauthToken,
        GenerateTemporaryTableCredentialResponse,
        TableInfo,
    )

    from ray.data._internal.datasource.databricks_credentials import (
        DatabricksCredentialProvider,
    )

logger = logging.getLogger(__name__)

_DELTA_UNIFORM_FORMATS_PROPERTY = "delta.universalFormat.enabledFormats"


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


@DeveloperAPI
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
class DatabricksUnityCatalog(Catalog):
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
        >>> catalog = ray.data.DatabricksUnityCatalog(  # doctest: +SKIP
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
        raise ValueError(f"DatabricksUnityCatalog does not support format={reader!r}")

    # ---- storage-credential vending (delta / parquet) ----------------------
    def _resolve_storage(self, table: str, reader: ReaderFormat) -> ResolvedSource:
        table_info = self._get_table_info(table)
        creds, table_url = self._get_creds(table_info.table_id)

        # Some reads need an explicit pyarrow filesystem:
        #  - AWS Delta: the vended session token isn't reliably propagated through
        #    `DeltaTable.to_pyarrow_dataset`'s auto-built filesystem.
        #  - GCP Parquet: a bare OAuth token has no env var pyarrow auto-reads,
        #    so the data scan needs an explicit GcsFileSystem.
        filesystem = None
        if creds.aws_temp_credentials is not None and reader is ReaderFormat.DELTA:
            filesystem = self._build_s3_filesystem(creds.aws_temp_credentials)
        elif creds.gcp_oauth_token is not None:
            if reader is ReaderFormat.DELTA:
                # Unity Catalog vends a GCP OAuth token, but deltalake's
                # object_store (<=0.13.x, bundled in deltalake<=1.6.1) only
                # accepts service-account-key auth for GCS -- it has no
                # bearer/OAuth-token config key -- so the Delta transaction-log
                # read can't use the vended token and silently falls back to GCE
                # metadata-server auth.
                raise RuntimeError(
                    "Reading a GCP-backed Delta table via Unity Catalog "
                    "credential vending is not supported."
                )
            filesystem = self._build_gcs_filesystem(
                creds.gcp_oauth_token, creds.expiration_time
            )

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

    # ---- Unity Catalog SDK helpers ----------------------------------------
    def _workspace_client(self) -> "WorkspaceClient":
        from databricks.sdk import WorkspaceClient

        return WorkspaceClient(host=self._base_url, token=self._provider.get_token())

    def _call_with_token_refresh(self, call):
        """Run ``call(workspace_client)``, retrying once on 401.

        Mirrors the previous ``request_with_401_retry`` behavior: on an
        authentication failure, invalidate the credential provider (so the next
        ``get_token()`` returns a fresh token) and retry once with a new client.
        Matters for refreshable providers; a no-op for static PATs.
        """
        from databricks.sdk.errors import Unauthenticated

        try:
            return call(self._workspace_client())
        except Unauthenticated:
            logger.info("Received 401 from Unity Catalog; refreshing credentials.")
            self._provider.invalidate()
            return call(self._workspace_client())

    def _get_table_info(self, table: str) -> "TableInfo":
        return self._call_with_token_refresh(lambda w: w.tables.get(full_name=table))

    def _get_creds(
        self, table_id: Optional[str]
    ) -> Tuple["GenerateTemporaryTableCredentialResponse", str]:
        from databricks.sdk.service.catalog import TableOperation

        assert table_id is not None
        creds = self._call_with_token_refresh(
            lambda w: w.temporary_table_credentials.generate_temporary_table_credentials(
                table_id=table_id, operation=TableOperation.READ
            )
        )
        return creds, creds.url

    @staticmethod
    def _infer_format(
        table_info: "TableInfo", table_url: str
    ) -> Optional[ReaderFormat]:
        """Best-effort format hint from table metadata or file extension."""
        dsf = table_info.data_source_format
        fmt = (dsf.value if dsf else "").lower()
        if fmt in {f.value for f in ReaderFormat}:
            # A UniForm Delta table reports DELTA but also exposes Iceberg
            # metadata. Prefer Iceberg: these tables require columnMapping, which
            # deltalake's pyarrow reader can't read, so the Delta path would fail.
            if fmt == ReaderFormat.DELTA.value:
                uniform = (table_info.properties or {}).get(
                    _DELTA_UNIFORM_FORMATS_PROPERTY, ""
                )
                if "iceberg" in uniform.lower():
                    return ReaderFormat.ICEBERG
            return ReaderFormat(fmt)

        storage_loc = table_info.storage_location or table_url
        if storage_loc:
            ext = os.path.splitext(storage_loc)[-1].replace(".", "").lower()
            if ext in (ReaderFormat.DELTA.value, ReaderFormat.PARQUET.value):
                return ReaderFormat(ext)
        return None

    def infer_format(self, table: str) -> Optional[ReaderFormat]:
        """Best-effort format hint from table metadata or file extension.

        Calling this function will query DatabricksUnityCatalog to get the
        relevant information."""
        info = self._get_table_info(table)
        _, table_url = self._get_creds(info.table_id)
        return self._infer_format(info, table_url)

    def _creds_to_env(
        self, creds: "GenerateTemporaryTableCredentialResponse"
    ) -> Dict[str, Optional[str]]:
        """Translate vended credentials into environment variables."""
        if creds.aws_temp_credentials is not None:
            aws = creds.aws_temp_credentials
            env = {
                "AWS_ACCESS_KEY_ID": aws.access_key_id,
                "AWS_SECRET_ACCESS_KEY": aws.secret_access_key,
                "AWS_SESSION_TOKEN": aws.session_token,
            }
            if self._region:
                env["AWS_REGION"] = self._region
                env["AWS_DEFAULT_REGION"] = self._region
            return env

        if creds.azure_user_delegation_sas is not None:
            return self._parse_azure_creds(creds.azure_user_delegation_sas)

        if creds.gcp_oauth_token is not None:
            # A bare GCP OAuth token has no env var pyarrow/deltalake auto-read;
            # it's delivered via an explicit GcsFileSystem (data scan) and via
            # `storage_options` (Delta log read) in `_resolve_storage` instead.
            return {}

        raise ValueError("No known credential type found in Databricks UC response.")

    @staticmethod
    def _apply_env(env_vars: Dict[str, Optional[str]]) -> None:
        """Set vended credentials in the environment and propagate to workers.

        Credentials are set on the driver's ``os.environ`` and, if Ray has not
        been initialized yet, into the cluster ``runtime_env`` so read tasks on
        workers inherit them. If Ray is already running we cannot retroactively
        amend its ``runtime_env``; driver-side env still covers driver reads
        (e.g. the Delta log) and single-node execution.

        TODO: remove once credential vending happens inside the read tasks.
        """
        import ray

        if not env_vars:
            return

        for k, v in env_vars.items():
            if v:
                os.environ[k] = v
        if not ray.is_initialized():
            ray.init(runtime_env={"env_vars": dict(env_vars)})

    def _build_s3_filesystem(self, aws: "AwsCredentials") -> "pyarrow.fs.FileSystem":
        if not self._region:
            raise ValueError(
                "The 'region' parameter is required for AWS S3 access. "
                "Please specify the AWS region (e.g., region='us-west-2')."
            )
        import pyarrow.fs as pafs

        return pafs.S3FileSystem(
            access_key=aws.access_key_id,
            secret_key=aws.secret_access_key,
            session_token=aws.session_token,
            region=self._region,
        )

    @staticmethod
    def _build_gcs_filesystem(
        gcp: "GcpOauthToken", expiration_time: Optional[int]
    ) -> "pyarrow.fs.FileSystem":
        import pyarrow.fs as pafs

        if expiration_time is None:
            # pyarrow requires an expiration alongside an access token.
            raise ValueError(
                "GCP credential vending did not return an expiration_time."
            )
        expiration = datetime.fromtimestamp(expiration_time / 1000, tz=timezone.utc)
        return pafs.GcsFileSystem(
            access_token=gcp.oauth_token,
            credential_token_expiration=expiration,
        )

    @staticmethod
    def _parse_azure_creds(sas: "AzureUserDelegationSas") -> Dict[str, str]:
        sas_token = sas.sas_token
        if sas_token and sas_token.startswith("?"):
            sas_token = sas_token[1:]
        if not sas_token:
            raise ValueError("Azure UC credentials missing a SAS token.")
        return {"AZURE_STORAGE_SAS_TOKEN": sas_token}
