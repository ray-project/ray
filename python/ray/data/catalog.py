"""Catalog connectors for Ray Data readers.

A :class:`Catalog` resolves a table name into a readable source (location +
credentials) for a reader such as :func:`ray.data.read_delta`,
:func:`ray.data.read_parquet`, or :func:`ray.data.read_iceberg`.
"""

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import KW_ONLY, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse

from packaging.version import parse as parse_version

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
        TableOperation,
    )

    from ray.data._internal.datasource.databricks_credentials import (
        DatabricksCredentialProvider,
    )

logger = logging.getLogger(__name__)

_DELTA_UNIFORM_FORMATS_PROPERTY = "delta.universalFormat.enabledFormats"

# Environment-variable names the underlying readers (pyarrow / deltalake's
# object_store) pick up vended credentials from.
_AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"
_AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
_AWS_SESSION_TOKEN = "AWS_SESSION_TOKEN"
_AWS_REGION = "AWS_REGION"
_AWS_DEFAULT_REGION = "AWS_DEFAULT_REGION"
_AZURE_STORAGE_SAS_TOKEN = "AZURE_STORAGE_SAS_TOKEN"

# deltalake's object_store config key for a SAS, passed via `storage_options`
# rather than the environment so it pickles into the read tasks.
_AZURE_STORAGE_SAS_TOKEN_OPTION = "azure_storage_sas_token"

# `pyarrow.fs.AzureFileSystem` gained a `sas_token` parameter in pyarrow 20.0.0
# (apache/arrow#45705). Before that it accepts only `account_key`, and reads no
# SAS environment variable, so a Unity Catalog SAS cannot reach it by any route.
_AZURE_SAS_PYARROW_VERSION_MIN = "20.0.0"


def _check_azure_sas_pyarrow_support() -> None:
    """Raise if pyarrow is too old to authenticate with a vended Azure SAS.

    Only the Parquet reader needs this: it is filesystem-only, so the SAS has to
    reach it through a `pyarrow.fs.AzureFileSystem`. Delta is unaffected -- its
    credential travels in `storage_options` to deltalake's object_store, which
    has its own Azure client and no pyarrow floor.

    Failing here is worth the strictness: pyarrow's Azure filesystem does not
    report a missing credential as a Python error. It falls through to
    `DefaultAzureCredential`, which either stalls on the instance-metadata
    endpoint or, over a plain-HTTP endpoint, calls `abort()` in C++ and takes
    the whole process down with SIGABRT -- uncatchable from Python.
    """
    from ray.data._internal.utils.arrow_utils import get_pyarrow_version

    version = get_pyarrow_version()
    minimum = parse_version(_AZURE_SAS_PYARROW_VERSION_MIN)
    if version is None:
        # Version undetectable (pyarrow vendored inside another package). Warn
        # rather than block -- the same stance `_check_pyarrow_version` takes.
        logger.warning(
            "Reading an Azure-backed Parquet table through Unity Catalog needs "
            "pyarrow >= %s to use the vended SAS, but the installed pyarrow "
            "version could not be determined.",
            _AZURE_SAS_PYARROW_VERSION_MIN,
        )
        return
    if version < minimum:
        raise ImportError(
            f"Reading an Azure-backed Parquet table through Unity Catalog "
            f"requires pyarrow >= {_AZURE_SAS_PYARROW_VERSION_MIN}, but "
            f"{version} is installed. Unity Catalog vends a SAS token, and "
            f"`pyarrow.fs.AzureFileSystem` only accepts one from pyarrow "
            f"{_AZURE_SAS_PYARROW_VERSION_MIN} onwards (apache/arrow#45705). "
            f'Upgrade with `pip install -U "pyarrow>='
            f'{_AZURE_SAS_PYARROW_VERSION_MIN}"`, or read the table as Delta '
            f"(`ray.data.read_delta`), which carries the SAS in "
            f"`storage_options` and has no pyarrow floor."
        )


def _parse_azure_url(url: str) -> Tuple[str, str, str]:
    """Split ``abfss://<container>@<account>.dfs.core.windows.net/<key>``.

    Returns ``(account, container, key)``. This is the shape Unity Catalog vends
    for ADLS Gen2; anything else is a bug in our own assumptions rather than
    something to recover from.
    """
    parsed = urlparse(url, allow_fragments=False)
    if parsed.scheme not in ("abfs", "abfss") or "@" not in parsed.netloc:
        raise ValueError(
            f"Expected an abfss://<container>@<account>.dfs.core.windows.net/... "
            f"URL from Unity Catalog, got {url!r}."
        )
    container, _, host = parsed.netloc.partition("@")
    account = host.split(".")[0]
    if not account or not container:
        raise ValueError(f"Could not parse account/container out of {url!r}.")
    return account, container, parsed.path.lstrip("/")


def _azure_url_without_authority(url: str) -> str:
    """Rewrite the vended URL to ``abfs://<container>/<key>``."""
    _, container, key = _parse_azure_url(url)
    return f"abfs://{container}/{key}" if key else f"abfs://{container}"


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
class CatalogAccessMode(str, Enum):
    """Whether the catalog should vend read or write credentials."""

    READ = "read"
    WRITE = "write"

    def as_databricks_table_op(self) -> "TableOperation":
        # Unity Catalog only exposes READ and READ_WRITE (there is no write-only
        # operation), so WRITE maps to READ_WRITE.
        from databricks.sdk.service.catalog import TableOperation

        if self == CatalogAccessMode.READ:
            return TableOperation.READ
        elif self == CatalogAccessMode.WRITE:
            return TableOperation.READ_WRITE
        raise ValueError("Unsupported CatalogAccessMode for Databricks TableOperation")


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
    def resolve(
        self,
        table: str,
        *,
        reader: ReaderFormat,
        mode: CatalogAccessMode = CatalogAccessMode.READ,
    ) -> ResolvedSource:
        """Resolve ``table`` for the given ``reader`` and access ``mode``."""
        ...


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
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

    _: KW_ONLY
    url: Optional[str] = None
    # `repr=False` keeps the token/provider out of the auto-generated repr.
    token: Optional[str] = field(default=None, repr=False)
    credential_provider: Optional["DatabricksCredentialProvider"] = field(
        default=None, repr=False
    )
    region: Optional[str] = None

    # Derived in __post_init__; declared (init=False) so type checkers know the
    # attributes exist, and excluded from repr/eq.
    _provider: "DatabricksCredentialProvider" = field(
        init=False, repr=False, compare=False
    )
    _base_url: str = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        from ray.data._internal.datasource.databricks_credentials import (
            UnityCatalogCredentialConfig,
            resolve_credential_provider,
        )

        # Derived (not init args); `object.__setattr__` is how a frozen dataclass
        # assigns inside __post_init__.
        provider = resolve_credential_provider(
            UnityCatalogCredentialConfig(
                credential_provider=self.credential_provider,
                url=self.url,
                token=self.token,
            )
        )
        object.__setattr__(self, "_provider", provider)
        object.__setattr__(self, "_base_url", _normalize_host(provider.get_host()))

    # ---- Catalog interface -------------------------------------------------
    def resolve(
        self,
        table: str,
        *,
        reader: ReaderFormat,
        mode: CatalogAccessMode = CatalogAccessMode.READ,
    ) -> ResolvedSource:
        assert reader is not None and isinstance(reader, ReaderFormat)
        assert mode is not None and isinstance(mode, CatalogAccessMode)
        if reader is ReaderFormat.ICEBERG:
            return self._resolve_iceberg(table)
        if reader in (ReaderFormat.DELTA, ReaderFormat.PARQUET):
            return self._resolve_storage(table, reader, mode)
        # Reached only if a new ReaderFormat is added without handling here.
        raise ValueError(f"DatabricksUnityCatalog does not support format={reader!r}")

    # ---- storage-credential vending (delta / parquet) ----------------------
    def _resolve_storage(
        self, table: str, reader: ReaderFormat, mode: CatalogAccessMode
    ) -> ResolvedSource:
        table_info = self._get_table_info(table)
        creds, table_url = self._get_creds(table_info.table_id, mode)

        # Some readers/writers need an explicit pyarrow filesystem:
        #  - AWS Delta: the vended session token isn't reliably propagated through
        #    `DeltaTable.to_pyarrow_dataset`'s auto-built filesystem.
        #  - AWS write: an S3FileSystem built from the default credential chain
        #    (the `filesystem=None` path) does NOT serialize its credentials, so a
        #    worker would rebuild it from *its own* environment. Reads get away
        #    with this because `_apply_env` can seed the vended creds into the
        #    cluster `runtime_env` while Ray is still uninitialized; a write always
        #    runs after Ray is initialized (a materialized Dataset already exists),
        #    so that propagation is unavailable. Build an explicit S3FileSystem
        #    whose creds *do* pickle into the datasink and reach the workers.
        #  - GCP Parquet: a bare OAuth token has no env var pyarrow auto-reads,
        #    so the data scan needs an explicit GcsFileSystem.
        filesystem = None
        if creds.aws_temp_credentials is not None and (
            reader is ReaderFormat.DELTA or mode is CatalogAccessMode.WRITE
        ):
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
                    "credential vending is not supported as deltalake "
                    "does not have the required object_store version."
                )
            filesystem = self._build_gcs_filesystem(
                creds.gcp_oauth_token, creds.expiration_time
            )

        # Azure. Delta needs no filesystem -- the SAS travels in
        # `storage_options` below, which deltalake's object_store reads and
        # which pickles into the read tasks. Parquet is filesystem-only, so it
        # needs an explicit `AzureFileSystem` carrying the SAS; that requires
        # pyarrow >= 20, hence the check.
        storage_options = None
        if creds.azure_user_delegation_sas is not None:
            storage_options = self._azure_storage_options(
                creds.azure_user_delegation_sas
            )
            if reader is ReaderFormat.PARQUET:
                _check_azure_sas_pyarrow_support()
                filesystem = self._build_azure_filesystem(
                    table_url, creds.azure_user_delegation_sas
                )
                # Drop the `<container>@<account>.dfs.core.windows.net`
                # authority. PyArrow only parses that Hadoop-style form when it
                # builds the filesystem from the URI itself; given a filesystem
                # it strips the scheme and treats the authority as part of the
                # path, so reads land on
                # `<account>.blob.core.windows.net/<container>@<account>...`
                # and 404. The account lives on the filesystem object instead.
                # Same reasoning as `_rewrite_azure_blob_https_url` in
                # `datasource/path_util.py`.
                table_url = _azure_url_without_authority(table_url)

        # Deliver vended credentials via environment variables as well. This is
        # the mechanism the underlying libraries read uniformly: pyarrow (Parquet
        # data, and S3/Azure/GCS auto-filesystems) and deltalake's object_store
        # (the Delta transaction *log* read in `DeltaTable(...)`). Still needed
        # for the readers that take no `storage_options` at all -- notably
        # `read_parquet`, which is filesystem-only.
        #
        # TODO: remove the env-var + ray.init mechanism once credential vending
        # is performed inside the read tasks themselves (worker-side).
        self._apply_env(self._creds_to_env(creds))

        return ResolvedSource(
            path=table_url,
            filesystem=filesystem,
            storage_options=storage_options,
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

    def _call_with_token_refresh(self, call: Callable) -> Any:
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
        self, table_id: Optional[str], mode: CatalogAccessMode = CatalogAccessMode.READ
    ) -> Tuple["GenerateTemporaryTableCredentialResponse", str]:
        assert table_id is not None
        operation = mode.as_databricks_table_op()
        creds = self._call_with_token_refresh(
            lambda w: w.temporary_table_credentials.generate_temporary_table_credentials(
                table_id=table_id, operation=operation
            )
        )
        return creds, creds.url

    @staticmethod
    def _infer_format(
        table_info: "TableInfo", table_url: str
    ) -> Optional[ReaderFormat]:
        """Best-effort format hint from table metadata or file extension."""
        from databricks.sdk.service.catalog import DataSourceFormat

        dsf = table_info.data_source_format
        if dsf == DataSourceFormat.DELTA:
            uniform = (table_info.properties or {}).get(
                _DELTA_UNIFORM_FORMATS_PROPERTY, ""
            )
            if "iceberg" in uniform.lower():
                return ReaderFormat.ICEBERG
            return ReaderFormat.DELTA
        elif dsf == DataSourceFormat.PARQUET:
            return ReaderFormat.PARQUET

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
                _AWS_ACCESS_KEY_ID: aws.access_key_id,
                _AWS_SECRET_ACCESS_KEY: aws.secret_access_key,
                _AWS_SESSION_TOKEN: aws.session_token,
            }
            if self.region:
                env[_AWS_REGION] = self.region
                env[_AWS_DEFAULT_REGION] = self.region
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
        if not self.region:
            raise ValueError(
                "The 'region' parameter is required for AWS S3 access. "
                "Please specify the AWS region (e.g., region='us-west-2')."
            )
        import pyarrow.fs as pafs

        return pafs.S3FileSystem(
            access_key=aws.access_key_id,
            secret_key=aws.secret_access_key,
            session_token=aws.session_token,
            region=self.region,
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
    def _azure_sas_token(sas: "AzureUserDelegationSas") -> str:
        sas_token = sas.sas_token
        # Unity Catalog may return the SAS as a full query string ("?sv=..."),
        # but every consumer wants it bare.
        if sas_token and sas_token.startswith("?"):
            sas_token = sas_token[1:]
        if not sas_token:
            raise ValueError("Azure UC credentials missing a SAS token.")
        return sas_token

    @classmethod
    def _parse_azure_creds(
        cls, sas: "AzureUserDelegationSas"
    ) -> Dict[str, Optional[str]]:
        creds: Dict[str, Optional[str]] = {
            _AZURE_STORAGE_SAS_TOKEN: cls._azure_sas_token(sas)
        }
        return creds

    @classmethod
    def _build_azure_filesystem(
        cls, table_url: str, sas: "AzureUserDelegationSas"
    ) -> "pyarrow.fs.FileSystem":
        """An `AzureFileSystem` carrying the vended SAS, for the Parquet scan.

        The leading ``?`` matters and is why this does not reuse
        `_azure_sas_token`'s bare form: pyarrow appends `sas_token` to the
        service URL verbatim, so without it the token is taken as a *path
        segment* -- requests go to
        `<account>.blob.core.windows.net/<the-whole-sas>/<container>/...` and
        fail unauthenticated. deltalake's object_store and
        ``AZURE_STORAGE_SAS_TOKEN`` both want it stripped, so the two channels
        genuinely disagree on the format.
        """
        import pyarrow.fs as pafs

        account, _, _ = _parse_azure_url(table_url)
        return pafs.AzureFileSystem(
            account_name=account, sas_token="?" + cls._azure_sas_token(sas)
        )

    @classmethod
    def _azure_storage_options(cls, sas: "AzureUserDelegationSas") -> Dict[str, str]:
        """Vended SAS in the form deltalake's object_store reads.

        `azure_storage_sas_token` is one of object_store's accepted aliases for
        the SAS config key. The account name is not included: it is already in
        the `abfss://<container>@<account>.dfs.core.windows.net/...` URL that
        accompanies these options, and duplicating it here would let the two
        disagree.
        """
        return {_AZURE_STORAGE_SAS_TOKEN_OPTION: cls._azure_sas_token(sas)}
