import atexit
import logging
import os
import tempfile
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

import requests  # HTTP library: https://requests.readthedocs.io/

import ray
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

# Unity Catalog REST API Documentation:
# https://docs.databricks.com/api/workspace/unity-catalog
# Credential Vending API:
# https://docs.databricks.com/en/data-governance/unity-catalog/credential-vending.html


@dataclass
class ColumnInfo:
    """Column metadata from Unity Catalog table schema.

    Reference: https://docs.databricks.com/api/workspace/tables/get
    """

    name: str
    type_text: str
    type_name: str
    position: int
    type_precision: int
    type_scale: int
    type_json: str
    nullable: bool
    comment: Optional[str] = None
    partition_index: Optional[int] = None

    @staticmethod
    def from_dict(obj: Dict) -> "ColumnInfo":
        """
        Safely construct ColumnInfo from Unity Catalog API response.

        Args:
            obj: Dictionary from Unity Catalog tables API columns field

        Returns:
            ColumnInfo instance with extracted fields
        """
        return ColumnInfo(
            name=obj["name"],
            type_text=obj["type_text"],
            type_name=obj["type_name"],
            position=obj["position"],
            type_precision=obj.get("type_precision", 0),
            type_scale=obj.get("type_scale", 0),
            type_json=obj.get("type_json", ""),
            nullable=obj.get("nullable", True),
            comment=obj.get("comment"),
            partition_index=obj.get("partition_index"),
        )


@dataclass
class EffectiveFlag:
    """Flag indicating effective settings inherited from parent resources."""

    value: str
    inherited_from_type: str
    inherited_from_name: str


@dataclass
class TableInfo:
    """Metadata for Unity Catalog tables.

    Represents the response from the Unity Catalog tables API.
    """

    name: str
    catalog_name: str
    schema_name: str
    table_type: str
    data_source_format: str
    columns: List[ColumnInfo]
    storage_location: str
    owner: str
    properties: Dict[str, str]
    securable_kind: str
    enable_auto_maintenance: str
    enable_predictive_optimization: str
    properties_pairs: Dict[str, Any]
    generation: int
    metastore_id: str
    full_name: str
    data_access_configuration_id: str
    created_at: int
    created_by: str
    updated_at: int
    updated_by: str
    table_id: str
    delta_runtime_properties_kvpairs: Dict[str, Any]
    securable_type: str
    effective_auto_maintenance_flag: Optional[EffectiveFlag] = None
    effective_predictive_optimization_flag: Optional[EffectiveFlag] = None
    browse_only: Optional[bool] = None
    metastore_version: Optional[int] = None

    @staticmethod
    def from_dict(obj: Dict) -> "TableInfo":
        """
        Parse table metadata from Unity Catalog API response.

        Args:
            obj: Dictionary from Unity Catalog tables API response

        Returns:
            TableInfo instance with parsed metadata
        """
        # Parse optional nested structures
        if obj.get("effective_auto_maintenance_flag"):
            effective_auto_maintenance_flag = EffectiveFlag(
                **obj["effective_auto_maintenance_flag"]
            )
        else:
            effective_auto_maintenance_flag = None

        if obj.get("effective_predictive_optimization_flag"):
            effective_predictive_optimization_flag = EffectiveFlag(
                **obj["effective_predictive_optimization_flag"]
            )
        else:
            effective_predictive_optimization_flag = None

        return TableInfo(
            name=obj["name"],
            catalog_name=obj["catalog_name"],
            schema_name=obj["schema_name"],
            table_type=obj["table_type"],
            data_source_format=obj.get("data_source_format", ""),
            columns=[ColumnInfo.from_dict(col) for col in obj.get("columns", [])],
            storage_location=obj.get("storage_location", ""),
            owner=obj.get("owner", ""),
            properties=obj.get("properties", {}),
            securable_kind=obj.get("securable_kind", ""),
            enable_auto_maintenance=obj.get("enable_auto_maintenance", ""),
            enable_predictive_optimization=obj.get(
                "enable_predictive_optimization", ""
            ),
            properties_pairs=obj.get("properties_pairs", {}),
            generation=obj.get("generation", 0),
            metastore_id=obj.get("metastore_id", ""),
            full_name=obj.get("full_name", ""),
            data_access_configuration_id=obj.get("data_access_configuration_id", ""),
            created_at=obj.get("created_at", 0),
            created_by=obj.get("created_by", ""),
            updated_at=obj.get("updated_at", 0),
            updated_by=obj.get("updated_by", ""),
            table_id=obj.get("table_id", ""),
            delta_runtime_properties_kvpairs=obj.get(
                "delta_runtime_properties_kvpairs", {}
            ),
            securable_type=obj.get("securable_type", ""),
            effective_auto_maintenance_flag=effective_auto_maintenance_flag,
            effective_predictive_optimization_flag=effective_predictive_optimization_flag,
            browse_only=obj.get("browse_only", False),
            metastore_version=obj.get("metastore_version", 0),
        )


class CloudProvider(Enum):
    """Cloud provider types for credential vending."""

    AWS = "aws"
    AZURE = "azure"
    GCP = "gcp"


@dataclass
class AWSCredentials:
    """AWS temporary credentials from Unity Catalog credential vending."""

    access_key_id: str
    secret_access_key: str
    session_token: str

    @staticmethod
    def from_dict(obj: Dict) -> "AWSCredentials":
        return AWSCredentials(
            access_key_id=obj["access_key_id"],
            secret_access_key=obj["secret_access_key"],
            session_token=obj["session_token"],
        )


@dataclass
class AzureSASCredentials:
    """Azure SAS token credentials from Unity Catalog credential vending."""

    sas_token: str

    @staticmethod
    def from_dict(obj: Dict) -> "AzureSASCredentials":
        return AzureSASCredentials(sas_token=obj["sas_token"])


@dataclass
class GCPOAuthCredentials:
    """GCP OAuth token credentials from Unity Catalog credential vending."""

    oauth_token: str

    @staticmethod
    def from_dict(obj: Dict) -> "GCPOAuthCredentials":
        return GCPOAuthCredentials(oauth_token=obj["oauth_token"])


@dataclass
class CredentialsResponse:
    """
    Response from Unity Catalog credential vending API.

    Contains cloud-specific temporary credentials and storage URL.
    """

    url: str
    cloud_provider: CloudProvider
    aws_credentials: Optional[AWSCredentials] = None
    azure_credentials: Optional[AzureSASCredentials] = None
    gcp_credentials: Optional[GCPOAuthCredentials] = None
    azure_sas_uri: Optional[str] = None
    gcp_service_account_json: Optional[str] = None

    @staticmethod
    def from_dict(obj: Dict) -> "CredentialsResponse":
        """
        Parse credentials response from Unity Catalog API.

        Handles multiple credential formats for each cloud provider.
        """
        url = obj.get("url", "")

        # Determine cloud provider and parse credentials
        if "aws_temp_credentials" in obj:
            return CredentialsResponse(
                url=url,
                cloud_provider=CloudProvider.AWS,
                aws_credentials=AWSCredentials.from_dict(obj["aws_temp_credentials"]),
            )
        elif "azure_user_delegation_sas" in obj:
            return CredentialsResponse(
                url=url,
                cloud_provider=CloudProvider.AZURE,
                azure_credentials=AzureSASCredentials.from_dict(
                    obj["azure_user_delegation_sas"]
                ),
            )
        elif "azuresasuri" in obj:
            # Legacy Azure format
            return CredentialsResponse(
                url=url,
                cloud_provider=CloudProvider.AZURE,
                azure_sas_uri=obj["azuresasuri"],
            )
        elif "gcp_oauth_token" in obj:
            return CredentialsResponse(
                url=url,
                cloud_provider=CloudProvider.GCP,
                gcp_credentials=GCPOAuthCredentials.from_dict(obj["gcp_oauth_token"]),
            )
        elif "gcp_service_account" in obj:
            # Legacy GCP format
            return CredentialsResponse(
                url=url,
                cloud_provider=CloudProvider.GCP,
                gcp_service_account_json=obj["gcp_service_account"],
            )
        else:
            raise ValueError(
                f"No recognized credential type in response. "
                f"Available keys: {list(obj.keys())}"
            )


@DeveloperAPI
class UnityCatalogConnector:
    """
    Connector for reading Unity Catalog Delta tables into Ray Datasets.

    This connector handles automatic credential vending for secure access to cloud
    storage backing Unity Catalog Delta tables. It supports AWS S3, Azure Data Lake
    Storage, and Google Cloud Storage with temporary, least-privilege credentials.

    This implementation specifically focuses on Delta Lake tables, which is the most
    common format in Unity Catalog deployments.

    Cloud providers:
        - AWS S3 (with temporary IAM credentials)
        - Azure Data Lake Storage (with SAS tokens)
        - Google Cloud Storage (with OAuth tokens or service account)

    Args:
        base_url: Databricks workspace URL (e.g., "https://dbc-xxx.cloud.databricks.com")
        token: Databricks Personal Access Token with appropriate permissions
        table: Unity Catalog table path in format "catalog.schema.table"
        region: Optional AWS region for S3 credential configuration
        reader_kwargs: Additional arguments passed to ray.data.read_delta()

    References:
        - Credential Vending: https://docs.databricks.com/en/data-governance/unity-catalog/credential-vending.html
        - Unity Catalog: https://docs.databricks.com/en/data-governance/unity-catalog/
    """

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        table: str,
        region: Optional[str] = None,
        reader_kwargs: Optional[Dict] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.table = table
        self.region = region
        self.reader_kwargs = reader_kwargs or {}

        # Storage for metadata and credentials
        self._table_info: Optional[TableInfo] = None
        self._table_id: Optional[str] = None
        self._creds_response: Optional[CredentialsResponse] = None
        self._storage_url: Optional[str] = None
        self._gcp_temp_file: Optional[str] = None

    def _create_auth_headers(self) -> Dict[str, str]:
        """
        Create authorization headers for Unity Catalog API requests.

        Returns:
            Dictionary with Authorization header
        """
        return {"Authorization": f"Bearer {self.token}"}

    def _fetch_table_metadata(self) -> TableInfo:
        """
        Fetch table metadata from Unity Catalog tables API.

        Returns:
            TableInfo object with table metadata

        Raises:
            requests.HTTPError: If API request fails
        """
        url = f"{self.base_url}/api/2.1/unity-catalog/tables/{self.table}"
        headers = self._create_auth_headers()
        logger.debug(f"Fetching table metadata for '{self.table}' from {url}")
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()

        # Parse table information
        table_info = TableInfo.from_dict(data)
        self._table_info = table_info
        self._table_id = table_info.table_id
        logger.debug(
            f"Retrieved table metadata: format={table_info.data_source_format}, "
            f"storage={table_info.storage_location}"
        )
        return table_info

    def _get_creds(self):
        """
        Request temporary credentials from Unity Catalog credential vending API.

        Uses the table credential vending API which is production-ready and publicly
        documented.

        Raises:
            requests.HTTPError: If API request fails
            ValueError: If credential vending is not available or configured
        """
        if not self._table_id:
            raise ValueError(
                "Table ID not available. Call _fetch_table_metadata() first."
            )

        url = f"{self.base_url}/api/2.1/unity-catalog/temporary-table-credentials"
        payload = {
            "table_id": self._table_id,
            "operation": "READ",
        }
        headers = self._create_auth_headers()
        headers["Content-Type"] = "application/json"

        logger.debug(f"Requesting temporary credentials for table '{self.table}'")

        resp = requests.post(url, json=payload, headers=headers)
        resp.raise_for_status()

        # Parse credentials response into structured dataclass
        self._creds_response = CredentialsResponse.from_dict(resp.json())
        logger.debug(
            f"Successfully obtained credentials, "
            f"cloud provider: {self._creds_response.cloud_provider.value}"
        )

        # Extract and store storage URL from credentials response
        if not self._creds_response.url:
            raise ValueError(
                f"No storage URL returned for table '{self.table}'. "
                f"Credentials response: {self._creds_response}"
            )
        self._storage_url = self._creds_response.url

    def _set_env(self):
        """
        Configure cloud-specific environment variables for credential access.

        Sets up temporary credentials in the Ray runtime environment for
        AWS S3, Azure Blob Storage, or Google Cloud Storage.

        Supported cloud providers:
            - AWS S3: Sets AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN
            - Azure: Sets AZURE_STORAGE_SAS_TOKEN
            - GCP: Sets GCP_OAUTH_TOKEN or GOOGLE_APPLICATION_CREDENTIALS

        Raises:
            ValueError: If no recognized credential type is found in response
        """
        env_vars = {}
        creds = self._creds_response

        if creds.cloud_provider == CloudProvider.AWS:
            # AWS S3 temporary credentials
            # https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp.html
            if creds.aws_credentials:
                env_vars["AWS_ACCESS_KEY_ID"] = creds.aws_credentials.access_key_id
                env_vars["AWS_SECRET_ACCESS_KEY"] = (
                    creds.aws_credentials.secret_access_key
                )
                env_vars["AWS_SESSION_TOKEN"] = creds.aws_credentials.session_token
                if self.region:
                    env_vars["AWS_REGION"] = self.region
                    env_vars["AWS_DEFAULT_REGION"] = self.region
            else:
                raise ValueError("AWS credentials not found in response")

        elif creds.cloud_provider == CloudProvider.AZURE:
            # Azure Blob Storage SAS token
            # https://learn.microsoft.com/en-us/azure/storage/common/storage-sas-overview
            if creds.azure_credentials:
                env_vars["AZURE_STORAGE_SAS_TOKEN"] = creds.azure_credentials.sas_token
            elif creds.azure_sas_uri:
                # Legacy Azure format
                env_vars["AZURE_STORAGE_SAS_TOKEN"] = creds.azure_sas_uri
            else:
                raise ValueError("Azure credentials not found in response")

        elif creds.cloud_provider == CloudProvider.GCP:
            # Google Cloud Platform credentials
            # https://cloud.google.com/docs/authentication/token-types#access
            if creds.gcp_credentials:
                env_vars["GCP_OAUTH_TOKEN"] = creds.gcp_credentials.oauth_token
            elif creds.gcp_service_account_json:
                # Legacy GCP service account format
                # Create a temporary file that persists for the session
                temp_file = tempfile.NamedTemporaryFile(
                    mode="w",
                    prefix="gcp_sa_",
                    suffix=".json",
                    delete=False,
                )
                temp_file.write(creds.gcp_service_account_json)
                temp_file.close()

                env_vars["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file.name
                self._gcp_temp_file = temp_file.name

                # Register cleanup on exit
                atexit.register(self._cleanup_gcp_temp_file_static, temp_file.name)
            else:
                raise ValueError("GCP credentials not found in response")

        else:
            raise ValueError(f"Unrecognized cloud provider: {creds.cloud_provider}")

        # Set environment variables in current process
        # Ray Data readers will inherit these environment variables
        for k, v in env_vars.items():
            os.environ[k] = v

    @staticmethod
    def _cleanup_gcp_temp_file_static(temp_file_path: str):
        """Clean up temporary GCP service account file if it exists."""
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except OSError:
                pass

    def _read_delta_with_credentials(self) -> "ray.data.Dataset":
        """
        Read Delta Lake table with Unity Catalog credentials.

        For Delta Lake tables on AWS S3, the deltalake library needs a configured
        PyArrow S3FileSystem to access tables with temporary session credentials.

        Returns:
            Ray Dataset containing the Delta table data

        Raises:
            ImportError: If deltalake or pyarrow is not installed
            ValueError: If credentials are not properly configured
            RuntimeError: If Delta table uses unsupported features
        """
        import pyarrow.fs as pafs

        creds = self._creds_response
        filesystem = None

        if creds.cloud_provider == CloudProvider.AWS:
            # Create PyArrow S3FileSystem with temporary credentials
            # https://arrow.apache.org/docs/python/generated/pyarrow.fs.S3FileSystem.html
            if creds.aws_credentials:
                filesystem = pafs.S3FileSystem(
                    access_key=creds.aws_credentials.access_key_id,
                    secret_key=creds.aws_credentials.secret_access_key,
                    session_token=creds.aws_credentials.session_token,
                    region=self.region or "us-east-1",
                )
            else:
                raise ValueError(
                    "AWS credentials not found in Unity Catalog response. "
                    "Cannot read Delta table without credentials."
                )

        elif creds.cloud_provider == CloudProvider.AZURE:
            # For Azure, the deltalake library can use environment variables
            # that were set in _set_env()
            filesystem = None

        elif creds.cloud_provider == CloudProvider.GCP:
            # For GCP, the deltalake library can use environment variables
            # that were set in _set_env()
            filesystem = None

        # Merge filesystem into reader_kwargs if not already present
        reader_kwargs = self.reader_kwargs.copy()
        if filesystem is not None and "filesystem" not in reader_kwargs:
            reader_kwargs["filesystem"] = filesystem

        # Call ray.data.read_delta with the configured filesystem
        try:
            return ray.data.read_delta(self._storage_url, **reader_kwargs)
        except Exception as e:
            # Provide helpful error messages for common Delta Lake issues
            error_msg = str(e)
            if (
                "DeletionVectors" in error_msg
                or "Unsupported reader features" in error_msg
            ):
                raise RuntimeError(
                    f"Delta table at '{self.table}' uses Deletion Vectors, which requires "
                    f"deltalake library version 0.10.0 or higher. Current error: {error_msg}\n\n"
                    f"Solutions:\n"
                    f"  1. Upgrade deltalake: pip install --upgrade deltalake>=0.10.0\n"
                    f"  2. Disable deletion vectors on the table in Databricks:\n"
                    f"     ALTER TABLE {self.table} SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false');\n"
                    f"     Then run VACUUM to apply changes.\n"
                    f"  3. Use ray.data.read_parquet() to read the underlying Parquet files directly "
                    f"(note: this bypasses Delta Lake transaction log and may include deleted records)."
                ) from e
            elif "ColumnMappingMode" in error_msg:
                raise RuntimeError(
                    f"Delta table at '{self.table}' uses column mapping, which may not be fully "
                    f"supported by your deltalake library version. Current error: {error_msg}\n\n"
                    f"Solutions:\n"
                    f"  1. Upgrade deltalake: pip install --upgrade deltalake\n"
                    f"  2. Check table properties in Databricks and disable column mapping if possible."
                ) from e
            else:
                # Re-raise other errors with additional context
                raise RuntimeError(
                    f"Failed to read Delta table at '{self.table}': {error_msg}\n\n"
                    f"This may be due to:\n"
                    f"  - Unsupported Delta Lake features\n"
                    f"  - Credential or permission issues\n"
                    f"  - Network connectivity problems\n"
                    f"  - Incompatible deltalake library version\n\n"
                    f"Try: pip install --upgrade deltalake pyarrow"
                ) from e

    def read(self) -> "ray.data.Dataset":
        """
        Read Unity Catalog Delta table into a Ray Dataset.

        This is the main entry point for reading data. It orchestrates:
        1. Fetch metadata from Unity Catalog
        2. Obtain temporary credentials via Unity Catalog credential vending
        3. Configure cloud credentials in the current process environment
        4. Read data using ray.data.read_delta()

        The credentials are set in the current process environment and will be
        inherited by Ray Data read tasks automatically.

        Returns:
            Ray Dataset containing the data from the specified Delta table

        Raises:
            ValueError: If configuration is invalid or table cannot be accessed
            requests.HTTPError: If Unity Catalog API requests fail
        """
        logger.info(f"Reading Unity Catalog Delta table: {self.table}")

        # Step 1: Get table metadata
        self._fetch_table_metadata()

        # Step 2: Get temporary credentials from Unity Catalog
        self._get_creds()

        # Step 3: Configure cloud credentials in current process environment
        self._set_env()

        # Step 4: Read Delta table with Unity Catalog credentials
        return self._read_delta_with_credentials()
