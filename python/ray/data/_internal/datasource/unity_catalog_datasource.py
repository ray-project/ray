import atexit
import os
import tempfile
import warnings
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

import requests  # HTTP library: https://requests.readthedocs.io/

import ray
from ray.data.datasource import Datasource

# Unity Catalog REST API Documentation:
# https://docs.databricks.com/api/workspace/unity-catalog
# Credential Vending API:
# https://docs.databricks.com/en/data-governance/unity-catalog/credential-vending.html

# Mapping of file formats to Ray Data reader function names
# https://docs.ray.io/en/latest/data/api/input_output.html
_FILE_FORMAT_TO_RAY_READER = {
    "delta": "read_delta",
    "parquet": "read_parquet",
    "csv": "read_csv",
    "json": "read_json",
    "text": "read_text",
    "images": "read_images",
    "avro": "read_avro",
    "numpy": "read_numpy",
    "binary": "read_binary_files",
    "videos": "read_videos",
    "audio": "read_audio",
    "lance": "read_lance",
    "iceberg": "read_iceberg",
    "hudi": "read_hudi",
}


@dataclass
class ColumnInfo:
    """Column metadata from Unity Catalog table schema."""

    name: str
    type_text: str
    type_name: str
    position: int
    type_precision: int
    type_scale: int
    type_json: str
    nullable: bool
    # Optional fields that may not be present in all API responses
    comment: Optional[str] = None
    partition_index: Optional[int] = None
    column_masks: Optional[List[Dict]] = None


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
            columns=[ColumnInfo(**col) for col in obj.get("columns", [])],
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


@dataclass
class VolumeInfo:
    """
    Metadata for Unity Catalog volumes.

    Represents the response from the Unity Catalog volumes API.
    """

    name: str
    catalog_name: str
    schema_name: str
    volume_type: str
    storage_location: str
    full_name: str
    owner: Optional[str] = None
    comment: Optional[str] = None
    created_at: Optional[int] = None
    created_by: Optional[str] = None
    updated_at: Optional[int] = None
    updated_by: Optional[str] = None
    volume_id: Optional[str] = None

    @staticmethod
    def from_dict(obj: Dict) -> "VolumeInfo":
        return VolumeInfo(
            name=obj["name"],
            catalog_name=obj["catalog_name"],
            schema_name=obj["schema_name"],
            volume_type=obj.get("volume_type", ""),
            storage_location=obj.get("storage_location", ""),
            full_name=obj.get("full_name", ""),
            owner=obj.get("owner"),
            comment=obj.get("comment"),
            created_at=obj.get("created_at"),
            created_by=obj.get("created_by"),
            updated_at=obj.get("updated_at"),
            updated_by=obj.get("updated_by"),
            volume_id=obj.get("volume_id"),
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
    # Legacy field for older Azure format
    azure_sas_uri: Optional[str] = None
    # Legacy field for older GCP format
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


class UnityCatalogConnector:
    """
    Connector for reading Unity Catalog tables and volumes into Ray Datasets.

    This connector handles automatic credential vending for secure access to cloud storage
    backing Unity Catalog tables and volumes. It supports AWS S3, Azure Data Lake Storage,
    and Google Cloud Storage with temporary, least-privilege credentials.

    Tables Support Status: PRODUCTION READY
        - Fully supported and documented
        - Credential vending API is stable and publicly available
        - Works across all Databricks workspaces

    Volumes Support Status: PRIVATE PREVIEW - LIMITED AVAILABILITY
        - API endpoint exists but may not be enabled in all workspaces
        - Credential vending may return errors even with correct permissions
        - Contact Databricks support if you encounter credential vending errors
        - Workaround: Use ray.data.read_*() with your own cloud credentials

    Supported Unity Catalog paths:
        - Tables: catalog.schema.table
        - Volumes: catalog.schema.volume/path/to/data (private preview)

    Supported data formats:
        delta, parquet, csv, json, text, images, avro, numpy, binary, videos,
        audio, lance, iceberg, hudi, and custom datasources

    Cloud providers:
        - AWS S3 (with temporary IAM credentials)
        - Azure Data Lake Storage (with SAS tokens)
        - Google Cloud Storage (with OAuth tokens or service account)

    Args:
        base_url: Databricks workspace URL (e.g., "https://dbc-xxx.cloud.databricks.com")
        token: Databricks Personal Access Token with appropriate permissions
        path: Unity Catalog path (table: "catalog.schema.table" or
            volume: "catalog.schema.volume/subpath")
        region: Optional AWS region for S3 credential configuration
        data_format: Optional format override (delta, parquet, images, etc.)
        custom_datasource: Optional custom Ray Data Datasource class
        operation: Credential operation type (default: "READ")
        reader_kwargs: Additional arguments passed to the Ray Data reader

    References:
        - Credential Vending: https://docs.databricks.com/en/data-governance/unity-catalog/credential-vending.html
        - Unity Catalog: https://docs.databricks.com/en/data-governance/unity-catalog/
    """

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        path: str,
        region: Optional[str] = None,
        data_format: Optional[str] = None,
        custom_datasource: Optional[Type[Datasource]] = None,
        operation: str = "READ",
        reader_kwargs: Optional[Dict] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.path = path
        self.data_format = data_format.lower() if data_format else None
        self.custom_datasource = custom_datasource
        self.region = region
        self.operation = operation
        self.reader_kwargs = reader_kwargs or {}

        # Determine if this is a table or volume path
        self._is_volume = self._detect_volume_path(path)

        # Warn about volumes private preview status and known issues
        if self._is_volume:
            warnings.warn(
                f"\n{'='*70}\n"
                f"Unity Catalog Volumes: PRIVATE PREVIEW Feature\n"
                f"{'='*70}\n"
                f"Path: {path}\n\n"
                f"Volumes support is in PRIVATE PREVIEW and may not be fully enabled\n"
                f"in your Databricks workspace. The credential vending API for volumes\n"
                f"is not yet publicly documented and may not be accessible.\n\n"
                f"KNOWN ISSUES:\n"
                f"  - Credential vending may fail with '400 Bad Request: Missing\n"
                f"    required field: operation' even though the operation field is\n"
                f"    provided. This indicates the API exists but isn't fully enabled.\n"
                f"  - The API endpoint may not be available in all workspaces.\n\n"
                f"REQUIREMENTS:\n"
                f"  1. Workspace must have volumes private preview enabled\n"
                f"  2. You must have READ VOLUME permission on the volume\n"
                f"  3. You must have EXTERNAL USE SCHEMA permission\n"
                f"  4. The volume must exist and be accessible\n\n"
                f"IF YOU ENCOUNTER ERRORS:\n"
                f"  - Contact Databricks support to verify volumes are enabled\n"
                f"  - Ask about enabling volumes credential vending for your workspace\n"
                f"  - Request documentation for the private preview API\n\n"
                f"WORKAROUND:\n"
                f"  Use ray.data.read_*() with your own cloud credentials to access\n"
                f"  the underlying storage directly. You can get the storage location\n"
                f"  from the Unity Catalog volumes metadata API:\n"
                f"    GET /api/2.1/unity-catalog/volumes/{{volume_full_name}}\n"
                f"{'='*70}",
                UserWarning,
                stacklevel=4,
            )

        # Storage for metadata and credentials
        self._table_info: Optional[TableInfo] = None
        self._volume_info: Optional[VolumeInfo] = None
        self._table_id: Optional[str] = None
        self._volume_path: Optional[str] = None
        self._creds_response: Optional[CredentialsResponse] = None
        self._storage_url: Optional[str] = None
        self._gcp_temp_file: Optional[str] = None

    @staticmethod
    def _detect_volume_path(path: str) -> bool:
        """
        Detect if the path refers to a volume or table.

        Unity Catalog paths follow these patterns:
        - Table: catalog.schema.table (3 parts separated by dots)
        - Volume: catalog.schema.volume/path/to/data (contains forward slash)
        - Volume: /Volumes/catalog/schema/volume/path (starts with /Volumes/)

        Args:
            path: Unity Catalog path

        Returns:
            True if path refers to a volume, False for table
        """
        # Check for /Volumes/ prefix (common pattern)
        if path.startswith("/Volumes/"):
            return True

        # Check if path contains a forward slash (indicating volume with subdirectory)
        if "/" in path:
            return True

        # Count the number of dot-separated parts
        parts = path.split(".")
        # If more than 3 parts, it's likely a volume (e.g., catalog.schema.volume.something)
        # However, standard format is catalog.schema.volume/path, so this is a fallback
        return len(parts) > 3

    def _parse_volume_path(self) -> Tuple[str, str]:
        """
        Parse volume path into volume identifier and subdirectory path.

        Handles multiple volume path formats:
        - catalog.schema.volume/path/to/data
        - /Volumes/catalog/schema/volume/path/to/data

        Returns:
            Tuple of (volume_full_name, sub_path)
            Examples:
                - ("catalog.schema.volume", "path/to/data")
                - ("catalog.schema.volume", "")
        """
        path = self.path

        # Handle /Volumes/ prefix
        if path.startswith("/Volumes/"):
            # Remove /Volumes/ prefix
            path = path[9:]  # len("/Volumes/") = 9

        if "/" in path:
            parts = path.split("/", 1)
            volume_full_name = parts[0].replace("/", ".")
            sub_path = parts[1] if len(parts) > 1 else ""
            return volume_full_name, sub_path
        else:
            # No subdirectory specified
            return path, ""

    def _get_table_info(self) -> Optional[TableInfo]:
        """
        Fetch table or volume metadata from Unity Catalog API.

        API Endpoints:
            Tables: GET /api/2.1/unity-catalog/tables/{full_name}
            Volumes: GET /api/2.1/unity-catalog/volumes/{full_name}

        Returns:
            TableInfo object for tables, None for volumes

        Raises:
            requests.HTTPError: If API request fails
        """
        if self._is_volume:
            # For volumes, use the volumes API to get metadata
            volume_full_name, _ = self._parse_volume_path()
            url = f"{self.base_url}/api/2.1/unity-catalog/volumes/{volume_full_name}"
            headers = {"Authorization": f"Bearer {self.token}"}
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json()

            # Parse and store volume information
            volume_info = VolumeInfo.from_dict(data)
            self._volume_info = volume_info
            self._volume_path = volume_info.storage_location
            # Volumes don't have table_id, use volume_full_name for credential requests
            return None
        else:
            # For tables, use the tables API
            url = f"{self.base_url}/api/2.1/unity-catalog/tables/{self.path}"
            headers = {"Authorization": f"Bearer {self.token}"}
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json()

            # Parse table information
            table_info = TableInfo.from_dict(data)
            self._table_info = table_info
            self._table_id = table_info.table_id
            return table_info

    def _get_creds(self):
        """
        Request temporary credentials from Unity Catalog credential vending API.

        Handles both table and volume credential requests with different support levels.

        Table Credentials (PRODUCTION READY):
            - Endpoint: /api/2.1/unity-catalog/temporary-table-credentials
            - Status: Public API, fully documented and supported
            - Reliability: Stable and available in all workspaces

        Volume Credentials (PRIVATE PREVIEW - LIMITED AVAILABILITY):
            - Endpoint: /api/2.1/unity-catalog/temporary-volume-credentials
            - Status: Private preview, not publicly documented
            - Known Issue: May return 400 "Missing required field: operation"
              even when operation field is provided, indicating the API exists
              but isn't fully enabled in the workspace

        Raises:
            requests.HTTPError: If API request fails
            ValueError: If credential vending is not available or configured
        """
        if self._is_volume:
            # Use volumes credential vending API (PRIVATE PREVIEW)
            volume_full_name, sub_path = self._parse_volume_path()
            url = f"{self.base_url}/api/2.1/unity-catalog/temporary-volume-credentials"
            payload = {
                "volume_id": volume_full_name,
                "operation": self.operation,
            }
        else:
            # Use table credential vending API (PUBLIC/PRODUCTION)
            if not self._table_id:
                raise ValueError(
                    "Table ID not available. Call _get_table_info() first."
                )
            url = f"{self.base_url}/api/2.1/unity-catalog/temporary-table-credentials"
            payload = {
                "table_id": self._table_id,
                "operation": self.operation,
            }

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

        try:
            resp = requests.post(url, json=payload, headers=headers)
            resp.raise_for_status()
            # Parse credentials response into structured dataclass
            self._creds_response = CredentialsResponse.from_dict(resp.json())
        except requests.HTTPError as e:
            if self._is_volume:
                # Provide detailed error messages for volumes based on testing findings
                error_msg = e.response.text if hasattr(e.response, "text") else str(e)

                if e.response.status_code == 404:
                    raise ValueError(
                        f"\n{'='*70}\n"
                        f"Unity Catalog Volumes Credential Vending API Not Found\n"
                        f"{'='*70}\n"
                        f"Volume: {self.path}\n"
                        f"API Endpoint: {url}\n"
                        f"Status: 404 Not Found\n\n"
                        f"The volumes credential vending API doesn't exist in your\n"
                        f"Databricks workspace.\n\n"
                        f"POSSIBLE CAUSES:\n"
                        f"  1. Volumes private preview is not enabled\n"
                        f"  2. Your Databricks runtime doesn't support volumes\n"
                        f"  3. The API endpoint has changed (volumes in private preview)\n\n"
                        f"NEXT STEPS:\n"
                        f"  - Contact Databricks support to enable volumes private preview\n"
                        f"  - Verify your workspace supports Unity Catalog volumes\n\n"
                        f"WORKAROUND:\n"
                        f"  Access storage directly using your own cloud credentials\n"
                        f"  with ray.data.read_*() functions.\n"
                        f"{'='*70}\n"
                        f"Original error: {e}"
                    ) from e

                elif e.response.status_code == 400:
                    # Check for the specific "Missing required field: operation" error
                    if (
                        "Missing required field" in error_msg
                        or "operation" in error_msg.lower()
                    ):
                        raise ValueError(
                            f"\n{'='*70}\n"
                            f"Unity Catalog Volumes Credential Vending Not Available\n"
                            f"{'='*70}\n"
                            f"Volume: {self.path}\n"
                            f"API Endpoint: {url}\n"
                            f"Status: 400 Bad Request\n"
                            f"Error: {error_msg}\n\n"
                            f"WHAT HAPPENED:\n"
                            f"  The volumes credential vending API endpoint exists but is\n"
                            f"  rejecting requests. This is a known issue with volumes in\n"
                            f"  private preview.\n\n"
                            f"WHAT WE TRIED:\n"
                            f"  - Requested temporary READ credentials\n"
                            f"  - Payload: {payload}\n"
                            f"  - Response: '{error_msg}'\n\n"
                            f"WHAT THIS MEANS:\n"
                            f"  1. API endpoint exists but isn't fully enabled in your\n"
                            f"     workspace (requires additional configuration)\n"
                            f"  2. Your workspace may need explicit enablement\n"
                            f"  3. Additional feature flags may be required\n\n"
                            f"RECOMMENDED ACTIONS:\n"
                            f"  1. Contact Databricks support with this error message\n"
                            f"  2. Ask them to:\n"
                            f"     - Verify volumes credential vending is enabled\n"
                            f"     - Provide correct API payload format\n"
                            f"     - Enable required feature flags\n"
                            f"  3. Request access to private preview documentation\n\n"
                            f"WORKAROUND:\n"
                            f"  Use ray.data.read_*() with your own cloud credentials:\n"
                            f"  1. Get storage location from volumes metadata API:\n"
                            f"       GET /api/2.1/unity-catalog/volumes/{self._parse_volume_path()[0]}\n"
                            f"  2. Use returned 'storage_location' with ray.data.read_*()\n"
                            f"  3. Provide your own AWS/Azure/GCP credentials\n"
                            f"{'='*70}"
                        ) from e
                    else:
                        raise ValueError(
                            f"Invalid request for volume '{self.path}': {error_msg}\n"
                            f"Status: {e.response.status_code}\n"
                            f"Ensure you have READ VOLUME and EXTERNAL USE SCHEMA permissions."
                        ) from e

                elif e.response.status_code == 403:
                    raise ValueError(
                        f"\n{'='*70}\n"
                        f"Permission Denied for Unity Catalog Volume\n"
                        f"{'='*70}\n"
                        f"Volume: {self.path}\n"
                        f"Status: 403 Forbidden\n"
                        f"Error: {error_msg}\n\n"
                        f"You don't have the required permissions.\n\n"
                        f"REQUIRED PERMISSIONS:\n"
                        f"  - READ VOLUME on '{self.path}'\n"
                        f"  - EXTERNAL USE SCHEMA on the parent schema\n"
                        f"  - USE CATALOG on the parent catalog\n\n"
                        f"TO GRANT PERMISSIONS:\n"
                        f"  A Databricks admin can run:\n"
                        f"    GRANT READ VOLUME ON VOLUME {self.path} TO `your-user-or-group`;\n"
                        f"{'='*70}"
                    ) from e
            # Re-raise for non-volume errors or unhandled status codes
            raise

        # Extract storage URL from credentials response
        if self._is_volume:
            # For volumes, construct full path with subdirectory
            volume_full_name, sub_path = self._parse_volume_path()
            base_url = self._creds_response.url
            if not base_url:
                raise ValueError(
                    f"No storage URL returned for volume '{volume_full_name}'. "
                    f"Credentials response: {self._creds_response}"
                )
            # Construct full path, handling trailing/leading slashes
            if sub_path:
                self._storage_url = f"{base_url.rstrip('/')}/{sub_path.lstrip('/')}"
            else:
                self._storage_url = base_url
        else:
            # For tables, use URL directly from response
            if not self._creds_response.url:
                raise ValueError(
                    f"No storage URL returned for table '{self.path}'. "
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
              (IAM temporary credentials)
            - Azure: Sets AZURE_STORAGE_SAS_TOKEN
              (Shared Access Signature token)
            - GCP: Sets GCP_OAUTH_TOKEN or GOOGLE_APPLICATION_CREDENTIALS
              (OAuth token or service account credentials)

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
                env_vars[
                    "AWS_SECRET_ACCESS_KEY"
                ] = creds.aws_credentials.secret_access_key
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
                    delete=False,  # Don't delete immediately
                )
                temp_file.write(creds.gcp_service_account_json)
                temp_file.close()

                env_vars["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file.name
                self._gcp_temp_file = temp_file.name

                # Register cleanup on exit
                atexit.register(lambda: self._cleanup_gcp_temp_file())
            else:
                raise ValueError("GCP credentials not found in response")

        else:
            raise ValueError(f"Unrecognized cloud provider: {creds.cloud_provider}")

        # Set environment variables in current process
        # Ray Data readers will inherit these environment variables
        for k, v in env_vars.items():
            os.environ[k] = v

    def _infer_data_format(self) -> str:
        """
        Infer data format from metadata, explicit specification, or file extensions.

        Priority order:
        1. Explicitly specified format (self.data_format)
        2. Table data_source_format from metadata (for tables)
        3. File extension from storage location URL

        Returns:
            Inferred format string (e.g., "delta", "parquet", "csv")

        Raises:
            ValueError: If format cannot be inferred
        """
        # Use explicit format if provided
        if self.data_format:
            return self.data_format

        # For tables, try to get format from metadata
        if not self._is_volume and self._table_info:
            if self._table_info.data_source_format:
                return self._table_info.data_source_format.lower()

        # Try to infer from storage URL file extension
        if self._storage_url:
            # Extract extension from URL (handle query parameters)
            url_path = self._storage_url.split("?")[0]
            ext = os.path.splitext(url_path)[-1].replace(".", "").lower()
            if ext and ext in _FILE_FORMAT_TO_RAY_READER:
                return ext

        # Default to parquet for volumes if no extension found
        if self._is_volume:
            return "parquet"

        raise ValueError(
            f"Could not infer data format for path '{self.path}'. "
            f"Please specify the format explicitly using the 'data_format' parameter. "
            f"Supported formats: {', '.join(_FILE_FORMAT_TO_RAY_READER.keys())}"
        )

    @staticmethod
    def _get_ray_reader(data_format: str) -> Callable[..., "ray.data.Dataset"]:
        """
        Get the appropriate Ray Data reader function for the specified format.

        Args:
            data_format: Format name (e.g., "delta", "parquet", "images")

        Returns:
            Ray Data reader function (e.g., ray.data.read_parquet)

        Raises:
            ValueError: If format is not supported by Ray Data
        """
        fmt = data_format.lower()

        # Check if format is in the standard mapping
        if fmt in _FILE_FORMAT_TO_RAY_READER:
            reader_name = _FILE_FORMAT_TO_RAY_READER[fmt]
            reader_func = getattr(ray.data, reader_name, None)
            if reader_func:
                return reader_func

        raise ValueError(
            f"Unsupported data format: '{fmt}'. "
            f"Supported formats: {', '.join(sorted(_FILE_FORMAT_TO_RAY_READER.keys()))}"
        )

    def _cleanup_gcp_temp_file(self):
        """Clean up temporary GCP service account file if it exists."""
        if self._gcp_temp_file and os.path.exists(self._gcp_temp_file):
            try:
                os.unlink(self._gcp_temp_file)
            except OSError:
                # File already deleted or inaccessible, ignore
                pass

    def read(self) -> "ray.data.Dataset":
        """
        Read data from Unity Catalog table or volume into a Ray Dataset.

        This is the main entry point for reading data. It orchestrates:
        1. Fetch metadata from Unity Catalog (for tables) or validate volume access
        2. Obtain temporary credentials via Unity Catalog credential vending
        3. Configure cloud credentials in the current process environment
        4. Read data using the appropriate Ray Data reader or custom datasource

        The credentials are set in the current process environment and will be
        inherited by Ray Data read tasks automatically.

        Returns:
            Ray Dataset containing the data from the specified table or volume

        Raises:
            ValueError: If configuration is invalid, path cannot be accessed, or
                format cannot be inferred
            requests.HTTPError: If Unity Catalog API requests fail
        """
        # Step 1: Get metadata (for both tables and volumes)
        self._get_table_info()

        # Step 2: Get temporary credentials from Unity Catalog
        # This may fail for volumes - see detailed error messages in _get_creds()
        self._get_creds()

        # Step 3: Configure cloud credentials in current process environment
        # Ray Data readers will inherit these environment variables
        self._set_env()

        # Step 4: Read data using custom datasource or standard Ray Data reader
        if self.custom_datasource is not None:
            # Use custom datasource if provided
            datasource_instance = self.custom_datasource(
                self._storage_url, **self.reader_kwargs
            )
            return ray.data.read_datasource(datasource_instance)
        else:
            # Use standard Ray Data reader based on inferred or specified format
            data_format = self._infer_data_format()
            reader = self._get_ray_reader(data_format)
            return reader(self._storage_url, **self.reader_kwargs)
