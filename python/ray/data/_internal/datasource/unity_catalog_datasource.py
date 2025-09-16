import os
import tempfile
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import requests

import ray

_FILE_FORMAT_TO_RAY_READER = {
    "delta": "read_delta",
    "parquet": "read_parquet",
}


@dataclass
class ColumnInfo:
    name: str
    type_text: str
    type_name: str
    position: int
    type_precision: int
    type_scale: int
    type_json: str
    nullable: bool


@dataclass
class EffectiveFlag:
    value: str
    inherited_from_type: str
    inherited_from_name: str


@dataclass
class TableInfo:
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


class UnityCatalogConnector:
    """
    Load a Unity Catalog table or files into a Ray Dataset, handling cloud credentials automatically.

    Currently only support Databricks-managed Unity Catalog

    Supported formats: delta, parquet.
    Supports AWS, Azure, and GCP with automatic credential handoff.
    """

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        table_full_name: str,
        region: Optional[str] = None,
        data_format: Optional[str] = "delta",
        operation: str = "READ",
        reader_kwargs: Optional[Dict] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.table_full_name = table_full_name
        self.data_format = data_format.lower() if data_format else None
        self.region = region
        self.operation = operation
        self.reader_kwargs = reader_kwargs or {}

    def _get_table_info(self) -> TableInfo:
        url = f"{self.base_url}/api/2.1/unity-catalog/tables/{self.table_full_name}"
        headers = {"Authorization": f"Bearer {self.token}"}
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        data = TableInfo.from_dict(data)
        self._table_info = data
        self._table_id = data.table_id
        return data

    def _get_creds(self):
        url = f"{self.base_url}/api/2.1/unity-catalog/temporary-table-credentials"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        payload = {"table_id": self._table_id, "operation": self.operation}
        resp = requests.post(url, json=payload, headers=headers)
        resp.raise_for_status()
        self._creds_response = resp.json()
        self._table_url = self._creds_response["url"]

    def _set_env(self):
        env_vars = {}
        creds = self._creds_response

        if "aws_temp_credentials" in creds:
            aws = creds["aws_temp_credentials"]
            env_vars["AWS_ACCESS_KEY_ID"] = aws["access_key_id"]
            env_vars["AWS_SECRET_ACCESS_KEY"] = aws["secret_access_key"]
            env_vars["AWS_SESSION_TOKEN"] = aws["session_token"]
            if self.region:
                env_vars["AWS_REGION"] = self.region
                env_vars["AWS_DEFAULT_REGION"] = self.region
        elif "azuresasuri" in creds:
            env_vars["AZURE_STORAGE_SAS_TOKEN"] = creds["azuresasuri"]
        elif "gcp_service_account" in creds:
            gcp_json = creds["gcp_service_account"]
            with tempfile.NamedTemporaryFile(
                prefix="gcp_sa_", suffix=".json", delete=True
            ) as temp_file:
                temp_file.write(gcp_json.encode())
                temp_file.flush()
                env_vars["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file.name
        else:
            raise ValueError(
                "No known credential type found in Databricks UC response."
            )

        for k, v in env_vars.items():
            os.environ[k] = v
        self._runtime_env = {"env_vars": env_vars}

    def _infer_data_format(self) -> str:
        if self.data_format:
            return self.data_format

        info = self._table_info or self._get_table_info()
        if info.data_source_format != "":
            fmt = info.data_source_format.lower()
            return fmt

        storage_loc = info.storage_location or getattr(self, "_table_url", None)
        if storage_loc:
            ext = os.path.splitext(storage_loc)[-1].replace(".", "").lower()
            if ext in _FILE_FORMAT_TO_RAY_READER:
                return ext

        raise ValueError("Could not infer data format from table metadata.")

    @staticmethod
    def _get_ray_reader(data_format: str) -> Callable[..., Any]:
        fmt = data_format.lower()
        if fmt in _FILE_FORMAT_TO_RAY_READER:
            reader_func = getattr(ray.data, _FILE_FORMAT_TO_RAY_READER[fmt], None)
            if reader_func:
                return reader_func
        raise ValueError(f"Unsupported data format: {fmt}")

    def read(self):
        self._get_table_info()
        self._get_creds()
        self._set_env()

        ray.init(ignore_reinit_error=True, runtime_env=self._runtime_env)

        data_format = self._infer_data_format()
        reader = self._get_ray_reader(data_format)

        url = self._table_url
        ds = reader(url, **self.reader_kwargs)
        return ds
