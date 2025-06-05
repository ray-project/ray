import requests
import ray
import os
import json

class DatabricksUCCloudReader:
    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        table_full_name: str,
        data_format: str,
        region: str = None,
        operation: str = "READ",
        ray_kwargs: dict = None,
        ray_read_kwargs: dict = None
    ):
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.table_full_name = table_full_name
        self.data_format = data_format.lower()
        self.region = region
        self.operation = operation
        self.ray_kwargs = ray_kwargs or {}
        self.ray_read_kwargs = ray_read_kwargs or {}

        self._table_id = None
        self._creds_response = None
        self._table_url = None

    def _get_table_id(self):
        url = f"{self.base_url}/api/2.1/unity-catalog/tables/{self.table_full_name}"
        headers = {"Authorization": f"Bearer {self.token}"}
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        print(f"[INFO] Table info: {json.dumps(resp.json(), indent=2)}")
        self._table_id = resp.json()["table_id"]
        print(f"[INFO] Table ID: {self._table_id}")

    def _get_creds(self):
        url = f"{self.base_url}/api/2.1/unity-catalog/temporary-table-credentials"
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        payload = {"table_id": self._table_id, "operation": self.operation}
        resp = requests.post(url, json=payload, headers=headers)
        resp.raise_for_status()
        print(f"[INFO] Temporary credential response: {json.dumps(resp.json(), indent=2)}")
        self._creds_response = resp.json()
        self._table_url = self._creds_response["url"]

    def _set_env(self):
        env_vars = {}
        if "aws_temp_credentials" in self._creds_response:
            aws = self._creds_response["aws_temp_credentials"]
            env_vars["AWS_ACCESS_KEY_ID"] = aws["access_key_id"]
            env_vars["AWS_SECRET_ACCESS_KEY"] = aws["secret_access_key"]
            env_vars["AWS_SESSION_TOKEN"] = aws["session_token"]
            if self.region:
                env_vars["AWS_REGION"] = self.region
                env_vars["AWS_DEFAULT_REGION"] = self.region
        elif "azuresasuri" in self._creds_response:
            env_vars["AZURE_STORAGE_SAS_TOKEN"] = self._creds_response["azuresasuri"]
        elif "gcp_service_account" in self._creds_response:
            env_vars["GOOGLE_APPLICATION_CREDENTIALS"] = self._creds_response["gcp_service_account"]
        else:
            raise ValueError("Unknown cred type in response!")

        # Set for parent process (for Ray and IO libraries)
        for k, v in env_vars.items():
            os.environ[k] = v

        self._runtime_env = {"env_vars": env_vars}
        print(f"[INFO] Set env vars: {list(env_vars)}")

    def _ray_init(self):
        # Ray cluster may already be running.
        try:
            ray.init(ignore_reinit_error=True, runtime_env=self._runtime_env, **self.ray_kwargs)
        except Exception as e:
            print(f"[WARN] Ray may already be initialized: {e}")

    def _get_reader(self):
        if self.data_format == "delta":
            return ray.data.read_delta
        elif self.data_format == "iceberg":
            return ray.data.read_iceberg
        elif self.data_format == "parquet":
            return ray.data.read_parquet
        elif self.data_format == "csv":
            return ray.data.read_csv
        elif self.data_format == "json":
            return ray.data.read_json
        else:
            raise ValueError(f"Unknown data_format: {self.data_format}")

    def read(self):
        self._get_table_id()
        self._get_creds()
        self._set_env()
        self._ray_init()

        reader = self._get_reader()
        print(f"[INFO] Reading {self.data_format} table from {self._table_url} with Ray.")
        ds = reader(self._table_url, **self.ray_read_kwargs)
        print(f"[INFO] Dataset schema:")
        print(ds.schema())
        print(f"[INFO] First 5 rows:")
        print(ds.take(5))
        return ds