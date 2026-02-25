from typing import Any, Dict, Optional

from ray._private.utils import split_address
from ray.dashboard.modules.dashboard_sdk import SubmissionClient

try:
    import aiohttp
    import requests
except ImportError:
    aiohttp = None
    requests = None


DEPLOY_PATH = "/api/serve/applications/"
DELETE_PATH = "/api/serve/applications/"
STATUS_PATH = "/api/serve/applications/"


class ServeSubmissionClient(SubmissionClient):
    def __init__(
        self,
        dashboard_head_address: str,
        create_cluster_if_needed=False,
        cookies: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ):
        if requests is None:
            raise RuntimeError(
                "The Serve CLI requires the ray[default] "
                'installation: `pip install "ray[default]"`'
            )

        invalid_address_message = (
            "Got an unexpected address"
            f'"{dashboard_head_address}" while trying '
            "to connect to the Ray dashboard. The Serve SDK/CLI requires the "
            "Ray dashboard's HTTP(S) address (which should start with "
            '"http://" or "https://". If this address '
            "wasn't passed explicitly, it may be set in the "
            "RAY_DASHBOARD_ADDRESS environment variable."
        )

        if "://" not in dashboard_head_address:
            raise ValueError(invalid_address_message)

        module_string, _ = split_address(dashboard_head_address)

        # If user passes in ray://, raise error. Serve submission should
        # not use a Ray client address.
        if module_string not in ["http", "https"]:
            raise ValueError(invalid_address_message)

        super().__init__(
            address=dashboard_head_address,
            create_cluster_if_needed=create_cluster_if_needed,
            cookies=cookies,
            metadata=metadata,
            headers=headers,
        )
        self._check_connection_and_version_with_url(
            min_version="1.12",
            version_error_message="Serve CLI is not supported on the Ray "
            "cluster. Please ensure the cluster is "
            "running Ray 1.12 or higher.",
            url="/api/ray/version",
        )

    def get_serve_details(self) -> Dict:
        response = self._do_request("GET", STATUS_PATH)
        if response.status_code != 200:
            self._raise_error(response)

        return response.json()

    def deploy_applications(self, config: Dict):
        """Deploy multiple applications."""
        response = self._do_request("PUT", DEPLOY_PATH, json_data=config)
        if response.status_code != 200:
            self._raise_error(response)

    def delete_applications(self):
        response = self._do_request("DELETE", DELETE_PATH)
        if response.status_code != 200:
            self._raise_error(response)
