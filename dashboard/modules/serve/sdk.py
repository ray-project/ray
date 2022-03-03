from typing import Any, Dict, Optional
try:
    import aiohttp
    import requests
except ImportError:
    aiohttp = None
    requests = None

from ray.dashboard.modules.dashboard_sdk import SubmissionClient

class ServeSubmissionClient(SubmissionClient):
    def __init__(
        self,
        address: str,
        create_cluster_if_needed=False,
        cookies: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ):
        if requests is None:
            raise RuntimeError(
                "The Serve CLI requires the ray[default] "
                "installation: `pip install 'ray[default']``"
            )
        super().__init__(
            address=address,
            create_cluster_if_needed=create_cluster_if_needed,
            cookies=cookies,
            metadata=metadata,
            headers=headers,
        )
        self._check_connection_and_version(
            min_version="1.12",
            version_error_message="Serve CLI is not supported on the Ray "
            "cluster. Please ensure the cluster is "
            "running Ray 1.12 or higher.",
        )
