from typing import Dict, List, Optional
from urllib.parse import urlparse

import requests

from ray._private import ray_constants
from ray._private.authentication.http_token_authentication import (
    format_authentication_http_error,
    get_auth_headers_if_auth_enabled,
)
from ray._private.protobuf_compat import message_to_dict
from ray._raylet import RayEvent, serialize_events_to_ray_events_data
from ray.core.generated.events_event_aggregator_service_pb2 import RayEventsData

_DEFAULT_TIMEOUT_S = 10
_EXTERNAL_RAY_EVENTS_PATH = "/api/v0/external/ray_events"


class DashboardHeadRayEventPublisher:
    """Publish structured RayEvents to the dashboard head HTTP API."""

    def __init__(
        self,
        gcs_client=None,
        dashboard_url: Optional[str] = None,
        timeout_s: float = _DEFAULT_TIMEOUT_S,
        headers: Optional[Dict[str, str]] = None,
        auth_token: Optional[str] = None,
        session: Optional[requests.Session] = None,
    ):
        if gcs_client is None and dashboard_url is None:
            raise ValueError("Either gcs_client or dashboard_url must be provided.")

        self._gcs_client = gcs_client
        self._dashboard_url = self._normalize_dashboard_url(dashboard_url)
        self._timeout_s = timeout_s
        self._headers = dict(headers or {})
        has_authorization_header = any(
            header_name.lower() == "authorization"
            for header_name in self._headers.keys()
        )
        if auth_token is not None and not has_authorization_header:
            token = auth_token
            if not auth_token.startswith("Bearer "):
                token = f"Bearer {auth_token}"
            self._headers["Authorization"] = token
        self._session = session or requests.Session()

    def publish(self, event: RayEvent) -> None:
        self.publish_batch([event])

    def publish_batch(self, events: List[RayEvent]) -> None:
        if not events:
            return

        events_data = RayEventsData()
        events_data.ParseFromString(serialize_events_to_ray_events_data(events))
        payload = [
            message_to_dict(
                event,
                always_print_fields_with_no_presence=True,
                preserving_proto_field_name=False,
                use_integers_for_enums=False,
            )
            for event in events_data.events
        ]

        response = self._session.post(
            f"{self._get_dashboard_url()}{_EXTERNAL_RAY_EVENTS_PATH}",
            json=payload,
            headers=self._build_headers(),
            timeout=self._timeout_s,
        )
        if response.ok:
            return

        error = format_authentication_http_error(response.status_code, response.text)
        if error is not None:
            raise RuntimeError(error)
        response.raise_for_status()

    def _build_headers(self) -> Dict[str, str]:
        headers = dict(self._headers)
        auth_headers = get_auth_headers_if_auth_enabled(headers)
        headers.update(auth_headers)
        return headers

    def _get_dashboard_url(self) -> str:
        if self._dashboard_url is not None:
            return self._dashboard_url

        dashboard_url = self._gcs_client.internal_kv_get(
            ray_constants.DASHBOARD_ADDRESS.encode(),
            namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
            timeout=_DEFAULT_TIMEOUT_S,
        )
        if dashboard_url is None:
            raise RuntimeError("Dashboard address not found in GCS.")

        self._dashboard_url = self._normalize_dashboard_url(dashboard_url.decode())
        return self._dashboard_url

    @staticmethod
    def _normalize_dashboard_url(url: Optional[str]) -> Optional[str]:
        if url is None:
            return None
        parsed = urlparse(url)
        if not parsed.scheme or (
            parsed.scheme and not parsed.netloc and "://" not in url
        ):
            url = f"http://{url}"
        return url.rstrip("/")
