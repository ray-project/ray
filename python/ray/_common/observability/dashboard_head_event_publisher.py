import logging
import threading
from collections import deque
from typing import Deque, Dict, List, Optional
from urllib.parse import urlparse

import requests

from ray._private import ray_constants
from ray._private.authentication.authentication_utils import is_token_auth_enabled
from ray._private.authentication.http_token_authentication import (
    format_authentication_http_error,
    get_auth_headers_if_auth_enabled,
)
from ray._raylet import GcsClient, RayEvent, serialize_events_to_ray_events_data_json
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_S = 1
_EXTERNAL_RAY_EVENTS_PATH = "/api/v0/external/ray_events"
# Max events buffered while the dashboard is unreachable; oldest are dropped.
_MAX_BUFFERED_EVENTS = 10000


@DeveloperAPI
class DashboardHeadRayEventPublisher:
    """Publish structured RayEvents to the dashboard head HTTP API."""

    def __init__(
        self,
        gcs_client: GcsClient = None,
        dashboard_url: Optional[str] = None,
        timeout_s: float = _DEFAULT_TIMEOUT_S,
        headers: Optional[Dict[str, str]] = None,
        auth_token: Optional[str] = None,
        session: Optional[requests.Session] = None,
    ):
        if gcs_client is None and dashboard_url is None:
            raise ValueError("Either gcs_client or dashboard_url must be provided.")

        # NOTE: A long-lived publisher may outlive a
        # GCS restart, and the cached gcs_client cannot be used across one.
        # TODO: recreate the GCS client on failure once publishers need
        # to survive GCS restarts.
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
        # Events waiting to be published, e.g. emitted before the dashboard is up.
        self._pending: Deque[RayEvent] = deque(maxlen=_MAX_BUFFERED_EVENTS)
        self._lock = threading.Lock()

    def publish(self, event: RayEvent) -> None:
        self.publish_batch([event])

    def publish_batch(self, events: List[RayEvent]) -> None:
        """Publish events, buffering them while the dashboard is unreachable.

        Transient failures (connection errors, timeouts, 5xx, dashboard address
        not yet in GCS) keep events buffered for the next publish and do not
        raise. A 4xx response means the dashboard rejected the batch: it is
        dropped and the error is raised.
        """
        with self._lock:
            # TODO: buffered events should ideally never hit this
            # limit, but a long dashboard outage can overflow it and lose
            # events. add metrics to track this.
            overflow = len(self._pending) + len(events) - _MAX_BUFFERED_EVENTS
            if overflow > 0:
                logger.warning(
                    "Event buffer is full (%d events); dropping the %d oldest "
                    "buffered event(s).",
                    _MAX_BUFFERED_EVENTS,
                    overflow,
                )
            self._pending.extend(events)
            if not self._pending:
                return
            # TODO: publish in bounded batches instead of the entire
            # pending buffer in a single request.
            pending = list(self._pending)
            try:
                self._do_publish(pending)
            except requests.HTTPError as e:
                status = e.response.status_code if e.response is not None else None
                if status is not None and status < 500:
                    self._pending.clear()
                    raise
                self._warn_buffering(e)
            except (requests.RequestException, RuntimeError) as e:
                # Dashboard is not reachable (yet); retry on the next publish.
                if self._gcs_client is not None:
                    self._dashboard_url = None
                self._warn_buffering(e)
            else:
                self._pending.clear()

    def _warn_buffering(self, error: Exception) -> None:
        logger.warning(
            "Failed to publish to the dashboard, buffering %d event(s): %s",
            len(self._pending),
            error,
        )

    def _do_publish(self, events: List[RayEvent]) -> None:
        response = self._session.post(
            f"{self._get_dashboard_url()}{_EXTERNAL_RAY_EVENTS_PATH}",
            data=serialize_events_to_ray_events_data_json(events),
            headers=self._build_headers(),
            timeout=self._timeout_s,
        )
        if response.ok:
            return

        # 401/403 only mean auth failures when token auth is enabled; the
        # endpoint uses 422 for allowlist rejections.
        if is_token_auth_enabled():
            error = format_authentication_http_error(
                response.status_code, response.text
            )
            if error is not None:
                raise requests.HTTPError(error, response=response)
        response.raise_for_status()

    def _build_headers(self) -> Dict[str, str]:
        headers = dict(self._headers)
        headers.setdefault("Content-Type", "application/json")
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
        url = url.strip()
        if not url:
            return None
        # urlparse misparses "host:port" (e.g. "localhost:8265") as
        # scheme="localhost", path="8265" with no netloc. Detect the
        # absence of a real "://" separator to catch that case together
        # with the no-scheme case.
        parsed = urlparse(url)
        if not parsed.netloc and "://" not in url:
            url = f"http://{url}"
        return url.rstrip("/")
