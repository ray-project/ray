from typing import Dict

from ray.serve._private.common import RequestProtocol
from ray.serve._private.constants import REQUEST_LATENCY_BUCKETS_MS
from ray.util import metrics


class ProxyMetrics:
    """E2E request metrics shared by the proxies and direct-ingress replicas.

    Defines and emits the standard ``serve_num_{protocol}_requests`` family of
    metrics. Both the proxy (which sees all proxy-routed traffic) and a
    direct-ingress replica (which sees traffic that bypasses the proxy) record
    them, so the series are disjoint at runtime.
    """

    def __init__(
        self,
        protocol: RequestProtocol,
        *,
        source: str,
        node_id: str,
        node_ip_address: str,
    ):
        """Create the metric objects.

        Args:
            protocol: Request protocol these metrics describe (e.g. HTTP).
            source: Human-readable origin of the metrics, "proxy" or "ingress".
                Only affects the metric descriptions so that they remain
                accurate for each call site.
            node_id: Default tag value for the ongoing-requests gauge.
            node_ip_address: Default tag value for the ongoing-requests gauge.
        """
        protocol_name = protocol.lower()

        self.request_counter = metrics.Counter(
            f"serve_num_{protocol_name}_requests",
            description=f"The number of {protocol} requests processed.",
            tag_keys=("route", "method", "application", "status_code"),
        )

        self.request_error_counter = metrics.Counter(
            f"serve_num_{protocol_name}_error_requests",
            description=f"The number of errored {protocol} responses.",
            tag_keys=(
                "route",
                "error_code",
                "method",
                "application",
            ),
        )

        self.deployment_request_error_counter = metrics.Counter(
            f"serve_num_deployment_{protocol_name}_error_requests",
            description=(
                f"The number of errored {protocol} "
                "responses returned by each deployment."
            ),
            tag_keys=(
                "deployment",
                "error_code",
                "method",
                "route",
                "application",
            ),
        )

        self.processing_latency_tracker = metrics.Histogram(
            f"serve_{protocol_name}_request_latency_ms",
            description=(
                f"The end-to-end latency of {protocol} requests "
                f"(measured from the Serve {protocol} {source})."
            ),
            boundaries=REQUEST_LATENCY_BUCKETS_MS,
            tag_keys=(
                "method",
                "route",
                "application",
                "status_code",
            ),
        )

        self.num_ongoing_requests_gauge = metrics.Gauge(
            name=f"serve_num_ongoing_{protocol_name}_requests",
            description=(
                f"The number of ongoing requests in this {protocol} {source}."
            ),
            tag_keys=("node_id", "node_ip_address"),
        ).set_default_tags(
            {
                "node_id": node_id,
                "node_ip_address": node_ip_address,
            }
        )

    @staticmethod
    def request_tags(
        *, route: str, method: str, application: str, status_code: str
    ) -> Dict[str, str]:
        """Tags for the request counter and processing-latency tracker."""
        return {
            "route": route,
            "method": method,
            "application": application,
            "status_code": status_code,
        }

    @staticmethod
    def request_error_tags(
        *, route: str, method: str, application: str, status_code: str
    ) -> Dict[str, str]:
        """Tags for the request error counter."""
        return {
            "route": route,
            "method": method,
            "application": application,
            "error_code": status_code,
        }

    @staticmethod
    def deployment_error_tags(
        *,
        route: str,
        method: str,
        application: str,
        status_code: str,
        deployment: str,
    ) -> Dict[str, str]:
        """Tags for the per-deployment request error counter."""
        return {
            "route": route,
            "method": method,
            "application": application,
            "error_code": status_code,
            "deployment": deployment,
        }

    def record_request(
        self,
        *,
        route: str,
        method: str,
        application: str,
        status_code: str,
        latency_ms: float,
        is_error: bool,
        deployment_name: str,
    ):
        """Emit the per-request metrics directly (no batching)."""
        request_tags = self.request_tags(
            route=route,
            method=method,
            application=application,
            status_code=status_code,
        )
        self.request_counter.inc(tags=request_tags)
        self.processing_latency_tracker.observe(latency_ms, tags=request_tags)
        if is_error:
            self.request_error_counter.inc(
                tags=self.request_error_tags(
                    route=route,
                    method=method,
                    application=application,
                    status_code=status_code,
                )
            )
            self.deployment_request_error_counter.inc(
                tags=self.deployment_error_tags(
                    route=route,
                    method=method,
                    application=application,
                    status_code=status_code,
                    deployment=deployment_name,
                )
            )

    def set_num_ongoing_requests(self, num_ongoing_requests: int):
        """Set the ongoing-requests gauge."""
        self.num_ongoing_requests_gauge.set(num_ongoing_requests)
