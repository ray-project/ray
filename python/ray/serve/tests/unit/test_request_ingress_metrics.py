import sys

import pytest

from ray.serve._private.request_ingress_metrics import (
    DEPLOYMENT_REQUEST_ERROR_TAG_KEYS,
    REQUEST_ERROR_TAG_KEYS,
    REQUEST_TAG_KEYS,
    RequestIngressMetrics,
)


def test_cached_tag_keys_rebuild_the_direct_tags():
    """The replica's cached buffers key tags by tuple; keys must round-trip.

    `record_ingress_request_metrics` builds these keys on the per-request path
    and `_report_cached_metrics` rebuilds the tag dict from them, so a mismatch
    between the key order and the tag-key order would silently emit metrics
    under the wrong tag keys.
    """
    route, method, application, status_code, deployment = (
        "/route",
        "Method",
        "app",
        "OK",
        "deployment",
    )

    request_key = RequestIngressMetrics.request_tags_key(
        route=route,
        method=method,
        application=application,
        status_code=status_code,
    )
    assert RequestIngressMetrics.tags_from_key(
        request_key, REQUEST_TAG_KEYS
    ) == RequestIngressMetrics.request_tags(
        route=route,
        method=method,
        application=application,
        status_code=status_code,
    )

    error_key = RequestIngressMetrics.request_error_tags_key(
        route=route,
        method=method,
        application=application,
        status_code=status_code,
    )
    assert RequestIngressMetrics.tags_from_key(
        error_key, REQUEST_ERROR_TAG_KEYS
    ) == RequestIngressMetrics.request_error_tags(
        route=route,
        method=method,
        application=application,
        status_code=status_code,
    )

    deployment_error_key = RequestIngressMetrics.deployment_error_tags_key(
        route=route,
        method=method,
        application=application,
        status_code=status_code,
        deployment=deployment,
    )
    assert RequestIngressMetrics.tags_from_key(
        deployment_error_key, DEPLOYMENT_REQUEST_ERROR_TAG_KEYS
    ) == RequestIngressMetrics.deployment_error_tags(
        route=route,
        method=method,
        application=application,
        status_code=status_code,
        deployment=deployment,
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
