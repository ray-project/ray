import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from ray.util import metrics

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.observability.metrics.event_loop_monitoring import (
    EVENT_LOOP_LATENCY_HISTOGRAM_BOUNDARIES,
    setup_event_loop_monitoring,
)
from ray.llm._internal.serve.observability.metrics.fastapi_utils import (
    FASTAPI_API_SERVER_TAG_KEY,
    FASTAPI_BASE_HTTP_METRIC_TAG_KEYS,
    get_app_name,
)
from ray.llm._internal.serve.observability.metrics.middleware import (
    MeasureHTTPRequestMetricsMiddleware,
)
from ray.llm._internal.serve.configs.constants import ENABLE_VERBOSE_TELEMETRY

logger = get_logger(__name__)

ray_llm_build_info_gauge = metrics.Gauge(
    "ray_llm_build_info",
    description="Metadata about the ray-llm build.",
    tag_keys=("git_commit",),
)


_HTTP_HANDLER_LATENCY_S_HISTOGRAM_BUCKETS = [
    0.01,
    0.05,
    0.1,
    0.25,
    0.5,
    0.75,
    1,
    1.5,
    2,
    5,
    10,
    30,
    60,
    120,
    300,
]


async def add_fastapi_event_loop_monitoring(app: FastAPI):
    tags = {FASTAPI_API_SERVER_TAG_KEY: get_app_name(app)}
    tag_keys = tuple(tags.keys())

    # Store the task handle to prevent it from being garbage collected
    app.state.fastapi_event_loop_schedule_latency = metrics.Histogram(
        "fastapi_event_loop_schedule_latency",
        description="Latency of getting yielded control on the FastAPI event loop in seconds",
        boundaries=EVENT_LOOP_LATENCY_HISTOGRAM_BOUNDARIES,
        tag_keys=tag_keys,
    )
    app.state.fastapi_event_loop_monitoring_iterations = metrics.Counter(
        "fastapi_event_loop_monitoring_iterations",
        description="Number of times the FastAPI event loop has iterated to get anyscale_fastapi_event_loop_schedule_latency.",
        tag_keys=tag_keys,
    )
    app.state.fastapi_event_loop_monitoring_tasks = metrics.Gauge(
        "fastapi_event_loop_monitoring_tasks",
        description="Number of outstanding tasks on the FastAPI event loop.",
        tag_keys=tag_keys,
    )

    app.state.fastapi_event_loop_schedule_latency_metrics_task = (
        setup_event_loop_monitoring(
            asyncio.get_running_loop(),
            app.state.fastapi_event_loop_schedule_latency,
            app.state.fastapi_event_loop_monitoring_iterations,
            app.state.fastapi_event_loop_monitoring_tasks,
            tags,
        )
    )


def add_http_metrics_middleware(app: FastAPI):
    if not ENABLE_VERBOSE_TELEMETRY:
        logger.debug(
            "ENABLE_VERBOSE_TELEMETRY is false, not setting up FastAPI telemetry"
        )
        return

    logger.info("ENABLE_VERBOSE_TELEMETRY is true, setting up FastAPI telemetry")
    base_tag_keys = FASTAPI_BASE_HTTP_METRIC_TAG_KEYS

    logger.debug("Setting up FastAPI telemetry")

    app.state.http_requests_metrics = metrics.Counter(
        "http_requests",
        description=(
            "Total number of HTTP requests by status code, handler and method."
        ),
        tag_keys=base_tag_keys,
    )
    # NOTE: Custom decorators are not applied to histogram-based metrics
    #       to make sure we can keep cardinality of those in check
    app.state.http_requests_latency_metrics = metrics.Histogram(
        "http_request_duration_seconds",
        description="Duration in seconds of HTTP requests.",
        boundaries=_HTTP_HANDLER_LATENCY_S_HISTOGRAM_BUCKETS,
        tag_keys=base_tag_keys,
    )

    app.add_middleware(MeasureHTTPRequestMetricsMiddleware)

    logger.debug("Setting up FastAPI telemetry completed")


async def set_ray_llm_build_info():
    git_commit = os.environ.get("GIT_COMMIT")
    if git_commit:
        tags = {"git_commit": git_commit}
        ray_llm_build_info_gauge.set(1, tags)


@asynccontextmanager
async def metrics_lifespan(app: FastAPI):
    """Lifespan for a FastAPI app that sets up metrics observability."""

    if ENABLE_VERBOSE_TELEMETRY:
        await add_fastapi_event_loop_monitoring(app)

    await set_ray_llm_build_info()

    yield
