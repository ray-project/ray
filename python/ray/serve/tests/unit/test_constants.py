import os
from unittest.mock import patch

import pytest

from ray.serve._private.constants import (
    ASYNC_CONCURRENCY,
    CLIENT_CHECK_CREATION_POLLING_INTERVAL_S,
    CLIENT_POLLING_INTERVAL_S,
    CONTROL_LOOP_INTERVAL_S,
    CONTROLLER_MAX_CONCURRENCY,
    DEFAULT_AUTOSCALING_POLICY,
    DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_S,
    DEFAULT_GRACEFUL_SHUTDOWN_WAIT_LOOP_S,
    DEFAULT_GRPC_SERVER_OPTIONS,
    DEFAULT_HEALTH_CHECK_PERIOD_S,
    DEFAULT_HEALTH_CHECK_TIMEOUT_S,
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
    DEFAULT_LATENCY_BUCKET_MS,
    DEFAULT_MAX_ONGOING_REQUESTS,
    DEFAULT_REQUEST_ROUTER_PATH,
    DEFAULT_REQUEST_ROUTING_STATS_PERIOD_S,
    DEFAULT_REQUEST_ROUTING_STATS_TIMEOUT_S,
    DEFAULT_TARGET_ONGOING_REQUESTS,
    DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S,
    GRPC_CONTEXT_ARG_NAME,
    HANDLE_METRIC_PUSH_INTERVAL_S,
    HEALTH_CHECK_METHOD,
    HEALTHY_MESSAGE,
    HTTP_PROXY_TIMEOUT,
    MAX_CACHED_HANDLES,
    MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT,
    MAX_PER_REPLICA_RETRY_COUNT,
    MAX_REPLICAS_PER_NODE_MAX_VALUE,
    METRICS_PUSHER_GRACEFUL_SHUTDOWN_TIMEOUT_S,
    MIGRATION_MESSAGE,
    MODEL_LOAD_LATENCY_BUCKETS_MS,
    PROXY_DRAIN_CHECK_PERIOD_S,
    PROXY_HEALTH_CHECK_PERIOD_S,
    PROXY_HEALTH_CHECK_TIMEOUT_S,
    PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD,
    PROXY_MIN_DRAINING_PERIOD_S,
    PROXY_READY_CHECK_TIMEOUT_S,
    PUSH_MULTIPLEXED_MODEL_IDS_INTERVAL_S,
    RAY_GCS_RPC_TIMEOUT_S,
    RAY_SERVE_ALWAYS_RUN_PROXY_ON_HEAD_NODE,
    RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE,
    RAY_SERVE_CONTROLLER_CALLBACK_IMPORT_PATH,
    RAY_SERVE_ENABLE_JSON_LOGGING,
    RAY_SERVE_ENABLE_MEMORY_PROFILING,
    RAY_SERVE_ENABLE_PROXY_GC_OPTIMIZATIONS,
    RAY_SERVE_ENABLE_TASK_EVENTS,
    RAY_SERVE_FORCE_LOCAL_TESTING_MODE,
    RAY_SERVE_FORCE_STOP_UNHEALTHY_REPLICAS,
    RAY_SERVE_GRPC_MAX_MESSAGE_SIZE,
    RAY_SERVE_HANDLE_AUTOSCALING_METRIC_RECORD_PERIOD_S,
    RAY_SERVE_HIGH_PRIORITY_CUSTOM_RESOURCES,
    RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S,
    RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH,
    RAY_SERVE_KV_TIMEOUT_S,
    RAY_SERVE_LOG_ENCODING,
    RAY_SERVE_LOG_TO_STDERR,
    RAY_SERVE_MAX_QUEUE_LENGTH_RESPONSE_DEADLINE_S,
    RAY_SERVE_METRICS_EXPORT_INTERVAL_MS,
    RAY_SERVE_MIN_HANDLE_METRICS_TIMEOUT_S,
    RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S,
    RAY_SERVE_PROXY_GC_THRESHOLD,
    RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING,
    RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING,
    RAY_SERVE_QUEUE_LENGTH_CACHE_TIMEOUT_S,
    RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S,
    RAY_SERVE_REPLICA_AUTOSCALING_METRIC_RECORD_PERIOD_S,
    RAY_SERVE_REQUEST_PATH_LOG_BUFFER_SIZE,
    RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S,
    RAY_SERVE_ROUTER_RETRY_BACKOFF_MULTIPLIER,
    RAY_SERVE_ROUTER_RETRY_INITIAL_BACKOFF_S,
    RAY_SERVE_ROUTER_RETRY_MAX_BACKOFF_S,
    RAY_SERVE_RUN_SYNC_IN_THREADPOOL,
    RAY_SERVE_RUN_SYNC_IN_THREADPOOL_WARNING,
    RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD,
    RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY,
    RECOVERING_LONG_POLL_BROADCAST_TIMEOUT_S,
    REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD,
    REQUEST_LATENCY_BUCKETS_MS,
    REQUEST_ROUTING_STATS_METHOD,
    SERVE_CONTROLLER_NAME,
    SERVE_DEFAULT_APP_NAME,
    SERVE_HTTP_REQUEST_ID_HEADER,
    SERVE_LOG_APPLICATION,
    SERVE_LOG_COMPONENT,
    SERVE_LOG_COMPONENT_ID,
    SERVE_LOG_DEPLOYMENT,
    SERVE_LOG_EXTRA_FIELDS,
    SERVE_LOG_LEVEL_NAME,
    SERVE_LOG_MESSAGE,
    SERVE_LOG_RECORD_FORMAT,
    SERVE_LOG_REPLICA,
    SERVE_LOG_REQUEST_ID,
    SERVE_LOG_ROUTE,
    SERVE_LOG_TIME,
    SERVE_LOG_UNWANTED_ATTRS,
    SERVE_LOGGER_NAME,
    SERVE_MULTIPLEXED_MODEL_ID,
    SERVE_NAMESPACE,
    SERVE_PROXY_NAME,
    SERVE_ROOT_URL_ENV_KEY,
)


def test_defaults():
    assert MODEL_LOAD_LATENCY_BUCKETS_MS == DEFAULT_LATENCY_BUCKET_MS
    assert SERVE_LOGGER_NAME == "ray.serve"
    assert SERVE_CONTROLLER_NAME == "SERVE_CONTROLLER_ACTOR"
    assert SERVE_PROXY_NAME == "SERVE_PROXY_ACTOR"
    assert SERVE_NAMESPACE == "serve"
    assert DEFAULT_HTTP_HOST == "127.0.0.1"
    assert DEFAULT_HTTP_PORT == 8000
    assert DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S == 90
    assert SERVE_DEFAULT_APP_NAME == "default"
    assert ASYNC_CONCURRENCY == 1_000_000
    assert CONTROL_LOOP_INTERVAL_S == 0.1
    assert HTTP_PROXY_TIMEOUT == 60
    assert MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT == 20
    assert MAX_PER_REPLICA_RETRY_COUNT == 3
    assert REQUEST_LATENCY_BUCKETS_MS == DEFAULT_LATENCY_BUCKET_MS
    assert MODEL_LOAD_LATENCY_BUCKETS_MS == DEFAULT_LATENCY_BUCKET_MS
    assert HEALTH_CHECK_METHOD == "check_health"
    assert SERVE_ROOT_URL_ENV_KEY == "RAY_SERVE_ROOT_URL"
    assert MAX_CACHED_HANDLES == 100
    assert CONTROLLER_MAX_CONCURRENCY == 15_000
    assert DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_S == 20
    assert DEFAULT_GRACEFUL_SHUTDOWN_WAIT_LOOP_S == 2
    assert DEFAULT_HEALTH_CHECK_PERIOD_S == 10
    assert DEFAULT_HEALTH_CHECK_TIMEOUT_S == 30
    assert DEFAULT_MAX_ONGOING_REQUESTS == 5
    assert DEFAULT_TARGET_ONGOING_REQUESTS == 2
    assert PROXY_HEALTH_CHECK_TIMEOUT_S == 10.0
    assert PROXY_HEALTH_CHECK_PERIOD_S == 10.0
    assert PROXY_READY_CHECK_TIMEOUT_S == 5.0
    assert PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD == 3
    assert PROXY_MIN_DRAINING_PERIOD_S == 30.0
    assert PROXY_DRAIN_CHECK_PERIOD_S == 5
    assert REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD == 3
    assert CLIENT_POLLING_INTERVAL_S == 1.0
    assert CLIENT_CHECK_CREATION_POLLING_INTERVAL_S == 0.1
    assert HANDLE_METRIC_PUSH_INTERVAL_S == 10.0
    assert RAY_SERVE_KV_TIMEOUT_S is None
    assert RAY_GCS_RPC_TIMEOUT_S == 3.0
    assert RECOVERING_LONG_POLL_BROADCAST_TIMEOUT_S == 10.0
    assert PUSH_MULTIPLEXED_MODEL_IDS_INTERVAL_S == 0.1
    assert (
        MIGRATION_MESSAGE
        == "See https://docs.ray.io/en/latest/serve/index.html for more information."
    )
    assert RAY_SERVE_LOG_ENCODING == "TEXT"
    assert RAY_SERVE_ENABLE_JSON_LOGGING is False
    assert RAY_SERVE_LOG_TO_STDERR is True
    assert SERVE_LOG_REQUEST_ID == "request_id"
    assert SERVE_LOG_ROUTE == "route"
    assert SERVE_LOG_APPLICATION == "application"
    assert SERVE_LOG_DEPLOYMENT == "deployment"
    assert SERVE_LOG_REPLICA == "replica"
    assert SERVE_LOG_COMPONENT == "component_name"
    assert SERVE_LOG_COMPONENT_ID == "component_id"
    assert SERVE_LOG_MESSAGE == "message"
    assert SERVE_LOG_LEVEL_NAME == "levelname"
    assert SERVE_LOG_TIME == "asctime"
    assert SERVE_LOG_RECORD_FORMAT == {
        "request_id": "%(request_id)s",
        "application": "%(application)s",
        "message": "-- %(message)s",
        "levelname": "%(levelname)s",
        "asctime": "%(asctime)s",
    }
    assert SERVE_LOG_UNWANTED_ATTRS == {
        "serve_access_log",
        "task_id",
        "job_id",
    }
    assert RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S == 0
    assert RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S is None
    assert SERVE_LOG_EXTRA_FIELDS == "ray_serve_extra_fields"
    assert SERVE_MULTIPLEXED_MODEL_ID == "serve_multiplexed_model_id"
    assert SERVE_HTTP_REQUEST_ID_HEADER == "x-request-id"
    assert RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING is True
    assert RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING is True
    assert RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH is None
    assert RAY_SERVE_CONTROLLER_CALLBACK_IMPORT_PATH is None
    assert RAY_SERVE_REPLICA_AUTOSCALING_METRIC_RECORD_PERIOD_S == 0.5
    assert RAY_SERVE_HANDLE_AUTOSCALING_METRIC_RECORD_PERIOD_S == 0.5
    assert RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S == 1.0
    assert RAY_SERVE_ENABLE_MEMORY_PROFILING is False
    assert MAX_REPLICAS_PER_NODE_MAX_VALUE == 100
    assert GRPC_CONTEXT_ARG_NAME == "grpc_context"
    assert RAY_SERVE_FORCE_STOP_UNHEALTHY_REPLICAS is False
    assert RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S == 0.1
    assert RAY_SERVE_MAX_QUEUE_LENGTH_RESPONSE_DEADLINE_S == 1.0
    assert RAY_SERVE_QUEUE_LENGTH_CACHE_TIMEOUT_S == 10.0
    assert RAY_SERVE_ROUTER_RETRY_INITIAL_BACKOFF_S == 0.025
    assert RAY_SERVE_ROUTER_RETRY_BACKOFF_MULTIPLIER == 2
    assert RAY_SERVE_ROUTER_RETRY_MAX_BACKOFF_S == 0.5
    assert (
        DEFAULT_AUTOSCALING_POLICY
        == "ray.serve.autoscaling_policy:default_autoscaling_policy"
    )
    assert RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE is True
    assert RAY_SERVE_MIN_HANDLE_METRICS_TIMEOUT_S == 10.0
    assert RAY_SERVE_ALWAYS_RUN_PROXY_ON_HEAD_NODE is True
    assert RAY_SERVE_GRPC_MAX_MESSAGE_SIZE == 2_147_483_647
    assert DEFAULT_GRPC_SERVER_OPTIONS == [
        ("grpc.max_send_message_length", 2_147_483_647),
        ("grpc.max_receive_message_length", 2_147_483_647),
    ]
    assert METRICS_PUSHER_GRACEFUL_SHUTDOWN_TIMEOUT_S == 10
    assert RAY_SERVE_ENABLE_TASK_EVENTS is False
    assert RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY is False
    assert RAY_SERVE_HIGH_PRIORITY_CUSTOM_RESOURCES == []
    assert RAY_SERVE_FORCE_LOCAL_TESTING_MODE is False
    assert RAY_SERVE_RUN_SYNC_IN_THREADPOOL is False
    assert RAY_SERVE_RUN_SYNC_IN_THREADPOOL_WARNING == (
        "Calling sync method '{method_name}' directly on the "
        "asyncio loop. In a future version, sync methods will be run in a "
        "threadpool by default. Ensure your sync methods are thread safe "
        "or keep the existing behavior by making them `async def`. Opt "
        "into the new behavior by setting "
        "RAY_SERVE_RUN_SYNC_IN_THREADPOOL=1."
    )
    assert RAY_SERVE_ENABLE_PROXY_GC_OPTIMIZATIONS is True
    assert RAY_SERVE_PROXY_GC_THRESHOLD == 10_000
    assert RAY_SERVE_METRICS_EXPORT_INTERVAL_MS == 100
    assert DEFAULT_REQUEST_ROUTER_PATH == (
        "ray.serve._private.request_router:PowerOfTwoChoicesRequestRouter"
    )
    assert DEFAULT_REQUEST_ROUTING_STATS_PERIOD_S == 10
    assert DEFAULT_REQUEST_ROUTING_STATS_TIMEOUT_S == 30
    assert REQUEST_ROUTING_STATS_METHOD == "record_routing_stats"
    assert RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD is True
    assert RAY_SERVE_REQUEST_PATH_LOG_BUFFER_SIZE == 1
    assert HEALTHY_MESSAGE == "success"


def _check_constant_value(env_dict: dict[str, str], constant_name: str, expected):
    """Test that an environment variable correctly affects a constant value."""
    with patch.dict(os.environ, env_dict, clear=True):
        # Re-import to recalculate the constant
        from importlib import reload

        import ray.serve._private.constants

        reload(ray.serve._private.constants)

        actual_value = getattr(ray.serve._private.constants, constant_name)
        assert actual_value == expected


def _check_constant_default(constant_name: str, expected):
    """Test constant value without any environment variables set."""
    _check_constant_value({}, constant_name, expected)


class TestComplexConstants:
    def test_control_loop_interval(self):
        c_name = "CONTROL_LOOP_INTERVAL_S"
        env_name = f"RAY_SERVE_{c_name}"
        _check_constant_default(c_name, 0.1)
        _check_constant_value({env_name: "10"}, c_name, 10)
        _check_constant_value({env_name: "0"}, c_name, 0.0)

        with pytest.raises(AssertionError, match=".*cannot be negative.*"):
            _check_constant_value({env_name: "-1"}, c_name, None)

    def test_max_cached_handles(self):
        c_name = "MAX_CACHED_HANDLES"
        env_name = c_name
        _check_constant_default(c_name, 100)
        _check_constant_value({env_name: "10"}, c_name, 10)

        with pytest.raises(ValueError, match=".*Expected positive integer.*"):
            _check_constant_value({env_name: "0"}, c_name, None)
        with pytest.raises(ValueError, match=".*Expected positive integer.*"):
            _check_constant_value({env_name: "-1"}, c_name, None)

    def test_controller_max_concurrency(self):
        c_name = "CONTROLLER_MAX_CONCURRENCY"
        env_name = c_name
        _check_constant_default(c_name, 15_000)
        _check_constant_value({env_name: "10"}, c_name, 10)

        with pytest.raises(ValueError, match=".*Expected positive integer.*"):
            _check_constant_value({env_name: "0"}, c_name, None)
        with pytest.raises(ValueError, match=".*Expected positive integer.*"):
            _check_constant_value({env_name: "-1"}, c_name, None)

    def test_proxy_health_check_timeout(self):
        c_name = "PROXY_HEALTH_CHECK_TIMEOUT_S"
        env_name = f"RAY_SERVE_{c_name}"
        _check_constant_default(c_name, 10)
        _check_constant_value({env_name: "20"}, c_name, 20)

        _check_constant_value({env_name: "0"}, c_name, 10)
        _check_constant_value({env_name: "-1"}, c_name, -1)

    def test_proxy_health_check_period(self):
        c_name = "PROXY_HEALTH_CHECK_PERIOD_S"
        env_name = f"RAY_SERVE_{c_name}"
        _check_constant_default(c_name, 10)
        _check_constant_value({env_name: "20"}, c_name, 20)

        _check_constant_value({env_name: "0"}, c_name, 10)
        _check_constant_value({env_name: "-1"}, c_name, -1)

    def test_proxy_ready_check_timeout(self):
        c_name = "PROXY_READY_CHECK_TIMEOUT_S"
        env_name = f"RAY_SERVE_{c_name}"
        _check_constant_default(c_name, 5)
        _check_constant_value({env_name: "20"}, c_name, 20)

        _check_constant_value({env_name: "0"}, c_name, 5)
        _check_constant_value({env_name: "-1"}, c_name, -1)

    def test_proxy_min_draining_period(self):
        c_name = "PROXY_MIN_DRAINING_PERIOD_S"
        env_name = f"RAY_SERVE_{c_name}"
        _check_constant_default(c_name, 30)
        _check_constant_value({env_name: "20"}, c_name, 20)

        _check_constant_value({env_name: "0"}, c_name, 30)
        _check_constant_value({env_name: "-1"}, c_name, -1)

    def test_ray_serve_kv_timeout(self):
        c_name = "RAY_SERVE_KV_TIMEOUT_S"
        env_name = c_name
        _check_constant_default(c_name, None)
        _check_constant_value({env_name: "20"}, c_name, 20)

        _check_constant_value({env_name: "0"}, c_name, None)
        _check_constant_value({env_name: "-1"}, c_name, -1)

    def test_ray_serve_request_processing_timeout(self):
        c_name = "RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S"
        env_name = c_name
        env_name_2 = "SERVE_REQUEST_PROCESSING_TIMEOUT_S"
        _check_constant_default(c_name, None)
        _check_constant_value({env_name: "20"}, c_name, 20)
        _check_constant_value({env_name_2: "20"}, c_name, 20)
        _check_constant_value({env_name: "10", env_name_2: "20"}, c_name, 10)

        _check_constant_value({env_name: "0"}, c_name, None)
        _check_constant_value({env_name_2: "0"}, c_name, None)
        _check_constant_value({env_name: "-1"}, c_name, -1)
        _check_constant_value({env_name_2: "-1"}, c_name, -1)


class TestErrors:
    def test_cast_to_int(self):
        c_name = "DEFAULT_HTTP_PORT"
        env_name = f"RAY_SERVE_{c_name}"
        _check_constant_default(c_name, 8000)
        _check_constant_value({env_name: "10"}, c_name, 10)

        with pytest.raises(
            ValueError,
            match=".*Environment variable `RAY_SERVE_DEFAULT_HTTP_PORT` value `0.1` cannot be converted to int!*",
        ):
            _check_constant_value({env_name: "0.1"}, c_name, None)

    def test_cast_to_float(self):
        c_name = "CONTROL_LOOP_INTERVAL_S"
        env_name = f"RAY_SERVE_{c_name}"
        _check_constant_default(c_name, 0.1)
        _check_constant_value({env_name: "10"}, c_name, 10)
        _check_constant_value({env_name: "0"}, c_name, 0.0)

        with pytest.raises(
            ValueError,
            match=".*Environment variable `RAY_SERVE_CONTROL_LOOP_INTERVAL_S` value `abc` cannot be converted to float!*",
        ):
            _check_constant_value({env_name: "abc"}, c_name, None)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
