import functools
import json
import logging
import math
import os
import threading
import time
import urllib.parse
import urllib.request
import weakref
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple, Union

from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import (
    SERVE_AUTOSCALING_DECISION_COUNTERS_KEY,
    SERVE_AUTOSCALING_DECISION_TIMESTAMP_KEY,
    SERVE_LOGGER_NAME,
)
from ray.serve.config import AutoscalingConfig, AutoscalingContext
from ray.util.annotations import PublicAPI

logger = logging.getLogger(SERVE_LOGGER_NAME)

# Tolerance for delay elapsed-time comparisons.  Subtracting two large
# time.time() values (or test fake clocks derived from tick counters) can
# drift slightly below the true elapsed interval in IEEE 754 (e.g. 400.0s
# configured delay may compare as 399.9999999999999 >= 400.0).
_DELAY_ELAPSED_EPS_S = 1e-6


def _apply_scaling_factors(
    desired_num_replicas: Union[int, float],
    current_num_replicas: int,
    autoscaling_config: AutoscalingConfig,
) -> int:
    """Apply scaling factors to the desired number of replicas.
    Returns the scaled number of replicas depending on the scaling factor.
    The computation uses the difference between desired and current to scale.

    """
    # When scaling from zero, the scaling factor is not meaningful: the
    # entire desired count would be treated as the delta and amplified,
    # creating a feedback loop that compounds every control-loop tick.
    # Return the raw desired value and let bounds handle the rest.
    if current_num_replicas == 0:
        return math.ceil(desired_num_replicas)

    replicas_delta = desired_num_replicas - current_num_replicas
    scaling_factor = (
        autoscaling_config.get_upscaling_factor()
        if replicas_delta > 0
        else autoscaling_config.get_downscaling_factor()
    )
    scaled_num_replicas = math.ceil(
        current_num_replicas + scaling_factor * replicas_delta
    )
    # If the scaled_replicas are stuck during downscaling because of scaling factor, decrement by 1.
    if (
        math.ceil(float(desired_num_replicas)) < current_num_replicas
        and scaled_num_replicas == current_num_replicas
    ):
        scaled_num_replicas -= 1
    return scaled_num_replicas


def _apply_delay_logic(
    desired_num_replicas: int,
    curr_target_num_replicas: int,
    config: AutoscalingConfig,
    policy_state: Dict[str, Any],
    _now: Optional[float] = None,
) -> Tuple[int, Dict[str, Any]]:

    """Apply delay logic to the desired number of replicas.

    Uses wall-clock timestamps to measure delay instead of counting iterations,
    so the effective delay matches the configured delay_s regardless of how long
    each control loop iteration takes.
    """
    now = _now if _now is not None else time.time()
    decision_num_replicas = curr_target_num_replicas
    # decision_counter encodes direction: >0 means upscale, <0 means downscale.
    # We keep it for backward-compatible state transitions but the actual delay
    # check uses the timestamp.
    decision_counter = policy_state.get(SERVE_AUTOSCALING_DECISION_COUNTERS_KEY, 0)
    decision_timestamp = policy_state.get(
        SERVE_AUTOSCALING_DECISION_TIMESTAMP_KEY, None
    )

    # Scale up.
    if desired_num_replicas > curr_target_num_replicas:
        # If the previous decision was to scale down, reset.
        if decision_counter < 0:
            decision_counter = 0
            decision_timestamp = None
        decision_counter += 1

        # Record the timestamp when we first start wanting to scale up.
        if decision_timestamp is None:
            decision_timestamp = now

        # Only actually scale the replicas if enough wall-clock time has
        # elapsed since the first consecutive scale-up decision.
        if now - decision_timestamp + _DELAY_ELAPSED_EPS_S >= config.upscale_delay_s:
            decision_counter = 0
            decision_timestamp = None
            decision_num_replicas = desired_num_replicas

    # Scale down.
    elif desired_num_replicas < curr_target_num_replicas:
        # If the previous decision was to scale up, reset.
        if decision_counter > 0:
            decision_counter = 0
            decision_timestamp = None
        decision_counter -= 1

        # Downscaling to zero is only allowed from 1 -> 0
        is_scaling_to_zero = curr_target_num_replicas == 1
        # Determine the delay to use
        if is_scaling_to_zero:
            if config.downscale_to_zero_delay_s is not None:
                delay_s = config.downscale_to_zero_delay_s
            else:
                delay_s = config.downscale_delay_s
        else:
            delay_s = config.downscale_delay_s
            # The desired_num_replicas>0 for downscaling cases other than 1->0
            desired_num_replicas = max(1, desired_num_replicas)

        # Record the timestamp when we first start wanting to scale down.
        if decision_timestamp is None:
            decision_timestamp = now

        # Only actually scale the replicas if enough wall-clock time has
        # elapsed since the first consecutive scale-down decision.
        if now - decision_timestamp + _DELAY_ELAPSED_EPS_S >= delay_s:
            decision_counter = 0
            decision_timestamp = None
            decision_num_replicas = desired_num_replicas

    # Do nothing.
    else:
        decision_counter = 0
        decision_timestamp = None

    policy_state[SERVE_AUTOSCALING_DECISION_COUNTERS_KEY] = decision_counter
    policy_state[SERVE_AUTOSCALING_DECISION_TIMESTAMP_KEY] = decision_timestamp
    return decision_num_replicas, policy_state


def _apply_default_params(
    desired_num_replicas: Union[int, float],
    ctx: AutoscalingContext,
    policy_state: Dict[str, Any],
) -> Tuple[int, Dict[str, Any]]:
    """Apply the default parameters to the desired number of replicas."""

    desired_num_replicas = _apply_scaling_factors(
        desired_num_replicas, ctx.current_num_replicas, ctx.config
    )

    # If curr num replicas is 0 and the policy wants to scale up (e.g. based on internal
    # signals like queue length), bypass the delay logic for immediate scale-up.
    if ctx.current_num_replicas == 0 and desired_num_replicas > 0:
        return desired_num_replicas, policy_state

    # Apply delay logic
    # Only send the internal state here to avoid overwriting the custom policy state.
    final_num_replicas, updated_state = _apply_delay_logic(
        max(0, desired_num_replicas), ctx.target_num_replicas, ctx.config, policy_state
    )

    return final_num_replicas, updated_state


def _extract_internal_policy_state(policy_state: Dict[str, Any]) -> Dict[str, Any]:
    """Extract the internal states from a policy state dict."""
    return {
        SERVE_AUTOSCALING_DECISION_COUNTERS_KEY: policy_state.get(
            SERVE_AUTOSCALING_DECISION_COUNTERS_KEY, 0
        ),
        SERVE_AUTOSCALING_DECISION_TIMESTAMP_KEY: policy_state.get(
            SERVE_AUTOSCALING_DECISION_TIMESTAMP_KEY, None
        ),
    }


def _apply_default_params_and_merge_state(
    policy_state: Dict[str, Any],
    user_policy_state: Dict[str, Any],
    desired_num_replicas: Union[int, float],
    ctx: AutoscalingContext,
) -> Tuple[int, Dict[str, Any]]:

    internal_policy_state = _extract_internal_policy_state(policy_state)
    # Only pass the internal state used for delay counters so we don't
    # overwrite any custom user state.
    final_num_replicas, updated_state = _apply_default_params(
        desired_num_replicas, ctx, internal_policy_state
    )
    # Merge internal updated_state with the user's custom policy state.
    if updated_state:
        user_policy_state.update(updated_state)
    return final_num_replicas, user_policy_state


def _merge_user_state_with_internal_state(
    policy_state: Dict[str, Any],
    user_policy_state: Dict[str, Any],
) -> Dict[str, Any]:
    """Merge user state with previous policy state, preserving internal keys.

    This mutates and returns `user_policy_state`.
    """
    internal_policy_state = _extract_internal_policy_state(policy_state)
    user_policy_state.update(internal_policy_state)
    return user_policy_state


def _get_cold_start_scale_up_replicas(ctx: AutoscalingContext) -> Optional[int]:
    """
    Returns the desired number of replicas if the cold start fast path applies, otherwise returns None.
    """
    if ctx.current_num_replicas == 0 and ctx.total_num_requests > 0:
        return max(
            math.ceil(1 * ctx.config.get_upscaling_factor()),
            ctx.target_num_replicas,
        )

    return None


def _apply_autoscaling_config(
    policy_func: Callable[
        [AutoscalingContext], Tuple[Union[int, float], Dict[str, Any]]
    ]
) -> Callable[[AutoscalingContext], Tuple[int, Dict[str, Any]]]:
    """
    Wraps a custom policy function to automatically apply:
    - upscaling_factor / downscaling_factor
    - min_replicas / max_replicas bounds
    - upscale_delay_s / downscale_delay_s / downscale_to_zero_delay_s
    """

    @functools.wraps(policy_func)
    def wrapped_policy(ctx: AutoscalingContext) -> Tuple[int, Dict[str, Any]]:

        # Cold start fast path: 0 replicas bypasses delay logic for immediate scale-up
        cold_start_replicas = _get_cold_start_scale_up_replicas(ctx)
        if cold_start_replicas is not None:
            return cold_start_replicas, ctx.policy_state
        policy_state = ctx.policy_state.copy()
        desired_num_replicas, updated_custom_policy_state = policy_func(ctx)
        final_num_replicas, final_state = _apply_default_params_and_merge_state(
            policy_state, updated_custom_policy_state, desired_num_replicas, ctx
        )

        return final_num_replicas, final_state

    return wrapped_policy


def _apply_app_level_autoscaling_config(
    policy_func: Callable[
        [Dict[DeploymentID, AutoscalingContext]],
        Tuple[
            Dict[DeploymentID, Union[int, float]],
            Optional[Dict[DeploymentID, Dict]],
        ],
    ]
) -> Callable[
    [Dict[DeploymentID, AutoscalingContext]],
    Tuple[Dict[DeploymentID, int], Dict[DeploymentID, Dict]],
]:
    """
    Wraps an application-level custom policy function to automatically apply per-deployment:
    - upscaling_factor / downscaling_factor
    - min_replicas / max_replicas bounds
    - upscale_delay_s / downscale_delay_s / downscale_to_zero_delay_s
    """

    @functools.wraps(policy_func)
    def wrapped_policy(
        contexts: Dict[DeploymentID, AutoscalingContext]
    ) -> Tuple[Dict[DeploymentID, int], Dict[DeploymentID, Dict]]:

        # Store the policy state per deployment
        state_per_deployment = {}
        for dep_id, ctx in contexts.items():
            state_per_deployment[dep_id] = ctx.policy_state.copy()

        # Send to the actual policy
        desired_num_replicas_dict, updated_custom_policy_state = policy_func(contexts)
        updated_custom_policy_state = updated_custom_policy_state or {}

        # Build per-deployment replicas count and state dictionary.
        final_decisions: Dict[DeploymentID, int] = {}
        final_state: Dict[DeploymentID, Dict] = {}
        for dep_id, ctx in contexts.items():
            custom_policy_state_per_deployment = (
                updated_custom_policy_state.get(dep_id) or {}
            ).copy()
            if dep_id not in desired_num_replicas_dict:
                final_state[dep_id] = _merge_user_state_with_internal_state(
                    state_per_deployment[dep_id],
                    custom_policy_state_per_deployment,
                )
                continue
            # Cold start fast path: 0 replicas bypasses delay logic for immediate scale-up
            cold_start_replicas = _get_cold_start_scale_up_replicas(ctx)
            if cold_start_replicas is not None:
                final_decisions[dep_id] = cold_start_replicas
                # Merge user policy state with internal policy state
                final_state[dep_id] = _merge_user_state_with_internal_state(
                    state_per_deployment[dep_id],
                    custom_policy_state_per_deployment,
                )
                continue
            final_num_replicas, final_dep_state = _apply_default_params_and_merge_state(
                state_per_deployment[dep_id],
                custom_policy_state_per_deployment,
                desired_num_replicas_dict[dep_id],
                ctx,
            )
            final_decisions[dep_id] = final_num_replicas
            final_state[dep_id] = final_dep_state
        return final_decisions, final_state

    return wrapped_policy


def _core_replica_queue_length_policy(
    ctx: AutoscalingContext,
) -> Tuple[float, Dict[str, Any]]:
    num_running_replicas = ctx.current_num_replicas
    config = ctx.config
    if num_running_replicas == 0:
        return ctx.target_num_replicas, {}
    target_num_requests = config.get_target_ongoing_requests() * num_running_replicas
    error_ratio = ctx.total_num_requests / target_num_requests
    desired_num_replicas = num_running_replicas * error_ratio
    return desired_num_replicas, {}


@PublicAPI(stability="stable")
def replica_queue_length_autoscaling_policy(
    ctx: AutoscalingContext,
) -> Tuple[Union[int, float], Dict[str, Any]]:
    """The default autoscaling policy based on basic thresholds for scaling.
    There is a minimum threshold for the average queue length in the cluster
    to scale up and a maximum threshold to scale down. Each period, a 'scale
    up' or 'scale down' decision is made. This decision must be made for a
    specified number of periods in a row before the number of replicas is
    actually scaled. See config options for more details.  Assumes
    `get_decision_num_replicas` is called once every CONTROL_LOOP_PERIOD_S
    seconds.
    """
    # Adding this guard makes the public policy safe to call directly.
    cold_start_replicas = _get_cold_start_scale_up_replicas(ctx)
    if cold_start_replicas is not None:
        return cold_start_replicas, ctx.policy_state
    return _core_replica_queue_length_policy(ctx)


default_autoscaling_policy = replica_queue_length_autoscaling_policy


# ---------------------------------------------------------------------------
# Prometheus-backed autoscaling building blocks
# ---------------------------------------------------------------------------

DEFAULT_PROMETHEUS_FETCH_INTERVAL_S = 5.0
DEFAULT_PROMETHEUS_CACHE_TTL_S = 15.0
DEFAULT_PROMETHEUS_QUERY_TIMEOUT_S = 5.0
_PROMETHEUS_HEADERS_ENV_VAR = "RAY_PROMETHEUS_HEADERS"


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class PrometheusScalar:
    """A scalar returned by an instant Prometheus query."""

    value: float
    timestamp: float


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class PrometheusSample:
    """One labeled sample in a Prometheus instant vector."""

    labels: Mapping[str, str]
    value: float
    timestamp: float

    def __post_init__(self):
        object.__setattr__(self, "labels", MappingProxyType(dict(self.labels)))


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class PrometheusVector:
    """An instant vector returned by a Prometheus query."""

    samples: Tuple[PrometheusSample, ...]

    def __post_init__(self):
        object.__setattr__(self, "samples", tuple(self.samples))


PrometheusQueryResult = Union[PrometheusScalar, PrometheusVector]


def _parse_prometheus_headers(headers: Any) -> Dict[str, str]:
    """Parse and validate Prometheus HTTP headers.

    Matches the formats accepted by the Ray dashboard: a JSON object or a
    JSON list of ``[name, value]`` pairs. Duplicate names in the list format
    are collapsed because ``urllib.request`` represents headers as a mapping.
    """
    if isinstance(headers, str):
        headers = json.loads(headers)
    if isinstance(headers, list):
        try:
            headers = dict(headers)
        except (TypeError, ValueError):
            headers = None
    if not isinstance(headers, dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in headers.items()
    ):
        raise ValueError(
            "Prometheus headers must be a JSON object with string keys and "
            "values, or a JSON list of [name, value] pairs."
        )
    return dict(headers)


def _normalize_query_url(address: str) -> str:
    """Return the ``/api/v1/query`` URL for a Prometheus address.

    Accepts ``host:port`` or ``http(s)://host:port``, and an address that
    already ends in the query path.
    """
    address = address.rstrip("/")
    if not address.startswith(("http://", "https://")):
        address = f"http://{address}"
    if address.endswith("/api/v1/query"):
        return address
    return f"{address}/api/v1/query"


def _parse_prometheus_value(value: Any) -> Optional[Tuple[float, float]]:
    """Parse ``[timestamp, value]``, returning None for a non-finite value."""
    if not isinstance(value, list) or len(value) != 2:
        raise ValueError("Prometheus sample must contain [timestamp, value].")
    timestamp = float(value[0])
    sample_value = float(value[1])
    if not math.isfinite(timestamp):
        raise ValueError("Prometheus sample timestamp must be finite.")
    if not math.isfinite(sample_value):
        return None
    return sample_value, timestamp


def _query_prometheus(
    query_url: str,
    query: str,
    timeout_s: float,
    headers: Optional[Dict[str, str]] = None,
) -> Optional[PrometheusQueryResult]:
    """Run one instant PromQL query and parse its scalar or vector result."""
    url = query_url + "?" + urllib.parse.urlencode({"query": query})
    request = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(request, timeout=timeout_s) as resp:
        body = json.load(resp)

    if not isinstance(body, dict) or body.get("status") != "success":
        error = body.get("error", "unknown error") if isinstance(body, dict) else body
        raise ValueError(f"Prometheus query failed: {error}")

    data = body.get("data")
    if not isinstance(data, dict):
        raise ValueError("Prometheus query response is missing its data object.")

    result_type = data.get("resultType")
    result = data.get("result")
    if result_type == "scalar":
        parsed = _parse_prometheus_value(result)
        if parsed is None:
            return None
        value, timestamp = parsed
        return PrometheusScalar(value=value, timestamp=timestamp)

    if result_type != "vector":
        raise ValueError(f"Unsupported Prometheus result type: {result_type!r}.")
    if not isinstance(result, list):
        raise ValueError("Prometheus vector result must be a list.")

    samples = []
    for item in result:
        if not isinstance(item, dict):
            raise ValueError("Prometheus vector sample must be an object.")
        if "histogram" in item or "value" not in item:
            raise ValueError("Native histogram query results are not supported.")
        labels = item.get("metric", {})
        if not isinstance(labels, dict) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in labels.items()
        ):
            raise ValueError("Prometheus vector labels must map strings to strings.")
        parsed = _parse_prometheus_value(item["value"])
        # Prometheus can return NaN or Inf for an empty range such as an idle
        # histogram_quantile. Treat that sample as no data.
        if parsed is None:
            continue
        value, timestamp = parsed
        samples.append(
            PrometheusSample(labels=labels, value=value, timestamp=timestamp)
        )

    return PrometheusVector(samples=tuple(samples))


def _single_prometheus_value(result: PrometheusQueryResult) -> Optional[float]:
    """Return the only value in a scalar or single-sample vector."""
    if isinstance(result, PrometheusScalar):
        return result.value
    if len(result.samples) == 1:
        return result.samples[0].value
    return None


def _fetch_prometheus_results(
    address: str,
    queries: List[str],
    timeout_s: float = DEFAULT_PROMETHEUS_QUERY_TIMEOUT_S,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, PrometheusQueryResult]:
    """Evaluate ``queries``, independently omitting no-data or failed results."""
    query_url = _normalize_query_url(address)
    out: Dict[str, PrometheusQueryResult] = {}
    for query in queries:
        try:
            result = _query_prometheus(query_url, query, timeout_s, headers)
            if result is not None:
                out[query] = result
        except Exception as exc:
            logger.warning("Failed to evaluate Prometheus query %r: %s", query, exc)
            logger.debug(
                "Failed to evaluate Prometheus query %r.", query, exc_info=True
            )
    return out


class _MetricCache:
    """Shared state between a policy and its refresh thread.

    Kept separate so the thread never holds a reference to the policy, which
    lets the policy be garbage collected and its thread stopped on reconfig.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.results: Optional[Dict[str, PrometheusQueryResult]] = None
        self.timestamp = 0.0


def _run_refresh(
    cache: _MetricCache,
    address: str,
    queries: List[str],
    headers: Dict[str, str],
    interval_s: float,
) -> None:
    while not cache.stop.is_set():
        try:
            results = _fetch_prometheus_results(address, queries, headers=headers)
            with cache.lock:
                cache.results = results or None
                cache.timestamp = time.monotonic()
        except Exception:
            logger.warning("Prometheus autoscaling fetch failed.", exc_info=True)
        cache.stop.wait(interval_s)


@PublicAPI(stability="alpha")
class PrometheusQueryMixin:
    """Keeps Prometheus query results fresh for an autoscaling policy.

    Mix into a policy and read ``self.prometheus_results`` from ``__call__``.
    Scalar results and instant vectors, including empty and multi-sample
    vectors, retain their Prometheus result types. ``self.prometheus_metrics``
    is a convenience view containing only scalars and single-sample vectors.

    The first read starts a daemon thread that evaluates the queries every
    ``fetch_interval_s``. Reads never block on the network and return ``None``
    when Prometheus is unset, unreachable, or the cache is older than
    ``cache_ttl_s``.

    ``prometheus_address`` defaults to the ``RAY_PROMETHEUS_HOST`` environment
    variable, which Ray's dashboard and managed clusters already set, so the
    common case needs no address. HTTP headers are read from the JSON-encoded
    ``RAY_PROMETHEUS_HEADERS`` environment variable used by the dashboard.
    """

    def __init__(
        self,
        *,
        prometheus_address: Optional[str] = None,
        prometheus_queries: Optional[List[str]] = None,
        fetch_interval_s: float = DEFAULT_PROMETHEUS_FETCH_INTERVAL_S,
        cache_ttl_s: float = DEFAULT_PROMETHEUS_CACHE_TTL_S,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._prometheus_address = prometheus_address or os.environ.get(
            "RAY_PROMETHEUS_HOST"
        )
        self._prometheus_headers = _parse_prometheus_headers(
            os.environ.get(_PROMETHEUS_HEADERS_ENV_VAR, "{}")
        )
        if isinstance(prometheus_queries, str):
            prometheus_queries = [prometheus_queries]
        self._prometheus_queries = list(prometheus_queries or [])
        self._fetch_interval_s = fetch_interval_s
        self._cache_ttl_s = cache_ttl_s
        self._cache = _MetricCache()
        self._started = False

    def _ensure_refreshing(self) -> None:
        if self._started:
            return
        self._started = True
        if not (self._prometheus_address and self._prometheus_queries):
            return
        cache = self._cache
        threading.Thread(
            target=_run_refresh,
            args=(
                cache,
                self._prometheus_address,
                self._prometheus_queries,
                self._prometheus_headers,
                self._fetch_interval_s,
            ),
            name="serve-prometheus-autoscaling-fetch",
            daemon=True,
        ).start()
        # Stop the thread once this policy is garbage collected, e.g. when it
        # is replaced on a config change.
        weakref.finalize(self, cache.stop.set)

    def _read_cached_results(
        self,
    ) -> Optional[Dict[str, PrometheusQueryResult]]:
        """Return a shallow copy of the fresh result cache."""
        self._ensure_refreshing()
        cache = self._cache
        with cache.lock:
            if cache.results is None:
                return None
            if time.monotonic() - cache.timestamp > self._cache_ttl_s:
                return None
            return dict(cache.results)

    @property
    def prometheus_results(
        self,
    ) -> Optional[Dict[str, PrometheusQueryResult]]:
        """Latest typed scalar and vector results, or None if unavailable."""
        return self._read_cached_results()

    @property
    def prometheus_metrics(self) -> Optional[Dict[str, float]]:
        """Latest unambiguous scalar values, or None if none are available."""
        results = self._read_cached_results()
        if results is None:
            return None
        metrics = {
            query: value
            for query, result in results.items()
            if (value := _single_prometheus_value(result)) is not None
        }
        return metrics or None
