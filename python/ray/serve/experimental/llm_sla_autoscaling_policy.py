"""Experimental SLO-aware autoscaling policy for aggregated LLM deployments.

The policy learns a deployment-local, per-replica capacity model from vLLM
metrics exported to Prometheus.  It is intentionally conservative: learned
recommendations are only used in workload regions represented in the replay
buffer, and Ray Serve's request-based autoscaler remains the fallback.

This module does not import scikit-learn at module import time.  The optional
dependency is loaded when :class:`SLAOnlineAutoscalingPolicy` is instantiated.
"""

import logging
import math
import threading
import time
import weakref
from collections import deque
from dataclasses import dataclass
from typing import (
    Any,
    Deque,
    Dict,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Protocol,
    Tuple,
)

import numpy as np

from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.autoscaling_policy import (
    PrometheusQueryMixin,
    PrometheusQueryResult,
    PrometheusScalar,
    PrometheusVector,
    replica_queue_length_autoscaling_policy,
)
from ray.serve.config import AutoscalingContext
from ray.util import metrics
from ray.util.annotations import PublicAPI

logger = logging.getLogger(SERVE_LOGGER_NAME)

_FEATURE_COUNT = 7
_TTFT_MONOTONIC_CONSTRAINTS = (1, 1, 1, 1, 1, 0, -1)
_ITL_MONOTONIC_CONSTRAINTS = (1, 1, 1, 1, 1, 1, -1)


@dataclass(frozen=True)
class _MetricsSnapshot:
    offered_rps: float
    mean_isl: float
    mean_osl: float
    cache_hit_rate: float
    mean_ttft_ms: float
    mean_itl_ms: float
    request_samples: float
    token_samples: float
    worker_ttft_ms: Mapping[str, float]
    worker_itl_ms: Mapping[str, float]


class _MetricsSource(Protocol):
    def bind(self, ctx: AutoscalingContext) -> None:
        ...

    def read(self) -> Optional[_MetricsSnapshot]:
        ...


class _PrometheusQueries(NamedTuple):
    offered_rps: str
    mean_isl: str
    mean_osl: str
    cache_hit_rate: str
    worker_ttft: str
    worker_ttft_samples: str
    worker_itl: str
    worker_itl_samples: str
    generated_tokens: str


def _prometheus_label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _build_prometheus_queries(
    *, deployment: str, application: str, window_s: float
) -> _PrometheusQueries:
    """Build deployment-scoped queries using the dashboard's WorkerId join."""
    deployment = _prometheus_label_value(deployment)
    application = _prometheus_label_value(application)
    selector = f'deployment="{deployment}",application="{application}"'
    window = f"{window_s:g}s"
    lookup = (
        "max by (WorkerId, replica) "
        f"(ray_serve_deployment_request_counter_total{{{selector}}} * 0 + 1)"
    )

    def worker_rate(metric: str) -> str:
        return f"sum by (WorkerId) (rate({metric}[{window}]))"

    def worker_increase(metric: str) -> str:
        return f"sum by (WorkerId) (increase({metric}[{window}]))"

    def join(expr: str) -> str:
        return f"({expr}) * on (WorkerId) group_left(replica) " f"({lookup})"

    def worker_mean(metric: str) -> str:
        numerator = worker_rate(f"{metric}_sum")
        denominator = worker_rate(f"{metric}_count")
        mean = (
            f"(({numerator}) / ({denominator})) and on (WorkerId) "
            f"(({denominator}) > 0)"
        )
        return join(mean)

    def deployment_mean(metric: str) -> str:
        numerator = join(worker_rate(f"{metric}_sum"))
        denominator = join(worker_rate(f"{metric}_count"))
        return f"sum({numerator}) / sum({denominator})"

    cache_hits = join(worker_rate("ray_vllm_prefix_cache_hits_total"))
    cache_queries = join(worker_rate("ray_vllm_prefix_cache_queries_total"))
    ttft_metric = "ray_vllm_time_to_first_token_seconds"
    itl_metric = "ray_vllm_request_time_per_output_token_seconds"

    return _PrometheusQueries(
        offered_rps=(
            "sum(rate(ray_serve_num_router_requests_total" f"{{{selector}}}[{window}]))"
        ),
        mean_isl=deployment_mean("ray_vllm_request_prompt_tokens"),
        mean_osl=deployment_mean("ray_vllm_request_generation_tokens"),
        cache_hit_rate=f"sum({cache_hits}) / sum({cache_queries})",
        worker_ttft=worker_mean(ttft_metric),
        worker_ttft_samples=join(worker_increase(f"{ttft_metric}_count")),
        worker_itl=worker_mean(itl_metric),
        worker_itl_samples=join(worker_increase(f"{itl_metric}_count")),
        generated_tokens=f"sum({join(worker_increase('ray_vllm_generation_tokens_total'))})",
    )


def _single_value(result: Optional[PrometheusQueryResult]) -> Optional[float]:
    if isinstance(result, PrometheusScalar):
        return result.value
    if isinstance(result, PrometheusVector) and len(result.samples) == 1:
        return result.samples[0].value
    return None


def _worker_values(
    result: Optional[PrometheusQueryResult], *, scale: float = 1.0
) -> Dict[str, float]:
    if not isinstance(result, PrometheusVector):
        return {}
    values = {}
    for sample in result.samples:
        worker = sample.labels.get("replica") or sample.labels.get("WorkerId")
        if worker:
            values[worker] = sample.value * scale
    return values


def _weighted_mean(
    values: Mapping[str, float], weights: Mapping[str, float]
) -> Optional[float]:
    common = values.keys() & weights.keys()
    total_weight = sum(max(0.0, weights[key]) for key in common)
    if total_weight <= 0:
        return None
    return sum(values[key] * max(0.0, weights[key]) for key in common) / total_weight


class _PrometheusSLAMetricsSource(PrometheusQueryMixin):
    def __init__(
        self,
        *,
        observation_window_s: float,
        prometheus_address: Optional[str],
        fetch_interval_s: float,
        cache_ttl_s: float,
    ):
        super().__init__(
            prometheus_address=prometheus_address,
            prometheus_queries=[],
            fetch_interval_s=fetch_interval_s,
            cache_ttl_s=cache_ttl_s,
        )
        self._observation_window_s = observation_window_s
        self._queries: Optional[_PrometheusQueries] = None
        self._deployment_key: Optional[Tuple[str, str]] = None

    def bind(self, ctx: AutoscalingContext) -> None:
        key = (ctx.deployment_name, ctx.app_name or "")
        if self._deployment_key == key:
            return
        if self._deployment_key is not None or self._started:
            raise RuntimeError(
                "An SLA metrics source cannot be rebound to a deployment."
            )
        self._deployment_key = key
        self._queries = _build_prometheus_queries(
            deployment=key[0],
            application=key[1],
            window_s=self._observation_window_s,
        )
        self._prometheus_queries = list(self._queries)

    def read(self) -> Optional[_MetricsSnapshot]:
        if self._queries is None:
            return None
        results = self.prometheus_results
        if results is None:
            return None

        q = self._queries
        offered_rps = _single_value(results.get(q.offered_rps))
        mean_isl = _single_value(results.get(q.mean_isl))
        mean_osl = _single_value(results.get(q.mean_osl))
        worker_ttft = _worker_values(results.get(q.worker_ttft), scale=1000.0)
        worker_ttft_samples = _worker_values(results.get(q.worker_ttft_samples))
        worker_itl = _worker_values(results.get(q.worker_itl), scale=1000.0)
        worker_itl_samples = _worker_values(results.get(q.worker_itl_samples))
        mean_ttft = _weighted_mean(worker_ttft, worker_ttft_samples)
        mean_itl = _weighted_mean(worker_itl, worker_itl_samples)
        generated_tokens = _single_value(results.get(q.generated_tokens))

        required = (offered_rps, mean_isl, mean_osl, mean_ttft, mean_itl)
        if any(value is None or not math.isfinite(value) for value in required):
            return None

        cache_hit_rate = _single_value(results.get(q.cache_hit_rate))
        if cache_hit_rate is None or not math.isfinite(cache_hit_rate):
            cache_hit_rate = 0.0

        request_samples = sum(max(0.0, v) for v in worker_ttft_samples.values())
        token_samples = generated_tokens or 0.0
        return _MetricsSnapshot(
            offered_rps=max(0.0, offered_rps),
            mean_isl=max(0.0, mean_isl),
            mean_osl=max(0.0, mean_osl),
            cache_hit_rate=min(1.0, max(0.0, cache_hit_rate)),
            mean_ttft_ms=max(0.0, mean_ttft),
            mean_itl_ms=max(0.0, mean_itl),
            request_samples=request_samples,
            token_samples=max(0.0, token_samples),
            worker_ttft_ms=worker_ttft,
            worker_itl_ms=worker_itl,
        )


@dataclass(frozen=True)
class _TrainingRow:
    features: Tuple[float, ...]
    ttft_ms: float
    itl_ms: float
    ttft_weight: float
    itl_weight: float
    timestamp: float
    replica_count: int


_BucketKey = Tuple[int, int, int, int]


def _factor_four_bucket(value: float) -> int:
    if value <= 0:
        return -1000
    return math.floor(math.log(value, 4))


def _bucket_key(features: Tuple[float, ...]) -> _BucketKey:
    mean_isl = features[4]
    queued_requests_per_replica = features[3] / mean_isl if mean_isl > 0 else 0.0
    if queued_requests_per_replica < 0.5:
        queue_band = 0
    elif queued_requests_per_replica <= 2.0:
        queue_band = 1
    else:
        queue_band = 2
    mean_context = features[4] + features[5] / 2.0
    return (
        _factor_four_bucket(features[1]),
        _factor_four_bucket(features[2]),
        _factor_four_bucket(mean_context),
        queue_band,
    )


class _ReplayBuffer:
    def __init__(self, *, max_rows: int, rows_per_bucket: int):
        self._max_rows = max_rows
        self._rows_per_bucket = rows_per_bucket
        self._buckets: Dict[_BucketKey, Deque[_TrainingRow]] = {}

    def add(self, row: _TrainingRow) -> None:
        key = _bucket_key(row.features)
        bucket = self._buckets.setdefault(key, deque(maxlen=self._rows_per_bucket))
        bucket.append(row)
        while len(self) > self._max_rows:
            oldest_key = min(
                self._buckets,
                key=lambda candidate: self._buckets[candidate][0].timestamp,
            )
            self._buckets[oldest_key].popleft()
            if not self._buckets[oldest_key]:
                del self._buckets[oldest_key]

    def rows(self) -> Tuple[_TrainingRow, ...]:
        return tuple(
            sorted(
                (row for bucket in self._buckets.values() for row in bucket),
                key=lambda row: row.timestamp,
            )
        )

    def bucket_rows(self, features: Tuple[float, ...]) -> Tuple[_TrainingRow, ...]:
        return tuple(self._buckets.get(_bucket_key(features), ()))

    @property
    def num_buckets(self) -> int:
        return len(self._buckets)

    def __len__(self) -> int:
        return sum(len(bucket) for bucket in self._buckets.values())


@dataclass(frozen=True)
class _ModelSnapshot:
    ttft_model: Any
    itl_model: Any
    trained_at: float
    num_rows: int


@dataclass(frozen=True)
class _TrainingRequest:
    rows: Tuple[_TrainingRow, ...]
    requested_at: float


class _TrainerState:
    def __init__(self, estimator_cls: Any):
        self.estimator_cls = estimator_cls
        self.lock = threading.Lock()
        self.wake = threading.Event()
        self.stop = threading.Event()
        self.pending: Optional[_TrainingRequest] = None
        self.model: Optional[_ModelSnapshot] = None
        self.last_error: Optional[str] = None
        self.last_fit_duration_ms = 0.0


def _new_estimator(estimator_cls: Any, monotonic_cst: Tuple[int, ...]) -> Any:
    return estimator_cls(
        loss="squared_error",
        learning_rate=0.05,
        max_iter=100,
        max_leaf_nodes=15,
        min_samples_leaf=5,
        l2_regularization=1.0,
        early_stopping=False,
        monotonic_cst=monotonic_cst,
        random_state=0,
    )


def _fit_model_snapshot(
    estimator_cls: Any,
    request: _TrainingRequest,
) -> _ModelSnapshot:
    features = np.asarray([row.features for row in request.rows], dtype=np.float64)
    if features.ndim != 2 or features.shape[1] != _FEATURE_COUNT:
        raise ValueError("Invalid SLA autoscaling training feature matrix.")
    ttft = np.asarray([row.ttft_ms for row in request.rows], dtype=np.float64)
    itl = np.asarray([row.itl_ms for row in request.rows], dtype=np.float64)
    ttft_weights = np.asarray(
        [max(row.ttft_weight, 1.0) for row in request.rows], dtype=np.float64
    )
    itl_weights = np.asarray(
        [max(row.itl_weight, 1.0) for row in request.rows], dtype=np.float64
    )

    ttft_model = _new_estimator(estimator_cls, _TTFT_MONOTONIC_CONSTRAINTS)
    itl_model = _new_estimator(estimator_cls, _ITL_MONOTONIC_CONSTRAINTS)
    ttft_model.fit(features, ttft, sample_weight=ttft_weights)
    itl_model.fit(features, itl, sample_weight=itl_weights)
    return _ModelSnapshot(
        ttft_model=ttft_model,
        itl_model=itl_model,
        trained_at=request.requested_at,
        num_rows=len(request.rows),
    )


def _trainer_loop(state: _TrainerState) -> None:
    while not state.stop.is_set():
        state.wake.wait()
        state.wake.clear()
        if state.stop.is_set():
            break
        with state.lock:
            request = state.pending
            state.pending = None
        if request is None:
            continue

        started = time.monotonic()
        try:
            model = _fit_model_snapshot(state.estimator_cls, request)
            error = None
        except Exception as exc:
            logger.exception("Failed to refit the SLA autoscaling model.")
            model = None
            error = str(exc)
        duration_ms = (time.monotonic() - started) * 1000.0
        with state.lock:
            if model is not None:
                state.model = model
            state.last_error = error
            state.last_fit_duration_ms = duration_ms


def _stop_trainer(state: _TrainerState) -> None:
    state.stop.set()
    state.wake.set()


@dataclass(frozen=True)
class _CapacityRecommendation:
    replicas: int
    predicted_ttft_ms: float
    predicted_itl_ms: float


def _features_for_replicas(
    snapshot: _MetricsSnapshot, queued_requests: float, replicas: int
) -> Tuple[float, ...]:
    if replicas <= 0:
        raise ValueError("replicas must be positive")
    effective_prefill = (
        snapshot.offered_rps * snapshot.mean_isl * (1.0 - snapshot.cache_hit_rate)
    )
    expected_decode = snapshot.offered_rps * snapshot.mean_osl
    queued_prompt_tokens = max(0.0, queued_requests) * snapshot.mean_isl
    return (
        snapshot.offered_rps / replicas,
        effective_prefill / replicas,
        expected_decode / replicas,
        queued_prompt_tokens / replicas,
        snapshot.mean_isl,
        snapshot.mean_osl,
        snapshot.cache_hit_rate,
    )


@PublicAPI(stability="alpha")
class SLAOnlineAutoscalingPolicy:
    """Experimental online SLO-aware policy for aggregated LLM replicas.

    The initial implementation targets vLLM metrics and mean TTFT/ITL.  Set
    ``advisory=False`` only after validating its recommendations for the model,
    accelerator, and traffic distribution used by the deployment.
    """

    def __init__(
        self,
        *,
        ttft_ms: float,
        itl_ms: float,
        advisory: bool = True,
        metrics_source: str = "prometheus",
        prometheus_address: Optional[str] = None,
        observation_window_s: float = 30.0,
        topology_stabilization_s: float = 60.0,
        retrain_interval_s: float = 60.0,
        decision_interval_s: float = 1.0,
        fetch_interval_s: float = 5.0,
        cache_ttl_s: float = 15.0,
        min_training_rows: int = 30,
        min_candidate_bucket_samples: int = 3,
        rows_per_bucket: int = 8,
        max_training_rows: int = 512,
        max_candidate_evaluations: int = 1024,
        min_request_samples: float = 5.0,
        min_token_samples: float = 5.0,
        downscale_sensitivity: float = 0.8,
        queue_near_zero_threshold: float = 0.5,
        _metrics_source: Optional[_MetricsSource] = None,
    ):
        if ttft_ms <= 0 or itl_ms <= 0:
            raise ValueError("ttft_ms and itl_ms must be greater than zero.")
        positive_values = {
            "observation_window_s": observation_window_s,
            "topology_stabilization_s": topology_stabilization_s,
            "retrain_interval_s": retrain_interval_s,
            "decision_interval_s": decision_interval_s,
            "fetch_interval_s": fetch_interval_s,
            "cache_ttl_s": cache_ttl_s,
        }
        if any(value <= 0 for value in positive_values.values()):
            raise ValueError(
                "All observation, stabilization, refit, decision, fetch, and cache "
                "intervals must be greater than zero."
            )
        if not 0 < downscale_sensitivity < 1:
            raise ValueError("downscale_sensitivity must be between zero and one.")
        if min_training_rows < 1 or min_candidate_bucket_samples < 1:
            raise ValueError("Training and candidate sample minimums must be positive.")
        if rows_per_bucket < min_candidate_bucket_samples:
            raise ValueError(
                "rows_per_bucket must be at least min_candidate_bucket_samples."
            )
        if max_training_rows < min_training_rows:
            raise ValueError("max_training_rows must be at least min_training_rows.")
        if max_candidate_evaluations < 1:
            raise ValueError("max_candidate_evaluations must be positive.")
        if metrics_source != "prometheus" and _metrics_source is None:
            raise ValueError("Only metrics_source='prometheus' is currently supported.")

        try:
            from sklearn.ensemble import HistGradientBoostingRegressor
        except ImportError as exc:
            raise ImportError(
                "SLAOnlineAutoscalingPolicy requires scikit-learn. Install it in "
                "the Serve controller's runtime environment with "
                "`pip install scikit-learn`."
            ) from exc

        self.ttft_ms = float(ttft_ms)
        self.itl_ms = float(itl_ms)
        self.advisory = advisory
        self._observation_window_s = observation_window_s
        self._topology_stabilization_s = topology_stabilization_s
        self._retrain_interval_s = retrain_interval_s
        self._decision_interval_s = decision_interval_s
        self._min_training_rows = min_training_rows
        self._min_candidate_bucket_samples = min_candidate_bucket_samples
        self._min_request_samples = min_request_samples
        self._min_token_samples = min_token_samples
        self._max_candidate_evaluations = max_candidate_evaluations
        self._downscale_sensitivity = downscale_sensitivity
        self._queue_near_zero_threshold = queue_near_zero_threshold

        self._metrics_source: _MetricsSource = _metrics_source or (
            _PrometheusSLAMetricsSource(
                observation_window_s=observation_window_s,
                prometheus_address=prometheus_address,
                fetch_interval_s=fetch_interval_s,
                cache_ttl_s=cache_ttl_s,
            )
        )
        self._buffer = _ReplayBuffer(
            max_rows=max_training_rows, rows_per_bucket=rows_per_bucket
        )
        self._trainer_state = _TrainerState(HistGradientBoostingRegressor)
        self._trainer = threading.Thread(
            target=_trainer_loop,
            args=(self._trainer_state,),
            name="serve-sla-autoscaling-trainer",
            daemon=True,
        )
        self._trainer.start()
        weakref.finalize(self, _stop_trainer, self._trainer_state)

        self._topology: Optional[Tuple[str, ...]] = None
        self._topology_changed_at: Optional[float] = None
        self._last_observation_at: Optional[float] = None
        self._last_refit_requested_at: Optional[float] = None
        self._last_observation_queue: Optional[float] = None
        self._last_decision_at: Optional[float] = None
        self._cached_capacity: Optional[_CapacityRecommendation] = None
        self._cached_capacity_reason = "model_not_ready"
        self._last_inference_duration_ms = 0.0
        self._last_logged_reason: Optional[str] = None

        tag_keys = ("deployment", "application")
        self._recommended_replicas_gauge = metrics.Gauge(
            "serve_sla_autoscaling_recommended_replicas",
            description="Raw replica recommendation from the experimental SLA policy.",
            tag_keys=tag_keys,
        )
        self._model_ready_gauge = metrics.Gauge(
            "serve_sla_autoscaling_model_ready",
            description="Whether the experimental SLA model is ready for inference.",
            tag_keys=tag_keys,
        )
        self._training_rows_gauge = metrics.Gauge(
            "serve_sla_autoscaling_training_rows",
            description="Number of observations retained by the SLA policy.",
            tag_keys=tag_keys,
        )
        self._training_buckets_gauge = metrics.Gauge(
            "serve_sla_autoscaling_training_buckets",
            description="Number of populated SLA workload buckets.",
            tag_keys=tag_keys,
        )
        self._predicted_ttft_gauge = metrics.Gauge(
            "serve_sla_autoscaling_predicted_ttft_ms",
            description="Predicted mean TTFT for the recommended replica count.",
            tag_keys=tag_keys,
        )
        self._predicted_itl_gauge = metrics.Gauge(
            "serve_sla_autoscaling_predicted_itl_ms",
            description="Predicted mean ITL for the recommended replica count.",
            tag_keys=tag_keys,
        )
        self._fit_duration_gauge = metrics.Gauge(
            "serve_sla_autoscaling_fit_duration_ms",
            description="Duration of the most recent background model refit.",
            tag_keys=tag_keys,
        )
        self._inference_duration_gauge = metrics.Gauge(
            "serve_sla_autoscaling_inference_duration_ms",
            description="Duration of the most recent vectorized SLA model inference.",
            tag_keys=tag_keys,
        )

    def _read_model(self) -> Optional[_ModelSnapshot]:
        with self._trainer_state.lock:
            return self._trainer_state.model

    def _request_refit(self, now: float) -> None:
        rows = self._buffer.rows()
        if len(rows) < self._min_training_rows:
            return
        if (
            self._last_refit_requested_at is not None
            and now - self._last_refit_requested_at < self._retrain_interval_s
        ):
            return
        request = _TrainingRequest(rows=rows, requested_at=now)
        with self._trainer_state.lock:
            # A single latest-only slot prevents a slow fit from creating backlog.
            self._trainer_state.pending = request
        self._trainer_state.wake.set()
        self._last_refit_requested_at = now

    def _update_topology(self, ctx: AutoscalingContext, now: float) -> bool:
        topology = tuple(sorted(replica.unique_id for replica in ctx.running_replicas))
        if topology != self._topology:
            self._topology = topology
            self._topology_changed_at = now
            self._last_decision_at = None
            self._cached_capacity = None
            return True
        return False

    def _topology_is_stable(self, now: float) -> bool:
        return (
            self._topology_changed_at is not None
            and now - self._topology_changed_at >= self._topology_stabilization_s
        )

    def _maybe_record_observation(
        self,
        ctx: AutoscalingContext,
        snapshot: _MetricsSnapshot,
        now: float,
    ) -> None:
        if ctx.current_num_replicas <= 0 or not self._topology_is_stable(now):
            return
        if ctx.current_num_replicas != ctx.target_num_replicas:
            return
        if (
            self._last_observation_at is not None
            and now - self._last_observation_at < self._observation_window_s
        ):
            return
        if (
            snapshot.request_samples < self._min_request_samples
            or snapshot.token_samples < self._min_token_samples
        ):
            return
        ready = {replica.unique_id for replica in ctx.running_replicas}
        if not (
            ready <= snapshot.worker_ttft_ms.keys()
            and ready <= snapshot.worker_itl_ms.keys()
        ):
            return

        features = _features_for_replicas(
            snapshot, ctx.total_queued_requests, ctx.current_num_replicas
        )
        if not all(math.isfinite(value) for value in features):
            return
        self._buffer.add(
            _TrainingRow(
                features=features,
                ttft_ms=snapshot.mean_ttft_ms,
                itl_ms=snapshot.mean_itl_ms,
                ttft_weight=snapshot.request_samples,
                itl_weight=snapshot.token_samples,
                timestamp=now,
                replica_count=ctx.current_num_replicas,
            )
        )
        self._last_observation_at = now
        self._last_observation_queue = ctx.total_queued_requests
        self._request_refit(now)

    def _candidate_is_supported(self, features: Tuple[float, ...]) -> bool:
        return (
            len(self._buffer.bucket_rows(features))
            >= self._min_candidate_bucket_samples
        )

    def _compute_capacity_recommendation(
        self,
        ctx: AutoscalingContext,
        snapshot: _MetricsSnapshot,
        model: _ModelSnapshot,
    ) -> Tuple[Optional[_CapacityRecommendation], str]:
        candidates: List[int] = []
        features: List[Tuple[float, ...]] = []
        minimum = max(1, ctx.capacity_adjusted_min_replicas)
        maximum = ctx.capacity_adjusted_max_replicas
        if maximum - minimum + 1 > self._max_candidate_evaluations:
            return None, "candidate_range_too_large"
        for replicas in range(minimum, maximum + 1):
            candidate_features = _features_for_replicas(
                snapshot, ctx.total_queued_requests, replicas
            )
            if self._candidate_is_supported(candidate_features):
                candidates.append(replicas)
                features.append(candidate_features)

        if not candidates:
            return None, "unsupported_workload"

        matrix = np.asarray(features, dtype=np.float64)
        ttft_predictions = np.asarray(model.ttft_model.predict(matrix), dtype=float)
        itl_predictions = np.asarray(model.itl_model.predict(matrix), dtype=float)
        if (
            ttft_predictions.shape != (len(candidates),)
            or itl_predictions.shape != (len(candidates),)
            or not np.all(np.isfinite(ttft_predictions))
            or not np.all(np.isfinite(itl_predictions))
        ):
            return None, "invalid_prediction"

        for index, replicas in enumerate(candidates):
            predicted_ttft = max(0.0, float(ttft_predictions[index]))
            predicted_itl = max(0.0, float(itl_predictions[index]))
            if predicted_ttft <= self.ttft_ms and predicted_itl <= self.itl_ms:
                return (
                    _CapacityRecommendation(
                        replicas=replicas,
                        predicted_ttft_ms=predicted_ttft,
                        predicted_itl_ms=predicted_itl,
                    ),
                    "capacity",
                )
        return None, "no_supported_candidate_meets_slo"

    def _get_capacity_recommendation(
        self,
        ctx: AutoscalingContext,
        snapshot: _MetricsSnapshot,
        now: float,
    ) -> Tuple[Optional[_CapacityRecommendation], str]:
        model = self._read_model()
        if model is None:
            return None, "model_not_ready"
        if (
            self._last_decision_at is not None
            and now - self._last_decision_at < self._decision_interval_s
        ):
            return self._cached_capacity, self._cached_capacity_reason
        started = time.monotonic()
        try:
            recommendation, reason = self._compute_capacity_recommendation(
                ctx, snapshot, model
            )
        except Exception:
            logger.exception("SLA capacity inference failed; using fallback policy.")
            recommendation, reason = None, "inference_error"
        self._last_inference_duration_ms = (time.monotonic() - started) * 1000.0
        self._last_decision_at = now
        self._cached_capacity = recommendation
        self._cached_capacity_reason = reason
        return recommendation, reason

    def _all_workers_overloaded(
        self, ctx: AutoscalingContext, snapshot: _MetricsSnapshot
    ) -> bool:
        ready = {replica.unique_id for replica in ctx.running_replicas}
        if not ready:
            return False
        ttft_overloaded = ready <= snapshot.worker_ttft_ms.keys() and all(
            snapshot.worker_ttft_ms[replica] > self.ttft_ms for replica in ready
        )
        itl_overloaded = ready <= snapshot.worker_itl_ms.keys() and all(
            snapshot.worker_itl_ms[replica] > self.itl_ms for replica in ready
        )
        return ttft_overloaded or itl_overloaded

    def _safe_to_scale_down(
        self,
        ctx: AutoscalingContext,
        snapshot: _MetricsSnapshot,
        recommendation: _CapacityRecommendation,
        default_desired: float,
    ) -> bool:
        current = ctx.current_num_replicas
        candidate = current - 1
        if candidate < max(1, ctx.capacity_adjusted_min_replicas):
            return False
        if ctx.current_num_replicas != ctx.target_num_replicas:
            return False
        ready = {replica.unique_id for replica in ctx.running_replicas}
        if not ready:
            return False
        ttft_safe = ready <= snapshot.worker_ttft_ms.keys() and all(
            snapshot.worker_ttft_ms[replica]
            <= self.ttft_ms * self._downscale_sensitivity
            for replica in ready
        )
        itl_safe = ready <= snapshot.worker_itl_ms.keys() and all(
            snapshot.worker_itl_ms[replica] <= self.itl_ms * self._downscale_sensitivity
            for replica in ready
        )
        queue_safe = (
            ctx.total_queued_requests <= self._queue_near_zero_threshold
            and self._last_observation_queue is not None
            and ctx.total_queued_requests <= self._last_observation_queue + 1e-9
        )
        candidate_features = _features_for_replicas(
            snapshot, ctx.total_queued_requests, candidate
        )
        historical_rows = self._buffer.bucket_rows(candidate_features)
        historical_safe = (
            len(historical_rows) >= self._min_candidate_bucket_samples
            and sum(
                row.ttft_ms <= self.ttft_ms and row.itl_ms <= self.itl_ms
                for row in historical_rows
            )
            >= self._min_candidate_bucket_samples
        )
        prediction_safe = (
            recommendation.replicas <= candidate
            and recommendation.predicted_ttft_ms
            <= self.ttft_ms * self._downscale_sensitivity
            and recommendation.predicted_itl_ms
            <= self.itl_ms * self._downscale_sensitivity
        )
        return (
            ttft_safe
            and itl_safe
            and queue_safe
            and historical_safe
            and prediction_safe
            and default_desired <= candidate
        )

    def _record_diagnostics(
        self,
        ctx: AutoscalingContext,
        desired: float,
        recommendation: Optional[_CapacityRecommendation],
    ) -> None:
        tags = {
            "deployment": ctx.deployment_name,
            "application": ctx.app_name or "",
        }
        self._recommended_replicas_gauge.set(desired, tags=tags)
        self._model_ready_gauge.set(float(self._read_model() is not None), tags=tags)
        self._training_rows_gauge.set(len(self._buffer), tags=tags)
        self._training_buckets_gauge.set(self._buffer.num_buckets, tags=tags)
        if recommendation is not None:
            self._predicted_ttft_gauge.set(recommendation.predicted_ttft_ms, tags=tags)
            self._predicted_itl_gauge.set(recommendation.predicted_itl_ms, tags=tags)
        with self._trainer_state.lock:
            fit_duration = self._trainer_state.last_fit_duration_ms
        self._fit_duration_gauge.set(fit_duration, tags=tags)
        self._inference_duration_gauge.set(self._last_inference_duration_ms, tags=tags)

    def __call__(self, ctx: AutoscalingContext) -> Tuple[float, Dict[str, Any]]:
        now = ctx.current_time if ctx.current_time is not None else time.time()
        self._metrics_source.bind(ctx)
        self._update_topology(ctx, now)

        default_desired, _ = replica_queue_length_autoscaling_policy(ctx)
        default_desired = float(default_desired)
        snapshot = self._metrics_source.read()
        reason = "metrics_unavailable"
        recommendation = None
        learned_desired = default_desired

        if snapshot is not None:
            self._maybe_record_observation(ctx, snapshot, now)
            recommendation, reason = self._get_capacity_recommendation(
                ctx, snapshot, now
            )

            if ctx.current_num_replicas != ctx.target_num_replicas:
                learned_desired = max(default_desired, float(ctx.target_num_replicas))
                reason = "scaling_in_progress"
            else:
                if recommendation is not None:
                    if recommendation.replicas > ctx.current_num_replicas:
                        learned_desired = max(
                            learned_desired, float(recommendation.replicas)
                        )
                    elif recommendation.replicas < ctx.current_num_replicas:
                        if self._safe_to_scale_down(
                            ctx, snapshot, recommendation, default_desired
                        ):
                            learned_desired = float(ctx.current_num_replicas - 1)
                            reason = "safe_scale_down"
                        else:
                            # Once the capacity model has coverage, do not let the
                            # fallback policy bypass the stricter scale-down gate.
                            learned_desired = max(
                                default_desired, float(ctx.current_num_replicas)
                            )
                    else:
                        learned_desired = max(
                            default_desired, float(ctx.current_num_replicas)
                        )
                if self._all_workers_overloaded(ctx, snapshot):
                    learned_desired = max(
                        learned_desired, float(ctx.current_num_replicas + 1)
                    )
                    reason = "observed_slo_violation"

        desired = default_desired if self.advisory else learned_desired
        if reason != self._last_logged_reason:
            logger.info(
                "SLA autoscaling policy for %s changed state to %s "
                "(recommended=%s, advisory=%s).",
                ctx.deployment_id,
                reason,
                learned_desired,
                self.advisory,
            )
            self._last_logged_reason = reason
        self._record_diagnostics(ctx, learned_desired, recommendation)
        state = {
            "sla_advisory": self.advisory,
            "sla_reason": reason,
            "sla_recommended_replicas": learned_desired,
            "sla_model_ready": self._read_model() is not None,
            "sla_training_rows": len(self._buffer),
            "sla_training_buckets": self._buffer.num_buckets,
        }
        return desired, state


__all__ = ["SLAOnlineAutoscalingPolicy"]
