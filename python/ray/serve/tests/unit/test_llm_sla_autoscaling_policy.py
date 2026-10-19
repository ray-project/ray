import math
import threading
import time

import numpy as np
import pytest

from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve.autoscaling_policy import (
    PrometheusSample,
    PrometheusScalar,
    PrometheusVector,
)
from ray.serve.config import AutoscalingConfig, AutoscalingContext
from ray.serve.experimental.llm_sla_autoscaling_policy import (
    SLAOnlineAutoscalingPolicy,
    _build_prometheus_queries,
    _CapacityRecommendation,
    _features_for_replicas,
    _fit_model_snapshot,
    _MetricsSnapshot,
    _ModelSnapshot,
    _PrometheusSLAMetricsSource,
    _ReplayBuffer,
    _TrainingRequest,
    _TrainingRow,
)

pytest.importorskip("sklearn")


class _FakeMetricsSource:
    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.bound = []

    def bind(self, ctx):
        self.bound.append(ctx.deployment_id)

    def read(self):
        return self.snapshot


class _Predictor:
    def __init__(self, multiplier):
        self.multiplier = multiplier

    def predict(self, matrix):
        return matrix[:, 0] * self.multiplier


def _snapshot(*, worker_values=10.0, offered_rps=40.0):
    return _MetricsSnapshot(
        offered_rps=offered_rps,
        mean_isl=100.0,
        mean_osl=20.0,
        cache_hit_rate=0.25,
        mean_ttft_ms=100.0,
        mean_itl_ms=10.0,
        request_samples=100.0,
        token_samples=1000.0,
        worker_ttft_ms={"r1": worker_values, "r2": worker_values},
        worker_itl_ms={"r1": worker_values, "r2": worker_values},
    )


def _context(
    *,
    now=100.0,
    current=2,
    target=2,
    total_requests=0.0,
    queued=0.0,
    minimum=1,
    maximum=5,
):
    deployment_id = DeploymentID(name="llm", app_name="app")
    replicas = [
        ReplicaID(unique_id=f"r{i + 1}", deployment_id=deployment_id)
        for i in range(current)
    ]
    return AutoscalingContext(
        deployment_id=deployment_id,
        deployment_name=deployment_id.name,
        app_name=deployment_id.app_name,
        current_num_replicas=current,
        target_num_replicas=target,
        running_replicas=replicas,
        total_num_requests=total_requests,
        total_queued_requests=queued,
        aggregated_metrics=None,
        raw_metrics=None,
        capacity_adjusted_min_replicas=minimum,
        capacity_adjusted_max_replicas=maximum,
        policy_state={},
        last_scale_up_time=None,
        last_scale_down_time=None,
        current_time=now,
        config=AutoscalingConfig(
            min_replicas=minimum,
            max_replicas=maximum,
            target_ongoing_requests=10,
        ),
    )


def _policy(source, **kwargs):
    return SLAOnlineAutoscalingPolicy(
        ttft_ms=300,
        itl_ms=50,
        observation_window_s=1,
        topology_stabilization_s=1,
        retrain_interval_s=1,
        decision_interval_s=1,
        min_training_rows=5,
        min_candidate_bucket_samples=3,
        rows_per_bucket=8,
        max_training_rows=32,
        _metrics_source=source,
        **kwargs,
    )


def test_prometheus_queries_are_deployment_scoped_and_escaped():
    queries = _build_prometheus_queries(
        deployment='llm"deployment', application="app", window_s=30
    )
    assert 'deployment="llm\\"deployment"' in queries.offered_rps
    assert 'application="app"' in queries.offered_rps
    assert "ray_serve_num_router_requests_total" in queries.offered_rps
    assert "ray_vllm_time_to_first_token_seconds" in queries.worker_ttft
    assert "group_left(replica)" in queries.worker_ttft


def test_prometheus_source_builds_weighted_deployment_snapshot():
    source = _PrometheusSLAMetricsSource(
        observation_window_s=30,
        prometheus_address=None,
        fetch_interval_s=5,
        cache_ttl_s=15,
    )
    source.bind(_context())
    queries = source._queries
    assert queries is not None

    def vector(*values):
        return PrometheusVector(
            samples=tuple(
                PrometheusSample(labels={"replica": replica}, value=value, timestamp=1)
                for replica, value in values
            )
        )

    source._cache.results = {
        queries.offered_rps: PrometheusScalar(value=12, timestamp=1),
        queries.mean_isl: PrometheusScalar(value=100, timestamp=1),
        queries.mean_osl: PrometheusScalar(value=20, timestamp=1),
        queries.cache_hit_rate: PrometheusScalar(value=0.25, timestamp=1),
        queries.worker_ttft: vector(("r1", 0.1), ("r2", 0.2)),
        queries.worker_ttft_samples: vector(("r1", 1), ("r2", 3)),
        queries.worker_itl: vector(("r1", 0.01), ("r2", 0.03)),
        queries.worker_itl_samples: vector(("r1", 1), ("r2", 3)),
        queries.generated_tokens: PrometheusScalar(value=1000, timestamp=1),
    }
    source._cache.timestamp = time.monotonic()

    snapshot = source.read()
    assert snapshot is not None
    assert snapshot.mean_ttft_ms == 175
    assert snapshot.mean_itl_ms == 25
    assert snapshot.request_samples == 4
    assert snapshot.token_samples == 1000


def test_features_use_offered_work_and_candidate_replica_count():
    snapshot = _snapshot(offered_rps=12)
    features = _features_for_replicas(snapshot, queued_requests=4, replicas=3)
    assert features == (
        4.0,
        300.0,
        80.0,
        400 / 3,
        100.0,
        20.0,
        0.25,
    )


def test_replay_buffer_retains_recent_bucket_diversity():
    buffer = _ReplayBuffer(max_rows=4, rows_per_bucket=2)
    for index, prefill in enumerate((1.0, 2.0, 100.0, 200.0, 4.0)):
        features = (1.0, prefill, 1.0, 0.0, 10.0, 10.0, 0.0)
        buffer.add(
            _TrainingRow(
                features=features,
                ttft_ms=10,
                itl_ms=1,
                ttft_weight=1,
                itl_weight=1,
                timestamp=float(index),
                replica_count=1,
            )
        )
    assert len(buffer) == 4
    assert buffer.num_buckets == 3
    assert [row.timestamp for row in buffer.rows()] == [1.0, 2.0, 3.0, 4.0]


def test_hgb_models_fit_two_weighted_targets_with_monotonic_constraints():
    from sklearn.ensemble import HistGradientBoostingRegressor

    rows = []
    for index in range(40):
        load = float(index + 1)
        rows.append(
            _TrainingRow(
                features=(load, load * 10, load * 2, load, 100, 20, 0.0),
                ttft_ms=100 + load * 5,
                itl_ms=10 + load,
                ttft_weight=index + 1,
                itl_weight=(index + 1) * 10,
                timestamp=float(index),
                replica_count=1,
            )
        )
    model = _fit_model_snapshot(
        HistGradientBoostingRegressor,
        _TrainingRequest(rows=tuple(rows), requested_at=100),
    )
    low = np.asarray([[5, 50, 10, 5, 100, 20, 0.0]])
    high = np.asarray([[35, 350, 70, 35, 100, 20, 0.0]])
    assert model.ttft_model.predict(high)[0] >= model.ttft_model.predict(low)[0]
    assert model.itl_model.predict(high)[0] >= model.itl_model.predict(low)[0]


def test_advisory_mode_reports_guard_but_returns_default_policy():
    source = _FakeMetricsSource(_snapshot(worker_values=500))
    policy = _policy(source, advisory=True)
    desired, state = policy(_context())
    assert desired == 0
    assert state["sla_recommended_replicas"] == 3
    assert state["sla_reason"] == "observed_slo_violation"


def test_active_observed_latency_guard_requires_all_ready_workers():
    source = _FakeMetricsSource(_snapshot(worker_values=500))
    policy = _policy(source, advisory=False)
    desired, state = policy(_context())
    assert desired == 3
    assert state["sla_reason"] == "observed_slo_violation"

    source.snapshot = _MetricsSnapshot(
        **{
            **_snapshot(worker_values=500).__dict__,
            "worker_ttft_ms": {"r1": 500},
            "worker_itl_ms": {"r1": 100},
        }
    )
    desired, _ = policy(_context(now=102))
    assert desired == 0


def test_pending_scale_operation_holds_existing_target():
    source = _FakeMetricsSource(_snapshot(worker_values=10))
    policy = _policy(source, advisory=False)
    desired, state = policy(_context(current=2, target=4))
    assert desired == 4
    assert state["sla_reason"] == "scaling_in_progress"


def test_stable_windows_only_are_added_to_training_buffer():
    source = _FakeMetricsSource(_snapshot())
    policy = _policy(source, advisory=True)
    policy(_context(now=0))
    assert len(policy._buffer) == 0
    policy(_context(now=0.5))
    assert len(policy._buffer) == 0
    policy(_context(now=1.0))
    assert len(policy._buffer) == 1
    policy(_context(now=1.5, target=3))
    assert len(policy._buffer) == 1


def test_capacity_search_vectorizes_supported_candidates():
    source = _FakeMetricsSource(_snapshot())
    policy = _policy(source, advisory=False)
    ctx = _context(minimum=1, maximum=4)
    snapshot = source.snapshot
    for replicas in range(1, 5):
        features = _features_for_replicas(snapshot, 0, replicas)
        for offset in range(3):
            policy._buffer.add(
                _TrainingRow(
                    features=features,
                    ttft_ms=100,
                    itl_ms=10,
                    ttft_weight=10,
                    itl_weight=100,
                    timestamp=float(replicas * 10 + offset),
                    replica_count=replicas,
                )
            )
    model = _ModelSnapshot(
        ttft_model=_Predictor(10),
        itl_model=_Predictor(1),
        trained_at=1,
        num_rows=len(policy._buffer),
    )
    recommendation, reason = policy._compute_capacity_recommendation(
        ctx, snapshot, model
    )
    assert reason == "capacity"
    assert recommendation == _CapacityRecommendation(
        replicas=2, predicted_ttft_ms=200, predicted_itl_ms=20
    )


def test_capacity_search_abstains_when_candidate_range_is_unbounded():
    source = _FakeMetricsSource(_snapshot())
    policy = _policy(source, advisory=False, max_candidate_evaluations=2)
    model = _ModelSnapshot(
        ttft_model=_Predictor(1),
        itl_model=_Predictor(1),
        trained_at=1,
        num_rows=1,
    )
    recommendation, reason = policy._compute_capacity_recommendation(
        _context(minimum=1, maximum=4), source.snapshot, model
    )
    assert recommendation is None
    assert reason == "candidate_range_too_large"


def test_capacity_coverage_prevents_default_policy_from_bypassing_scale_down_gate(
    monkeypatch,
):
    source = _FakeMetricsSource(_snapshot(worker_values=10))
    policy = _policy(source, advisory=False)
    monkeypatch.setattr(
        policy,
        "_get_capacity_recommendation",
        lambda ctx, snapshot, now: (
            _CapacityRecommendation(2, 100, 10),
            "capacity",
        ),
    )
    desired, _ = policy(_context(current=2, target=2, total_requests=0))
    assert desired == 2


def test_refit_is_enqueued_without_waiting_for_training(monkeypatch):
    source = _FakeMetricsSource(_snapshot())
    policy = _policy(source)
    gate = threading.Event()

    class SlowEstimator:
        def __init__(self, **kwargs):
            pass

        def fit(self, features, targets, sample_weight):
            gate.wait(timeout=2)
            return self

        def predict(self, matrix):
            return np.zeros(len(matrix))

    policy._trainer_state.estimator_cls = SlowEstimator
    for index in range(5):
        policy._buffer.add(
            _TrainingRow(
                features=(1, 1, 1, 1, 1, 1, 0),
                ttft_ms=1,
                itl_ms=1,
                ttft_weight=1,
                itl_weight=1,
                timestamp=index,
                replica_count=1,
            )
        )
    started = time.monotonic()
    policy._request_refit(10)
    assert time.monotonic() - started < 0.1
    gate.set()


def test_scale_down_requires_safe_history_and_headroom():
    source = _FakeMetricsSource(_snapshot(worker_values=10))
    policy = _policy(source, advisory=False)
    ctx = _context(current=2, target=2, queued=0)
    candidate_features = _features_for_replicas(source.snapshot, 0, 1)
    for index in range(3):
        policy._buffer.add(
            _TrainingRow(
                features=candidate_features,
                ttft_ms=100,
                itl_ms=10,
                ttft_weight=10,
                itl_weight=100,
                timestamp=index,
                replica_count=1,
            )
        )
    policy._last_observation_queue = 0
    assert policy._safe_to_scale_down(
        ctx,
        source.snapshot,
        _CapacityRecommendation(1, 200, 20),
        default_desired=0,
    )
    assert not policy._safe_to_scale_down(
        ctx,
        source.snapshot,
        _CapacityRecommendation(1, 250, 20),
        default_desired=0,
    )


def test_feature_values_are_finite_for_zero_load():
    snapshot = _snapshot(offered_rps=0)
    assert all(
        math.isfinite(value)
        for value in _features_for_replicas(snapshot, queued_requests=0, replicas=1)
    )
