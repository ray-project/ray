import sys
import time

import pytest

from ray.serve._private.common import DeploymentID
from ray.serve.config import AutoscalingContext
from ray.serve.llm.autoscaling import SLOAutoscalingPolicy


def _ctx(current=2, total_requests=0.0, target=None, now=1000.0, state=None):
    target = current if target is None else target
    return AutoscalingContext(
        deployment_id=DeploymentID(name="d", app_name="a"),
        deployment_name="d",
        app_name="a",
        current_num_replicas=current,
        target_num_replicas=target,
        running_replicas=[],
        total_num_requests=total_requests,
        total_queued_requests=0.0,
        aggregated_metrics={},
        raw_metrics={},
        capacity_adjusted_min_replicas=0,
        capacity_adjusted_max_replicas=100,
        policy_state=state or {},
        last_scale_up_time=None,
        last_scale_down_time=None,
        current_time=now,
        config=None,
        total_pending_async_requests=0,
    )


def _policy(ttft=None, hit=None, inflight=None, **kw):
    """Build a policy with its metric cache preset; no background fetch thread."""
    pol = SLOAutoscalingPolicy(
        ttft_target_s=2.0,
        model_id="m",
        prometheus_address="localhost:9090",
        **kw,
    )
    pol._started = True
    vals = {}
    if ttft is not None:
        vals[pol.ttft_query] = ttft
    if hit is not None:
        vals[pol.hit_rate_query] = hit
    if inflight is not None:
        vals[pol.inflight_query] = inflight
    pol._cache.values = vals
    pol._cache.timestamp = time.monotonic()
    return pol


class TestInnerLoop:
    def test_scales_on_concurrency(self):
        # seed capacity 8, 24 ongoing -> ceil(24/8) = 3
        dec, _ = _policy()(_ctx(current=2, total_requests=24.0))
        assert dec == 3.0

    def test_idle_scales_to_zero(self):
        dec, _ = _policy()(_ctx(current=3, total_requests=0.0))
        assert dec == 0.0

    def test_engine_queue_drives_scale_up(self):
        # Serve sees 8 ongoing (8/8 = 1), but the engine reports 24 in-flight
        # (24/8 = 3): the larger view wins.
        dec, _ = _policy(inflight=24.0)(_ctx(current=2, total_requests=8.0))
        assert dec == 3.0

    def test_decision_independent_of_target(self):
        # Load-based absolute count, not a target-relative step: no scaling-factor
        # inversion regardless of target.
        a, _ = _policy()(_ctx(current=2, total_requests=24.0, target=2))
        b, _ = _policy()(_ctx(current=2, total_requests=24.0, target=99))
        assert a == b == 3.0


class TestSelfTuning:
    def test_high_ttft_lowers_capacity_and_scales_up(self):
        dec, state = _policy(ttft=4.0, hit=0.5)(_ctx(current=2, total_requests=16.0))
        assert state["c_concurrency"] < 8.0
        assert dec >= 3.0

    def test_low_ttft_raises_capacity(self):
        dec, state = _policy(ttft=0.5, hit=0.5)(_ctx(current=2, total_requests=16.0))
        assert state["c_concurrency"] > 8.0

    def test_deadband_holds_capacity(self):
        # 1.9s is within 10% of the 2.0s goal.
        _, state = _policy(ttft=1.9, hit=0.5)(_ctx(current=2, total_requests=16.0))
        assert state["c_concurrency"] == 8.0

    def test_rampup_freezes_tuner(self):
        _, state = _policy(ttft=9.0, hit=0.5)(
            _ctx(current=1, total_requests=16.0, target=4)
        )
        assert state["c_concurrency"] == 8.0

    def test_idle_freezes_tuner(self):
        _, state = _policy(ttft=9.0, hit=0.5)(_ctx(current=2, total_requests=0.0))
        assert state["c_concurrency"] == 8.0

    def test_hit_rate_swing_freezes_tuner(self):
        prior = {"last_tune_s": 0.0, "last_hit_rate": 0.2, "c_concurrency": 8.0}
        _, state = _policy(ttft=9.0, hit=0.9)(
            _ctx(current=2, total_requests=16.0, state=prior)
        )
        assert state["c_concurrency"] == 8.0

    def test_interval_gates_tuning(self):
        prior = {"last_tune_s": 990.0, "c_concurrency": 8.0}
        # now=1000, interval default 30 -> only 10s elapsed -> no tune
        _, state = _policy(ttft=9.0, hit=0.5)(
            _ctx(current=2, total_requests=16.0, now=1000.0, state=prior)
        )
        assert state["c_concurrency"] == 8.0


class TestDegradation:
    def test_no_prometheus_data_uses_concurrency(self):
        pol = _policy()  # empty cache values -> metrics {}
        dec, state = pol(_ctx(current=2, total_requests=16.0))
        assert dec == 2.0  # 16/8
        assert state["c_concurrency"] == 8.0


class TestConstruction:
    def test_requires_model_id(self):
        with pytest.raises(ValueError, match="model_id"):
            SLOAutoscalingPolicy(ttft_target_s=2.0, prometheus_address="x")

    def test_queries_scoped_to_model(self):
        pol = SLOAutoscalingPolicy(
            ttft_target_s=2.0, model_id="my-org/m", prometheus_address="x"
        )
        assert 'model_name="my-org/m"' in pol.ttft_query
        assert 'model_name="my-org/m"' in pol.inflight_query


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
