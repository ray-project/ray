import sys
import time

import pytest

from ray.serve._private.common import DeploymentID
from ray.serve.config import AutoscalingContext
from ray.serve.llm.autoscaling import TTFTAutoscalingPolicy


def _ctx(current=2, total_requests=0.0):
    return AutoscalingContext(
        deployment_id=DeploymentID(name="d", app_name="a"),
        deployment_name="d",
        app_name="a",
        current_num_replicas=current,
        target_num_replicas=current,
        running_replicas=[],
        total_num_requests=total_requests,
        total_queued_requests=0.0,
        aggregated_metrics={},
        raw_metrics={},
        capacity_adjusted_min_replicas=0,
        capacity_adjusted_max_replicas=10,
        policy_state={},
        last_scale_up_time=None,
        last_scale_down_time=None,
        current_time=None,
        config=None,
        total_pending_async_requests=0,
    )


def _policy_with_ttft(value, ttft_target_s=1.0, idle_threshold=1.0):
    """A policy whose cache holds ``value`` for its own query; no fetch thread."""
    pol = TTFTAutoscalingPolicy(
        ttft_target_s=ttft_target_s,
        idle_threshold=idle_threshold,
        model_id="m",
        prometheus_address="localhost:9090",
    )
    pol._started = True  # skip the background fetch thread
    pol._cache.values = {pol.query: value}
    pol._cache.timestamp = time.monotonic()
    return pol


class TestTTFTPolicyDecisions:
    def test_no_metrics_holds(self):
        pol = TTFTAutoscalingPolicy(model_id="m")
        pol._started = True
        assert pol(_ctx(current=2)) == (2.0, {"signal": "no_metrics"})

    def test_missing_query_is_no_data(self):
        pol = TTFTAutoscalingPolicy(model_id="m", prometheus_address="x")
        pol._started = True
        pol._cache.values = {"some_other_query": 1.0}
        pol._cache.timestamp = time.monotonic()
        dec, state = pol(_ctx(current=2))
        assert dec == 2.0 and state["signal"] == "no_data"

    def test_scale_up_above_target(self):
        dec, state = _policy_with_ttft(5.0)(_ctx(current=2))
        assert dec == 3.0 and state["signal"] == "scale_up"

    def test_scale_down_when_idle(self):
        dec, state = _policy_with_ttft(0.1)(_ctx(current=3, total_requests=0.0))
        assert dec == 2.0 and state["signal"] == "scale_down"

    def test_steady_when_busy(self):
        dec, state = _policy_with_ttft(0.1)(_ctx(current=3, total_requests=9.0))
        assert dec == 3.0 and state["signal"] == "steady"

    def test_scale_down_floors_at_zero(self):
        dec, state = _policy_with_ttft(0.1)(_ctx(current=1, total_requests=0.0))
        assert dec == 0.0 and state["signal"] == "scale_down"


class TestQueryScoping:
    def test_model_id_scopes_query(self):
        pol = TTFTAutoscalingPolicy(model_id="my-org/m", prometheus_address="x")
        assert 'model_name="my-org/m"' in pol.query

    def test_explicit_query_overrides_model_id(self):
        pol = TTFTAutoscalingPolicy(
            model_id="my-org/m", query="custom_query", prometheus_address="x"
        )
        assert pol.query == "custom_query"

    def test_requires_model_id_or_query(self):
        with pytest.raises(ValueError, match="model_id"):
            TTFTAutoscalingPolicy(prometheus_address="x")


class TestModelIdAutoFill:
    """get_deployment_options fills model_id from the LLMConfig."""

    def _config(self, policy_function=TTFTAutoscalingPolicy, **policy_kwargs):
        from ray.serve.config import AutoscalingConfig, AutoscalingPolicy

        return AutoscalingConfig(
            min_replicas=1,
            max_replicas=4,
            policy=AutoscalingPolicy(
                policy_function=policy_function, policy_kwargs=policy_kwargs
            ),
        )

    def test_fills_model_id(self):
        from ray.llm._internal.serve.core.server.llm_server import (
            _fill_autoscaling_model_id,
        )

        config = self._config(ttft_target_s=2.0)
        _fill_autoscaling_model_id({"autoscaling_config": config}, "my-org/m")
        assert config.policy.policy_kwargs["model_id"] == "my-org/m"

    def test_keeps_explicit_model_id(self):
        from ray.llm._internal.serve.core.server.llm_server import (
            _fill_autoscaling_model_id,
        )

        config = self._config(model_id="explicit")
        _fill_autoscaling_model_id({"autoscaling_config": config}, "my-org/m")
        assert config.policy.policy_kwargs["model_id"] == "explicit"

    def test_ignores_policy_without_model_id(self):
        from ray.llm._internal.serve.core.server.llm_server import (
            _fill_autoscaling_model_id,
        )

        def plain_policy(ctx):
            return 1.0, {}

        config = self._config(policy_function=plain_policy)
        _fill_autoscaling_model_id({"autoscaling_config": config}, "m")
        assert "model_id" not in config.policy.policy_kwargs

    def test_no_autoscaling_config_is_noop(self):
        from ray.llm._internal.serve.core.server.llm_server import (
            _fill_autoscaling_model_id,
        )

        _fill_autoscaling_model_id({}, "m")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
