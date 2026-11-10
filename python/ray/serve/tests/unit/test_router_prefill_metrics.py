import asyncio
import math
import sys
from unittest.mock import Mock, patch

import pytest
import pytest_asyncio

from ray.serve._private.common import DeploymentHandleSource, DeploymentID
from ray.serve._private.router import RouterMetricsManager
from ray.serve._private.test_utils import FakeCounter, FakeGauge

PREFILL = "outstanding_effective_prefill_tokens"


@pytest_asyncio.fixture
async def managers():
    managers = []

    def create(handle):
        tags = ("deployment", "application", "handle", "actor_id")
        manager = RouterMetricsManager(
            DeploymentID("model", "app"),
            handle,
            "actor",
            DeploymentHandleSource.PROXY,
            Mock(),
            FakeCounter(tag_keys=(*tags, "route")),
            FakeGauge(tag_keys=tags),
            FakeGauge(tag_keys=tags),
            asyncio.get_running_loop(),
        )
        managers.append(manager)
        return manager

    yield create
    for manager in managers:
        await manager.shutdown()


@pytest.mark.asyncio
async def test_router_prefill_gauge_is_lazy_and_records_raw_snapshots(managers):
    manager = managers("router-1")
    with patch("ray.serve._private.router.metrics.Gauge") as gauge_class:
        manager.set_custom_router_metrics({"other_metric": 10})
        gauge_class.assert_not_called()
        for value in (12000, 3000, 0):
            manager.set_custom_router_metrics({PREFILL: value})
            gauge_class.return_value.set.assert_called_with(value)
            assert manager._latest_custom_router_metrics[PREFILL] == value
        gauge_class.assert_called_once()
        assert gauge_class.call_args.args == ("serve_router_uncached_prefill_tokens",)
        assert gauge_class.call_args.kwargs["tag_keys"] == (
            "deployment",
            "application",
            "handle",
            "actor_id",
        )
        gauge_class.return_value.set_default_tags.assert_called_once_with(
            {
                "deployment": "model",
                "application": "app",
                "handle": "router-1",
                "actor_id": "actor",
            }
        )


@pytest.mark.asyncio
async def test_router_prefill_gauges_keep_handles_separate(managers):
    first, second = managers("router-1"), managers("router-2")
    with patch("ray.serve._private.router.metrics.Gauge") as gauge_class:
        first_gauge, second_gauge = Mock(), Mock()
        gauge_class.side_effect = [first_gauge, second_gauge]
        first.set_custom_router_metrics({PREFILL: 10000})
        second.set_custom_router_metrics({PREFILL: 2000})
        first_gauge.set.assert_called_once_with(10000)
        second_gauge.set.assert_called_once_with(2000)
        assert first_gauge.set_default_tags.call_args.args[0]["handle"] == "router-1"
        assert second_gauge.set_default_tags.call_args.args[0]["handle"] == "router-2"


@pytest.mark.asyncio
async def test_router_prefill_gauge_clears_and_recovers(managers):
    manager = managers("router")
    with patch("ray.serve._private.router.metrics.Gauge") as gauge_class:
        manager.set_custom_router_metrics({PREFILL: 1000})
        gauge = gauge_class.return_value
        manager.set_custom_router_metrics({})
        assert math.isnan(gauge.set.call_args.args[0])
        manager.set_custom_router_metrics({PREFILL: 0})
        gauge.set.assert_called_with(0)
        await manager.shutdown()
        assert math.isnan(gauge.set.call_args.args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [math.nan, math.inf, "bad"])
async def test_invalid_router_snapshot_does_not_change_gauge(managers, invalid):
    manager = managers("router")
    with patch("ray.serve._private.router.metrics.Gauge") as gauge_class:
        manager.set_custom_router_metrics({PREFILL: 1000})
        with pytest.raises(TypeError):
            manager.set_custom_router_metrics({PREFILL: 0, "invalid": invalid})
        gauge_class.return_value.set.assert_called_once_with(1000)
        assert manager._latest_custom_router_metrics == {PREFILL: 1000}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
