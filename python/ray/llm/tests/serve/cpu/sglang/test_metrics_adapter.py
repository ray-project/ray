"""Unit tests for the SGLang metrics adapter.

These tests exercise the adapter without booting an actual SGLang engine
or relying on a fully built Ray. The strategy is to feed the adapter fake
``prometheus_client`` Metric objects (the same shape ``CollectorRegistry
.collect()`` returns) and inspect what reaches a stubbed
``ray.util.metrics`` API. ``ray.init`` is not required.

The adapter module is loaded directly from its file path so the tests do
not trigger the heavy ``ray.llm._internal.serve`` package import chain
(which depends on built ``ray.serve`` protobufs). In CI the standard
import path works the same way; this loader is a local-dev convenience.
"""
import asyncio
import importlib.util
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[7]
_ADAPTER_PATH = (
    _REPO_ROOT / "python/ray/llm/_internal/serve/engines/sglang/metrics_adapter.py"
)
_spec = importlib.util.spec_from_file_location(
    "_test_metrics_adapter_under_test", _ADAPTER_PATH
)
adapter_mod = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = adapter_mod
_spec.loader.exec_module(adapter_mod)

SGLangMetricsAdapter = adapter_mod.SGLangMetricsAdapter
_rename_metric = adapter_mod._rename_metric
_read_scrape_interval = adapter_mod._read_scrape_interval


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeSample:
    def __init__(self, name: str, labels: Dict[str, str], value: float):
        self.name = name
        self.labels = labels
        self.value = value


class FakeFamily:
    def __init__(
        self,
        name: str,
        type_: str,
        documentation: str,
        samples: List[FakeSample],
    ):
        self.name = name
        self.type = type_
        self.documentation = documentation
        self.samples = samples


class FakeRegistry:
    def __init__(self, families: List[FakeFamily]):
        self._families = families

    def collect(self):
        return list(self._families)


class FakeRayMetric:
    """Stand-in for ``ray.util.metrics.{Counter, Gauge}``.

    Records ``inc`` / ``set`` calls so tests can assert on emission.
    """

    instances: List["FakeRayMetric"] = []

    def __init__(
        self, name: str, description: str = "", tag_keys: Tuple[str, ...] = ()
    ):
        self.name = name
        self.description = description
        self.tag_keys = tuple(tag_keys)
        self.events: List[Tuple[str, float, Dict[str, str]]] = []
        FakeRayMetric.instances.append(self)

    def inc(self, value: float = 1, tags: Optional[Dict[str, str]] = None):
        self.events.append(("inc", float(value), dict(tags or {})))

    def set(self, value: float, tags: Optional[Dict[str, str]] = None):
        self.events.append(("set", float(value), dict(tags or {})))


@pytest.fixture(autouse=True)
def _stub_ray_util_metrics(monkeypatch):
    """Replace ``ray.util.metrics`` so tests do not need ray.init()."""
    fake_module = type(sys)("ray.util.metrics")
    fake_module.Counter = FakeRayMetric
    fake_module.Gauge = FakeRayMetric
    fake_module.Histogram = FakeRayMetric
    monkeypatch.setitem(sys.modules, "ray.util.metrics", fake_module)
    FakeRayMetric.instances.clear()
    yield
    FakeRayMetric.instances.clear()


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestRenameMetric:
    def test_strips_sglang_prefix(self):
        assert _rename_metric("sglang:prompt_tokens_total") == (
            "ray_serve_sglang_prompt_tokens_total"
        )

    def test_passes_through_already_renamed(self):
        # If a sample.name comes in without the prefix, prepend the ray prefix.
        assert _rename_metric("token_usage") == "ray_serve_sglang_token_usage"

    def test_handles_bucket_suffix(self):
        assert _rename_metric("sglang:time_to_first_token_seconds_bucket") == (
            "ray_serve_sglang_time_to_first_token_seconds_bucket"
        )


class TestReadScrapeInterval:
    def test_default_when_unset(self, monkeypatch):
        monkeypatch.delenv("RAY_SGLANG_METRICS_SCRAPE_INTERVAL_SECONDS", raising=False)
        assert _read_scrape_interval(default=10.0) == 10.0

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("RAY_SGLANG_METRICS_SCRAPE_INTERVAL_SECONDS", "5")
        assert _read_scrape_interval(default=10.0) == 5.0

    def test_clamps_below_floor(self, monkeypatch):
        monkeypatch.setenv("RAY_SGLANG_METRICS_SCRAPE_INTERVAL_SECONDS", "0.1")
        assert _read_scrape_interval() == 1.0

    def test_invalid_falls_back(self, monkeypatch):
        monkeypatch.setenv("RAY_SGLANG_METRICS_SCRAPE_INTERVAL_SECONDS", "not-a-number")
        assert _read_scrape_interval(default=12.0) == 12.0


# ---------------------------------------------------------------------------
# claim_multiproc_dir
# ---------------------------------------------------------------------------


class TestClaimMultiprocDir:
    def test_creates_when_unset(self, monkeypatch, tmp_path):
        monkeypatch.delenv("PROMETHEUS_MULTIPROC_DIR", raising=False)
        path = SGLangMetricsAdapter.claim_multiproc_dir()
        assert os.path.isdir(path)
        assert os.environ["PROMETHEUS_MULTIPROC_DIR"] == path

    def test_reuses_existing_env(self, monkeypatch, tmp_path):
        target = tmp_path / "preset"
        monkeypatch.setenv("PROMETHEUS_MULTIPROC_DIR", str(target))
        path = SGLangMetricsAdapter.claim_multiproc_dir()
        assert path == str(target)
        assert os.path.isdir(path)


# ---------------------------------------------------------------------------
# Counter / gauge / histogram forwarding
# ---------------------------------------------------------------------------


def _scrape(adapter: SGLangMetricsAdapter, families: List[FakeFamily]) -> int:
    return adapter._scrape_once(FakeRegistry(families))


def _events_for(name: str) -> List[Tuple[str, float, Dict[str, str]]]:
    """Find events on the FakeRayMetric instance whose name == ``name``."""
    out = []
    for inst in FakeRayMetric.instances:
        if inst.name == name:
            out.extend(inst.events)
    return out


class TestCounterForwarding:
    def test_first_observation_is_baseline_only(self):
        adapter = SGLangMetricsAdapter(scrape_interval_seconds=10)
        family = FakeFamily(
            "sglang:prompt_tokens",
            "counter",
            "prefill tokens",
            [
                FakeSample(
                    "sglang:prompt_tokens_total",
                    {"model_name": "m", "tp_rank": "0"},
                    100.0,
                )
            ],
        )
        emitted = _scrape(adapter, [family])
        assert emitted == 0
        assert _events_for("ray_serve_sglang_prompt_tokens_total") == []

    def test_subsequent_scrape_emits_delta(self):
        adapter = SGLangMetricsAdapter(scrape_interval_seconds=10)
        labels = {"model_name": "m", "tp_rank": "0"}

        # Baseline.
        _scrape(
            adapter,
            [
                FakeFamily(
                    "sglang:prompt_tokens",
                    "counter",
                    "",
                    [FakeSample("sglang:prompt_tokens_total", labels, 100.0)],
                )
            ],
        )

        # Increase by 50.
        emitted = _scrape(
            adapter,
            [
                FakeFamily(
                    "sglang:prompt_tokens",
                    "counter",
                    "",
                    [FakeSample("sglang:prompt_tokens_total", labels, 150.0)],
                )
            ],
        )

        assert emitted == 1
        events = _events_for("ray_serve_sglang_prompt_tokens_total")
        assert events == [("inc", 50.0, {"model_name": "m", "tp_rank": "0"})]

    def test_counter_reset_does_not_emit_negative(self):
        adapter = SGLangMetricsAdapter(scrape_interval_seconds=10)
        labels = {"model_name": "m"}

        _scrape(
            adapter,
            [
                FakeFamily(
                    "sglang:requests",
                    "counter",
                    "",
                    [FakeSample("sglang:requests_total", labels, 200.0)],
                )
            ],
        )

        # Reset to a smaller value (engine restart).
        emitted = _scrape(
            adapter,
            [
                FakeFamily(
                    "sglang:requests",
                    "counter",
                    "",
                    [FakeSample("sglang:requests_total", labels, 5.0)],
                )
            ],
        )
        assert emitted == 0
        assert _events_for("ray_serve_sglang_requests_total") == []

        # Next observation that exceeds the new baseline emits normally.
        emitted = _scrape(
            adapter,
            [
                FakeFamily(
                    "sglang:requests",
                    "counter",
                    "",
                    [FakeSample("sglang:requests_total", labels, 12.0)],
                )
            ],
        )
        assert emitted == 1
        assert _events_for("ray_serve_sglang_requests_total") == [
            ("inc", 7.0, {"model_name": "m"}),
        ]


class TestGaugeForwarding:
    def test_gauge_passthrough(self):
        adapter = SGLangMetricsAdapter(scrape_interval_seconds=10)
        family = FakeFamily(
            "sglang:token_usage",
            "gauge",
            "KV cache usage",
            [
                FakeSample(
                    "sglang:token_usage",
                    {"model_name": "m", "tp_rank": "0"},
                    0.42,
                )
            ],
        )
        emitted = _scrape(adapter, [family])
        assert emitted == 1
        events = _events_for("ray_serve_sglang_token_usage")
        assert events == [("set", 0.42, {"model_name": "m", "tp_rank": "0"})]


class TestHistogramForwarding:
    def test_emits_bucket_sum_count_with_le_label(self):
        adapter = SGLangMetricsAdapter(scrape_interval_seconds=10)
        labels = {"model_name": "m"}
        family = FakeFamily(
            "sglang:time_to_first_token_seconds",
            "histogram",
            "TTFT",
            [
                FakeSample(
                    "sglang:time_to_first_token_seconds_bucket",
                    {**labels, "le": "0.1"},
                    254.0,
                ),
                FakeSample(
                    "sglang:time_to_first_token_seconds_bucket",
                    {**labels, "le": "0.5"},
                    1432.0,
                ),
                FakeSample(
                    "sglang:time_to_first_token_seconds_sum",
                    labels,
                    247.3,
                ),
                FakeSample(
                    "sglang:time_to_first_token_seconds_count",
                    labels,
                    1523.0,
                ),
            ],
        )
        emitted = _scrape(adapter, [family])

        # 4 samples — 2 buckets + sum + count.
        assert emitted == 4

        bucket_events = _events_for(
            "ray_serve_sglang_time_to_first_token_seconds_bucket"
        )
        assert ("set", 254.0, {"model_name": "m", "le": "0.1"}) in bucket_events
        assert ("set", 1432.0, {"model_name": "m", "le": "0.5"}) in bucket_events

        assert _events_for("ray_serve_sglang_time_to_first_token_seconds_sum") == [
            ("set", 247.3, {"model_name": "m"})
        ]
        assert _events_for("ray_serve_sglang_time_to_first_token_seconds_count") == [
            ("set", 1523.0, {"model_name": "m"})
        ]


class TestUnhandledFamilyTypes:
    def test_summary_skipped(self):
        adapter = SGLangMetricsAdapter(scrape_interval_seconds=10)
        family = FakeFamily(
            "sglang:something",
            "summary",
            "",
            [FakeSample("sglang:something", {}, 1.0)],
        )
        emitted = _scrape(adapter, [family])
        assert emitted == 0
        assert _events_for("ray_serve_sglang_something") == []

    def test_self_metric_family_is_skipped(self):
        adapter = SGLangMetricsAdapter(scrape_interval_seconds=10)
        family = FakeFamily(
            "ray_serve_sglang_adapter_scrapes_total",
            "counter",
            "",
            [FakeSample("ray_serve_sglang_adapter_scrapes_total", {}, 99.0)],
        )
        emitted = _scrape(adapter, [family])
        assert emitted == 0


# ---------------------------------------------------------------------------
# Schema mismatch
# ---------------------------------------------------------------------------


class TestLabelSchemaMismatch:
    def test_mismatched_label_keys_dropped(self):
        adapter = SGLangMetricsAdapter(scrape_interval_seconds=10)

        # First observation fixes tag_keys=("model_name",).
        _scrape(
            adapter,
            [
                FakeFamily(
                    "sglang:token_usage",
                    "gauge",
                    "",
                    [FakeSample("sglang:token_usage", {"model_name": "m"}, 0.1)],
                )
            ],
        )

        # Second observation adds tp_rank — schema mismatch.
        emitted = _scrape(
            adapter,
            [
                FakeFamily(
                    "sglang:token_usage",
                    "gauge",
                    "",
                    [
                        FakeSample(
                            "sglang:token_usage",
                            {"model_name": "m", "tp_rank": "0"},
                            0.5,
                        )
                    ],
                )
            ],
        )
        assert emitted == 0

        # Original event was emitted, second was dropped.
        events = _events_for("ray_serve_sglang_token_usage")
        assert len(events) == 1
        assert events[0] == ("set", 0.1, {"model_name": "m"})


# ---------------------------------------------------------------------------
# Self-metrics
# ---------------------------------------------------------------------------


class TestSelfMetrics:
    def test_scrape_iteration_records_outcome_and_emitted(self):
        adapter = SGLangMetricsAdapter(scrape_interval_seconds=10)
        labels = {"model_name": "m"}

        # Establish counter baseline first so the next scrape emits 1 sample.
        _scrape(
            adapter,
            [
                FakeFamily(
                    "sglang:prompt_tokens",
                    "counter",
                    "",
                    [FakeSample("sglang:prompt_tokens_total", labels, 10.0)],
                )
            ],
        )

        # Use the iteration helper so self-metrics get updated.
        adapter._scrape_iteration(
            FakeRegistry(
                [
                    FakeFamily(
                        "sglang:prompt_tokens",
                        "counter",
                        "",
                        [FakeSample("sglang:prompt_tokens_total", labels, 25.0)],
                    )
                ]
            )
        )

        scrapes = _events_for("ray_serve_sglang_adapter_scrapes_total")
        assert any(ev[0] == "inc" and ev[2] == {"outcome": "success"} for ev in scrapes)

        emitted_total = _events_for("ray_serve_sglang_adapter_emitted_samples_total")
        assert any(ev[0] == "inc" and ev[1] >= 1 for ev in emitted_total)

        last_scrape = _events_for("ray_serve_sglang_adapter_last_scrape_unix_seconds")
        assert last_scrape and last_scrape[-1][0] == "set"


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


class TestLifecycle:
    def test_start_idempotent(self):
        adapter = SGLangMetricsAdapter(scrape_interval_seconds=10)

        async def _drive():
            adapter.start()
            first_task = adapter._task
            adapter.start()
            second_task = adapter._task
            await adapter.stop()
            return first_task is second_task

        assert asyncio.run(_drive()) is True

    def test_stop_when_never_started(self):
        adapter = SGLangMetricsAdapter(scrape_interval_seconds=10)
        # Should not raise.
        asyncio.run(adapter.stop())


if __name__ == "__main__":
    sys.exit(pytest.main(["-xvs", __file__]))
