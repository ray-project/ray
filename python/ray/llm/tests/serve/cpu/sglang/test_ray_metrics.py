"""Unit tests for the SGLang -> ray.util.metrics stat_loggers wiring.

These tests exercise the wiring without sglang installed: fake
``sglang.srt.observability.ray_wrappers`` modules are injected into
``sys.modules``. The module under test is loaded directly from its file path
so the tests do not trigger the heavy ``ray.llm._internal.serve`` package
import chain (which depends on built ``ray.serve`` protobufs). In CI the
standard import path works the same way; this loader is a local-dev
convenience.
"""
import importlib.util
import sys
import types
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[7]
_MODULE_PATH = (
    _REPO_ROOT / "python/ray/llm/_internal/serve/engines/sglang/ray_metrics.py"
)
_spec = importlib.util.spec_from_file_location(
    "_test_ray_metrics_under_test", _MODULE_PATH
)
ray_metrics = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = ray_metrics
_spec.loader.exec_module(ray_metrics)


_SGLANG_MODULES = (
    "sglang",
    "sglang.version",
    "sglang.srt",
    "sglang.srt.observability",
    "sglang.srt.observability.ray_wrappers",
)

_EXPECTED_ROLES = {
    "scheduler": "RaySchedulerMetricsCollector",
    "tokenizer": "RayTokenizerMetricsCollector",
    "storage": "RayStorageMetricsCollector",
    "radix_cache": "RayRadixCacheMetricsCollector",
    "expert_dispatch": "RayExpertDispatchCollector",
}


class _FakeWrapperMetric:
    """Stand-in for sglang's Ray metric wrappers; records init args."""

    def __init__(self, *args, **kwargs):
        self.init_args = args
        self.init_kwargs = kwargs


def _install_fake_sglang(monkeypatch, with_ray_wrappers: bool = True):
    """Install a fake sglang module tree; returns the ray_wrappers module."""
    for name in _SGLANG_MODULES:
        monkeypatch.delitem(sys.modules, name, raising=False)
    # The shim-class cache is keyed off whichever (fake) sglang module built
    # it; reset it so each test sees classes built from its own fakes.
    ray_metrics._shim_classes.clear()

    sglang = types.ModuleType("sglang")
    version = types.ModuleType("sglang.version")
    version.__version__ = "0.5.15"
    sglang.version = version
    srt = types.ModuleType("sglang.srt")
    observability = types.ModuleType("sglang.srt.observability")

    monkeypatch.setitem(sys.modules, "sglang", sglang)
    monkeypatch.setitem(sys.modules, "sglang.version", version)
    monkeypatch.setitem(sys.modules, "sglang.srt", srt)
    monkeypatch.setitem(sys.modules, "sglang.srt.observability", observability)

    if not with_ray_wrappers:
        # Leave the parent packages importable but the ray_wrappers submodule
        # missing, mimicking sglang < 0.5.15.
        version.__version__ = "0.5.14"
        return None

    wrappers = types.ModuleType("sglang.srt.observability.ray_wrappers")
    for cls_name in _EXPECTED_ROLES.values():
        # Mirror sglang's collector shape: wrapper classes hang off the
        # _xxx_cls DI attributes; unused hooks stay None.
        collector_cls = type(
            cls_name,
            (),
            {
                "_counter_cls": _FakeWrapperMetric,
                "_gauge_cls": _FakeWrapperMetric,
                "_histogram_cls": None,
                "_summary_cls": None,
            },
        )
        setattr(wrappers, cls_name, collector_cls)
    observability.ray_wrappers = wrappers
    monkeypatch.setitem(sys.modules, "sglang.srt.observability.ray_wrappers", wrappers)
    return wrappers


class TestBuildRayStatLoggers:
    def test_returns_all_five_roles(self, monkeypatch):
        wrappers = _install_fake_sglang(monkeypatch)
        loggers = ray_metrics.build_ray_stat_loggers()
        assert set(loggers) == set(_EXPECTED_ROLES)
        for role, cls_name in _EXPECTED_ROLES.items():
            assert issubclass(loggers[role], getattr(wrappers, cls_name))

    def test_wrapper_hooks_are_shimmed_and_none_hooks_stay_none(self, monkeypatch):
        _install_fake_sglang(monkeypatch)
        loggers = ray_metrics.build_ray_stat_loggers()
        for collector_cls in loggers.values():
            assert issubclass(collector_cls._counter_cls, _FakeWrapperMetric)
            assert collector_cls._counter_cls is not _FakeWrapperMetric
            assert collector_cls._histogram_cls is None
            assert collector_cls._summary_cls is None

    def test_non_ascii_documentation_is_folded(self, monkeypatch):
        # ray._raylet metric constructors encode descriptions as ASCII;
        # sglang 0.5.15 ships descriptions with em dashes that would
        # otherwise crash the scheduler at startup.
        _install_fake_sglang(monkeypatch)
        loggers = ray_metrics.build_ray_stat_loggers()
        gauge_cls = loggers["scheduler"]._gauge_cls

        by_kwarg = gauge_cls(name="sglang:weight_load", documentation="load — seconds")
        by_kwarg.init_kwargs["documentation"].encode("ascii")

        by_pos = gauge_cls("sglang:weight_load", "load — seconds", ["a"])
        by_pos.init_args[1].encode("ascii")

        ascii_doc = gauge_cls("sglang:ok", "plain ascii")
        assert ascii_doc.init_args[1] == "plain ascii"

    def test_collector_classes_are_picklable(self, monkeypatch):
        # SGLang spawns its scheduler with multiprocessing "spawn" and pickles
        # ServerArgs (including the stat_loggers classes) into the child, so
        # the shimmed classes must pickle by module path.
        import pickle

        _install_fake_sglang(monkeypatch)
        loggers = ray_metrics.build_ray_stat_loggers()
        for role, cls in loggers.items():
            restored = pickle.loads(pickle.dumps(cls))
            assert restored is cls, role

    def test_old_sglang_raises_informative_import_error(self, monkeypatch):
        _install_fake_sglang(monkeypatch, with_ray_wrappers=False)
        with pytest.raises(ImportError) as exc_info:
            ray_metrics.build_ray_stat_loggers()
        message = str(exc_info.value)
        assert ray_metrics.MIN_SGLANG_VERSION_FOR_RAY_METRICS in message
        assert "0.5.14" in message  # the detected installed version
        assert "log_engine_metrics" in message


class TestConfigureSGLangEngineMetrics:
    def test_defaults_enable_metrics_and_installs_loggers(self, monkeypatch):
        wrappers = _install_fake_sglang(monkeypatch)
        engine_kwargs = {}
        ray_metrics.configure_sglang_engine_metrics(engine_kwargs)
        assert engine_kwargs["enable_metrics"] is True
        assert set(engine_kwargs["stat_loggers"]) == set(_EXPECTED_ROLES)
        assert issubclass(
            engine_kwargs["stat_loggers"]["tokenizer"],
            wrappers.RayTokenizerMetricsCollector,
        )

    def test_explicit_enable_metrics_false_is_kept_and_warned(
        self, monkeypatch, caplog
    ):
        _install_fake_sglang(monkeypatch)
        engine_kwargs = {"enable_metrics": False}
        with caplog.at_level("WARNING"):
            ray_metrics.configure_sglang_engine_metrics(engine_kwargs)
        assert engine_kwargs["enable_metrics"] is False
        assert any("enable_metrics=False" in r.message for r in caplog.records)
        # The DI map is still installed; it is inert while metrics are off.
        assert set(engine_kwargs["stat_loggers"]) == set(_EXPECTED_ROLES)

    def test_explicit_enable_metrics_true_is_kept(self, monkeypatch):
        _install_fake_sglang(monkeypatch)
        engine_kwargs = {"enable_metrics": True}
        ray_metrics.configure_sglang_engine_metrics(engine_kwargs)
        assert engine_kwargs["enable_metrics"] is True

    def test_user_stat_loggers_take_precedence_per_role(self, monkeypatch):
        wrappers = _install_fake_sglang(monkeypatch)

        class UserSchedulerCollector:
            pass

        engine_kwargs = {"stat_loggers": {"scheduler": UserSchedulerCollector}}
        ray_metrics.configure_sglang_engine_metrics(engine_kwargs)
        loggers = engine_kwargs["stat_loggers"]
        assert loggers["scheduler"] is UserSchedulerCollector
        assert issubclass(loggers["tokenizer"], wrappers.RayTokenizerMetricsCollector)
        assert set(loggers) == set(_EXPECTED_ROLES)

    def test_old_sglang_warns_and_leaves_engine_kwargs_untouched(
        self, monkeypatch, caplog
    ):
        # log_engine_metrics defaults to True, so an sglang without the Ray
        # metric backend must degrade to a warning instead of failing the
        # replica at startup.
        _install_fake_sglang(monkeypatch, with_ray_wrappers=False)
        engine_kwargs = {}
        with caplog.at_level("WARNING"):
            ray_metrics.configure_sglang_engine_metrics(engine_kwargs)
        assert engine_kwargs == {}
        assert any(
            ray_metrics.MIN_SGLANG_VERSION_FOR_RAY_METRICS in r.message
            for r in caplog.records
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
