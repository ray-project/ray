"""Wiring for SGLang engine metrics on Ray's metric agent.

When ``log_engine_metrics: true`` is set on an SGLang-backed LLM deployment,
Ray Serve LLM injects SGLang's Ray metric backend
(``sglang.srt.observability.ray_wrappers``, available since sglang 0.5.15)
into the engine through the ``ServerArgs.stat_loggers`` dependency-injection
map. The engine's metrics collectors then emit directly through
``ray.util.metrics``, so SGLang engine metrics appear on Ray's Prometheus
endpoint and dashboard with Ray Serve's replica tags attached, the same
one-flag experience as the vLLM backend's ``RayPrometheusStatLogger``.

Process topology caveat: SGLang runs its scheduler and detokenizer in
engine-spawned subprocesses that are not Ray workers, and ``ray.util.metrics``
recording is a silent no-op outside a Ray worker. Request-level metrics
(``sglang:time_to_first_token_seconds``, ``sglang:inter_token_latency_seconds``,
``sglang:e2e_request_latency_seconds``, prompt/generation token counters, ...)
are recorded by the ``TokenizerMetricsCollector`` inside the replica actor
process and are exported. Scheduler-process gauges are exported only when the
schedulers themselves run as Ray actors (SGLang's ``RayEngine``).
"""

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

# First sglang release that ships sglang.srt.observability.ray_wrappers
# (sgl-project/sglang#26252).
MIN_SGLANG_VERSION_FOR_RAY_METRICS = "0.5.15"

# Role keys are the documented contract of sglang's ServerArgs.stat_loggers;
# values are the Ray-backed collector subclasses shipped by sglang.
_ROLE_TO_RAY_COLLECTOR = {
    "scheduler": "RaySchedulerMetricsCollector",
    "tokenizer": "RayTokenizerMetricsCollector",
    "storage": "RayStorageMetricsCollector",
    "radix_cache": "RayRadixCacheMetricsCollector",
    "expert_dispatch": "RayExpertDispatchCollector",
}

# The wrapper-class DI hooks shared by all sglang metrics collectors.
_WRAPPER_CLS_ATTRS = ("_counter_cls", "_gauge_cls", "_histogram_cls", "_summary_cls")

# Module attribute name under which each shimmed collector class is exposed,
# keyed by role. SGLang launches its schedulers with multiprocessing "spawn"
# and pickles ServerArgs (including the stat_loggers classes) into the child,
# so these classes must be resolvable by module path; the module-level
# __getattr__ below rebuilds them on demand in freshly spawned processes.
_ROLE_TO_SHIM_ATTR = {
    role: f"{cls_name}AsciiDoc" for role, cls_name in _ROLE_TO_RAY_COLLECTOR.items()
}
_SHIM_ATTR_TO_ROLE = {attr: role for role, attr in _ROLE_TO_SHIM_ATTR.items()}

# Cache of the shimmed collector classes, keyed by role.
_shim_classes: Dict[str, type] = {}


def _ascii_fold(text: Any) -> Any:
    """Replace non-ASCII characters so Ray's metric layer accepts the string.

    ``ray._raylet`` metric constructors call ``description.encode("ascii")``,
    and several sglang metric descriptions contain non-ASCII punctuation
    (e.g. the em dash in ``weight_load_duration_seconds``), which would crash
    the engine's scheduler at startup.
    """
    if isinstance(text, str):
        try:
            text.encode("ascii")
        except UnicodeEncodeError:
            return text.encode("ascii", "replace").decode()
    return text


def _with_ascii_documentation(wrapper_cls: type) -> type:
    """Subclass a sglang Ray metric wrapper to ASCII-fold its documentation.

    TODO: drop this shim once sglang's ray_wrappers sanitizes descriptions the
    way it already sanitizes metric names, and that release becomes the
    minimum supported version here.
    """

    class _AsciiDocWrapper(wrapper_cls):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            if "documentation" in kwargs:
                kwargs["documentation"] = _ascii_fold(kwargs["documentation"])
            elif len(args) >= 2:
                args = (args[0], _ascii_fold(args[1]), *args[2:])
            super().__init__(*args, **kwargs)

    _AsciiDocWrapper.__name__ = wrapper_cls.__name__
    _AsciiDocWrapper.__qualname__ = wrapper_cls.__qualname__
    return _AsciiDocWrapper


def build_ray_stat_loggers() -> Dict[str, type]:
    """Build the ServerArgs.stat_loggers map that routes SGLang's metrics
    collectors through ``ray.util.metrics``.

    Returns:
        Mapping from each collector role to a subclass of the Ray-backed
        collector shipped by sglang, with its metric wrapper classes wrapped
        to ASCII-fold descriptions (see ``_with_ascii_documentation``).

    Raises:
        ImportError: if the installed sglang does not ship the Ray metric
            backend (requires sglang >= 0.5.15).
    """
    try:
        from sglang.srt.observability import ray_wrappers
    except ImportError as e:
        try:
            from sglang.version import __version__ as installed
        except ImportError:
            installed = "unknown"
        raise ImportError(
            "log_engine_metrics=True requires SGLang's Ray metric backend "
            "(sglang.srt.observability.ray_wrappers), which ships in sglang "
            f">= {MIN_SGLANG_VERSION_FOR_RAY_METRICS} (installed: {installed}). "
            f"Upgrade with `pip install 'sglang[all]>="
            f"{MIN_SGLANG_VERSION_FOR_RAY_METRICS}'` or set "
            "log_engine_metrics=False."
        ) from e

    if not _shim_classes:
        for role, cls_name in _ROLE_TO_RAY_COLLECTOR.items():
            collector_cls = getattr(ray_wrappers, cls_name)
            overrides = {}
            for attr in _WRAPPER_CLS_ATTRS:
                wrapper_cls = getattr(collector_cls, attr, None)
                if wrapper_cls is not None:
                    overrides[attr] = _with_ascii_documentation(wrapper_cls)
            shim_attr = _ROLE_TO_SHIM_ATTR[role]
            shim_cls = type(shim_attr, (collector_cls,), overrides)
            shim_cls.__module__ = __name__
            shim_cls.__qualname__ = shim_attr
            _shim_classes[role] = shim_cls
    return dict(_shim_classes)


def __getattr__(name: str) -> Any:
    """Resolve shimmed collector classes by name (PEP 562).

    Unpickling ServerArgs.stat_loggers in a spawned SGLang scheduler process
    looks these classes up as module attributes before any caller has invoked
    build_ray_stat_loggers() in that process.

    Args:
        name: attribute being resolved on this module.

    Returns:
        The shimmed collector class registered under ``name``.

    Raises:
        AttributeError: if ``name`` is not one of the shimmed collector
            class names.
    """
    role = _SHIM_ATTR_TO_ROLE.get(name)
    if role is not None:
        return build_ray_stat_loggers()[role]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def configure_sglang_engine_metrics(engine_kwargs: Dict[str, Any]) -> None:
    """Mutate ``engine_kwargs`` so the SGLang engine emits its metrics through
    ``ray.util.metrics``.

    Enables SGLang's metrics collection unless the user explicitly disabled it,
    and installs the Ray-backed collectors on ``stat_loggers``. Entries the
    user already registered on ``stat_loggers`` take precedence per role.

    If the installed sglang predates the Ray metric backend, the deployment
    still boots: a warning is logged and ``engine_kwargs`` is left untouched,
    so serving works without engine metrics. ``log_engine_metrics`` defaults
    to True, and a missing observability feature should not take the engine
    down with it.
    """
    try:
        stat_loggers = build_ray_stat_loggers()
    except ImportError as e:
        logger.warning("%s Engine metrics are disabled for this deployment.", e)
        return

    if engine_kwargs.get("enable_metrics") is False:
        logger.warning(
            "log_engine_metrics=True but engine_kwargs.enable_metrics=False; "
            "SGLang's collectors will not record samples and no engine metrics "
            "will reach Ray. Drop one of the two flags to silence this."
        )
    else:
        engine_kwargs.setdefault("enable_metrics", True)

    stat_loggers.update(engine_kwargs.get("stat_loggers") or {})
    engine_kwargs["stat_loggers"] = stat_loggers
