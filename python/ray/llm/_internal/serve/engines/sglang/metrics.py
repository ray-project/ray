"""Ray metrics bridge for SGLang's embedded engine collectors."""

from __future__ import annotations

import math
from typing import Any, Dict, Iterable, Mapping, Optional, Type


DEFAULT_HISTOGRAM_BOUNDARIES = (0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10)


def _to_tags(labels: Mapping[str, Any]) -> Dict[str, str]:
    return {key: "" if value is None else str(value) for key, value in labels.items()}


def _metric_name(name: str) -> str:
    # SGLang exposes prometheus series like "sglang:num_running_reqs".
    # Ray accepts this today but warns that ":" will be rejected later.
    return name.replace(":", "_")


def _to_ray_histogram_boundaries(values: Iterable[Any]) -> tuple[float, ...]:
    return tuple(
        float(value)
        for value in values
        if isinstance(value, (int, float)) and value > 0 and math.isfinite(value)
    )


class _RayMetricChild:
    def __init__(
        self,
        metric: Any,
        labels: Mapping[str, Any],
        bucket_count: int = 1,
    ):
        self._metric = metric
        self._labels = _to_tags(labels)
        self._sum = _NoopAccumulator()
        self._buckets = [_NoopAccumulator() for _ in range(bucket_count)]

    def inc(self, value: float = 1.0) -> None:
        self._metric.inc(value, tags=self._labels)

    def set(self, value: Optional[float]) -> None:
        self._metric.set(value, tags=self._labels)

    def observe(self, value: float) -> None:
        self._metric.observe(value, tags=self._labels)


class _NoopAccumulator:
    def inc(self, value: float = 1.0) -> None:
        return


class _RayPrometheusMetric:
    _ray_metric_cls = None

    def __init__(
        self,
        name: str,
        documentation: str = "",
        labelnames: Optional[Iterable[str]] = None,
        **kwargs: Any,
    ) -> None:
        self._name = _metric_name(name)
        self._documentation = documentation
        self._labelnames = tuple(labelnames or ())
        self._metric = self._build_metric(kwargs)

    def _build_metric(self, kwargs: Mapping[str, Any]) -> Any:
        metric_cls = self._ray_metric_cls
        if metric_cls is None:
            from ray.util import metrics

            metric_cls = self._default_ray_metric_cls(metrics)
        return metric_cls(
            self._name,
            description=self._documentation,
            tag_keys=self._labelnames,
        )

    @staticmethod
    def _default_ray_metric_cls(metrics_module: Any) -> Type[Any]:
        raise NotImplementedError

    def labels(self, *labelvalues: Any, **labelkwargs: Any) -> _RayMetricChild:
        if labelvalues:
            if len(labelvalues) != len(self._labelnames):
                raise ValueError(
                    f"Expected {len(self._labelnames)} labels, got {len(labelvalues)}"
                )
            labels = dict(zip(self._labelnames, labelvalues))
            labels.update(labelkwargs)
        else:
            labels = labelkwargs
        return _RayMetricChild(self._metric, labels)


class RayPrometheusCounter(_RayPrometheusMetric):
    @staticmethod
    def _default_ray_metric_cls(metrics_module: Any) -> Type[Any]:
        return metrics_module.Counter


class RayPrometheusGauge(_RayPrometheusMetric):
    @staticmethod
    def _default_ray_metric_cls(metrics_module: Any) -> Type[Any]:
        return metrics_module.Gauge


class RayPrometheusHistogram(_RayPrometheusMetric):
    def __init__(
        self,
        name: str,
        documentation: str = "",
        labelnames: Optional[Iterable[str]] = None,
        **kwargs: Any,
    ) -> None:
        ray_boundaries = _to_ray_histogram_boundaries(
            kwargs.get("buckets", kwargs.get("boundaries", ()))
        )
        if not ray_boundaries:
            ray_boundaries = DEFAULT_HISTOGRAM_BOUNDARIES
        self._ray_boundaries = ray_boundaries
        super().__init__(name, documentation, labelnames, **kwargs)

    def _build_metric(self, kwargs: Mapping[str, Any]) -> Any:
        metric_cls = self._ray_metric_cls
        if metric_cls is None:
            from ray.util import metrics

            metric_cls = metrics.Histogram
        return metric_cls(
            self._name,
            description=self._documentation,
            boundaries=list(self._ray_boundaries),
            tag_keys=self._labelnames,
        )

    def labels(self, *labelvalues: Any, **labelkwargs: Any) -> _RayMetricChild:
        child = super().labels(*labelvalues, **labelkwargs)
        child._buckets = [
            _NoopAccumulator() for _ in range(len(self._ray_boundaries) + 1)
        ]
        return child

    @staticmethod
    def _default_ray_metric_cls(metrics_module: Any) -> Type[Any]:
        return metrics_module.Histogram


class RayPrometheusSummary(RayPrometheusHistogram):
    pass


def build_sglang_ray_stat_loggers() -> Dict[str, Type[Any]]:
    from sglang.srt.observability.metrics_collector import (
        STAT_LOGGER_ROLE_EXPERT_DISPATCH,
        STAT_LOGGER_ROLE_RADIX_CACHE,
        STAT_LOGGER_ROLE_SCHEDULER,
        STAT_LOGGER_ROLE_STORAGE,
        STAT_LOGGER_ROLE_TOKENIZER,
        ExpertDispatchCollector,
        RadixCacheMetricsCollector,
        SchedulerMetricsCollector,
        StorageMetricsCollector,
        TokenizerMetricsCollector,
    )

    class RaySGLangSchedulerMetricsCollector(SchedulerMetricsCollector):
        _counter_cls = RayPrometheusCounter
        _gauge_cls = RayPrometheusGauge
        _histogram_cls = RayPrometheusHistogram
        _summary_cls = RayPrometheusSummary

    class RaySGLangStorageMetricsCollector(StorageMetricsCollector):
        _counter_cls = RayPrometheusCounter
        _gauge_cls = RayPrometheusGauge
        _histogram_cls = RayPrometheusHistogram
        _summary_cls = RayPrometheusSummary

    class RaySGLangTokenizerMetricsCollector(TokenizerMetricsCollector):
        _counter_cls = RayPrometheusCounter
        _gauge_cls = RayPrometheusGauge
        _histogram_cls = RayPrometheusHistogram
        _summary_cls = RayPrometheusSummary

        def observe_inter_token_latency(
            self,
            labels: Dict[str, str],
            internval: float,
            num_new_tokens: int,
        ) -> None:
            if num_new_tokens <= 0:
                return
            histogram = self.histogram_inter_token_latency.labels(**labels)
            for _ in range(num_new_tokens):
                histogram.observe(internval)

    class RaySGLangRadixCacheMetricsCollector(RadixCacheMetricsCollector):
        _counter_cls = RayPrometheusCounter
        _gauge_cls = RayPrometheusGauge
        _histogram_cls = RayPrometheusHistogram
        _summary_cls = RayPrometheusSummary

    class RaySGLangExpertDispatchCollector(ExpertDispatchCollector):
        _counter_cls = RayPrometheusCounter
        _gauge_cls = RayPrometheusGauge
        _histogram_cls = RayPrometheusHistogram
        _summary_cls = RayPrometheusSummary

    return {
        STAT_LOGGER_ROLE_SCHEDULER: RaySGLangSchedulerMetricsCollector,
        STAT_LOGGER_ROLE_TOKENIZER: RaySGLangTokenizerMetricsCollector,
        STAT_LOGGER_ROLE_STORAGE: RaySGLangStorageMetricsCollector,
        STAT_LOGGER_ROLE_RADIX_CACHE: RaySGLangRadixCacheMetricsCollector,
        STAT_LOGGER_ROLE_EXPERT_DISPATCH: RaySGLangExpertDispatchCollector,
    }


def configure_sglang_engine_metrics(engine_kwargs: Dict[str, Any]) -> None:
    """Enable SGLang metrics and route supported collectors through Ray metrics."""

    engine_kwargs["enable_metrics"] = True
    existing_loggers = engine_kwargs.get("stat_loggers") or {}
    ray_loggers = build_sglang_ray_stat_loggers()
    ray_loggers.update(existing_loggers)
    engine_kwargs["stat_loggers"] = ray_loggers
