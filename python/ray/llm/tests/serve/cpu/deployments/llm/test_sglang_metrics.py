from ray.llm._internal.serve.engines.sglang import metrics as sglang_metrics


class _FakeCounter:
    def __init__(self, name, description="", tag_keys=None):
        self.name = name
        self.description = description
        self.tag_keys = tag_keys
        self.records = []

    def inc(self, value=1.0, tags=None):
        self.records.append(("inc", value, tags))


class _FakeGauge:
    def __init__(self, name, description="", tag_keys=None):
        self.name = name
        self.description = description
        self.tag_keys = tag_keys
        self.records = []

    def set(self, value, tags=None):
        self.records.append(("set", value, tags))


class _FakeHistogram:
    def __init__(self, name, description="", boundaries=None, tag_keys=None):
        self.name = name
        self.description = description
        self.boundaries = boundaries
        self.tag_keys = tag_keys
        self.records = []

    def observe(self, value, tags=None):
        self.records.append(("observe", value, tags))


def test_ray_prometheus_counter_matches_prometheus_label_shape(monkeypatch):
    monkeypatch.setattr(sglang_metrics.RayPrometheusCounter, "_ray_metric_cls", _FakeCounter)

    counter = sglang_metrics.RayPrometheusCounter(
        "sglang:num_requests_total",
        "Total requests",
        labelnames=("model_name", "tp_rank"),
    )
    counter.labels(model_name="llama", tp_rank=0).inc(3)

    assert counter._metric.name == "sglang_num_requests_total"
    assert counter._metric.tag_keys == ("model_name", "tp_rank")
    assert counter._metric.records == [
        ("inc", 3, {"model_name": "llama", "tp_rank": "0"})
    ]


def test_ray_prometheus_gauge_and_histogram(monkeypatch):
    monkeypatch.setattr(sglang_metrics.RayPrometheusGauge, "_ray_metric_cls", _FakeGauge)
    monkeypatch.setattr(
        sglang_metrics.RayPrometheusHistogram,
        "_ray_metric_cls",
        _FakeHistogram,
    )

    gauge = sglang_metrics.RayPrometheusGauge(
        "sglang:num_queue_reqs",
        labelnames=("model_name",),
        multiprocess_mode="mostrecent",
    )
    gauge.labels("llama").set(7)

    histogram = sglang_metrics.RayPrometheusHistogram(
        "sglang:queue_time_seconds",
        labelnames=("model_name",),
        buckets=(0, 0.001, 0.01, 1),
    )
    histogram_child = histogram.labels(model_name="llama")
    histogram_child.observe(0.25)

    assert gauge._metric.records == [("set", 7, {"model_name": "llama"})]
    assert histogram._metric.boundaries == [0.001, 0.01, 1]
    assert histogram._metric.records == [("observe", 0.25, {"model_name": "llama"})]
    assert len(histogram_child._buckets) == 5


def test_configure_sglang_engine_metrics_preserves_user_stat_loggers(monkeypatch):
    user_tokenizer_logger = object()

    monkeypatch.setattr(
        sglang_metrics,
        "build_sglang_ray_stat_loggers",
        lambda: {"scheduler": object(), "tokenizer": object()},
    )

    engine_kwargs = {"stat_loggers": {"tokenizer": user_tokenizer_logger}}
    sglang_metrics.configure_sglang_engine_metrics(engine_kwargs)

    assert engine_kwargs["enable_metrics"] is True
    assert "scheduler" in engine_kwargs["stat_loggers"]
    assert engine_kwargs["stat_loggers"]["tokenizer"] is user_tokenizer_logger
