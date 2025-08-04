from typing import Optional, Type, cast

import prometheus_client
from vllm.config import SpeculativeConfig, SupportsMetricsInfo, VllmConfig
from vllm.engine.metrics import (
    _RayCounterWrapper,
    _RayGaugeWrapper,
    _RayHistogramWrapper,
)
from vllm.v1.engine import FinishReason
from vllm.v1.metrics.loggers import StatLoggerBase, build_1_2_5_buckets
from vllm.v1.metrics.stats import IterationStats, SchedulerStats
from vllm.v1.spec_decode.metrics import SpecDecodingStats

from ray.util import metrics as ray_metrics


class SpecDecodingProm:
    """Record spec decoding metrics in Prometheus.

    The acceptance rate can be calculated using a PromQL query:

      rate(vllm:spec_decode_num_accepted_tokens_total[$interval]) /
      rate(vllm:spec_decode_num_draft_tokens_total[$interval])

    The mean acceptance length can be calculated using:

      rate(vllm:spec_decode_num_accepted_tokens_total[$interval]) /
      rate(vllm:spec_decode_num_drafts[$interval])

    A per-position acceptance rate vector can be computed using

      vllm:spec_decode_num_accepted_tokens_per_pos[$interval] /
      vllm:spec_decode_num_drafts[$interval]
    """

    _counter_cls = prometheus_client.Counter

    def __init__(
        self,
        speculative_config: Optional[SpeculativeConfig],
        labelnames: list[str],
        labelvalues: list[str],
    ):
        self.spec_decoding_enabled = speculative_config is not None
        if not self.spec_decoding_enabled:
            return

        self.counter_spec_decode_num_drafts = self._counter_cls(
            name="vllm:spec_decode_num_drafts_total",
            documentation="Number of spec decoding drafts.",
            labelnames=labelnames,
        ).labels(*labelvalues)
        self.counter_spec_decode_num_draft_tokens = self._counter_cls(
            name="vllm:spec_decode_num_draft_tokens_total",
            documentation="Number of draft tokens.",
            labelnames=labelnames,
        ).labels(*labelvalues)
        self.counter_spec_decode_num_accepted_tokens = self._counter_cls(
            name="vllm:spec_decode_num_accepted_tokens_total",
            documentation="Number of accepted tokens.",
            labelnames=labelnames,
        ).labels(*labelvalues)

        assert speculative_config is not None
        num_spec_tokens = (
            speculative_config.num_speculative_tokens
            if self.spec_decoding_enabled
            else 0
        )
        pos_labelnames = labelnames + ["position"]
        base_counter = self._counter_cls(
            name="vllm:spec_decode_num_accepted_tokens_per_pos",
            documentation="Accepted tokens per draft position.",
            labelnames=pos_labelnames,
        )
        self.counter_spec_decode_num_accepted_tokens_per_pos: list[
            prometheus_client.Counter
        ] = []
        for pos in range(num_spec_tokens):
            pos_labelvalues = labelvalues + [str(pos)]
            self.counter_spec_decode_num_accepted_tokens_per_pos.append(
                base_counter.labels(*pos_labelvalues)
            )

    def observe(self, spec_decoding_stats: SpecDecodingStats):
        if not self.spec_decoding_enabled:
            return
        self.counter_spec_decode_num_drafts.inc(spec_decoding_stats.num_drafts)
        self.counter_spec_decode_num_draft_tokens.inc(
            spec_decoding_stats.num_draft_tokens
        )
        self.counter_spec_decode_num_accepted_tokens.inc(
            spec_decoding_stats.num_accepted_tokens
        )
        for pos, counter in enumerate(
            self.counter_spec_decode_num_accepted_tokens_per_pos
        ):
            counter.inc(spec_decoding_stats.num_accepted_tokens_per_pos[pos])


class Metrics:
    """
    vLLM uses a multiprocessing-based frontend for the OpenAI server.
    This means that we need to run prometheus_client in multiprocessing mode
    See https://prometheus.github.io/client_python/multiprocess/ for more
    details on limitations.
    """

    _gauge_cls = prometheus_client.Gauge
    _counter_cls = prometheus_client.Counter
    _histogram_cls = prometheus_client.Histogram
    _spec_decoding_cls = SpecDecodingProm

    def __init__(self, vllm_config: VllmConfig, engine_index: int = 0):
        self._unregister_vllm_metrics()

        # Use this flag to hide metrics that were deprecated in
        # a previous release and which will be removed future
        self.show_hidden_metrics = vllm_config.observability_config.show_hidden_metrics

        labels = {
            "model_name": vllm_config.model_config.served_model_name,
            "engine": str(engine_index),
        }
        labelnames = list(labels.keys())

        max_model_len = vllm_config.model_config.max_model_len

        self.spec_decoding_prom = self._spec_decoding_cls(
            vllm_config.speculative_config, labelnames, labels.values()
        )

        #
        # Scheduler state
        #
        self.gauge_scheduler_running = self._gauge_cls(
            name="vllm:num_requests_running",
            documentation="Number of requests in model execution batches.",
            labelnames=labelnames,
        ).labels(**labels)

        self.gauge_scheduler_waiting = self._gauge_cls(
            name="vllm:num_requests_waiting",
            documentation="Number of requests waiting to be processed.",
            labelnames=labelnames,
        ).labels(**labels)

        #
        # GPU cache
        #
        self.gauge_gpu_cache_usage = self._gauge_cls(
            name="vllm:gpu_cache_usage_perc",
            documentation="GPU KV-cache usage. 1 means 100 percent usage.",
            labelnames=labelnames,
        ).labels(**labels)

        self.counter_gpu_prefix_cache_queries = self._counter_cls(
            name="vllm:gpu_prefix_cache_queries",
            documentation="GPU prefix cache queries, in terms of number of queried blocks.",
            labelnames=labelnames,
        ).labels(**labels)

        self.counter_gpu_prefix_cache_hits = self._counter_cls(
            name="vllm:gpu_prefix_cache_hits",
            documentation="GPU prefix cache hits, in terms of number of cached blocks.",
            labelnames=labelnames,
        ).labels(**labels)

        #
        # Counters
        #
        self.counter_num_preempted_reqs = self._counter_cls(
            name="vllm:num_preemptions_total",
            documentation="Cumulative number of preemption from the engine.",
            labelnames=labelnames,
        ).labels(**labels)

        self.counter_prompt_tokens = self._counter_cls(
            name="vllm:prompt_tokens_total",
            documentation="Number of prefill tokens processed.",
            labelnames=labelnames,
        ).labels(**labels)

        self.counter_generation_tokens = self._counter_cls(
            name="vllm:generation_tokens_total",
            documentation="Number of generation tokens processed.",
            labelnames=labelnames,
        ).labels(**labels)

        self.counter_request_success: dict[FinishReason, prometheus_client.Counter] = {}
        counter_request_success_base = self._counter_cls(
            name="vllm:request_success_total",
            documentation="Count of successfully processed requests.",
            labelnames=labelnames + ["finished_reason"],
        )

        for reason in FinishReason:
            request_success_labels = {"finished_reason": str(reason), **labels}
            self.counter_request_success[reason] = counter_request_success_base.labels(
                **request_success_labels
            )

        #
        # Histograms of counts
        #
        self.histogram_num_prompt_tokens_request = self._histogram_cls(
            name="vllm:request_prompt_tokens",
            documentation="Number of prefill tokens processed.",
            buckets=build_1_2_5_buckets(max_model_len),
            labelnames=labelnames,
        ).labels(**labels)

        self.histogram_num_generation_tokens_request = self._histogram_cls(
            name="vllm:request_generation_tokens",
            documentation="Number of generation tokens processed.",
            buckets=build_1_2_5_buckets(max_model_len),
            labelnames=labelnames,
        ).labels(**labels)

        self.histogram_iteration_tokens = self._histogram_cls(
            name="vllm:iteration_tokens_total",
            documentation="Histogram of number of tokens per engine_step.",
            buckets=[1, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384],
            labelnames=labelnames,
        ).labels(**labels)

        self.histogram_max_num_generation_tokens_request = self._histogram_cls(
            name="vllm:request_max_num_generation_tokens",
            documentation="Histogram of maximum number of requested generation tokens.",
            buckets=build_1_2_5_buckets(max_model_len),
            labelnames=labelnames,
        ).labels(**labels)

        self.histogram_n_request = self._histogram_cls(
            name="vllm:request_params_n",
            documentation="Histogram of the n request parameter.",
            buckets=[1, 2, 5, 10, 20],
            labelnames=labelnames,
        ).labels(**labels)

        self.histogram_max_tokens_request = self._histogram_cls(
            name="vllm:request_params_max_tokens",
            documentation="Histogram of the max_tokens request parameter.",
            buckets=build_1_2_5_buckets(max_model_len),
            labelnames=labelnames,
        ).labels(**labels)

        #
        # Histogram of timing intervals
        #
        self.histogram_time_to_first_token = self._histogram_cls(
            name="vllm:time_to_first_token_seconds",
            documentation="Histogram of time to first token in seconds.",
            buckets=[
                0.001,
                0.005,
                0.01,
                0.02,
                0.04,
                0.06,
                0.08,
                0.1,
                0.25,
                0.5,
                0.75,
                1.0,
                2.5,
                5.0,
                7.5,
                10.0,
                20.0,
                40.0,
                80.0,
                160.0,
                640.0,
                2560.0,
            ],
            labelnames=labelnames,
        ).labels(**labels)

        self.histogram_time_per_output_token = self._histogram_cls(
            name="vllm:time_per_output_token_seconds",
            documentation="Histogram of time per output token in seconds.",
            buckets=[
                0.01,
                0.025,
                0.05,
                0.075,
                0.1,
                0.15,
                0.2,
                0.3,
                0.4,
                0.5,
                0.75,
                1.0,
                2.5,
                5.0,
                7.5,
                10.0,
                20.0,
                40.0,
                80.0,
            ],
            labelnames=labelnames,
        ).labels(**labels)

        request_latency_buckets = [
            0.3,
            0.5,
            0.8,
            1.0,
            1.5,
            2.0,
            2.5,
            5.0,
            10.0,
            15.0,
            20.0,
            30.0,
            40.0,
            50.0,
            60.0,
            120.0,
            240.0,
            480.0,
            960.0,
            1920.0,
            7680.0,
        ]
        self.histogram_e2e_time_request = self._histogram_cls(
            name="vllm:e2e_request_latency_seconds",
            documentation="Histogram of e2e request latency in seconds.",
            buckets=request_latency_buckets,
            labelnames=labelnames,
        ).labels(**labels)
        self.histogram_queue_time_request = self._histogram_cls(
            name="vllm:request_queue_time_seconds",
            documentation="Histogram of time spent in WAITING phase for request.",
            buckets=request_latency_buckets,
            labelnames=labelnames,
        ).labels(**labels)
        self.histogram_inference_time_request = self._histogram_cls(
            name="vllm:request_inference_time_seconds",
            documentation="Histogram of time spent in RUNNING phase for request.",
            buckets=request_latency_buckets,
            labelnames=labelnames,
        ).labels(**labels)
        self.histogram_prefill_time_request = self._histogram_cls(
            name="vllm:request_prefill_time_seconds",
            documentation="Histogram of time spent in PREFILL phase for request.",
            buckets=request_latency_buckets,
            labelnames=labelnames,
        ).labels(**labels)
        self.histogram_decode_time_request = self._histogram_cls(
            name="vllm:request_decode_time_seconds",
            documentation="Histogram of time spent in DECODE phase for request.",
            buckets=request_latency_buckets,
            labelnames=labelnames,
        ).labels(**labels)

        #
        # LoRA metrics
        #
        self.gauge_lora_info: Optional[prometheus_client.Gauge] = None
        if vllm_config.lora_config is not None:
            self.labelname_max_lora = "max_lora"
            self.labelname_waiting_lora_adapters = "waiting_lora_adapters"
            self.labelname_running_lora_adapters = "running_lora_adapters"
            self.max_lora = vllm_config.lora_config.max_loras
            self.gauge_lora_info = self._gauge_cls(
                name="vllm:lora_requests_info",
                documentation="Running stats on lora requests.",
                labelnames=[
                    self.labelname_max_lora,
                    self.labelname_waiting_lora_adapters,
                    self.labelname_running_lora_adapters,
                ],
            )

    @staticmethod
    def _unregister_vllm_metrics():
        # Unregister any existing vLLM collectors (for CI/CD
        for collector in list(prometheus_client.REGISTRY._collector_to_names):
            if hasattr(collector, "_name") and "vllm" in collector._name:
                prometheus_client.REGISTRY.unregister(collector)


class RaySpecDecodingProm(SpecDecodingProm):
    """
    RaySpecDecodingProm is used by RayMetrics to log to Ray metrics.
    Provides the same metrics as SpecDecodingProm but uses Ray's util.metrics library.
    """

    _counter_cls: Type[prometheus_client.Counter] = cast(
        Type[prometheus_client.Counter], _RayCounterWrapper
    )


class RayMetrics(Metrics):
    """
    RayMetrics is used by RayPrometheusStatLogger to log to Ray metrics.
    Provides the same metrics as Metrics but uses Ray's util.metrics library.
    """

    _gauge_cls: Type[prometheus_client.Gauge] = cast(
        Type[prometheus_client.Gauge], _RayGaugeWrapper
    )
    _counter_cls: Type[prometheus_client.Counter] = cast(
        Type[prometheus_client.Counter], _RayCounterWrapper
    )
    _histogram_cls: Type[prometheus_client.Histogram] = cast(
        Type[prometheus_client.Histogram], _RayHistogramWrapper
    )
    _spec_decoding_cls: Type[SpecDecodingProm] = cast(
        Type[SpecDecodingProm], RaySpecDecodingProm
    )

    def __init__(self, vllm_config: VllmConfig, engine_index: int = 0):
        if ray_metrics is None:
            raise ImportError("RayMetrics requires Ray to be installed.")
        super().__init__(vllm_config, engine_index)

    def _unregister_vllm_metrics(self) -> None:
        # No-op on purpose
        pass


# TODO(seiji): remove this whole file once we bump to vLLM that includes
# https://github.com/vllm-project/vllm/pull/19113
class PrometheusStatLogger(StatLoggerBase):
    _metrics_cls = Metrics

    def __init__(self, vllm_config: VllmConfig, engine_index: int = 0):
        self.metrics = self._metrics_cls(
            vllm_config=vllm_config, engine_index=engine_index
        )
        self.vllm_config = vllm_config
        #
        # Cache config info metric
        #
        self.log_metrics_info("cache_config", vllm_config.cache_config)

    def log_metrics_info(self, type: str, config_obj: SupportsMetricsInfo):
        metrics_info = config_obj.metrics_info()

        name, documentation = None, None
        if type == "cache_config":
            name = "vllm:cache_config_info"
            documentation = "Information of the LLMEngine CacheConfig"
        assert name is not None, f"Unknown metrics info type {type}"

        # Info type metrics are syntactic sugar for a gauge permanently set to 1
        # Since prometheus multiprocessing mode does not support Info, emulate
        # info here with a gauge.
        info_gauge = self._metrics_cls._gauge_cls(
            name=name, documentation=documentation, labelnames=metrics_info.keys()
        ).labels(**metrics_info)
        info_gauge.set(1)

    def record(
        self, scheduler_stats: SchedulerStats, iteration_stats: Optional[IterationStats]
    ):
        """Log to prometheus."""
        self.metrics.gauge_scheduler_running.set(scheduler_stats.num_running_reqs)
        self.metrics.gauge_scheduler_waiting.set(scheduler_stats.num_waiting_reqs)

        # https://github.com/vllm-project/vllm/pull/18354 (part of vllm 0.9.2)
        # renamed gpu_cache_usage to kv_cache_usage.
        kv_cache_usage = (
            scheduler_stats.kv_cache_usage
            if hasattr(scheduler_stats, "kv_cache_usage")
            else scheduler_stats.gpu_cache_usage
        )
        self.metrics.gauge_gpu_cache_usage.set(kv_cache_usage)

        self.metrics.counter_gpu_prefix_cache_queries.inc(
            scheduler_stats.prefix_cache_stats.queries
        )
        self.metrics.counter_gpu_prefix_cache_hits.inc(
            scheduler_stats.prefix_cache_stats.hits
        )

        if scheduler_stats.spec_decoding_stats is not None:
            self.metrics.spec_decoding_prom.observe(scheduler_stats.spec_decoding_stats)

        if iteration_stats is None:
            return

        self.metrics.counter_num_preempted_reqs.inc(iteration_stats.num_preempted_reqs)
        self.metrics.counter_prompt_tokens.inc(iteration_stats.num_prompt_tokens)
        self.metrics.counter_generation_tokens.inc(
            iteration_stats.num_generation_tokens
        )
        self.metrics.histogram_iteration_tokens.observe(
            iteration_stats.num_prompt_tokens + iteration_stats.num_generation_tokens
        )

        for max_gen_tokens in iteration_stats.max_num_generation_tokens_iter:
            self.metrics.histogram_max_num_generation_tokens_request.observe(
                max_gen_tokens
            )
        for n_param in iteration_stats.n_params_iter:
            self.metrics.histogram_n_request.observe(n_param)
        for ttft in iteration_stats.time_to_first_tokens_iter:
            self.metrics.histogram_time_to_first_token.observe(ttft)
        for tpot in iteration_stats.time_per_output_tokens_iter:
            self.metrics.histogram_time_per_output_token.observe(tpot)

        for finished_request in iteration_stats.finished_requests:
            self.metrics.counter_request_success[finished_request.finish_reason].inc()
            self.metrics.histogram_e2e_time_request.observe(
                finished_request.e2e_latency
            )
            self.metrics.histogram_queue_time_request.observe(
                finished_request.queued_time
            )
            self.metrics.histogram_prefill_time_request.observe(
                finished_request.prefill_time
            )
            self.metrics.histogram_inference_time_request.observe(
                finished_request.inference_time
            )
            self.metrics.histogram_decode_time_request.observe(
                finished_request.decode_time
            )
            self.metrics.histogram_num_prompt_tokens_request.observe(
                finished_request.num_prompt_tokens
            )
            self.metrics.histogram_num_generation_tokens_request.observe(
                finished_request.num_generation_tokens
            )
            self.metrics.histogram_max_tokens_request.observe(
                finished_request.max_tokens_param
            )

        if self.metrics.gauge_lora_info is not None:
            running_lora_adapters = ",".join(
                iteration_stats.running_lora_adapters.keys()
            )
            waiting_lora_adapters = ",".join(
                iteration_stats.waiting_lora_adapters.keys()
            )
            lora_info_labels = {
                self.metrics.labelname_running_lora_adapters: running_lora_adapters,
                self.metrics.labelname_waiting_lora_adapters: waiting_lora_adapters,
                self.metrics.labelname_max_lora: self.metrics.max_lora,
            }
            self.metrics.gauge_lora_info.labels(
                **lora_info_labels
            ).set_to_current_time()

    def log_engine_initialized(self):
        self.log_metrics_info("cache_config", self.vllm_config.cache_config)


class RayPrometheusStatLogger(PrometheusStatLogger):
    """RayPrometheusStatLogger uses Ray metrics instead."""

    _metrics_cls = RayMetrics

    def info(self, type: str, obj: SupportsMetricsInfo) -> None:
        return None
