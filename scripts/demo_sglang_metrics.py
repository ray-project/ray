"""Mock SGLang metrics generator for Ray Serve LLM dashboard prototype.

Pushes synthetic ``sglang:*`` metrics through ``ray.util.metrics`` so the
SGLang dashboard panels render with realistic-looking data while the
upstream SGLang ``stat_loggers`` DI surface (sgl-project/sglang#24610)
and Ray-backed wrapper (RFC #24467 item 2) are still in flight.

Replace this with the real wrapper once those land. Until then this
mirrors the metric names the dashboard panels query.

Usage::

    ray start --head --dashboard-host 0.0.0.0 --metrics-export-port 8080
    python scripts/demo_sglang_metrics.py
    # In another terminal, open Grafana and pick the
    # ``Serve LLM SGLang Dashboard`` from the dropdown.

Note on Gauges: ``ray.util.metrics.Gauge.set()`` called from the driver
process is not exported by Ray's metric agent. Gauges live in a tiny Ray
actor (_GaugeActor) so the values are emitted from an actor process.
"""

import argparse
import random
import time

import ray
from ray.util.metrics import Counter, Gauge, Histogram


@ray.remote(num_cpus=0)
class _GaugeActor:
    def __init__(self, tag_keys):
        self._num_running = Gauge(
            name="sglang_num_running_reqs",
            description="Running requests.",
            tag_keys=tag_keys,
        )
        self._num_queue = Gauge(
            name="sglang_num_queue_reqs",
            description="Queued requests.",
            tag_keys=tag_keys,
        )
        self._num_paused = Gauge(
            name="sglang_num_paused_reqs",
            description="Paused requests.",
            tag_keys=tag_keys,
        )
        self._token_usage = Gauge(
            name="sglang_token_usage",
            description="KV cache token utilization.",
            tag_keys=tag_keys,
        )
        self._cache_hit_rate = Gauge(
            name="sglang_cache_hit_rate",
            description="Prefix cache hit rate.",
            tag_keys=tag_keys,
        )
        # Bridge metric so dashboard join (WorkerId) resolves for actor-emitted gauges.
        self._serve_counter = Counter(
            name="serve_deployment_request_counter_total",
            description="Mock Ray Serve deployment request counter for dashboard join.",
            tag_keys=tag_keys,
        )

    def update(
        self,
        tags,
        num_running,
        num_queue,
        num_paused,
        token_usage,
        cache_hit_rate,
        batch_size,
    ):
        self._num_running.set(num_running, tags=tags)
        self._num_queue.set(num_queue, tags=tags)
        self._num_paused.set(num_paused, tags=tags)
        self._token_usage.set(token_usage, tags=tags)
        self._cache_hit_rate.set(cache_hit_rate, tags=tags)
        self._serve_counter.inc(batch_size, tags=tags)


def main(model_name: str, deployment: str, replica: str, interval: float) -> None:
    ray.init(ignore_reinit_error=True)

    common_tags = {
        "model_name": model_name,
        "deployment": deployment,
        "replica": replica,
    }
    tag_keys = tuple(common_tags.keys())

    # Counters (work from driver)
    prompt_tokens = Counter(
        name="sglang_prompt_tokens_total",
        description="Number of prefill tokens processed.",
        tag_keys=tag_keys,
    )
    generation_tokens = Counter(
        name="sglang_generation_tokens_total",
        description="Number of generation tokens processed.",
        tag_keys=tag_keys,
    )
    num_requests = Counter(
        name="sglang_num_requests_total",
        description="Number of requests handled.",
        tag_keys=tag_keys,
    )
    num_aborted = Counter(
        name="sglang_num_aborted_requests_total",
        description="Number of aborted requests.",
        tag_keys=tag_keys,
    )
    # Bridge metric for dashboard join (mirrors what Ray Serve normally emits).
    serve_request_counter = Counter(
        name="serve_deployment_request_counter_total",
        description="Mock Ray Serve deployment request counter for dashboard join.",
        tag_keys=tag_keys,
    )

    # Gauges live in an actor (driver-set Gauges are not exported).
    gauge_actor = _GaugeActor.remote(tag_keys)

    # Histograms (work from driver)
    ttft = Histogram(
        name="sglang_time_to_first_token_seconds",
        description="Time to first token.",
        boundaries=[0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0],
        tag_keys=tag_keys,
    )
    itl = Histogram(
        name="sglang_inter_token_latency_seconds",
        description="Inter-token latency (TPOT).",
        boundaries=[0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5],
        tag_keys=tag_keys,
    )
    e2e = Histogram(
        name="sglang_e2e_request_latency_seconds",
        description="End-to-end request latency.",
        boundaries=[0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0],
        tag_keys=tag_keys,
    )
    queue_time = Histogram(
        name="sglang_queue_time_seconds",
        description="Queue wait time.",
        boundaries=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
        tag_keys=tag_keys,
    )
    prompt_hist = Histogram(
        name="sglang_prompt_tokens_histogram",
        description="Prompt length distribution.",
        boundaries=[64, 128, 256, 512, 1024, 2048, 4096],
        tag_keys=tag_keys,
    )
    gen_hist = Histogram(
        name="sglang_generation_tokens_histogram",
        description="Generation length distribution.",
        boundaries=[16, 32, 64, 128, 256, 512, 1024],
        tag_keys=tag_keys,
    )

    print(
        f"Pushing synthetic sglang:* metrics every {interval}s "
        f"(model={model_name}, deployment={deployment}, replica={replica}). "
        "Ctrl-C to stop."
    )

    while True:
        batch_size = random.randint(2, 8)
        prompt_len = random.randint(64, 2048)
        gen_len = random.randint(16, 512)

        prompt_tokens.inc(batch_size * prompt_len, tags=common_tags)
        generation_tokens.inc(batch_size * gen_len, tags=common_tags)
        num_requests.inc(batch_size, tags=common_tags)
        serve_request_counter.inc(batch_size, tags=common_tags)
        if random.random() < 0.05:
            num_aborted.inc(1, tags=common_tags)

        gauge_actor.update.remote(
            common_tags,
            random.randint(0, 32),
            random.randint(0, 8),
            random.randint(0, 2),
            min(1.0, random.expovariate(3)),
            random.uniform(0.3, 0.95),
            batch_size,
        )

        for _ in range(batch_size):
            ttft.observe(random.expovariate(2), tags=common_tags)
            for _ in range(min(gen_len, 50)):
                itl.observe(random.expovariate(20), tags=common_tags)
            e2e.observe(random.expovariate(0.3), tags=common_tags)
            queue_time.observe(random.expovariate(50), tags=common_tags)
            prompt_hist.observe(prompt_len, tags=common_tags)
            gen_hist.observe(gen_len, tags=common_tags)

        time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-name", default="meta-llama/Llama-3.1-8B-Instruct")
    parser.add_argument("--deployment", default="sglang_engine")
    parser.add_argument("--replica", default="sglang_engine#demo")
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args()
    main(
        model_name=args.model_name,
        deployment=args.deployment,
        replica=args.replica,
        interval=args.interval,
    )
