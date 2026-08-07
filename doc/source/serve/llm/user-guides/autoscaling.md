(serve-llm-ttft-autoscaling)=
# Autoscaling on time-to-first-token

Ray Serve LLM ships `TTFTAutoscalingPolicy`, a built-in autoscaling policy that scales replicas on vLLM's p99 time-to-first-token (TTFT). TTFT rises when replicas are saturated and prompts start queueing, so it's a direct signal of user-facing latency pressure. The policy reads the metric from Prometheus through [`PrometheusQueryMixin`](../../api/doc/ray.serve.autoscaling_policy.PrometheusQueryMixin.rst), so metric reads never block the autoscaling loop.

## How it decides

On each autoscaling tick the policy reads the p99 TTFT for the model and compares it to `ttft_target_s`:

- **Scale up.** When p99 TTFT is above `ttft_target_s`, the policy asks for one more replica, so it reacts to latency pressure right away.
- **Scale down.** Low TTFT alone doesn't prove excess capacity, so the policy removes a replica only when p99 TTFT is below target *and* ongoing requests per replica drop below `idle_threshold`.
- **Hold.** When Prometheus data is unavailable or stale, the policy holds the current replica count rather than guessing.

`min_replicas` and `max_replicas` from `AutoscalingConfig` still bound the result, so scale-to-zero and upper limits work as usual.

## Prerequisites

- **Export vLLM engine metrics to Prometheus.** The policy queries `ray_vllm_time_to_first_token_seconds`. Enable engine metrics and point Ray at Prometheus as described in {doc}`Observability and monitoring <observability>` and {ref}`collect-metrics`.
- **`RAY_PROMETHEUS_HOST`.** The policy reads its Prometheus address from this environment variable by default, which Ray's dashboard and managed clusters already set. Pass `prometheus_address` in `policy_kwargs` to override it.

## Configure

Set the policy in the deployment's `autoscaling_config`. `model_id` is required so the p99 TTFT query is scoped to this model:

```python
from ray import serve
from ray.serve.config import AutoscalingConfig, AutoscalingPolicy
from ray.serve.llm import LLMConfig
from ray.serve.llm.autoscaling import TTFTAutoscalingPolicy

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="qwen-0.5b",
        model_source="Qwen/Qwen2.5-0.5B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=8,
            policy=AutoscalingPolicy(
                policy_function=TTFTAutoscalingPolicy,
                policy_kwargs=dict(
                    ttft_target_s=2.0,
                    model_id="qwen-0.5b",
                ),
            ),
        )
    ),
    accelerator_type="A10G",
)
```

## Parameters

Pass these through `policy_kwargs`:

| Parameter | Default | Description |
|---|---|---|
| `ttft_target_s` | `2.0` | p99 TTFT threshold in seconds. Above it, the policy scales up. |
| `idle_threshold` | `1.0` | Max ongoing requests per replica to treat a replica as idle for scale-down. |
| `model_id` | required | Scopes the p99 TTFT query to this vLLM model. Required unless `query` is given. |
| `query` | p99 TTFT | PromQL to read instead of the default. Must resolve to a single-sample instant vector. |
| `prometheus_address` | `RAY_PROMETHEUS_HOST` | Prometheus server, `host:port` or a full URL. |
| `fetch_interval_s` | `5.0` | How often the background thread re-queries Prometheus. |
| `cache_ttl_s` | `15.0` | How long a cached value stays valid before reads return no data. |

## Scale on a different metric

`TTFTAutoscalingPolicy` builds on `PrometheusQueryMixin`. To scale any deployment on a different Prometheus metric, mix `PrometheusQueryMixin` into your own policy. See [Prometheus-based autoscaling](serve-advanced-autoscaling-prometheus) in the Serve autoscaling guide.
