from typing import Any, Dict, Tuple, Union

from ray.serve.config import AutoscalingContext
from ray.serve.prometheus_autoscaling_policy import PrometheusAutoscalingPolicy


class TTFTAutoscalingPolicy(PrometheusAutoscalingPolicy):
    """Autoscale base on p99 'time to first token' from vLLM metrics.

    Args:
        prometheus_query_url: Forwarded to the base class.
        target_p99_ttft_s: Latency target. Above this, scale up by one.
        **kwargs: Forwarded to PrometheusAutoscalingPolicy
    """

    P99_TTFT_QUERY = (
        "histogram_quantile(0.99, "
        "sum(rate(vllm:time_to_first_token_seconds_bucket[1m])) by (le))"
    )

    def __init__(
        self,
        prometheus_query_url: str,
        target_p99_ttft_s: float = 2.0,
        **kwargs: Any,
    ):
        super().__init__(
            prometheus_query_url=prometheus_query_url,
            queries=[self.P99_TTFT_QUERY],
            **kwargs,
        )
        self._target_p99_ttft_s = target_p99_ttft_s

    def decide(
        self, ctx: AutoscalingContext, metrics: Dict[str, float]
    ) -> Tuple[Union[int, float], Dict[str, Any]]:
        p99 = metrics.get(self.P99_TTFT_QUERY)
        if p99 is None:
            return float(ctx.current_num_replicas), {"signal": "no_data"}

        if p99 > self._target_p99_ttft_s:
            return float(ctx.current_num_replicas + 1), {
                "signal": "scale_up",
                "p99_ttft_s": p99,
            }

        if ctx.current_num_replicas > 0:
            requests_per_replica = ctx.total_num_requests / ctx.current_num_replicas
        else:
            requests_per_replica = 0.0

        if requests_per_replica < 1.0:
            return float(max(1, ctx.current_num_replicas - 1)), {
                "signal": "scale_down",
                "p99_ttft_s": p99,
            }

        return float(ctx.current_num_replicas), {
            "signal": "steady",
            "p99_ttft_s": p99,
        }
