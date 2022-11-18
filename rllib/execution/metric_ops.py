from typing import Any, Dict, List

from ray.rllib.evaluation.metrics import collect_episodes, summarize_episodes
from ray.rllib.execution.common import (
    AGENT_STEPS_SAMPLED_COUNTER,
    STEPS_SAMPLED_COUNTER,
    STEPS_TRAINED_COUNTER,
    STEPS_TRAINED_THIS_ITER_COUNTER,
    _get_shared_metrics,
)
from ray.rllib.evaluation.worker_set import WorkerSet


class CollectMetrics:
    """Callable that collects metrics from workers.

    The metrics are smoothed over a given history window.

    This should be used with the .for_each() operator. For a higher level
    API, consider using StandardMetricsReporting instead.

    Examples:
        >>> from ray.rllib.execution.metric_ops import CollectMetrics
        >>> train_op, workers = ... # doctest: +SKIP
        >>> output_op = train_op.for_each(CollectMetrics(workers)) # doctest: +SKIP
        >>> print(next(output_op)) # doctest: +SKIP
        {"episode_reward_max": ..., "episode_reward_mean": ..., ...}
    """

    def __init__(
        self,
        workers: WorkerSet,
        min_history: int = 100,
        timeout_seconds: int = 180,
        keep_per_episode_custom_metrics: bool = False,
        selected_workers: List[int] = None,
        by_steps_trained: bool = False,
    ):
        self.workers = workers
        self.episode_history = []
        self.min_history = min_history
        self.timeout_seconds = timeout_seconds
        self.keep_custom_metrics = keep_per_episode_custom_metrics
        self.selected_workers = selected_workers
        self.by_steps_trained = by_steps_trained

    def __call__(self, _: Any) -> Dict:
        # Collect worker metrics.
        episodes = collect_episodes(
            self.workers,
            self.selected_workers or self.workers.healthy_worker_ids(),
            timeout_seconds=self.timeout_seconds,
        )
        orig_episodes = list(episodes)
        missing = self.min_history - len(episodes)
        if missing > 0:
            episodes = self.episode_history[-missing:] + episodes
            assert len(episodes) <= self.min_history
        self.episode_history.extend(orig_episodes)
        self.episode_history = self.episode_history[-self.min_history :]
        res = summarize_episodes(episodes, orig_episodes, self.keep_custom_metrics)

        # Add in iterator metrics.
        metrics = _get_shared_metrics()
        custom_metrics_from_info = metrics.info.pop("custom_metrics", {})
        timers = {}
        counters = {}
        info = {}
        info.update(metrics.info)
        for k, counter in metrics.counters.items():
            counters[k] = counter
        for k, timer in metrics.timers.items():
            timers["{}_time_ms".format(k)] = round(timer.mean * 1000, 3)
            if timer.has_units_processed():
                timers["{}_throughput".format(k)] = round(timer.mean_throughput, 3)
        res.update(
            {
                "num_healthy_workers": self.workers.num_healthy_workers(),
                "timesteps_total": (
                    metrics.counters[STEPS_TRAINED_COUNTER]
                    if self.by_steps_trained
                    else metrics.counters[STEPS_SAMPLED_COUNTER]
                ),
                # tune.Trainable uses timesteps_this_iter for tracking
                # total timesteps.
                "timesteps_this_iter": metrics.counters[
                    STEPS_TRAINED_THIS_ITER_COUNTER
                ],
                "agent_timesteps_total": metrics.counters.get(
                    AGENT_STEPS_SAMPLED_COUNTER, 0
                ),
            }
        )
        res["timers"] = timers
        res["info"] = info
        res["info"].update(counters)
        res["custom_metrics"] = res.get("custom_metrics", {})
        res["episode_media"] = res.get("episode_media", {})
        res["custom_metrics"].update(custom_metrics_from_info)
        return res
