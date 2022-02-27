from typing import Any, Dict, List, Optional
import time

from ray.actor import ActorHandle
from ray.util.iter import LocalIterator
from ray.rllib.evaluation.metrics import collect_episodes, summarize_episodes
from ray.rllib.execution.common import (
    AGENT_STEPS_SAMPLED_COUNTER,
    STEPS_SAMPLED_COUNTER,
    STEPS_TRAINED_COUNTER,
    STEPS_TRAINED_THIS_ITER_COUNTER,
    _get_shared_metrics,
)
from ray.rllib.evaluation.worker_set import WorkerSet


def StandardMetricsReporting(
    train_op: LocalIterator[Any],
    workers: WorkerSet,
    config: dict,
    selected_workers: List[ActorHandle] = None,
    by_steps_trained: bool = False,
) -> LocalIterator[dict]:
    """Operator to periodically collect and report metrics.

    Args:
        train_op (LocalIterator): Operator for executing training steps.
            We ignore the output values.
        workers (WorkerSet): Rollout workers to collect metrics from.
        config (dict): Trainer configuration, used to determine the frequency
            of stats reporting.
        selected_workers (list): Override the list of remote workers
            to collect metrics from.
        by_steps_trained (bool): If True, uses the `STEPS_TRAINED_COUNTER`
            instead of the `STEPS_SAMPLED_COUNTER` in metrics.

    Returns:
        LocalIterator[dict]: A local iterator over training results.

    Examples:
        >>> train_op = ParallelRollouts(...).for_each(TrainOneStep(...))
        >>> metrics_op = StandardMetricsReporting(train_op, workers, config)
        >>> next(metrics_op)
        {"episode_reward_max": ..., "episode_reward_mean": ..., ...}
    """

    output_op = (
        train_op.filter(
            OncePerTimestepsElapsed(
                config["timesteps_per_iteration"], by_steps_trained=by_steps_trained
            )
        )
        .filter(OncePerTimeInterval(config["min_time_s_per_reporting"]))
        .for_each(
            CollectMetrics(
                workers,
                min_history=config["metrics_num_episodes_for_smoothing"],
                timeout_seconds=config["metrics_episode_collection_timeout_s"],
                keep_per_episode_custom_metrics=config[
                    "keep_per_episode_custom_metrics"
                ],
                selected_workers=selected_workers,
                by_steps_trained=by_steps_trained,
            )
        )
    )
    return output_op


class CollectMetrics:
    """Callable that collects metrics from workers.

    The metrics are smoothed over a given history window.

    This should be used with the .for_each() operator. For a higher level
    API, consider using StandardMetricsReporting instead.

    Examples:
        >>> output_op = train_op.for_each(CollectMetrics(workers))
        >>> print(next(output_op))
        {"episode_reward_max": ..., "episode_reward_mean": ..., ...}
    """

    def __init__(
        self,
        workers: WorkerSet,
        min_history: int = 100,
        timeout_seconds: int = 180,
        keep_per_episode_custom_metrics: bool = False,
        selected_workers: List[ActorHandle] = None,
        by_steps_trained: bool = False,
    ):
        self.workers = workers
        self.episode_history = []
        self.to_be_collected = []
        self.min_history = min_history
        self.timeout_seconds = timeout_seconds
        self.keep_custom_metrics = keep_per_episode_custom_metrics
        self.selected_workers = selected_workers
        self.by_steps_trained = by_steps_trained

    def __call__(self, _: Any) -> Dict:
        # Collect worker metrics.
        episodes, self.to_be_collected = collect_episodes(
            self.workers.local_worker(),
            self.selected_workers or self.workers.remote_workers(),
            self.to_be_collected,
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
                "num_healthy_workers": len(self.workers.remote_workers()),
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


class OncePerTimeInterval:
    """Callable that returns True once per given interval.

    This should be used with the .filter() operator to throttle / rate-limit
    metrics reporting. For a higher-level API, consider using
    StandardMetricsReporting instead.

    Examples:
        >>> throttled_op = train_op.filter(OncePerTimeInterval(5))
        >>> start = time.time()
        >>> next(throttled_op)
        >>> print(time.time() - start)
        5.00001  # will be greater than 5 seconds
    """

    def __init__(self, delay: Optional[float] = None):
        self.delay = delay or 0.0
        self.last_returned_true = 0

    def __call__(self, item: Any) -> bool:
        # No minimum time to wait for -> Return True.
        if self.delay <= 0.0:
            return True
        # Return True, if time since last returned=True is larger than
        # `self.delay`.
        now = time.time()
        if now - self.last_returned_true > self.delay:
            self.last_returned_true = now
            return True
        return False


class OncePerTimestepsElapsed:
    """Callable that returns True once per given number of timesteps.

    This should be used with the .filter() operator to throttle / rate-limit
    metrics reporting. For a higher-level API, consider using
    StandardMetricsReporting instead.

    Examples:
        >>> throttled_op = train_op.filter(OncePerTimestepsElapsed(1000))
        >>> next(throttled_op)
        # will only return after 1000 steps have elapsed
    """

    def __init__(self, delay_steps: int, by_steps_trained: bool = False):
        """
        Args:
            delay_steps (int): The number of steps (sampled or trained) every
                which this op returns True.
            by_steps_trained (bool): If True, uses the `STEPS_TRAINED_COUNTER`
                instead of the `STEPS_SAMPLED_COUNTER` in metrics.
        """
        self.delay_steps = delay_steps
        self.by_steps_trained = by_steps_trained
        self.last_called = 0

    def __call__(self, item: Any) -> bool:
        if self.delay_steps <= 0:
            return True
        metrics = _get_shared_metrics()
        if self.by_steps_trained:
            now = metrics.counters[STEPS_TRAINED_COUNTER]
        else:
            now = metrics.counters[STEPS_SAMPLED_COUNTER]
        if now - self.last_called >= self.delay_steps:
            self.last_called = now
            return True
        return False
