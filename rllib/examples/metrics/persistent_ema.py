"""Example of plugging a custom EMA Stats class into the MetricsLogger via
``AlgorithmConfig.reporting(custom_stats_cls_lookup=...)``.

RLlib aggregates metrics through Stats objects (see
:py:class:`~ray.rllib.utils.metrics.stats.base.StatsBase`). A ``reduce=...``
keyword (e.g. ``"ema"``, ``"mean"``) maps to a Stats class via
:py:data:`~ray.rllib.utils.metrics.metrics_logger.DEFAULT_STATS_CLS_LOOKUP`.
By passing your own dictionary to
``AlgorithmConfig.reporting(custom_stats_cls_lookup=...)`` you can either
add a new key (referenced via ``log_value(..., reduce="<your_key>")``) or
replace an existing key to change RLlib's default reduction behaviour for
all metrics that internally use that reduction.

This example replaces the default ``"ema"`` key with ``PersistentEmaStats``,
a subclass of :py:class:`~ray.rllib.utils.metrics.stats.ema.EmaStats` that
keeps its running value across ``reduce()`` calls. The default ``EmaStats``
resets the running value to ``NaN`` after every reduce, so the next
iteration's EMA starts from scratch; ``PersistentEmaStats`` instead carries
the value forward, producing a metric that smoothly tracks across training
iterations. Because RLlib's internal timers (and other ``reduce="ema"``
metrics) go through the lookup, swapping the key is enough -- no per-metric
plumbing required.

A small ``VerifyPersistentEmaCallback`` is attached purely as a sanity
check: every ``on_train_result`` it asserts that RLlib's built-in
training-iteration timer is now an instance of ``PersistentEmaStats`` and
that its internal ``_value`` survives the reduce that just happened.

How to run this script
----------------------
``python persistent_ema.py --stop-iters=3``

For debugging:
``python persistent_ema.py --no-tune --num-env-runners=0 --stop-iters=3``
"""
import math

from ray.rllib.callbacks.callbacks import RLlibCallback
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.rllib.utils.metrics import TIMERS, TRAINING_ITERATION_TIMER
from ray.rllib.utils.metrics.metrics_logger import DEFAULT_STATS_CLS_LOOKUP
from ray.rllib.utils.metrics.stats.ema import EmaStats
from ray.tune.registry import get_trainable_cls


class PersistentEmaStats(EmaStats):
    # This is required such that RLlib can correctly restore metrics from checkpoints
    stats_cls_identifier = "ema"

    def reduce(self, compile: bool = True):
        saved_value = self._value
        result = super().reduce(compile=compile)
        self._value = saved_value
        return result


class VerifyPersistentEmaCallback(RLlibCallback):
    def on_train_result(self, *, algorithm, metrics_logger, result, **kwargs) -> None:
        timer_stats = metrics_logger.stats[TIMERS][TRAINING_ITERATION_TIMER]
        # Only check that our value is persisted
        assert not math.isnan(timer_stats._value)


parser = add_rllib_example_script_args(default_reward=50.0, default_iters=5)


if __name__ == "__main__":
    args = parser.parse_args()

    # Override the default "ema" key in the lookup. Every metric RLlib logs
    # with `reduce="ema"` (e.g. all of its built-in timers) will now use
    # `PersistentEmaStats`.
    custom_stats_lookup = {**DEFAULT_STATS_CLS_LOOKUP, "ema": PersistentEmaStats}

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("CartPole-v1")
        .reporting(custom_stats_cls_lookup=custom_stats_lookup)
        .callbacks(VerifyPersistentEmaCallback)
    )

    run_rllib_example_script_experiment(base_config, args)
