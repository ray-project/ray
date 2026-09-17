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

This example replaces the default ``"ema"`` key with ``ClearingEmaStats``,
a subclass of :py:class:`~ray.rllib.utils.metrics.stats.ema.EmaStats` that
resets its running value to ``NaN`` after every ``reduce()`` call. The
default ``EmaStats`` carries the running value forward across reduces so a
metric smoothly tracks across training iterations; ``ClearingEmaStats``
instead restarts each iteration's EMA from scratch. This reproduces the
short-lived behaviour from Ray 2.53/2.54 and is useful when you want each
reported value to reflect only the data observed within a single
iteration. Because RLlib's internal timers (and other ``reduce="ema"``
metrics) go through the lookup, swapping the key is enough -- no per-metric
plumbing required.

``VerifyClearingEmaCallback`` is attached purely as a sanity
check: every ``on_train_result`` it asserts that RLlib's built-in
training-iteration timer is now an instance of ``ClearingEmaStats`` and
that its internal ``_value`` was reset to ``NaN`` by the reduce that just
happened.

How to run this script
----------------------
``python clearing_ema.py --stop-iters=3``

For debugging:
``python clearing_ema.py --no-tune --num-env-runners=0 --stop-iters=3``
"""
import math

import numpy as np

from ray.rllib.callbacks.callbacks import RLlibCallback
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.rllib.utils.metrics import TIMERS, TRAINING_ITERATION_TIMER
from ray.rllib.utils.metrics.metrics_logger import DEFAULT_STATS_CLS_LOOKUP
from ray.rllib.utils.metrics.stats.ema import EmaStats
from ray.tune.registry import get_trainable_cls


class ClearingEmaStats(EmaStats):
    def reduce(self, compile: bool = True):
        result = super().reduce(compile=compile)
        self._value = np.nan
        return result


class VerifyClearingEmaCallback(RLlibCallback):
    def on_train_result(self, *, algorithm, metrics_logger, result, **kwargs) -> None:
        timer_stats = metrics_logger.stats[TIMERS][TRAINING_ITERATION_TIMER]
        # Only check that our value was cleared
        assert math.isnan(timer_stats._value)


parser = add_rllib_example_script_args(default_reward=50.0, default_iters=5)


if __name__ == "__main__":
    args = parser.parse_args()

    # Override the default "ema" key in the lookup. Every metric RLlib logs
    # with `reduce="ema"` (e.g. all of its built-in timers) will now use
    # `ClearingEmaStats`.
    custom_stats_lookup = {**DEFAULT_STATS_CLS_LOOKUP, "ema": ClearingEmaStats}

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("CartPole-v1")
        .reporting(custom_stats_cls_lookup=custom_stats_lookup)
        .callbacks(VerifyClearingEmaCallback)
    )

    run_rllib_example_script_experiment(base_config, args)
