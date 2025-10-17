import logging
from typing import Any, Dict, List, Optional, Tuple, Union, Type
import tree  # pip install dm_tree

from ray.rllib.utils import force_tuple, deep_update
from ray.rllib.utils.metrics.stats import (
    StatsBase,
    SumStats,
    MeanStats,
    EmaStats,
    MinStats,
    MaxStats,
    LifetimeSumStats,
    PercentilesStats,
    ItemStats,
    ItemSeriesStats,
)
from ray.rllib.utils.metrics.legacy_stats import Stats
from ray._common.deprecation import Deprecated, deprecation_warning
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.util.annotations import PublicAPI

_, tf, _ = try_import_tf()
torch, _ = try_import_torch()
logger = logging.getLogger("ray.rllib")

# This is used by default to look up classes to use for logging stats.
# You can override it and add new classes by passing a different lookup to the MetricsLogger constructor.
# These new classes can then be used to log stats by passing the corresponding identifier to the MetricsLogger.log method.
DEFAULT_STATS_CLS_LOOKUP = {
    "mean": MeanStats,
    "ema": EmaStats,
    "min": MinStats,
    "max": MaxStats,
    "sum": SumStats,
    "lifetime_sum": LifetimeSumStats,
    "percentiles": PercentilesStats,
    "item": ItemStats,
    "item_series": ItemSeriesStats,
}


def check_log_args(
    reduce: Optional[str] = None,
    window: Optional[Union[int, float]] = None,
    ema_coeff: Optional[float] = None,
    percentiles: Union[List[int], bool] = None,
    clear_on_reduce: bool = None,
    with_throughput: bool = None,
    throughput_ema_coeff: Optional[float] = None,
    reduce_per_index_on_aggregate: Optional[bool] = None,
):
    """Checks the arguments for logging methods and returns the validated arguments."""

    if percentiles and not reduce == "percentiles":
        raise ValueError("percentiles is only supported for reduce=percentiles")

    if reduce == "ema" and window is not None:
        deprecation_warning(
            "window is not supported for ema reduction. If you want to use a window, use mean reduction instead.",
            error=True,
        )
        window = None

    if reduce_per_index_on_aggregate is not None:
        deprecation_warning(
            "reduce_per_index_on_aggregate is deprecated. Aggregation now happens over all values"
            "of incoming stats objects, treating each incoming value with equal weight.",
            error=False,
        )

    if throughput_ema_coeff is not None:
        deprecation_warning(
            "throughput_ema_coeff is deprecated. Throughput is not smoothed with ema anymore"
            "but calculate once per MetricsLogger.reduce() call.",
            error=True,
        )

    if reduce == "mean":
        if ema_coeff is not None:
            deprecation_warning(
                "ema_coeff is not supported for mean reduction. Use ema instead.",
                error=True,
            )

    if with_throughput and not reduce == "sum":
        deprecation_warning(
            "with_throughput=True is only supported for reduce=sum. Use reduce=sum instead.",
            error=False,
        )
        logger.warning(
            "with_throughput=True is only supported for reduce=sum. Use reduce=sum instead."
        )

    return (
        reduce,
        window,
        ema_coeff,
        percentiles,
        clear_on_reduce,
        with_throughput,
        throughput_ema_coeff,
        reduce_per_index_on_aggregate,
    )


@PublicAPI(stability="alpha")
class MetricsLogger:
    """A generic class collecting and reducing metrics.

    Use this API to log and merge metrics.
    Mostly metrics should be logged in parallel components with MetricsLogger.log_value().
    RLlib will then aggregate metrics, reduce them and log them.

    The MetricsLogger supports logging anything that has a corresponding reduction method.
    These are defined natively in the Stats classes, which are used to log the metrics.
    Please take a look ray.rllib.utils.metrics.metrics_logger.DEFAULT_STATS_CLS_LOOKUP for the available reduction methods.
    You can provide your own reduce methods by extending ray.rllib.utils.metrics.metrics_logger.DEFAULT_STATS_CLS_LOOKUP and passing it to AlgorithmConfig.logging().

    Note: In our docstirngs we make heavy use of the phrae 'parallel components'.
    This pertains to the architecture of the logging system, where we have one 'root' MetricsLogger
    that is used to aggregate all metrics of n parallel ('non-root') MetricsLoggers that are used to log metrics for each parallel component.
    A parallel component is typically a single Learner worker, an EnvRunner, or a ConnectorV2 or any other component of which more than one instance is running in parallel.
    """

    def __init__(
        self,
        root=False,
        stats_cls_lookup: Optional[
            Dict[str, Type[StatsBase]]
        ] = DEFAULT_STATS_CLS_LOOKUP,
    ):
        """Initializes a MetricsLogger instance.

        Args:
            root: Whether this logger is a root logger. If True, lifetime stats (clear_on_reduce=False and reduction="sum") will not be cleared on reduce().
            stats_cls_lookup: A dictionary mapping reduction method names to Stats classes.
                If not provided, the default lookup (ray.rllib.utils.metrics.metrics_logger.DEFAULT_STATS_CLS_LOOKUP) will be used.
                You can provide your own reduce methods by extending ray.rllib.utils.metrics.metrics_logger.DEFAULT_STATS_CLS_LOOKUP and passing it to AlgorithmConfig.logging().
        """
        self.stats = {}
        self._tensor_mode = False
        # TODO (sven): We use a dummy RLock here for most RLlib algos, however, APPO
        #  and IMPALA require this to be an actual RLock (b/c of thread safety reasons).
        #  An actual RLock, however, breaks our current OfflineData and
        #  OfflinePreLearner logic, in which the Learner (which contains a
        #  MetricsLogger) is serialized and deserialized. We will have to fix this
        #  offline RL logic first, then can remove this hack here and return to always
        #  using the RLock.
        self._threading_lock = _DummyRLock()
        # Is this a root logger?
        self._is_root_logger = root
        self.stats_cls_lookup = stats_cls_lookup

    def __contains__(self, key: Union[str, Tuple[str, ...]]) -> bool:
        """Returns True, if `key` can be found in self.stats.

        Args:
            key: The key to find in self.stats. This must be either a str (single,
                top-level key) or a tuple of str (nested key).

        Returns:
            Whether `key` could be found in self.stats.
        """
        return self._key_in_stats(key)

    def peek(
        self,
        key: Union[str, Tuple[str, ...], None] = None,
        default=None,
        compile: bool = True,
        throughput: bool = False,
    ) -> Any:
        """Returns the reduced values found in this MetricsLogger.

        Note that calling this method does NOT cause an actual underlying value list
        reduction, even though reduced values are being returned. It'll keep all
        internal structures as-is. By default, this returns a single reduced value or, if
        the Stats object has no reduce method, a list of values. When when compile is False,
        the result is a list of one or more values.

        Args:
            key: The key/key sequence of the sub-structure of `self`, whose (reduced)
                values to return.
            default: An optional default value in case `key` cannot be found in `self`.
                If default is not provided and `key` cannot be found, throws a KeyError.
            compile: If True, the result is compiled into a single value if possible.
            throughput: If True, the throughput is returned instead of the
                actual (reduced) value.

        Returns:
            The (reduced) values of the (possibly nested) sub-structure found under
            the given key or key sequence.
        """
        if throughput:
            assert (
                self._is_root_logger
            ), "Throughput can only be peeked from a root logger"
            return self._get_throughputs(key=key, default=default)

        # Create a reduced view of the entire stats structure.
        def _nested_peek(stats):
            return tree.map_structure(
                # If the Stats object has a reduce method, we need to convert the list to a single value
                lambda s: (
                    s.peek(compile=compile)
                    if s._reduce_method is not None
                    else s.peek(compile=compile)[0]
                )
                if isinstance(s, StatsBase)
                else s,
                stats.copy(),
            )

        with self._threading_lock:
            if key is None:
                return _nested_peek(self.stats)
            else:
                if default is None:
                    stats = self._get_key(key, key_error=True)
                else:
                    stats = self._get_key(key, key_error=False)

                if isinstance(stats, StatsBase):
                    # If the Stats object has a reduce method, we need to convert the list to a single value
                    return stats.peek(compile=compile)

                elif isinstance(stats, dict) and stats:
                    return _nested_peek(stats)
                else:
                    return default

    @staticmethod
    def peek_results(results: Any, compile: bool = True) -> Any:
        """Performs `peek()` on any leaf element of an arbitrarily nested Stats struct.

        Args:
            results: The nested structure of Stats-leafs to be peek'd and returned.
            compile: If True, the result is compiled into a single value if possible.

        Returns:
            A corresponding structure of the peek'd `results` (reduced float/int values;
            no Stats objects).
        """
        return tree.map_structure(
            lambda s: s.peek(compile=compile) if isinstance(s, StatsBase) else s,
            results,
        )

    def log_value(
        self,
        key: Union[str, Tuple[str, ...]],
        value: Any,
        *,
        reduce: str = "ema",
        window: Optional[Union[int, float]] = None,
        ema_coeff: Optional[float] = None,
        percentiles: Optional[Union[List[int], bool]] = None,
        clear_on_reduce: Optional[bool] = None,
        with_throughput: Optional[bool] = None,
        throughput_ema_coeff: Optional[float] = None,
        reduce_per_index_on_aggregate: Optional[bool] = None,
    ) -> None:
        """Logs a new value under a (possibly nested) key to the logger.

        Args:
            key: The key (or nested key-tuple) to log the `value` under.
            value: The value to log. This should be a numeric value.
            reduce: The reduction method to apply when compiling metrics at the root logger.
                By default, the reduction methods to choose from here are the keys
                of rllib.utils.metrics.metrics_logger.DEFAULT_STATS_CLS_LOOKUP.
                You can provide your own reduce methods by extending
                rllib.utils.metrics.metrics_logger.DEFAULT_STATS_CLS_LOOKUP and passing it to AlgorithmConfig.logging()).
            window: An optional window size to reduce over.
                If not None, then the reduction operation is only applied to the most
                recent `window` items, and - after reduction - the internal values list
                under `key` is shortened to hold at most `window` items (the most
                recent ones). Must be None if `ema_coeff` is provided.
                If None (and `ema_coeff` is None), reduction must not be "mean".
            ema_coeff: An optional EMA coefficient to use if `reduce` is "mean"
                and no `window` is provided. Note that if both `window` and `ema_coeff`
                are provided, an error is thrown. Also, if `ema_coeff` is provided,
                `reduce` must be "mean".
                The reduction formula for EMA is:
                EMA(t1) = (1.0 - ema_coeff) * EMA(t0) + ema_coeff * new_value
            percentiles: If reduce is `None`, we can compute the percentiles of the
                values list given by `percentiles`. Defaults to [0, 0.5, 0.75, 0.9, 0.95,
                0.99, 1] if set to True. When using percentiles, a window must be provided.
                This window should be chosen carefully. RLlib computes exact percentiles and
                the computational complexity is O(m*n*log(n/m)) where n is the window size
                and m is the number of parallel metrics loggers involved (for example,
                m EnvRunners).
            clear_on_reduce: If True, all values under `key` will be emptied after
                `self.reduce()` is called. Setting this to True is useful for cases,
                in which the internal values list would otherwise grow indefinitely,
                for example if reduce is None and there is no `window` provided.
            with_throughput: Whether to track a throughput estimate together with this
                metric. This is only supported for `reduce=sum` and
                `clear_on_reduce=False` metrics (aka. "lifetime counts"). The `Stats`
                object under the logged key then keeps track of the time passed
                between two consecutive calls to `reduce()` and update its throughput
                estimate. The current throughput estimate of a key can be obtained
                through: <MetricsLogger>.peek(key, throughput=True).
            throughput_ema_coeff: Deprecated argument. Throughput is not smoothed with ema anymore
                but calculate once per MetricsLogger.reduce() call.
            reduce_per_index_on_aggregate: Deprecated argument. Aggregation now happens over all values
                of incoming stats objects once per MetricsLogger.reduce() call, treating each incoming value with equal weight.
        """

        (
            reduce,
            window,
            ema_coeff,
            percentiles,
            clear_on_reduce,
            with_throughput,
            throughput_ema_coeff,
            reduce_per_index_on_aggregate,
        ) = check_log_args(
            reduce,
            window,
            ema_coeff,
            percentiles,
            clear_on_reduce,
            with_throughput,
            throughput_ema_coeff,
            reduce_per_index_on_aggregate,
        )

        # No reduction (continue appending to list) AND no window.
        # -> We'll force-reset our values upon `reduce()`.
        if reduce is None and (window is None or window == float("inf")):
            clear_on_reduce = True

        if torch and torch.is_tensor(value):
            value = value.detach()
        elif tf and tf.is_tensor(value):
            value = tf.stop_gradient(value)

        with self._threading_lock:
            # `key` doesn't exist -> Automatically create it.
            if not self._key_in_stats(key):
                if reduce == "ema":
                    if ema_coeff is None:
                        ema_coeff = 0.01
                    if window is not None:
                        raise ValueError("window is not supported for ema reduction")
                    self._set_key(
                        key,
                        EmaStats(
                            _is_root_stats=self._is_root_logger,
                            ema_coeff=ema_coeff,
                            clear_on_reduce=clear_on_reduce,
                        ),
                    )
                elif reduce == "mean":
                    if ema_coeff is not None:
                        raise ValueError(
                            "ema_coeff is not supported for mean reduction"
                        )
                    self._set_key(
                        key,
                        MeanStats(
                            _is_root_stats=self._is_root_logger,
                            window=window,
                            clear_on_reduce=clear_on_reduce,
                        ),
                    )
                elif reduce == "min":
                    self._set_key(
                        key,
                        MinStats(
                            _is_root_stats=self._is_root_logger,
                            clear_on_reduce=clear_on_reduce,
                            window=window,
                        ),
                    )
                elif reduce == "max":
                    self._set_key(
                        key,
                        MaxStats(
                            _is_root_stats=self._is_root_logger,
                            clear_on_reduce=clear_on_reduce,
                            window=window,
                        ),
                    )
                elif reduce == "sum":
                    self._set_key(
                        key,
                        SumStats(
                            _is_root_stats=self._is_root_logger,
                            window=window,
                            clear_on_reduce=clear_on_reduce,
                            throughput=with_throughput,
                        ),
                    )
                elif reduce == "lifetime_sum":
                    self._set_key(
                        key,
                        LifetimeSumStats(
                            _is_root_stats=self._is_root_logger,
                            track_throughput_since_last_restore=with_throughput,
                        ),
                    )
                elif reduce == "percentiles":
                    self._set_key(
                        key,
                        PercentilesStats(
                            _is_root_stats=self._is_root_logger,
                            window=window,
                            clear_on_reduce=clear_on_reduce,
                            percentiles=percentiles,
                        ),
                    )
                elif reduce == "item":
                    assert window is None, "window is not supported for item reduction"
                    self._set_key(
                        key,
                        ItemStats(
                            _is_root_stats=self._is_root_logger,
                            clear_on_reduce=clear_on_reduce,
                        ),
                    )
                elif reduce == "item_series":
                    self._set_key(
                        key,
                        ItemSeriesStats(
                            _is_root_stats=self._is_root_logger,
                            window=window,
                            clear_on_reduce=clear_on_reduce,
                        ),
                    )
                else:
                    raise ValueError(
                        f"Invalid reduce method: {reduce}. You can provide your own reduce methods by extending rllib.utils.metrics.metrics_logger.DEFAULT_STATS_CLS_LOOKUP and passing it to AlgorithmConfig.logging()"
                    )

            stats = self._get_key(key)
            stats.push(value)

    @Deprecated(
        new="log_value", help="Use log_value multiple times instead.", error=True
    )
    def log_dict(self, *args, **kwargs) -> None:
        ...

    @Deprecated(new="aggregate", error=False)
    def merge_and_log_n_dicts(self, *args, **kwargs):
        return self.aggregate(*args, **kwargs)

    def aggregate(
        self,
        stats_dicts: List[Dict[str, Any]],
        *,
        key: Optional[Union[str, Tuple[str, ...]]] = None,
    ) -> None:
        """Merges n stats_dicts and logs result by merging on the time axis with existing stats.

        The n stats_dicts should be generated by n parallel components such that merging their
        respective stats in parallel is meaningful. Stats should only be aggregated at the root logger.

        Args:
            stats_dicts: List of n stats dicts to be merged and then logged.
            key: Optional top-level key under which to log all keys/key sequences
                found in the n `stats_dicts`.
        """
        assert (
            self._is_root_logger
        ), "Stats should only be aggregated at the root logger"

        all_keys = set()

        def traverse_and_add_paths(d, path=()):
            if isinstance(d, dict):
                new_dict = {}
                for key, value in d.items():
                    new_dict[key] = traverse_and_add_paths(value, path + (key,))
                return new_dict
            elif isinstance(d, list):
                all_keys.add(path)
                if len(d) == 1:
                    return d[0]
                return d
            else:
                # For lists and values, we add the path to the set of all keys
                all_keys.add(path)
                return d

        def build_nested_dict(stats_dict, key):
            if isinstance(key, str):
                return {key: stats_dict}
            elif len(key) > 1:
                # Key is tuple of keys so we build a nested dict recursively
                return {key[0]: build_nested_dict(stats_dict, key[1:])}
            else:
                return {key[0]: stats_dict}

        # We do one pass over all the stats_dicts_or_loggers to 1. prepend the key if provided and 2. collect all the keys that lead to leaves (which may be lists or values).
        incoming_stats_dicts_with_key = []
        for stats_dict in stats_dicts:
            if key is not None:
                stats_dict = build_nested_dict(stats_dict, key)
            stats_dict = traverse_and_add_paths(stats_dict)
            incoming_stats_dicts_with_key.append(stats_dict)

        tree.map_structure_with_path(
            lambda path, _: all_keys.add(force_tuple(path)),
            self.stats,
        )

        for key in all_keys:
            # Get all incoming Stats objects for this key
            incoming_stats = [
                self._get_key(key, stats=s)
                for s in incoming_stats_dicts_with_key
                if self._key_in_stats(key, stats=s)
            ]

            structure_under_key = self._get_key(key, stats=self.stats, key_error=False)
            # self._get_key returns {} if the key is not found
            own_stats = (
                None if isinstance(structure_under_key, dict) else structure_under_key
            )

            if own_stats is None:
                # This should happen the first time we reduce this stat to the root logger
                own_stats = incoming_stats[0].similar_to(incoming_stats[0])
                own_stats._is_root_stats = True

            merged_stats = own_stats.merge(incoming_stats=incoming_stats)

            self._set_key(key, merged_stats)

    def log_time(
        self,
        key: Union[str, Tuple[str, ...]],
        *,
        reduce: str = "mean",
        window: Optional[Union[int, float]] = None,
        ema_coeff: Optional[float] = None,
        percentiles: Union[List[int], bool] = False,
        clear_on_reduce: bool = False,
        with_throughput: bool = False,
        throughput_ema_coeff: Optional[float] = None,
        reduce_per_index_on_aggregate: Optional[bool] = None,
    ) -> StatsBase:
        """Measures and logs a time delta value under `key` when used with a with-block.

        Args:
            key: The key (or tuple of keys) to log the measured time delta under.
            reduce: The reduction method to apply when compiling metrics at the root logger.
                By default, the reduction methods to choose from here are the keys
                of rllib.utils.metrics.metrics_logger.DEFAULT_STATS_CLS_LOOKUP.
                You can provide your own reduce methods by extending rllib.utils.metrics.metrics_logger.DEFAULT_STATS_CLS_LOOKUP and passing it to AlgorithmConfig.logging()).
            window: An optional window size to reduce over.
                If not None, then the reduction operation is only applied to the most
                recent `window` items, and - after reduction - the internal values list
                under `key` is shortened to hold at most `window` items (the most
                recent ones).
                Must be None if `ema_coeff` is provided.
                If None (and `ema_coeff` is None), reduction must not be "mean".
            ema_coeff: An optional EMA coefficient to use if `reduce` is "mean"
                and no `window` is provided. Note that if both `window` and `ema_coeff`
                are provided, an error is thrown. Also, if `ema_coeff` is provided,
                `reduce` must be "mean".
                The reduction formula for EMA is:
                EMA(t1) = (1.0 - ema_coeff) * EMA(t0) + ema_coeff * new_value
            percentiles: If reduce is `None`, we can compute the percentiles of the
                values list given by `percentiles`. Defaults to [0, 0.5, 0.75, 0.9, 0.95,
                0.99, 1] if set to True. When using percentiles, a window must be provided.
                This window should be chosen carefully. RLlib computes exact percentiles and
                the computational complexity is O(m*n*log(n/m)) where n is the window size
                and m is the number of parallel metrics loggers involved (for example,
                m EnvRunners).
            clear_on_reduce: If True, all values under `key` will be emptied after
                `MetricsLogger.reduce()` is called. Setting this to True is useful for cases,
                in which the internal values list would otherwise grow indefinitely,
                for example if reduce is None and there is no `window` provided.
            with_throughput: Whether to track a throughput estimate together with this
                metric. This is only supported for `reduce=sum` and `reduce=lifetime_sum`.
                The current throughput estimate of a key can be obtained
                through: `MetricsLogger.peek(key, throughput=True)`.
            throughput_ema_coeff: Deprecated argument. Throughput is not smoothed with ema anymore
                but calculate once per MetricsLogger.reduce() call.
            reduce_per_index_on_aggregate: Deprecated argument. Aggregation now happens over all values
                of incoming stats objects once per MetricsLogger.reduce() call, treating each incoming value with equal weight.
        """
        (
            reduce,
            window,
            ema_coeff,
            percentiles,
            clear_on_reduce,
            with_throughput,
            throughput_ema_coeff,
            reduce_per_index_on_aggregate,
        ) = check_log_args(
            reduce,
            window,
            ema_coeff,
            percentiles,
            clear_on_reduce,
            with_throughput,
            throughput_ema_coeff,
            reduce_per_index_on_aggregate,
        )

        # No reduction (continue appending to list) AND no window.
        # -> We'll force-reset our values upon `reduce()`.
        if reduce is None and (window is None or window == float("inf")):
            clear_on_reduce = True

        if not self._key_in_stats(key):
            if reduce == "ema":
                self._set_key(
                    key,
                    EmaStats(
                        ema_coeff=ema_coeff,
                        clear_on_reduce=clear_on_reduce,
                    ),
                )
            elif reduce == "mean":
                self._set_key(
                    key,
                    MeanStats(
                        window=window,
                        clear_on_reduce=clear_on_reduce,
                    ),
                )
            elif reduce == "min":
                self._set_key(
                    key,
                    MinStats(
                        window=window,
                        clear_on_reduce=clear_on_reduce,
                    ),
                )
            elif reduce == "max":
                self._set_key(
                    key,
                    MaxStats(
                        window=window,
                        clear_on_reduce=clear_on_reduce,
                    ),
                )
            elif reduce == "sum":
                self._set_key(
                    key,
                    SumStats(
                        window=window,
                        clear_on_reduce=clear_on_reduce,
                        throughput=with_throughput,
                    ),
                )
            elif reduce == "lifetime_sum":
                self._set_key(
                    key,
                    LifetimeSumStats(
                        track_throughput_since_last_restore=with_throughput,
                        track_throughput_since_last_reduce=with_throughput,
                    ),
                )
            elif reduce == "percentiles":
                self._set_key(
                    key,
                    PercentilesStats(
                        window=window,
                        percentiles=percentiles,
                        clear_on_reduce=clear_on_reduce,
                        throughput=with_throughput,
                    ),
                )
            elif reduce == "item":
                raise ValueError("item reduction is not supported for time logging")
            elif reduce == "item_series":
                raise ValueError(
                    "item_series reduction is not supported for time logging"
                )
            elif reduce == "lifetime_sum":
                raise ValueError(
                    "lifetime_sum reduction is not supported for time logging"
                )
            else:
                raise ValueError(f"Invalid reduce method: {reduce}")

        # Return the Stats object, so a `with` clause can enter and exit it.
        return self._get_key(key)

    def reduce(self) -> Dict:
        """Reduces all logged values based on their settings and returns a result dict.

        DO NOT CALL THIS METHOD! This should be called only by RLlib when aggregating stats.

        Returns:
            A dict containing all ever logged nested keys to this MetricsLogger with the leafs being the reduced values.
        """
        # For better error message, catch the last key-path (reducing of which might
        # throw an error).
        PATH = None

        def _reduce(path, stats: StatsBase):
            nonlocal PATH
            PATH = path
            return stats.reduce(compile=self._is_root_logger)

        with self._threading_lock:
            reduced_stats_to_return = tree.map_structure_with_path(_reduce, self.stats)

        return reduced_stats_to_return

    @Deprecated(
        new="log_value",
        help="Use log_value with reduce='item' or another reduction method with a window of the appropriate size.",
        error=True,
    )
    def set_value(self, *args, **kwargs) -> None:
        ...

    def reset(self) -> None:
        """Resets all data stored in this MetricsLogger."""
        with self._threading_lock:
            self.stats = {}

    def delete(self, *key: Tuple[str, ...], key_error: bool = True) -> None:
        """Deletes the given `key` from this metrics logger's stats.

        Args:
            key: The key or key sequence (for nested location within self.stats),
                to delete from this MetricsLogger's stats.
            key_error: Whether to throw a KeyError if `key` cannot be found in `self`.

        Raises:
            KeyError: If `key` cannot be found in `self` AND `key_error` is True.
        """
        self._del_key(key, key_error)

    def get_state(self) -> Dict[str, Any]:
        """Returns the current state of `self` as a dict.

        Note that the state is merely the combination of all states of the individual
        `Stats` objects stored under `self.stats`.
        """
        stats_dict = {}

        def _map(path, stats):
            # Convert keys to strings for msgpack-friendliness.
            stats_dict["--".join(path)] = stats.get_state()

        with self._threading_lock:
            tree.map_structure_with_path(_map, self.stats)

        return {"stats": stats_dict}

    def set_state(self, state: Dict[str, Any]) -> None:
        """Sets the state of `self` to the given `state`.

        Args:
            state: The state to set `self` to.
        """
        with self._threading_lock:
            # Reset all existing stats to ensure a clean state transition
            self.stats = {}
            for flat_key, stats_state in state["stats"].items():
                if "stats_cls_identifier" in stats_state:
                    # Having a stats cls identifier means we are using the new stats classes.
                    cls_identifier = stats_state["stats_cls_identifier"]
                    assert (
                        cls_identifier in self.stats_cls_lookup
                    ), f"Stats class identifier {cls_identifier} not found in stats_cls_lookup"
                    _cls = self.stats_cls_lookup[cls_identifier]
                    stats = _cls.from_state(state=stats_state)
                else:
                    # We want to preserve compatibility with old checkpoints
                    # as much as possible.
                    stats = Stats.from_state(state=stats_state)

                self._set_key(flat_key.split("--"), stats)

    def _key_in_stats(self, flat_key, *, stats=None):
        flat_key = force_tuple(tree.flatten(flat_key))
        _dict = stats if stats is not None else self.stats
        for key in flat_key:
            if key not in _dict:
                return False
            _dict = _dict[key]
        return True

    def _get_key(self, flat_key, *, stats=None, key_error=True):
        flat_key = force_tuple(tree.flatten(flat_key))
        _dict = stats if stats is not None else self.stats
        for key in flat_key:
            try:
                _dict = _dict[key]
            except KeyError as e:
                if key_error:
                    raise e
                else:
                    return {}
        return _dict

    def _set_key(self, flat_key, stats):
        flat_key = force_tuple(tree.flatten(flat_key))

        with self._threading_lock:
            _dict = self.stats
            for i, key in enumerate(flat_key):
                # If we are at the end of the key sequence, set
                # the key, no matter, whether it already exists or not.
                if i == len(flat_key) - 1:
                    _dict[key] = stats
                    return
                # If an intermediary key in the sequence is missing,
                # add a sub-dict under this key.
                if key not in _dict:
                    _dict[key] = {}
                _dict = _dict[key]

    def _del_key(self, flat_key, key_error=False):
        flat_key = force_tuple(tree.flatten(flat_key))

        with self._threading_lock:
            # Erase the key from the (nested) `self.stats` dict.
            _dict = self.stats
            try:
                for i, key in enumerate(flat_key):
                    if i == len(flat_key) - 1:
                        del _dict[key]
                        return
                    _dict = _dict[key]
            except KeyError as e:
                if key_error:
                    raise e

    def _get_throughputs(
        self, key: Optional[Union[str, Tuple[str, ...]]] = None, default=None
    ) -> Union[Dict, float]:
        """Returns throughput values for Stats that have throughput tracking enabled.

        If no key is provided, returns a nested dict containing throughput values for all Stats
        that have throughput tracking enabled. If a key is provided, returns the throughput value
        for that specific key or nested structure.

        The throughput values represent the rate of change of the corresponding metrics per second.
        For example, if a metric represents the number of steps taken, its throughput value would
        represent steps per second.

        Args:
            key: Optional key or nested key path to get throughput for. If provided, returns just
                the throughput value for that key or nested structure. If None, returns a nested dict
                with throughputs for all metrics.
            default: Default value to return if no throughput values are found.
        Returns:
            If key is None: A nested dict with the same structure as self.stats but with "_throughput"
                appended to leaf keys and throughput values as leaf values. Only includes entries for
                Stats objects that have throughput tracking enabled.

            If key is provided: The throughput value for that specific key or nested structure.
        """

        def _nested_throughputs(stats):
            """Helper function to calculate throughputs for a nested structure."""

            def _transform(path, value):
                if isinstance(value, StatsBase) and value.has_throughputs:
                    # Convert path to tuple for consistent key handling
                    key = force_tuple(path)
                    # Add "_throughput" to the last key in the path
                    return key[:-1] + (key[-1] + "_throughputs",), value.throughputs
                return path, value

            result = {}
            for path, value in tree.flatten_with_path(stats):
                new_path, new_value = _transform(path, value)
                if isinstance(new_value, float):  # Only include throughput values
                    _dict = result
                    for k in new_path[:-1]:
                        if k not in _dict:
                            _dict[k] = {}
                        _dict = _dict[k]
                    _dict[new_path[-1]] = new_value
            return result

        with self._threading_lock:
            if key is not None:
                # Get the Stats object or nested structure for the key
                stats = self._get_key(key, key_error=False)

                if isinstance(stats, StatsBase):
                    if not stats.has_throughputs:
                        raise ValueError(
                            f"Key '{key}' does not have throughput tracking enabled"
                        )
                    return stats.throughputs
                elif stats == {}:
                    # If the key is not found, return the default value
                    return default
                else:
                    # stats is a non-empty dictionary
                    return _nested_throughputs(stats)

            throughputs = {}

            def _map(path, stats):
                if isinstance(stats, StatsBase) and stats.has_throughputs:
                    # Convert path to tuple for consistent key handling
                    key = force_tuple(path)
                    # Add "_throughput" to the last key in the path
                    key = key[:-1] + (key[-1] + "_throughput",)
                    # Set the throughput value in the nested structure
                    _dict = throughputs
                    for k in key[:-1]:
                        if k not in _dict:
                            _dict[k] = {}
                        _dict = _dict[k]
                    _dict[key[-1]] = stats.throughputs

            tree.map_structure_with_path(_map, self.stats)

            return throughputs if throughputs else default

    def compile(self) -> Dict:
        """Compiles all current values and throughputs into a single dictionary.

        This method combines the results of all stats and throughputs into a single
        dictionary, with throughput values having a "_throughput" suffix. This is useful
        for getting a complete snapshot of all metrics and their throughputs in one call.

        Returns:
            A nested dictionary containing both the current values and throughputs for all
            metrics. The structure matches self.stats, with throughput values having
            "_throughput" suffix in their keys.
        """
        # Get all current values
        values = self.reduce()

        # Get all throughputs
        throughputs = self._get_throughputs()

        deep_update(values, throughputs or {}, new_keys_allowed=True)

        def traverse_dict(d):
            if isinstance(d, dict):
                new_dict = {}
                for key, value in d.items():
                    new_dict[key] = traverse_dict(value)
                return new_dict
            elif isinstance(d, list):
                if len(d) == 1:
                    return d[0]
                # If value is a longer list, we should just return the list because there is no reduction method applied
                return d
            else:
                # If the value is not a list, it is a single value and we can yield it
                return d

        return traverse_dict(values)


class _DummyRLock:
    def acquire(self, blocking=True, timeout=-1):
        return True

    def release(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass
