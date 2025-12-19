import logging
import time
from typing import Any, Dict, List, Optional, Tuple, Type, Union

import numpy as np
import tree  # pip install dm_tree

from ray._common.deprecation import DEPRECATED_VALUE, Deprecated, deprecation_warning
from ray.rllib.utils import deep_update, force_tuple
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.metrics.stats import (
    EmaStats,
    ItemSeriesStats,
    ItemStats,
    LifetimeSumStats,
    MaxStats,
    MeanStats,
    MinStats,
    PercentilesStats,
    StatsBase,
    SumStats,
)
from ray.util.annotations import DeveloperAPI, PublicAPI

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


# Note(Artur): Delete this in a couple of Ray releases.
@DeveloperAPI
def stats_from_legacy_state(state: Dict[str, Any], is_root: bool = False) -> StatsBase:
    """Creates a Stats object from a legacy state."""
    cls_identifier = state["reduce"]
    new_state = {
        # Always set is_leaf to True for legacy stats for compatibility
        "is_leaf": not is_root,  # We assume that legacy stats have been logged correctly (to leaf stats only) because we have no way of checking otherwise.
        "is_root": is_root,
        "latest_merged": [],  # Always include a latest_merged field for compatibility.
    }
    if state.get("clear_on_reduce", True) is False:
        if cls_identifier == "sum":
            new_state["stats_cls_identifier"] = "lifetime_sum"
            # lifetime sum
            if is_root:
                # With the new stats, only the root logger tracks values for lifetime sum.
                new_state["lifetime_sum"] = np.nansum(state["values"])
            else:
                new_state["lifetime_sum"] = 0.0

            # old lifetime sum checkpoints always track a througput
            if state.get("throughput_stats") is not None:
                new_state["track_throughputs"] = True
            else:
                new_state["track_throughputs"] = False

            _cls = DEFAULT_STATS_CLS_LOOKUP["lifetime_sum"]
            stats = _cls.from_state(state=new_state)
            return stats
        else:
            deprecation_warning(
                "Legacy Stats class tracking throughput detected. This is not supported anymore.",
                error=False,
            )

    if cls_identifier == "mean":
        if state["ema_coeff"] is not None:
            cls_identifier = "ema"
            new_state["ema_coeff"] = state["ema_coeff"]
            new_state["value"] = np.nanmean(state["values"])
            new_state["stats_cls_identifier"] = "ema"
        else:
            cls_identifier = "mean"
            new_state["values"] = state["values"]
            new_state["window"] = state["window"]
    elif cls_identifier in ["min", "max", "sum"]:
        new_state["values"] = state["values"]
        new_state["window"] = state["window"]
        if cls_identifier == "sum" and state.get("throughput_stats") is not None:
            new_state["track_throughput"] = True
        else:
            new_state["track_throughput"] = False
    elif cls_identifier is None and state.get("percentiles", False) is not False:
        # This is a percentiles stats (reduce=None with percentiles specified)
        cls_identifier = "percentiles"
        new_state["values"] = state["values"]
        new_state["window"] = state["window"]
        new_state["percentiles"] = state["percentiles"]
        new_state["stats_cls_identifier"] = "percentiles"
    elif cls_identifier == "percentiles":
        new_state["values"] = state["values"]
        new_state["window"] = state["window"]
        new_state["percentiles"] = state["percentiles"]

    _cls = DEFAULT_STATS_CLS_LOOKUP[cls_identifier]
    new_state["stats_cls_identifier"] = cls_identifier
    stats = _cls.from_state(state=new_state)
    return stats


@PublicAPI(stability="alpha")
class MetricsLogger:
    """A generic class collecting and reducing metrics.

    Use this API to log and merge metrics.
    Metrics should be logged in parallel components with MetricsLogger.log_value().
    RLlib will then aggregate metrics, reduce them and report them.

    The MetricsLogger supports logging anything that has a corresponding reduction method.
    These are defined natively in the Stats classes, which are used to log the metrics.
    Please take a look ray.rllib.utils.metrics.metrics_logger.DEFAULT_STATS_CLS_LOOKUP for the available reduction methods.
    You can provide your own reduce methods by extending ray.rllib.utils.metrics.metrics_logger.DEFAULT_STATS_CLS_LOOKUP and passing it to AlgorithmConfig.logging().

    Notes on architecture:
    In our docstrings we make heavy use of the phrase 'parallel components'.
    This pertains to the architecture of the logging system, where we have one 'root' MetricsLogger
    that is used to aggregate all metrics of n parallel ('non-root') MetricsLoggers that are used to log metrics for each parallel component.
    A parallel component is typically a single Learner, an EnvRunner, or a ConnectorV2 or any other component of which more than one instance is running in parallel.
    We also allow intermediate MetricsLoggers that are no root MetricsLogger but are used to aggregate metrics. They are therefore neither root nor leaf.
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
            root: Whether this logger is a root logger. If True, lifetime sums (reduce="lifetime_sum") will not be cleared on reduce().
            stats_cls_lookup: A dictionary mapping reduction method names to Stats classes.
                If not provided, the default lookup (ray.rllib.utils.metrics.metrics_logger.DEFAULT_STATS_CLS_LOOKUP) will be used.
                You can provide your own reduce methods by extending ray.rllib.utils.metrics.metrics_logger.DEFAULT_STATS_CLS_LOOKUP and passing it to AlgorithmConfig.logging().
        """
        self.stats = {}
        # TODO (sven): We use a dummy RLock here for most RLlib algos, however, APPO
        #  and IMPALA require this to be an actual RLock (b/c of thread safety reasons).
        #  An actual RLock, however, breaks our current OfflineData and
        #  OfflinePreLearner logic, in which the Learner (which contains a
        #  MetricsLogger) is serialized and deserialized. We will have to fix this
        #  offline RL logic first, then can remove this hack here and return to always
        #  using the RLock.
        self._threading_lock = _DummyRLock()
        self._is_root_logger = root
        self._time_when_initialized = time.perf_counter()
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
        latest_merged_only: bool = False,
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
            latest_merged_only: If True, only considers the latest merged values.
                This parameter only works on aggregation loggers (root or intermediate).

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
        def _nested_peek(stats: Dict[str, Any]):
            def _peek_with_path(path: str, stats: StatsBase):
                try:
                    return stats.peek(
                        compile=compile, latest_merged_only=latest_merged_only
                    )
                except Exception as e:
                    raise ValueError(
                        f"Error peeking stats {stats} with compile={compile} at path {path}."
                    ) from e

            return tree.map_structure_with_path(_peek_with_path, stats.copy())

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
                    return stats.peek(
                        compile=compile, latest_merged_only=latest_merged_only
                    )

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

    def _maybe_create_stats_object(
        self,
        key: Union[str, Tuple[str, ...]],
        *,
        reduce: str = "ema",
        window: Optional[Union[int, float]] = None,
        ema_coeff: Optional[float] = None,
        percentiles: Optional[Union[List[int], bool]] = None,
        clear_on_reduce: Optional[bool] = DEPRECATED_VALUE,
        with_throughput: Optional[bool] = None,
        throughput_ema_coeff: Optional[float] = DEPRECATED_VALUE,
        reduce_per_index_on_aggregate: Optional[bool] = DEPRECATED_VALUE,
        **kwargs: Dict[str, Any],
    ) -> None:
        """Prepare the kwargs and create the stats object if it doesn't exist."""
        with self._threading_lock:
            # `key` doesn't exist -> Automatically create it.
            if not self._key_in_stats(key):
                if reduce == "ema" and ema_coeff is None:
                    ema_coeff = 0.01

                if percentiles and not reduce == "percentiles":
                    raise ValueError(
                        "percentiles is only supported for reduce=percentiles"
                    )

                if reduce == "ema" and window is not None:
                    deprecation_warning(
                        "window is not supported for ema reduction. If you want to use a window, use mean reduction instead.",
                        error=True,
                    )
                    window = None

                if reduce_per_index_on_aggregate is not DEPRECATED_VALUE:
                    deprecation_warning(
                        "reduce_per_index_on_aggregate is deprecated. Aggregation now happens over all values"
                        "of incoming stats objects, treating each incoming value with equal weight.",
                        error=False,
                    )

                if throughput_ema_coeff is not DEPRECATED_VALUE:
                    deprecation_warning(
                        "throughput_ema_coeff is deprecated. Throughput is not smoothed with ema anymore"
                        "but calculate once per MetricsLogger.reduce() call.",
                        error=True,
                    )

                if reduce == "mean":
                    if ema_coeff is not None:
                        deprecation_warning(
                            "ema_coeff is not supported for mean reduction. Use `reduce='ema'` instead.",
                            error=True,
                        )

                if with_throughput and reduce not in ["sum", "lifetime_sum"]:
                    deprecation_warning(
                        "with_throughput=True is only supported for reduce='sum' or reduce='lifetime_sum'. Use reduce='sum' or reduce='lifetime_sum' instead.",
                        error=False,
                    )
                try:
                    stats_cls = self.stats_cls_lookup[reduce]
                except KeyError:
                    raise ValueError(
                        f"Invalid reduce method '{reduce}' could not be found in stats_cls_lookup"
                    )

                if window is not None:
                    kwargs["window"] = window
                if ema_coeff is not None:
                    kwargs["ema_coeff"] = ema_coeff
                if percentiles is not None:
                    kwargs["percentiles"] = percentiles
                if with_throughput is not None:
                    kwargs["with_throughput"] = with_throughput

                # Only stats at the root logger can be root stats
                kwargs["is_root"] = self._is_root_logger
                # Any Stats that are created in a logger are leaf stats by definition.
                # If they are aggregated from another logger, they are not leaf stats.
                kwargs["is_leaf"] = True

                stats_object = stats_cls(**kwargs)
                self._set_key(key, stats_object)

    def log_value(
        self,
        key: Union[str, Tuple[str, ...]],
        value: Any,
        *,
        reduce: Optional[str] = None,
        window: Optional[Union[int, float]] = None,
        ema_coeff: Optional[float] = None,
        percentiles: Optional[Union[List[int], bool]] = None,
        clear_on_reduce: Optional[bool] = DEPRECATED_VALUE,
        with_throughput: Optional[bool] = None,
        throughput_ema_coeff: Optional[float] = DEPRECATED_VALUE,
        reduce_per_index_on_aggregate: Optional[bool] = DEPRECATED_VALUE,
        **kwargs: Dict[str, Any],
    ) -> None:
        """Logs a new value or item under a (possibly nested) key to the logger.

        Args:
            key: The key (or nested key-tuple) to log the `value` under.
            value: A numeric value, an item to log or a StatsObject containing multiple values to log.
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
                Defaults to 0.01.
            percentiles: If reduce is `None`, we can compute the percentiles of the
                values list given by `percentiles`. Defaults to [0, 0.5, 0.75, 0.9, 0.95,
                0.99, 1] if set to True. When using percentiles, a window must be provided.
                This window should be chosen carefully. RLlib computes exact percentiles and
                the computational complexity is O(m*n*log(n/m)) where n is the window size
                and m is the number of parallel metrics loggers involved (for example,
                m EnvRunners).
            clear_on_reduce: Deprecated. Use reduce="lifetime_sum" instead.
                If True, all values under `key` will be cleared after
                `self.reduce()` is called. Setting this to True is useful for cases,
                in which the internal values list would otherwise grow indefinitely,
                for example if reduce is None and there is no `window` provided.
            with_throughput: Whether to track a throughput estimate together with this
                metric. This is supported by default only for `reduce=sum` and `reduce=lifetime_sum`.
            throughput_ema_coeff: Deprecated argument. Throughput is not smoothed with ema anymore
                but calculate once per MetricsLogger.reduce() call.
            reduce_per_index_on_aggregate: Deprecated argument. Aggregation now happens over all values
                of incoming stats objects once per MetricsLogger.reduce() call, treating each incoming value with equal weight.
        """
        # Some compatibility logic to support the legacy usage of MetricsLogger:
        # 1. If no reduce method is provided and a window is provided, use mean reduction.
        if reduce is None and window is not None:
            reduce = "mean"
        if reduce is None:
            reduce = "ema"
        # 2. If clear_on_reduce is provided, warn about deprecation.
        if clear_on_reduce is not DEPRECATED_VALUE:
            deprecation_warning(
                "clear_on_reduce is deprecated. Use reduce='lifetime_sum' for sums. Provide a custom reduce method for other cases.",
                error=False,
            )
        # 3. If reduce is sum and clear_on_reduce is False, use lifetime_sum instead
        if reduce == "sum" and clear_on_reduce is False:
            reduce = "lifetime_sum"
            clear_on_reduce = None

        # Prepare the kwargs for the stats object and create it if it doesn't exist
        self._maybe_create_stats_object(
            key,
            reduce=reduce,
            window=window,
            ema_coeff=ema_coeff,
            percentiles=percentiles,
            clear_on_reduce=clear_on_reduce,
            with_throughput=with_throughput,
            throughput_ema_coeff=throughput_ema_coeff,
            reduce_per_index_on_aggregate=reduce_per_index_on_aggregate,
        )
        stats = self._get_key(key)
        stats.push(value)

    def log_dict(
        self,
        value_dict,
        *,
        key: Optional[Union[str, Tuple[str, ...]]] = None,
        reduce: Optional[str] = "mean",
        window: Optional[Union[int, float]] = None,
        ema_coeff: Optional[float] = None,
        percentiles: Optional[Union[List[int], bool]] = None,
        clear_on_reduce: Optional[bool] = DEPRECATED_VALUE,
        with_throughput: Optional[bool] = None,
        throughput_ema_coeff: Optional[float] = DEPRECATED_VALUE,
        reduce_per_index_on_aggregate: Optional[bool] = DEPRECATED_VALUE,
    ) -> None:
        """Logs all leafs of a possibly nested dict of values or Stats objects to this logger.

        Traverses through all leafs of `stats_dict` and - if a path cannot be found in
        this logger yet, will add the `Stats` found at the leaf under that new key.
        If a path already exists, will merge the found leaf (`Stats`) with the ones
        already logged before. This way, `stats_dict` does NOT have to have
        the same structure as what has already been logged to `self`, but can be used to
        log values under new keys or nested key paths.

        Passing a dict of stats objects allows you to merge dictionaries of stats objects that
        have been reduced by other, parallel components.

        See MetricsLogger.log_value for more details on the arguments.
        """
        assert isinstance(
            value_dict, dict
        ), f"`stats_dict` ({value_dict}) must be dict!"

        prefix_key = force_tuple(key)

        def _map(path, stat_or_value):
            extended_key = prefix_key + force_tuple(tree.flatten(path))

            self.log_value(
                extended_key,
                value=stat_or_value,
                reduce=reduce,
                window=window,
                ema_coeff=ema_coeff,
                percentiles=percentiles,
                clear_on_reduce=clear_on_reduce,
                with_throughput=with_throughput,
                throughput_ema_coeff=throughput_ema_coeff,
                reduce_per_index_on_aggregate=reduce_per_index_on_aggregate,
            )

        with self._threading_lock:
            tree.map_structure_with_path(_map, value_dict)

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
        respective stats in parallel is meaningful. Stats can be aggregated at root or intermediate loggers.
        This will replace most internal values with the result of the merge.
        For exceptions, see the documentation of the individual stats classes `merge` methods.

        Args:
            stats_dicts: List of n stats dicts to be merged and then logged.
            key: Optional top-level key under which to log all keys/key sequences
                found in the n `stats_dicts`.
        """
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
                # This should happen the first time we reduce this stat to the root logger.
                # Clone without internal values to create a fresh aggregator
                own_stats = incoming_stats[0].clone(
                    init_overrides={"is_root": self._is_root_logger, "is_leaf": False},
                )
                if own_stats.has_throughputs:
                    own_stats.initialize_throughput_reference_time(
                        self._time_when_initialized
                    )
            else:
                # If own_stats exists, it must be a non-leaf stats (created by previous aggregation)
                # We cannot aggregate into a leaf stats (created by direct logging)
                assert (
                    not own_stats.is_leaf
                ), f"Cannot aggregate into key '{key}' because it was created by direct logging. Aggregation keys must be separate from direct logging keys."

            own_stats.merge(incoming_stats=incoming_stats)

            self._set_key(key, own_stats)

    def log_time(
        self,
        key: Union[str, Tuple[str, ...]],
        *,
        reduce: str = "ema",
        window: Optional[Union[int, float]] = None,
        ema_coeff: Optional[float] = None,
        percentiles: Optional[Union[List[int], bool]] = None,
        clear_on_reduce: Optional[bool] = DEPRECATED_VALUE,
        with_throughput: Optional[bool] = None,
        throughput_ema_coeff: Optional[float] = DEPRECATED_VALUE,
        reduce_per_index_on_aggregate: Optional[bool] = DEPRECATED_VALUE,
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
            clear_on_reduce: Deprecated. Use reduce="lifetime_sum" instead.
                If True, all values under `key` will be cleared after
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
        # Prepare the kwargs for the stats object and create it if it doesn't exist
        self._maybe_create_stats_object(
            key,
            reduce=reduce,
            window=window,
            ema_coeff=ema_coeff,
            percentiles=percentiles,
            clear_on_reduce=clear_on_reduce,
            with_throughput=with_throughput,
            throughput_ema_coeff=throughput_ema_coeff,
            reduce_per_index_on_aggregate=reduce_per_index_on_aggregate,
        )
        # Return the Stats object, so a `with` clause can enter and exit it.
        return self._get_key(key)

    def reduce(self, compile: bool = False) -> Dict:
        """Reduces all logged values based on their settings and returns a result dict.

        Note to user: Do not call this method directly! This should be called only by RLlib when aggregating stats.

        Args:
            compile: If True, the result is compiled into a single value if possible.
                If it is not possible, the result is a list of values.
                If False, the result is a list of one or more values.

        Returns:
            A dict containing all ever logged nested keys to this MetricsLogger with the leafs being the reduced stats.
        """

        def _reduce(path: str, stats: StatsBase):
            try:
                return stats.reduce(compile=compile)
            except Exception as e:
                raise ValueError(
                    f"Error reducing stats {stats} with compile={compile} at path {path}."
                ) from e

        with self._threading_lock:
            return tree.map_structure_with_path(_reduce, self.stats)

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
                    ), f"Stats class identifier {cls_identifier} not found in stats_cls_lookup. This can happen if you are loading a stats from a checkpoint that was created with a different stats class lookup."
                    _cls = self.stats_cls_lookup[cls_identifier]
                    stats = _cls.from_state(state=stats_state)
                else:
                    # We want to preserve compatibility with old checkpoints
                    # as much as possible.
                    stats = stats_from_legacy_state(
                        state=stats_state, is_root=self._is_root_logger
                    )

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
        # Get all throughputs
        throughputs = self._get_throughputs()

        # Get all current values
        values = self.reduce(compile=True)

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

    @Deprecated(
        new="",
        help="Tensor mode is not required anymore.",
        error=False,
    )
    def activate_tensor_mode(self):
        pass

    @Deprecated(
        new="",
        help="Tensor mode is not required anymore.",
        error=False,
    )
    def deactivate_tensor_mode(self):
        pass


class _DummyRLock:
    def acquire(self, blocking=True, timeout=-1):
        return True

    def release(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass
