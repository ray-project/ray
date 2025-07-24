import logging
from typing import Any, Dict, List, Optional, Tuple, Union
import tree  # pip install dm_tree

from ray.rllib.utils import force_tuple, deep_update
from ray.rllib.utils.metrics.stats import Stats, merge_stats
from ray.rllib.utils.deprecation import Deprecated, deprecation_warning
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.util.annotations import PublicAPI
from ray.util import log_once

_, tf, _ = try_import_tf()
torch, _ = try_import_torch()
logger = logging.getLogger("ray.rllib")


@PublicAPI(stability="alpha")
class MetricsLogger:
    """A generic class collecting and processing metrics in RL training and evaluation.

    This class represents the main API used by all of RLlib's components (internal and
    user facing) in order to log, collect, and process (reduce) stats during training
    and evaluation/inference.

    It supports:
    - Logging of simple float/int values (for example a loss) over time or from
    parallel runs (n Learner workers, each one reporting a loss from their respective
    data shard).
    - Logging of images, videos, or other more complex data structures over time.
    - Reducing these collected values using a user specified reduction method (for
    example "min" or "mean") and other settings controlling the reduction and internal
    data, such as sliding windows or EMA coefficients.
    - Optionally clearing all logged values after a `reduce()` call to make space for
    new data.
    - Tracking throughputs of logged values.

    .. testcode::

        import time
        from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
        from ray.rllib.utils.test_utils import check

        logger = MetricsLogger(root=True)

        # 1) Logging float values (mean over window):
        # Log some loss under the "loss" key. By default, all logged values
        # under that key are averaged and reported back, once `reduce()` is called.
        logger.log_value("loss", 0.001, reduce="mean", window=10)
        logger.log_value("loss", 0.002, reduce="mean", window=10)
        # Peek at the current (reduced) value of "loss":
        check(logger.peek("loss"), 0.0015)  # <- expect average value
        # Actually reduce the underlying Stats object(s).
        results = logger.reduce()
        check(results["loss"], 0.0015)

        # 2) Logging float values (minimum over window):
        # Log the minimum of loss values under the "min_loss" key.
        logger.log_value("min_loss", 0.1, reduce="min", window=2)
        logger.log_value("min_loss", 0.01, reduce="min", window=2)
        logger.log_value("min_loss", 0.1, reduce="min", window=2)
        logger.log_value("min_loss", 0.02, reduce="min", window=2)
        # Peek at the current (reduced) value of "min_loss":
        check(logger.peek("min_loss"), 0.02)  # <- expect min value (over window=2)
        # Actually reduce the underlying Stats object(s).
        results = logger.reduce()
        check(results["min_loss"], 0.02)

        # 3) Log n counts in different (remote?) components and merge them on the
        # controller side.
        remote_logger_1 = MetricsLogger()
        remote_logger_2 = MetricsLogger()
        main_logger = MetricsLogger()
        remote_logger_1.log_value("count", 2, reduce="sum", clear_on_reduce=True)
        remote_logger_2.log_value("count", 3, reduce="sum", clear_on_reduce=True)
        # Reduce the two remote loggers ..
        remote_results_1 = remote_logger_1.reduce()
        remote_results_2 = remote_logger_2.reduce()
        # .. then merge the two results into the controller logger.
        main_logger.aggregate([remote_results_1, remote_results_2])
        check(main_logger.peek("count"), 5)

        # 4) Time blocks of code using EMA (coeff=0.1). Note that the higher the coeff
        # (the closer to 1.0), the more short term the EMA turns out.
        logger = MetricsLogger()

        # First delta measurement:
        with logger.log_time("my_block_to_be_timed", reduce="mean", ema_coeff=0.1):
            time.sleep(1.0)
        # EMA should be ~1sec.
        assert 1.1 > logger.peek("my_block_to_be_timed") > 0.9
        # Second delta measurement (note that we don't have to repeat the args again, as
        # the stats under that name have already been created above with the correct
        # args).
        with logger.log_time("my_block_to_be_timed"):
            time.sleep(2.0)
        # EMA should be ~1.1sec.
        assert 1.15 > logger.peek("my_block_to_be_timed") > 1.05

        # When calling `reduce()`, the internal values list gets cleaned up (reduced)
        # and reduction results are returned.
        results = logger.reduce()
        # EMA should be ~1.1sec.
        assert 1.15 > results["my_block_to_be_timed"] > 1.05

        # 5) Keeping track of throughputs and compiling all metrics
        logger = MetricsLogger()
        logger.log_value("samples", 1.0, reduce="sum", with_throughput=True, throughput_ema_coeff=1.0)
        time.sleep(1.0)
        logger.log_value("samples", 2.0, reduce="sum", with_throughput=True, throughput_ema_coeff=1.0)
        results = logger.compile()
        check(results["samples"], 3.0)
        # Since we have an ema_coeff of 1.0, the throughput should be the same as the last value we logged (after 1 second)
        check(results["samples_throughput"], 2.0, rtol=0.1)

    """

    def __init__(self, root=False):
        """Initializes a MetricsLogger instance.

        Args:
            root: Whether this logger is a root logger. If True, lifetime stats
                (clear_on_reduce=False and reduction="sum") will not be cleared on reduce().
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

        .. testcode::
            from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
            from ray.rllib.utils.test_utils import check

            logger = MetricsLogger()
            ema = 0.01

            # Log some (EMA reduced) values.
            key = ("some", "nested", "key", "sequence")
            logger.log_value(key, 2.0, ema_coeff=ema)
            logger.log_value(key, 3.0)

            # Expected reduced value:
            expected_reduced = (1.0 - ema) * 2.0 + ema * 3.0

            # Peek at the (reduced) value under `key`.
            check(logger.peek(key), expected_reduced)

            # Peek at the (reduced) nested struct under ("some", "nested").
            check(
                logger.peek(("some", "nested")),
                {"key": {"sequence": expected_reduced}},
            )

            # Log some more, check again.
            logger.log_value(key, 4.0)
            expected_reduced = (1.0 - ema) * expected_reduced + ema * 4.0
            check(logger.peek(key=key), expected_reduced)

        Returns:
            The (reduced) values of the (possibly nested) sub-structure found under
            the given key or key sequence.
        """
        if throughput:
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
                if isinstance(s, Stats)
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

                if isinstance(stats, Stats):
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
            lambda s: s.peek(compile=compile) if isinstance(s, Stats) else s, results
        )

    def log_value(
        self,
        key: Union[str, Tuple[str, ...]],
        value: Any,
        *,
        reduce: Optional[str] = "mean",
        window: Optional[Union[int, float]] = None,
        ema_coeff: Optional[float] = None,
        percentiles: Union[List[int], bool] = False,
        clear_on_reduce: bool = False,
        with_throughput: bool = False,
        throughput_ema_coeff: Optional[float] = None,
        reduce_per_index_on_aggregate: bool = False,
    ) -> None:
        """Logs a new value under a (possibly nested) key to the logger.

        .. testcode::

            from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
            from ray.rllib.utils.test_utils import check

            logger = MetricsLogger(root=True)

            # Log n simple float values under the "loss" key. By default, all logged
            # values under that key are averaged, once `reduce()` is called.
            logger.log_value("loss", 0.01, window=10)
            logger.log_value("loss", 0.02, window=10)
            logger.log_value("loss", 0.03, window=10)

            # Peek at the current (reduced) value.
            # Note that in the underlying structure, the internal values list still
            # contains all logged values (0.01, 0.02, and 0.03).
            check(logger.peek("loss"), 0.02)

            # Log 10x (window size) the same value.
            for _ in range(10):
                logger.log_value("loss", 0.05, window=10)
            check(logger.peek("loss"), 0.05)

            # Internals check (note that users should not be concerned with accessing
            # these). Len should always be 10, since the underlying struct is a
            # `deque(max_len=10)`.
            check(len(logger.stats["loss"].values), 10)

            # Only, when we call `reduce` does the underlying structure get "cleaned
            # up". In this case, the list is shortened to 10 items (window size).
            results = logger.reduce()
            check(results, {"loss": 0.05})
            check(len(logger.stats["loss"].values), 10)

            # Log a value under a deeper nested key.
            logger.log_value(("some", "nested", "key"), -1.0)
            check(logger.peek(("some", "nested", "key")), -1.0)

            # Log n values without reducing them (we want to just collect some items).
            logger.log_value("some_items", 5.0, reduce=None)
            logger.log_value("some_items", 6.0, reduce=None)
            logger.log_value("some_items", 7.0, reduce=None)
            # Peeking at these returns the full list of items (no reduction set up).
            check(logger.peek("some_items"), [5.0, 6.0, 7.0])
            # If you don't want the internal list to grow indefinitely, you should set
            # `clear_on_reduce=True`:
            logger.log_value("some_more_items", -5.0, reduce=None, clear_on_reduce=True)
            logger.log_value("some_more_items", -6.0, reduce=None, clear_on_reduce=True)
            logger.log_value("some_more_items", -7.0, reduce=None, clear_on_reduce=True )
            # Peeking at these returns the full list of items (no reduction set up).
            check(logger.peek("some_more_items"), [-5.0, -6.0, -7.0])
            # Reducing everything (and return plain values, not `Stats` objects).
            results = logger.reduce()
            check(results, {
                "loss": 0.05,
                "some": {
                    "nested": {
                        "key": -1.0,
                    },
                },
                "some_items": [5.0, 6.0, 7.0],  # reduce=None; list as-is
                "some_more_items": [-5.0, -6.0, -7.0],  # reduce=None; list as-is
            })
            # However, the `reduce()` call did empty the `some_more_items` list
            # (b/c we set `clear_on_reduce=True`).
            check(logger.peek("some_more_items"), [])
            # ... but not the "some_items" list (b/c `clear_on_reduce=False`).
            check(logger.peek("some_items"), [])

        Args:
            key: The key (or nested key-tuple) to log the `value` under.
            value: The value to log. This should be a numeric value.
            reduce: The reduction method to apply, once `self.reduce()` is called.
                If None, will collect all logged values under `key` in a list (and
                also return that list upon calling `self.reduce()`).
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
            throughput_ema_coeff: The EMA coefficient to use for throughput tracking.
                Only used if with_throughput=True. Defaults to 0.05 if with_throughput is True.
            reduce_per_index_on_aggregate: If True, when merging Stats objects in parallel, we reduce
                incoming values per index such that the new value at index `n` will be
                the reduced value of all incoming values at index `n`.
                If False, when reducing `n` Stats, the first `n` merged values will be
                the reduced value of all incoming values at index `0`, the next `n` merged
                values will be the reduced values of all incoming values at index `1`, etc.
        """
        # No reduction (continue appending to list) AND no window.
        # -> We'll force-reset our values upon `reduce()`.
        if reduce is None and (window is None or window == float("inf")):
            clear_on_reduce = True

        # Set default ema_coeff to 0.01 if reduce is "mean" and no window is provided
        if reduce == "mean" and window is None and ema_coeff is None:
            ema_coeff = 0.01

        if with_throughput and throughput_ema_coeff is None:
            throughput_ema_coeff = 0.05

        value = self._detach_tensor_if_necessary(value)

        with self._threading_lock:
            # `key` doesn't exist -> Automatically create it.
            if not self._key_in_stats(key):
                self._set_key(
                    key,
                    (
                        Stats(
                            value,
                            reduce=reduce,
                            percentiles=percentiles,
                            window=window,
                            ema_coeff=ema_coeff,
                            clear_on_reduce=clear_on_reduce,
                            throughput=with_throughput,
                            throughput_ema_coeff=throughput_ema_coeff,
                            reduce_per_index_on_aggregate=reduce_per_index_on_aggregate,
                        )
                    ),
                )
            else:
                stats = self._get_key(key)
                if reduce != stats._reduce_method and log_once(f"reduce_warning_{key}"):
                    logger.warning(
                        f"reduce should be the same for all logged values under the same key, "
                        f"but got argument reduce={reduce} while the existing Stats object {key} "
                        f"has reduce={stats._reduce_method}."
                    )
                if clear_on_reduce != stats._clear_on_reduce and log_once(
                    f"clear_on_reduce_warning_{key}"
                ):
                    logger.warning(
                        f"clear_on_reduce should be the same for all logged values under the same key, "
                        f"but got argument clear_on_reduce={clear_on_reduce} while the existing Stats object {key} "
                        f"has clear_on_reduce={stats._clear_on_reduce}."
                    )
                if with_throughput != bool(stats.has_throughput) and log_once(
                    f"with_throughput_warning_{key}"
                ):
                    logger.warning(
                        f"with_throughput should be the same for all logged values under the same key, "
                        f"but got argument with_throughput={with_throughput} while the existing Stats object {key} "
                        f"has has_throughput={stats.has_throughput}. This warning will always be shown if you are using an older checkpoint."
                    )
                if throughput_ema_coeff != stats._throughput_ema_coeff and log_once(
                    f"throughput_ema_coeff_warning_{key}"
                ):
                    logger.warning(
                        f"throughput_ema_coeff should be the same for all logged values under the same key, "
                        f"but got argument throughput_ema_coeff={throughput_ema_coeff} while the existing Stats object {key} "
                        f"has throughput_ema_coeff={stats._throughput_ema_coeff}. This warning will always be shown if you are using an older checkpoint."
                    )
                if window != stats._window and log_once(f"window_warning_{key}"):
                    logger.warning(
                        f"window should be the same for all logged values under the same key, "
                        f"but got argument window={window} while the existing Stats object {key} "
                        f"has window={stats._window}."
                    )
                if percentiles != getattr(stats, "_percentiles", False) and log_once(
                    f"percentiles_warning_{key}"
                ):
                    logger.warning(
                        "percentiles should be the same for all logged values under the same key, "
                        f"but got argument percentiles={percentiles} while the existing Stats object {key} "
                        f"has percentiles={getattr(stats, '_percentiles', False)}."
                    )

                if (
                    reduce_per_index_on_aggregate
                    != stats._reduce_per_index_on_aggregate
                    and log_once(f"reduce_per_index_on_aggregate_warning_{key}")
                ):
                    logger.warning(
                        f"reduce_per_index_on_aggregate should be the same for all logged values under the same key, "
                        f"but got argument reduce_per_index_on_aggregate={reduce_per_index_on_aggregate} while the existing Stats object {key} "
                        f"has reduce_per_index_on_aggregate={stats._reduce_per_index_on_aggregate}."
                    )

                stats.push(value)

    def log_dict(
        self,
        value_dict,
        *,
        key: Optional[Union[str, Tuple[str, ...]]] = None,
        reduce: Optional[str] = "mean",
        window: Optional[Union[int, float]] = None,
        ema_coeff: Optional[float] = None,
        percentiles: Union[List[int], bool] = False,
        clear_on_reduce: bool = False,
        with_throughput: bool = False,
        throughput_ema_coeff: Optional[float] = None,
        reduce_per_index_on_aggregate: bool = False,
    ) -> None:
        """Logs all leafs of a possibly nested dict of values to this logger.

        To aggregate logs from upstream components, use `aggregate`.

        This is a convinience function that is equivalent to:
        ```
        tree.map_structure_with_path(lambda path, value: logger.log_value(path, value, ...), value_dict)
        ```

        Traverses through all leafs of `stats_dict` and - if a path cannot be found in
        this logger yet, will add the `Stats` found at the leaf under that new key.
        If a path already exists, will merge the found leaf (`Stats`) with the ones
        already logged before. This way, `stats_dict` does NOT have to have
        the same structure as what has already been logged to `self`, but can be used to
        log values under new keys or nested key paths.

        .. testcode::
            from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
            from ray.rllib.utils.test_utils import check

            logger = MetricsLogger()

            # Log n dicts with keys "a" and (some) "b". By default, all logged values
            # under that key are averaged, once `reduce()` is called.
            logger.log_dict(
                {
                    "a": 0.1,
                    "b": -0.1,
                },
                window=10,
            )
            logger.log_dict({
                "b": -0.2,
            })  # don't have to repeat `window` arg if key already exists
            logger.log_dict({
                "a": 0.2,
                "c": {"d": 5.0},  # can also introduce an entirely new (nested) key
            })

            # Peek at the current (reduced) values under "a" and "b".
            check(logger.peek("a"), 0.15)
            check(logger.peek("b"), -0.15)
            check(logger.peek(("c", "d")), 5.0)

            # Reduced all stats.
            results = logger.reduce()
            check(results, {
                "a": 0.15,
                "b": -0.15,
                "c": {"d": 5.0},
            })

        Args:
            value_dict: The (possibly nested) dict with individual values as
                leafs to be logged to this logger.
            key: An additional key (or tuple of keys) to prepend to all the keys
                (or tuples of keys in case of nesting) found inside `stats_dict`.
                Useful to log the entire contents of `stats_dict` in a more organized
                fashion under one new key, for example logging the results returned by
                an EnvRunner under key
            reduce: The reduction method to apply, once `self.reduce()` is called.
                If None, will collect all logged values under `key` in a list (and
                also return that list upon calling `self.reduce()`).
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
            throughput_ema_coeff: The EMA coefficient to use for throughput tracking.
                Only used if with_throughput=True. Defaults to 0.05 if with_throughput is True.
            reduce_per_index_on_aggregate: If True, when merging Stats objects, we reduce
                incoming values per index such that the new value at index `n` will be
                the reduced value of all incoming values at index `n`.
                If False, when reducing `n` Stats, the first `n` merged values will be
                the reduced value of all incoming values at index `0`, the next `n` merged
                values will be the reduced values of all incoming values at index `1`, etc.
        """
        assert isinstance(
            value_dict, dict
        ), f"`stats_dict` ({value_dict}) must be dict!"

        prefix_key = force_tuple(key)

        def _map(path, stat_or_value):
            extended_key = prefix_key + force_tuple(tree.flatten(path))

            if isinstance(stat_or_value, Stats):
                deprecation_warning(
                    old="MetricsLogger.log_dict() for Stats objects",
                    new="MetricsLogger.aggregate()",
                    error=True,
                )

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
        respective stats in parallel is meaningful. This lets us aggregate stats in a tree structure of
        MetricsLoggers.

        If you want to log a dictionary of values (not Stats objects), use `log_dict`.

        .. testcode::

            from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
            from ray.rllib.utils.test_utils import check

            # Example: n Learners logging loss stats to be merged.
            # Note that losses should usually be logged with a window=1 so they don't
            # get smeared over time and instead provide an accurate picture of the
            # current situation.
            main_logger = MetricsLogger()

            logger_learner1 = MetricsLogger()
            logger_learner1.log_value("loss", 0.1, window=1)
            learner1_results = logger_learner1.reduce()

            logger_learner2 = MetricsLogger()
            logger_learner2.log_value("loss", 0.2, window=1)
            learner2_results = logger_learner2.reduce()

            # Merge the stats from both Learners.
            main_logger.aggregate(
                [learner1_results, learner2_results],
                key="learners",
            )
            check(main_logger.peek(("learners", "loss")), 0.15)

            # Example: m EnvRunners logging episode returns to be merged.
            main_logger = MetricsLogger()

            logger_env_runner1 = MetricsLogger()
            logger_env_runner1.log_value("mean_ret", 100.0, window=3)
            logger_env_runner1.log_value("mean_ret", 200.0, window=3)
            logger_env_runner1.log_value("mean_ret", 300.0, window=3)
            logger_env_runner1.log_value("mean_ret", 400.0, window=3)
            env_runner1_results = logger_env_runner1.reduce()

            logger_env_runner2 = MetricsLogger()
            logger_env_runner2.log_value("mean_ret", 150.0, window=3)
            logger_env_runner2.log_value("mean_ret", 250.0, window=3)
            logger_env_runner2.log_value("mean_ret", 350.0, window=3)
            logger_env_runner2.log_value("mean_ret", 450.0, window=3)
            env_runner2_results = logger_env_runner2.reduce()

            # Merge the stats from both EnvRunners.
            main_logger.aggregate(
                [env_runner1_results, env_runner2_results],
                key="env_runners",
            )
            # The expected procedure is as follows:
            # The individual internal values lists of the two loggers are as follows:
            # env runner 1: [200, 300, 400]
            # env runner 2: [250, 350, 450]
            # Move backwards from index=-1 (each time, loop through both env runners)
            # index=-1 -> [400, 450] -> mean -> [425] -> repeat 2 times (number
            #   of env runners) -> [425, 425]
            # index=-2 -> [300, 350] -> mean -> [325] -> repeat 2 times
            #   -> append -> [425, 425, 325, 325] -> STOP b/c we have reached >= window.
            # reverse the list -> [325, 325, 425, 425]
            # deque(max_len=3) -> [325, 425, 425]
            check(
                main_logger.stats["env_runners"]["mean_ret"].values,
                [325, 425, 425],
            )
            check(main_logger.peek(("env_runners", "mean_ret")), (325 + 425 + 425) / 3)

            # Example: Lifetime sum over n parallel components' stats.
            main_logger = MetricsLogger()

            logger1 = MetricsLogger()
            logger1.log_value("some_stat", 50, reduce="sum", window=None)
            logger1.log_value("some_stat", 25, reduce="sum", window=None)
            logger1_results = logger1.reduce()

            logger2 = MetricsLogger()
            logger2.log_value("some_stat", 75, reduce="sum", window=None)
            logger2_results = logger2.reduce()

            # Merge the stats from both Learners.
            main_logger.aggregate([logger1_results, logger2_results])
            check(main_logger.peek("some_stat"), 150)

            # Example: Sum over n parallel components' stats with a window of 3.
            main_logger = MetricsLogger()

            logger1 = MetricsLogger()
            logger1.log_value("some_stat", 50, reduce="sum", window=3)
            logger1.log_value("some_stat", 25, reduce="sum", window=3)
            logger1.log_value("some_stat", 10, reduce="sum", window=3)
            logger1.log_value("some_stat", 5, reduce="sum", window=3)
            logger1_results = logger1.reduce()

            logger2 = MetricsLogger()
            logger2.log_value("some_stat", 75, reduce="sum", window=3)
            logger2.log_value("some_stat", 100, reduce="sum", window=3)
            logger2_results = logger2.reduce()

            # Merge the stats from both Learners.
            main_logger.aggregate([logger1_results, logger2_results])
            # The expected procedure is as follows:
            # The individual internal values lists of the two loggers are as follows:
            # env runner 1: [50, 25, 10, 5]
            # env runner 2: [75, 100]
            # Move backwards from index=-1 (each time, loop through both loggers)
            # index=-1 -> [5, 100] -> reduce over both two indices -> [(5 + 100) / 2, (5 + 100) / 2] = [52.5, 52.5]
            # Result = [52.5, 52.5]
            # len() = 2 < window = 3
            # index=-2 -> [10, 75] -> reduce over both two indices -> [(10 + 75) / 2, (10 + 75) / 2] = [42.5, 42.5]
            # result = [42.5, 42.5, 52.5, 52.5]
            # len() = 4 >= window = 3
            check(main_logger.peek("some_stat"), 147.5)  # last 3 items (window) get sum'd

        Args:
            stats_dicts: List of n stats dicts to be merged and then logged.
            key: Optional top-level key under which to log all keys/key sequences
                found in the n `stats_dicts`.
        """
        assert isinstance(stats_dicts, list), "stats_dicts must be a list"
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

            merged_stats = merge_stats(
                base_stats=own_stats, incoming_stats=incoming_stats
            )

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
        throughput_ema_coeff: float = 0.05,
        reduce_per_index_on_aggregate: bool = False,
    ) -> Stats:
        """Measures and logs a time delta value under `key` when used with a with-block.

        .. testcode::

            import time
            from ray.rllib.utils.metrics.metrics_logger import MetricsLogger

            logger = MetricsLogger()

            # First delta measurement:
            with logger.log_time("my_block_to_be_timed", ema_coeff=0.1):
                time.sleep(1.0)

            # EMA should be ~1sec.
            assert 1.1 > logger.peek("my_block_to_be_timed") > 0.9

            # Second delta measurement (note that we don't have to repeat the args
            # again, as the stats under that name have already been created above with
            # the correct args).
            with logger.log_time("my_block_to_be_timed"):
                time.sleep(2.0)

            # EMA should be ~1.1sec.
            assert 1.15 > logger.peek("my_block_to_be_timed") > 1.05

            # When calling `reduce()`, the latest, reduced value is returned.
            results = logger.reduce()
            # EMA should be ~1.1sec.
            assert 1.15 > results["my_block_to_be_timed"] > 1.05

        Args:
            key: The key (or tuple of keys) to log the measured time delta under.
            reduce: The reduction method to apply, once `self.reduce()` is called.
                If None, will collect all logged values under `key` in a list (and
                also return that list upon calling `self.reduce()`).
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
            throughput_ema_coeff: The EMA coefficient to use for throughput tracking.
                Only used if with_throughput=True. Defaults to 0.05.
            reduce_per_index_on_aggregate: If True, when merging Stats objects, we reduce
                incoming values per index such that the new value at index `n` will be
                the reduced value of all incoming values at index `n`.
                If False, when reducing `n` Stats, the first `n` merged values will be
                the reduced value of all incoming values at index `0`, the next `n` merged
                values will be the reduced values of all incoming values at index `1`, etc.
        """
        # No reduction (continue appending to list) AND no window.
        # -> We'll force-reset our values upon `reduce()`.
        if reduce is None and (window is None or window == float("inf")):
            clear_on_reduce = True

        # Set default ema_coeff to 0.01 if reduce is "mean" and no window is provided
        if reduce == "mean" and window is None and ema_coeff is None:
            ema_coeff = 0.01

        if not self._key_in_stats(key):
            self._set_key(
                key,
                Stats(
                    init_values=None,
                    reduce=reduce,
                    percentiles=percentiles,
                    window=window,
                    ema_coeff=ema_coeff,
                    clear_on_reduce=clear_on_reduce,
                    throughput=with_throughput,
                    throughput_ema_coeff=throughput_ema_coeff,
                    reduce_per_index_on_aggregate=reduce_per_index_on_aggregate,
                ),
            )

        # Return the Stats object, so a `with` clause can enter and exit it.
        return self._get_key(key)

    def reduce(self) -> Dict:
        """Reduces all logged values based on their settings and returns a result dict.

        DO NOT CALL THIS METHOD under normal circumstances! RLlib's components call it
        right before a distinct step has been completed and the (MetricsLogger-based)
        results of that step need to be passed upstream to other components for further
        processing.

        The returned result dict has the exact same structure as the logged keys (or
        nested key sequences) combined. Values are Stats objects if this MetricsLogger
        is not a root logger. If this MetricsLogger is a root logger, the values are
        the actual reduced values.

        For example, imagine component A (e.g. an Algorithm) containing a MetricsLogger
        and n remote components (e.g. n EnvRunners), each with their own
        MetricsLogger object. Component A calls its n remote components, each of
        which returns an equivalent, reduced dict with values at the leafs.
        Component A can then further log these n result dicts through its own
        MetricsLogger through:
        `logger.aggregate([n returned result dicts from n subcomponents])`.

        .. testcode::
            from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
            from ray.rllib.utils.test_utils import check
            logger = MetricsLogger(root=True)

            # Log some values under different keys.
            logger.log_value("loss", 0.1, window=2)
            logger.log_value("loss", 0.2, window=2)
            logger.log_value("min_loss", 0.3, reduce="min", window=2)
            logger.log_value("min_loss", 0.1, reduce="min", window=2)
            # reduce() returns the reduced values.
            results = logger.reduce()
            check(results["loss"], 0.15)  # mean of [0.1, 0.2]
            check(results["min_loss"], 0.1)  # min of [0.3, 0.1]
            # We can also reduce a specific key using indexing.
            check(logger.reduce()["loss"], 0.15)  # mean of [0.1, 0.2]
            # Or reduce a nested key structure.
            logger.log_value(("nested", "key"), 1.0)
            check(logger.reduce()["nested"]["key"], 1.0)

        Returns:
            A (nested) dict matching the structure of `self.stats` (contains all ever
            logged keys to this MetricsLogger) with the leafs being (reduced) Stats
            objects if this MetricsLogger is not a root logger. If this MetricsLogger
            is a root logger, the leafs are the actual (reduced) values.
        """
        # For better error message, catch the last key-path (reducing of which might
        # throw an error).
        PATH = None

        def _reduce(path, stats: Stats):
            nonlocal PATH
            PATH = path
            return stats.reduce(compile=self._is_root_logger)

        try:
            with self._threading_lock:
                reduced_stats_to_return = tree.map_structure_with_path(
                    _reduce, self.stats
                )
        # Provide proper error message if reduction fails due to bad data.
        except Exception as e:
            raise ValueError(
                "There was an error while reducing the Stats object under key="
                f"{PATH}! Check, whether you logged invalid or incompatible "
                "values into this key over time in your custom code."
                f"\nThe values under this key are: {self._get_key(PATH).values}."
                f"\nThe original error was {str(e)}"
            )

        return reduced_stats_to_return

    def activate_tensor_mode(self):
        self._tensor_mode = True

    def deactivate_tensor_mode(self):
        self._tensor_mode = False

    @property
    def tensor_mode(self):
        return self._tensor_mode

    def _detach_tensor_if_necessary(self, value):
        if self.tensor_mode:
            if torch and torch.is_tensor(value):
                return value.detach()
            elif tf and tf.is_tensor(value):
                return tf.stop_gradient(value)
        return value

    def set_value(
        self,
        key: Union[str, Tuple[str, ...]],
        value: Any,
        *,
        reduce: Optional[str] = "mean",
        window: Optional[Union[int, float]] = None,
        ema_coeff: Optional[float] = None,
        percentiles: Union[List[int], bool] = False,
        clear_on_reduce: bool = False,
        with_throughput: bool = False,
        throughput_ema_coeff: float = 0.05,
        reduce_per_index_on_aggregate: bool = False,
    ) -> None:
        """Overrides the logged values under `key` with `value`.

        The internal values list under `key` is cleared and reset to [`value`]. If
        `key` already exists, this method will NOT alter the reduce settings. Otherwise,
        it will apply the provided reduce settings (`reduce`, `window`, `ema_coeff`,
        and `clear_on_reduce`).

        Args:
            key: The key to override.
            value: The new value to set the internal values list to (will be set to
                a list containing a single item `value`).
            reduce: The reduction method to apply, once `self.reduce()` is called.
                If None, will collect all logged values under `key` in a list (and
                also return that list upon calling `self.reduce()`).
                Note that this is only applied if `key` does not exist in `self` yet.
            window: An optional window size to reduce over.
                If not None, then the reduction operation is only applied to the most
                recent `window` items, and - after reduction - the internal values list
                under `key` is shortened to hold at most `window` items (the most
                recent ones).
                Must be None if `ema_coeff` is provided.
                If None (and `ema_coeff` is None), reduction must not be "mean".
                Note that this is only applied if `key` does not exist in `self` yet.
            ema_coeff: An optional EMA coefficient to use if `reduce` is "mean"
                and no `window` is provided. Note that if both `window` and `ema_coeff`
                are provided, an error is thrown. Also, if `ema_coeff` is provided,
                `reduce` must be "mean".
                The reduction formula for EMA is:
                EMA(t1) = (1.0 - ema_coeff) * EMA(t0) + ema_coeff * new_value
                Note that this is only applied if `key` does not exist in `self` yet.
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
                Note that this is only applied if `key` does not exist in `self` yet.
            with_throughput: Whether to track a throughput estimate together with this
                metric. This is only supported for `reduce=sum` and
                `clear_on_reduce=False` metrics (aka. "lifetime counts"). The `Stats`
                object under the logged key then keeps track of the time passed
                between two consecutive calls to `reduce()` and update its throughput
                estimate. The current throughput estimate of a key can be obtained
                through: peeked_value, throuthput_per_sec =
                <MetricsLogger>.peek([key], throughput=True).
            throughput_ema_coeff: The EMA coefficient to use for throughput tracking.
                Only used if with_throughput=True. Defaults to 0.05.
            reduce_per_index_on_aggregate: If True, when merging Stats objects, we reduce
                incoming values per index such that the new value at index `n` will be
                the reduced value of all incoming values at index `n`.
                If False, when reducing `n` Stats, the first `n` merged values will be
                the reduced value of all incoming values at index `0`, the next `n` merged
                values will be the reduced values of all incoming values at index `1`, etc.
                Note that this is only applied if `key` does not exist in `self` yet.
        """
        # Key already in self -> Erase internal values list with [`value`].
        if self._key_in_stats(key):
            with self._threading_lock:
                stats = self._get_key(key)
                stats.values = [value]
        # Key cannot be found in `self` -> Simply log as a (new) value.
        else:
            # Set default ema_coeff to 0.01 if reduce is "mean" and no window is provided
            if reduce == "mean" and window is None and ema_coeff is None:
                ema_coeff = 0.01
            self.log_value(
                key,
                value,
                reduce=reduce,
                window=window,
                ema_coeff=ema_coeff,
                percentiles=percentiles,
                clear_on_reduce=clear_on_reduce,
                with_throughput=with_throughput,
                throughput_ema_coeff=throughput_ema_coeff,
                reduce_per_index_on_aggregate=reduce_per_index_on_aggregate,
            )

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
            for flat_key, stats_state in state["stats"].items():
                self._set_key(flat_key.split("--"), Stats.from_state(stats_state))

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
                if isinstance(value, Stats) and value.has_throughput:
                    # Convert path to tuple for consistent key handling
                    key = force_tuple(path)
                    # Add "_throughput" to the last key in the path
                    return key[:-1] + (key[-1] + "_throughput",), value.throughput
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

                if isinstance(stats, Stats):
                    if not stats.has_throughput:
                        raise ValueError(
                            f"Key '{key}' does not have throughput tracking enabled"
                        )
                    return stats.throughput
                elif stats == {}:
                    # If the key is not found, return the default value
                    return default
                else:
                    # stats is a non-empty dictionary
                    return _nested_throughputs(stats)

            throughputs = {}

            def _map(path, stats):
                if isinstance(stats, Stats) and stats.has_throughput:
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
                    _dict[key[-1]] = stats.throughput

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
