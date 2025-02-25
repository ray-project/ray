import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import tree  # pip install dm_tree

from ray.rllib.utils import force_tuple
from ray.rllib.utils.metrics.stats import Stats
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.util.annotations import PublicAPI

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

    .. testcode::

        import time
        from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
        from ray.rllib.utils.test_utils import check

        logger = MetricsLogger()

        # 1) Logging float values (mean over window):
        # Log some loss under the "loss" key. By default, all logged values
        # under that key are averaged and reported back, once `reduce()` is called.
        logger.log_value("loss", 0.001, reduce="mean", window=10)
        logger.log_value("loss", 0.002)  # <- no need to repeat arg/options on same key
        # Peek at the current (reduced) value of "loss":
        check(logger.peek("loss"), 0.0015)  # <- expect average value
        # Actually reduce the underlying Stats object(s).
        results = logger.reduce()
        check(results["loss"], 0.0015)

        # 2) Logging float values (minimum over window):
        # Log the minimum of loss values under the "min_loss" key.
        logger.log_value("min_loss", 0.1, reduce="min", window=2)
        logger.log_value("min_loss", 0.01)
        logger.log_value("min_loss", 0.1)
        logger.log_value("min_loss", 0.02)
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
        main_logger.merge_and_log_n_dicts([remote_results_1, remote_results_2])
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


    """

    def __init__(self):
        """Initializes a MetricsLogger instance."""
        self.stats = {}
        self._tensor_mode = False
        self._tensor_keys = set()
        # TODO (sven): We use a dummy RLock here for most RLlib algos, however, APPO
        #  and IMPALA require this to be an actual RLock (b/c of thread safety reasons).
        #  An actual RLock, however, breaks our current OfflineData and
        #  OfflinePreLearner logic, in which the Learner (which contains a
        #  MetricsLogger) is serialized and deserialized. We will have to fix this
        #  offline RL logic first, then can remove this hack here and return to always
        #  using the RLock.
        self._threading_lock = _DummyRLock()

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
        key: Union[str, Tuple[str, ...]],
        *,
        default: Optional[Any] = None,
        throughput: bool = False,
    ) -> Any:
        """Returns the (reduced) value(s) found under the given key or key sequence.

        If `key` only reaches to a nested dict deeper in `self`, that
        sub-dictionary's entire values are returned as a (nested) dict with its leafs
        being the reduced peek values.

        Note that calling this method does NOT cause an actual underlying value list
        reduction, even though reduced values are being returned. It'll keep all
        internal structures as-is.

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
            check(logger.peek(key), expected_reduced)

        Args:
            key: The key/key sequence of the sub-structure of `self`, whose (reduced)
                values to return.
            default: An optional default value in case `key` cannot be found in `self`.
                If default is not provided and `key` cannot be found, throws a KeyError.
            throughput: Whether to return the current throughput estimate instead of the
                actual (reduced) value.

        Returns:
            The (reduced) values of the (possibly nested) sub-structure found under
            the given `key` or key sequence.

        Raises:
            KeyError: If `key` cannot be found AND `default` is not provided.
        """
        # Use default value, b/c `key` cannot be found in our stats.
        if not self._key_in_stats(key) and default is not None:
            return default

        # Otherwise, return the reduced Stats' (peek) value.
        struct = self._get_key(key)

        # Create a reduced view of the requested sub-structure or leaf (Stats object).
        with self._threading_lock:
            if isinstance(struct, Stats):
                return struct.peek(throughput=throughput)

            ret = tree.map_structure(
                lambda s: s.peek(throughput=throughput),
                struct.copy(),
            )
            return ret

    @staticmethod
    def peek_results(results: Any) -> Any:
        """Performs `peek()` on any leaf element of an arbitrarily nested Stats struct.

        Args:
            results: The nested structure of Stats-leafs to be peek'd and returned.

        Returns:
            A corresponding structure of the peek'd `results` (reduced float/int values;
            no Stats objects).
        """
        return tree.map_structure(
            lambda s: s.peek() if isinstance(s, Stats) else s, results
        )

    def log_value(
        self,
        key: Union[str, Tuple[str, ...]],
        value: Any,
        *,
        reduce: Optional[str] = "mean",
        window: Optional[Union[int, float]] = None,
        ema_coeff: Optional[float] = None,
        clear_on_reduce: bool = False,
        with_throughput: bool = False,
    ) -> None:
        """Logs a new value under a (possibly nested) key to the logger.

        .. testcode::

            from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
            from ray.rllib.utils.test_utils import check

            logger = MetricsLogger()

            # Log n simple float values under the "loss" key. By default, all logged
            # values under that key are averaged, once `reduce()` is called.
            logger.log_value("loss", 0.01, window=10)
            logger.log_value("loss", 0.02)  # don't have to repeat `window` if key
                                            # already exists
            logger.log_value("loss", 0.03)

            # Peek at the current (reduced) value.
            # Note that in the underlying structure, the internal values list still
            # contains all logged values (0.01, 0.02, and 0.03).
            check(logger.peek("loss"), 0.02)

            # Log 10x (window size) the same value.
            for _ in range(10):
                logger.log_value("loss", 0.05)
            check(logger.peek("loss"), 0.05)

            # Internals check (note that users should not be concerned with accessing
            # these). Len should always be 10, since the underlying struct is a
            # `deque(max_len=10)`.
            check(len(logger.stats["loss"].values), 10)

            # Only, when we call `reduce` does the underlying structure get "cleaned
            # up". In this case, the list is shortened to 10 items (window size).
            results = logger.reduce(return_stats_obj=False)
            check(results, {"loss": 0.05})
            check(len(logger.stats["loss"].values), 10)

            # Log a value under a deeper nested key.
            logger.log_value(("some", "nested", "key"), -1.0)
            check(logger.peek(("some", "nested", "key")), -1.0)

            # Log n values without reducing them (we want to just collect some items).
            logger.log_value("some_items", 5.0, reduce=None)
            logger.log_value("some_items", 6.0)
            logger.log_value("some_items", 7.0)
            # Peeking at these returns the full list of items (no reduction set up).
            check(logger.peek("some_items"), [5.0, 6.0, 7.0])
            # If you don't want the internal list to grow indefinitely, you should set
            # `clear_on_reduce=True`:
            logger.log_value("some_more_items", -5.0, reduce=None, clear_on_reduce=True)
            logger.log_value("some_more_items", -6.0)
            logger.log_value("some_more_items", -7.0)
            # Peeking at these returns the full list of items (no reduction set up).
            check(logger.peek("some_more_items"), [-5.0, -6.0, -7.0])
            # Reducing everything (and return plain values, not `Stats` objects).
            results = logger.reduce(return_stats_obj=False)
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
            value: The value to log.
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
                through: peeked_value, throuthput_per_sec =
                <MetricsLogger>.peek([key], throughput=True).
        """
        # No reduction (continue appending to list) AND no window.
        # -> We'll force-reset our values upon `reduce()`.
        if reduce is None and (window is None or window == float("inf")):
            clear_on_reduce = True

        self._check_tensor(key, value)

        with self._threading_lock:
            # `key` doesn't exist -> Automatically create it.
            if not self._key_in_stats(key):
                self._set_key(
                    key,
                    (
                        Stats.similar_to(value, init_value=value.values)
                        if isinstance(value, Stats)
                        else Stats(
                            value,
                            reduce=reduce,
                            window=window,
                            ema_coeff=ema_coeff,
                            clear_on_reduce=clear_on_reduce,
                            throughput=with_throughput,
                        )
                    ),
                )
            # If value itself is a `Stats`, we merge it on time axis into self's
            # `Stats`.
            elif isinstance(value, Stats):
                self._get_key(key).merge_on_time_axis(value)
            # Otherwise, we just push the value into self's `Stats`.
            else:
                self._get_key(key).push(value)

    def log_dict(
        self,
        stats_dict,
        *,
        key: Optional[Union[str, Tuple[str, ...]]] = None,
        reduce: Optional[str] = "mean",
        window: Optional[Union[int, float]] = None,
        ema_coeff: Optional[float] = None,
        clear_on_reduce: bool = False,
    ) -> None:
        """Logs all leafs (`Stats` or simple values) of a (nested) dict to this logger.

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
            results = logger.reduce(return_stats_obj=False)
            check(results, {
                "a": 0.15,
                "b": -0.15,
                "c": {"d": 5.0},
            })

        Args:
            stats_dict: The (possibly nested) dict with `Stats` or individual values as
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
            clear_on_reduce: If True, all values under `key` will be emptied after
                `self.reduce()` is called. Setting this to True is useful for cases,
                in which the internal values list would otherwise grow indefinitely,
                for example if reduce is None and there is no `window` provided.
        """
        assert isinstance(
            stats_dict, dict
        ), f"`stats_dict` ({stats_dict}) must be dict!"

        prefix_key = force_tuple(key)

        def _map(path, stat_or_value):
            extended_key = prefix_key + force_tuple(tree.flatten(path))

            self.log_value(
                extended_key,
                stat_or_value,
                reduce=reduce,
                window=window,
                ema_coeff=ema_coeff,
                clear_on_reduce=clear_on_reduce,
            )

        with self._threading_lock:
            tree.map_structure_with_path(_map, stats_dict)

    def merge_and_log_n_dicts(
        self,
        stats_dicts: List[Dict[str, Any]],
        *,
        key: Optional[Union[str, Tuple[str, ...]]] = None,
        # TODO (sven): Maybe remove these args. They don't seem to make sense in this
        #  method. If we do so, values in the dicts must be Stats instances, though.
        reduce: Optional[str] = "mean",
        window: Optional[Union[int, float]] = None,
        ema_coeff: Optional[float] = None,
        clear_on_reduce: bool = False,
    ) -> None:
        """Merges n dicts, generated by n parallel components, and logs the results.

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
            main_logger.merge_and_log_n_dicts(
                [learner1_results, learner2_results],
                key="learners",
            )
            check(main_logger.peek(("learners", "loss")), 0.15)

            # Example: m EnvRunners logging episode returns to be merged.
            main_logger = MetricsLogger()

            logger_env_runner1 = MetricsLogger()
            logger_env_runner1.log_value("mean_ret", 100.0, window=3)
            logger_env_runner1.log_value("mean_ret", 200.0)
            logger_env_runner1.log_value("mean_ret", 300.0)
            logger_env_runner1.log_value("mean_ret", 400.0)
            env_runner1_results = logger_env_runner1.reduce()

            logger_env_runner2 = MetricsLogger()
            logger_env_runner2.log_value("mean_ret", 150.0, window=3)
            logger_env_runner2.log_value("mean_ret", 250.0)
            logger_env_runner2.log_value("mean_ret", 350.0)
            logger_env_runner2.log_value("mean_ret", 450.0)
            env_runner2_results = logger_env_runner2.reduce()

            # Merge the stats from both EnvRunners.
            main_logger.merge_and_log_n_dicts(
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
            main_logger.merge_and_log_n_dicts([logger1_results, logger2_results])
            check(main_logger.peek("some_stat"), 150)

            # Example: Sum over n parallel components' stats with a window of 3.
            main_logger = MetricsLogger()

            logger1 = MetricsLogger()
            logger1.log_value("some_stat", 50, reduce="sum", window=3)
            logger1.log_value("some_stat", 25, reduce="sum")
            logger1.log_value("some_stat", 10, reduce="sum")
            logger1.log_value("some_stat", 5, reduce="sum")
            logger1_results = logger1.reduce()

            logger2 = MetricsLogger()
            logger2.log_value("some_stat", 75, reduce="sum", window=3)
            logger2.log_value("some_stat", 100, reduce="sum")
            logger2_results = logger2.reduce()

            # Merge the stats from both Learners.
            main_logger.merge_and_log_n_dicts([logger1_results, logger2_results])
            # The expected procedure is as follows:
            # The individual internal values lists of the two loggers are as follows:
            # env runner 1: [50, 25, 10, 5]
            # env runner 2: [75, 100]
            # Move backwards from index=-1 (each time, loop through both loggers)
            # index=-1 -> [5, 100] -> leave as-is, b/c we are sum'ing -> [5, 100]
            # index=-2 -> [10, 75] -> leave as-is -> [5, 100, 10, 75] -> STOP b/c we
            # have reached >= window.
            # reverse the list -> [75, 10, 100, 5]
            check(main_logger.peek("some_stat"), 115)  # last 3 items (window) get sum'd

        Args:
            stats_dicts: List of n stats dicts to be merged and then logged.
            key: Optional top-level key under which to log all keys/key sequences
                found in the n `stats_dicts`.
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
            clear_on_reduce: If True, all values under `key` will be emptied after
                `self.reduce()` is called. Setting this to True is useful for cases,
                in which the internal values list would otherwise grow indefinitely,
                for example if reduce is None and there is no `window` provided.
        """
        prefix_key = force_tuple(key)

        all_keys = set()
        for stats_dict in stats_dicts:
            tree.map_structure_with_path(
                lambda path, _: all_keys.add(force_tuple(path)),
                stats_dict,
            )

        # No reduction (continue appending to list) AND no window.
        # -> We'll force-reset our values upon `reduce()`.
        if reduce is None and (window is None or window == float("inf")):
            clear_on_reduce = True

        for key in all_keys:
            extended_key = prefix_key + key
            available_stats = [
                self._get_key(key, stats=s)
                for s in stats_dicts
                if self._key_in_stats(key, stats=s)
            ]
            base_stats = None
            more_stats = []
            for i, stat_or_value in enumerate(available_stats):
                # Value is NOT a Stats object -> Convert it to one.
                if not isinstance(stat_or_value, Stats):
                    self._check_tensor(extended_key, stat_or_value)
                    available_stats[i] = stat_or_value = Stats(
                        stat_or_value,
                        reduce=reduce,
                        window=window,
                        ema_coeff=ema_coeff,
                        clear_on_reduce=clear_on_reduce,
                    )

                # Create a new Stats object to merge everything into as parallel,
                # equally weighted Stats.
                if base_stats is None:
                    base_stats = Stats.similar_to(
                        stat_or_value,
                        init_value=stat_or_value.values,
                    )
                else:
                    more_stats.append(stat_or_value)

            # Special case: `base_stats` is a lifetime sum (reduce=sum,
            # clear_on_reduce=False) -> We subtract the previous value (from 2
            # `reduce()` calls ago) from all to-be-merged stats, so we don't count
            # twice the older sum from before.
            if (
                base_stats._reduce_method == "sum"
                and base_stats._window is None
                and base_stats._clear_on_reduce is False
            ):
                for stat in [base_stats] + more_stats:
                    stat.push(-stat.peek(previous=2))

            # There are more than one incoming parallel others -> Merge all of them
            # first in parallel.
            if len(more_stats) > 0:
                base_stats.merge_in_parallel(*more_stats)

            # `key` not in self yet -> Store merged stats under the new key.
            if not self._key_in_stats(extended_key):
                self._set_key(extended_key, base_stats)
            # `key` already exists in `self` -> Merge `base_stats` into self's entry
            # on time axis, meaning give the incoming values priority over already
            # existing ones.
            else:
                self._get_key(extended_key).merge_on_time_axis(base_stats)

    def log_time(
        self,
        key: Union[str, Tuple[str, ...]],
        *,
        reduce: Optional[str] = "mean",
        window: Optional[Union[int, float]] = None,
        ema_coeff: Optional[float] = None,
        clear_on_reduce: bool = False,
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
            clear_on_reduce: If True, all values under `key` will be emptied after
                `self.reduce()` is called. Setting this to True is useful for cases,
                in which the internal values list would otherwise grow indefinitely,
                for example if reduce is None and there is no `window` provided.
        """
        # No reduction (continue appending to list) AND no window.
        # -> We'll force-reset our values upon `reduce()`.
        if reduce is None and (window is None or window == float("inf")):
            clear_on_reduce = True

        if not self._key_in_stats(key):
            self._set_key(
                key,
                Stats(
                    reduce=reduce,
                    window=window,
                    ema_coeff=ema_coeff,
                    clear_on_reduce=clear_on_reduce,
                ),
            )

        # Return the Stats object, so a `with` clause can enter and exit it.
        return self._get_key(key)

    def reduce(
        self,
        key: Optional[Union[str, Tuple[str, ...]]] = None,
        *,
        return_stats_obj: bool = True,
    ) -> Dict:
        """Reduces all logged values based on their settings and returns a result dict.

        DO NOT CALL THIS METHOD under normal circumstances! RLlib's components call it
        right before a distinct step has been completed and the (MetricsLogger-based)
        results of that step need to be passed upstream to other components for further
        processing.

        The returned result dict has the exact same structure as the logged keys (or
        nested key sequences) combined. At the leafs of the returned structure are
        either `Stats` objects (`return_stats_obj=True`, which is the default) or
        primitive (non-Stats) values (`return_stats_obj=False`). In case of
        `return_stats_obj=True`, the returned dict with `Stats` at the leafs can
        conveniently be re-used upstream for further logging and reduction operations.

        For example, imagine component A (e.g. an Algorithm) containing a MetricsLogger
        and n remote components (e.g. n EnvRunners), each with their own
        MetricsLogger object. Component A calls its n remote components, each of
        which returns an equivalent, reduced dict with `Stats` as leafs.
        Component A can then further log these n result dicts through its own
        MetricsLogger through:
        `logger.merge_and_log_n_dicts([n returned result dicts from n subcomponents])`.

        The returned result dict has the exact same structure as the logged keys (or
        nested key sequences) combined. At the leafs of the returned structure are
        either `Stats` objects (`return_stats_obj=True`, which is the default) or
        primitive (non-Stats) values (`return_stats_obj=False`). In case of
        `return_stats_obj=True`, the returned dict with Stats at the leafs can be
        reused conveniently  downstream for further logging and reduction operations.

        For example, imagine component A (e.g. an Algorithm) containing a MetricsLogger
        and n remote components (e.g. n EnvRunner workers), each with their own
        MetricsLogger object. Component A calls its n remote components, each of
        which returns an equivalent, reduced dict with `Stats` instances as leafs.
        Component A can now further log these n result dicts through its own
        MetricsLogger:
        `logger.merge_and_log_n_dicts([n returned result dicts from the remote
        components])`.

        .. testcode::

            from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
            from ray.rllib.utils.test_utils import check

            # Log some (EMA reduced) values.
            logger = MetricsLogger()
            logger.log_value("a", 2.0)
            logger.log_value("a", 3.0)
            expected_reduced = (1.0 - 0.01) * 2.0 + 0.01 * 3.0
            # Reduce and return primitive values (not Stats objects).
            results = logger.reduce(return_stats_obj=False)
            check(results, {"a": expected_reduced})

            # Log some values to be averaged with a sliding window.
            logger = MetricsLogger()
            logger.log_value("a", 2.0, window=2)
            logger.log_value("a", 3.0)
            logger.log_value("a", 4.0)
            expected_reduced = (3.0 + 4.0) / 2  # <- win size is only 2; first logged
                                                # item not used
            # Reduce and return primitive values (not Stats objects).
            results = logger.reduce(return_stats_obj=False)
            check(results, {"a": expected_reduced})

            # Assume we have 2 remote components, each one returning an equivalent
            # reduced dict when called. We can simply use these results and log them
            # to our own MetricsLogger, then reduce over these 2 logged results.
            comp1_logger = MetricsLogger()
            comp1_logger.log_value("a", 1.0, window=10)
            comp1_logger.log_value("a", 2.0)
            result1 = comp1_logger.reduce()  # <- return Stats objects as leafs

            comp2_logger = MetricsLogger()
            comp2_logger.log_value("a", 3.0, window=10)
            comp2_logger.log_value("a", 4.0)
            result2 = comp2_logger.reduce()  # <- return Stats objects as leafs

            # Now combine the 2 equivalent results into 1 end result dict.
            downstream_logger = MetricsLogger()
            downstream_logger.merge_and_log_n_dicts([result1, result2])
            # What happens internally is that both values lists of the 2 components
            # are merged (concat'd) and randomly shuffled, then clipped at 10 (window
            # size). This is done such that no component has an "advantage" over the
            # other as we don't know the exact time-order in which these parallelly
            # running components logged their own "a"-values.
            # We execute similarly useful merging strategies for other reduce settings,
            # such as EMA, max/min/sum-reducing, etc..
            end_result = downstream_logger.reduce(return_stats_obj=False)
            check(end_result, {"a": 2.5})

        Args:
            key: Optional key or key sequence (for nested location within self.stats),
                limiting the reduce operation to that particular sub-structure of self.
                If None, will reduce all of self's Stats.
            return_stats_obj: Whether in the returned dict, the leafs should be Stats
                objects. This is the default as it enables users to continue using
                (and further logging) the results of this call inside another
                (downstream) MetricsLogger object.

        Returns:
            A (nested) dict matching the structure of `self.stats` (contains all ever
            logged keys to this MetricsLogger) with the leafs being (reduced) Stats
            objects if `return_stats_obj=True` or primitive values, carrying no
            reduction and history information, if `return_stats_obj=False`.
        """
        # For better error message, catch the last key-path (reducing of which might
        # throw an error).
        PATH = None

        def _reduce(path, stats: Stats):
            nonlocal PATH
            PATH = path
            return stats.reduce()

        if key is not None:
            stats_to_return = self._get_key(key, key_error=False)
        else:
            stats_to_return = self.stats

        try:
            with self._threading_lock:
                assert (
                    not self.tensor_mode
                ), "Can't reduce if `self.tensor_mode` is True!"
                reduced_stats_to_return = tree.map_structure_with_path(
                    _reduce, stats_to_return
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

        # Return (reduced) `Stats` objects as leafs.
        if return_stats_obj:
            return reduced_stats_to_return
        # Return actual (reduced) values (not reduced `Stats` objects) as leafs.
        else:
            return self.peek_results(reduced_stats_to_return)

    def activate_tensor_mode(self):
        """Switches to tensor-mode, in which in-graph tensors can be logged.

        Should be used before calling in-graph/copmiled functions, for example loss
        functions. The user can then still call the `log_...` APIs, but each incoming
        value will be checked for a) whether it is a tensor indeed and b) the `window`
        args must be 1 (MetricsLogger does not support any tensor-framework reducing
        operations).

        When in tensor-mode, we also track all incoming `log_...` values and return
        them TODO (sven) continue docstring

        """
        self._threading_lock.acquire()
        assert not self.tensor_mode
        self._tensor_mode = True

    def deactivate_tensor_mode(self):
        """Switches off tensor-mode."""
        assert self.tensor_mode
        self._tensor_mode = False
        # Return all logged tensors (logged during the tensor-mode phase).
        logged_tensors = {key: self._get_key(key).peek() for key in self._tensor_keys}
        # Clear out logged tensor keys.
        self._tensor_keys.clear()
        return logged_tensors

    def tensors_to_numpy(self, tensor_metrics):
        """Converts all previously logged and returned tensors back to numpy values."""
        for key, values in tensor_metrics.items():
            assert self._key_in_stats(key)
            self._get_key(key).set_to_numpy_values(values)
        self._threading_lock.release()

    @property
    def tensor_mode(self):
        return self._tensor_mode

    def set_value(
        self,
        key: Union[str, Tuple[str, ...]],
        value: Any,
        *,
        reduce: Optional[str] = "mean",
        window: Optional[Union[int, float]] = None,
        ema_coeff: Optional[float] = None,
        clear_on_reduce: bool = False,
        with_throughput: bool = False,
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
        """
        # Key already in self -> Erase internal values list with [`value`].
        if self._key_in_stats(key):
            stats = self._get_key(key)
            with self._threading_lock:
                stats.values = [value]
        # Key cannot be found in `self` -> Simply log as a (new) value.
        else:
            self.log_value(
                key,
                value,
                reduce=reduce,
                window=window,
                ema_coeff=ema_coeff,
                clear_on_reduce=clear_on_reduce,
                with_throughput=with_throughput,
            )

    def reset(self) -> None:
        """Resets all data stored in this MetricsLogger.

        .. testcode::

            from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
            from ray.rllib.utils.test_utils import check

            logger = MetricsLogger()
            logger.log_value("a", 1.0)
            check(logger.peek("a"), 1.0)
            logger.reset()
            check(logger.reduce(), {})
        """
        with self._threading_lock:
            self.stats = {}
            self._tensor_keys = set()

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

    def _check_tensor(self, key: Tuple[str], value) -> None:
        # `value` is a tensor -> Log it in our keys set.
        if self.tensor_mode and (
            (torch and torch.is_tensor(value)) or (tf and tf.is_tensor(value))
        ):
            self._tensor_keys.add(key)

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
            # Erase the tensor key as well, if applicable.
            if flat_key in self._tensor_keys:
                self._tensor_keys.discard(flat_key)

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


class _DummyRLock:
    def acquire(self, blocking=True, timeout=-1):
        return True

    def release(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass
