import logging
from typing import Any, Dict, Optional, Tuple, Union

import tree  # pip install dm_tree

from ray.rllib.utils import force_tuple
from ray.rllib.utils.metrics.stats import Stats
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.nested_dict import NestedDict
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
    - Resetting the logged values after a `reduce()` call in order to make space for
    new values to be logged.

    .. testcode::

        from ray.rllib.utils.metrics.metrics_logger import MetricsLogger

        logger = MetricsLogger()

        # Log n simple float values under the "loss" key. By default, all logged values
        # under that key are averaged over once `reduce()` is called.
        logger.log_value("loss", 0.001)
        logger.log_value("loss", 0.002)
        logger.log_value("loss", 0.003)
        # Peek at the current (reduced) value of "loss":

    """

    def __init__(self):
        """Initializes a MetricsLogger instance."""
        self.stats = NestedDict()
        self._tensor_mode = False
        self._tensor_keys = set()

    def log_value(
        self,
        key: Union[str, Tuple[str]],
        value: Any,
        *,
        reduce: Optional[str] = "mean",
        window: Optional[Union[int, float]] = None,
        ema_coeff: Optional[float] = None,
        clear_on_reduce: bool = False,
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
            # these).
            check(len(logger.stats["loss"].values), 13)

            # Only, when we call `reduce` does the underlying structure get "cleaned
            # up". In this case, the list is shortened to 10 items (window size).
            results = logger.reduce(return_stats_obj=False)
            check(results, {"loss": 0.05})
            check(len(logger.stats["loss"].values), 10)

            # Log a value under a deeper nested key.
            logger.log_value(("some", "nested", "key"), -1.0)
            check(logger.peek("some", "nested", "key"), -1.0)

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
            # Reducing everything.
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
            # ... but not the "some_items" list (b/c `reduce_on_reset=False`).
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
        """
        # No reduction (continue appending to list) AND no window.
        # -> We'll force-reset our values upon `reduce()`.
        if reduce is None and (window is None or window == float("inf")):
            clear_on_reduce = True

        self._check_tensor(key, value)

        if key not in self.stats:
            self.stats[key] = Stats(
                value,
                reduce=reduce,
                window=window,
                ema_coeff=ema_coeff,
                clear_on_reduce=clear_on_reduce,
            )
        # If value itself is a stat, we merge it on time axis into `self`.
        elif isinstance(value, Stats):
            self.stats[key].merge_on_time_axis(value)
        # Otherwise, we just push the value into `self`.
        else:
            self.stats[key].push(value)

    def log_dict(
        self,
        stats_dict,
        *,
        key: Optional[Union[str, Tuple[str]]] = None,
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
            })  # don't have to repeat `window` if key already exists
            logger.log_dict({
                "a": 0.2,
                "c": {"d": 5.0},  # can also introduce an entirely new (nested) key
            })

            # Peek at the current (reduced) values under "a" and "b".
            check(logger.peek("a"), 0.15)
            check(logger.peek("b"), -0.15)
            check(logger.peek("c", "d"), 5.0)

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
        stats_dict = NestedDict(stats_dict)
        prefix_key = force_tuple(key)

        for key, stat_or_value in stats_dict.items():
            extended_key = prefix_key + key
            # No reduction (continue appending to list) AND no window.
            # -> We'll force-reset our values upon `reduce()`.
            if reduce is None and (window is None or window == float("inf")):
                clear_on_reduce = True

            if not isinstance(stat_or_value, Stats):
                self._check_tensor(extended_key, stat_or_value)

                # `self` already has this key path -> Use c'tor options from self's
                # Stats.
                if extended_key in self.stats:
                    stat_or_value = Stats.similar_to(
                        self.stats[extended_key], init_value=stat_or_value
                    )
                else:
                    stat_or_value = Stats(
                        stat_or_value,
                        reduce=reduce,
                        window=window,
                        ema_coeff=ema_coeff,
                        clear_on_reduce=clear_on_reduce,
                    )

            # Merge incoming Stats into existing one (as a next timestep on top of
            # existing data).
            if extended_key in self.stats:
                self.stats[extended_key].merge_on_time_axis(stat_or_value)
            # Use incoming Stats object's values, but create a new Stats object (around
            # these values) to not mess with the original Stats object.
            else:
                self.stats[extended_key] = Stats.similar_to(
                    stat_or_value, init_value=stat_or_value.values
                )

    def log_n_dicts(
        self,
        stats_dicts,
        *,
        key: Optional[Union[str, Tuple[str]]] = None,
        reduce: Optional[str] = "mean",
        window: Optional[Union[int, float]] = None,
        ema_coeff: Optional[float] = None,
        clear_on_reduce: bool = False,
    ) -> None:
        """TODO (sven): docstr

        Args:
            stats_dicts:
            key:
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
        stats_dicts = [NestedDict(s) for s in stats_dicts]
        prefix_key = force_tuple(key)

        all_keys = set()
        for s in stats_dicts:
            all_keys |= set(s.keys())

        for key in all_keys:
            extended_key = prefix_key + key

            # No reduction (continue appending to list) AND no window.
            # -> We'll force-reset our values upon `reduce()`.
            if reduce is None and (window is None or window == float("inf")):
                clear_on_reduce = True

            available_stats = [s[key] for s in stats_dicts if key in s]
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
                # `key` not in self yet -> Create an empty Stats entry under that key.
                if extended_key not in self.stats:
                    self.stats[extended_key] = Stats.similar_to(stat_or_value)

                # Create a new Stats object to merge everything into as parallel,
                # equally weighted Stats.
                if base_stats is None:
                    base_stats = Stats.similar_to(
                        stat_or_value,
                        init_value=stat_or_value.values,
                    )
                else:
                    more_stats.append(stat_or_value)

            # There are more than one incoming parallel others -> Merge all of them
            # first in parallel.
            if len(more_stats) > 0:
                base_stats.merge_in_parallel(*more_stats)

            # Finally, merge `base_stats` into self's entry on time axis, meaning
            # give the incoming values priority over already existing ones.
            self.stats[extended_key].merge_on_time_axis(base_stats)

    def log_time(
        self,
        key: Union[str, Tuple[str]],
        *,
        reduce: Optional[str] = "mean",
        window: Optional[Union[int, float]] = None,
        ema_coeff: Optional[float] = None,
        clear_on_reduce: bool = False,
        # throughput_key: Optional[Union[str, Tuple[str]]] = None,
        # throughput_key_of_unit_count: Optional[Union[str, Tuple[str]]] = None,
    ) -> None:
        """Measures and logs a time delta value under `key` when used with a with-block.

        Additionally measures and logs the throughput for the timed code, iff
        `log_throughput=True` and `throughput_key_for_unit_count` is provided.

        .. testcode::

            from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
            # TODO (sven): finish test case

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
        if self.tensor_mode:
            raise RuntimeError(
                "`MetricsLogger.log_time()` cannot be called in tensor-mode! Make sure "
                "to deactivate tensor-mode first (`MetricsLogger."
                "deactivate_tensor_mode()`), before calling this method."
            )

        # No reduction (continue appending to list) AND no window.
        # -> We'll force-reset our values upon `reduce()`.
        if reduce is None and (window is None or window == float("inf")):
            clear_on_reduce = True

        if key not in self.stats:
            # TODO (sven): Figure out how to best implement an additional throughput
            #  measurement.
            # measure_throughput = None
            # if throughput_key_of_unit_count is not None:
            #    measure_throughput = True
            #    throughput_key = throughput_key or (key + "_throughput_per_s")

            self.stats[key] = Stats(
                reduce=reduce,
                window=window,
                ema_coeff=ema_coeff,
                clear_on_reduce=clear_on_reduce,
                # on_exit=(
                #    lambda stats: (
                #        self.log_value(
                #            throughput_key,
                #            self.peek(throughput_key_of_unit_count),
                #            reduce=reduce,
                #            window=window,
                #            ema_coeff=ema_coeff,
                #            clear_on_reduce=clear_on_reduce,
                #        )
                #    ),
                # ),
            )

        # Return the Stats object, so a `with` clause can enter and exit it.
        return self.stats[key]

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
        assert not self.tensor_mode
        self._tensor_mode = True

    def deactivate_tensor_mode(self):
        """Switches off tensor-mode."""
        assert self.tensor_mode
        self._tensor_mode = False
        # Return all logged tensors (logged during the tensor-mode phase).
        ret = {key: self.stats[key].peek() for key in self._tensor_keys}
        # Clear out logged tensor keys.
        self._tensor_keys.clear()
        return ret

    def tensors_to_numpy(self, tensor_metrics):
        """Converts all previously logged and returned tensors back to numpy values."""
        for key, value in tensor_metrics.items():
            assert key in self.stats
            self.stats[key].numpy(value)

    @property
    def tensor_mode(self):
        return self._tensor_mode

    def peek(self, *key, default: Optional[Any] = None) -> Any:
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
                logger.peek("some", "nested"),  # <- *args work as well
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

        Returns:
            The (reduced) values of the (possibly nested) sub-structure found under
            the given `key` or key sequence.

        Raises:
            KeyError: If `key` cannot be found AND `default` is not provided.
        """
        # Use default value, b/c `key` cannot be found in our stats.
        if key not in self.stats and default is not None:
            return default

        # Create a reduced view of the requested sub-structure or leaf (Stats object).
        ret = tree.map_structure(lambda s: s.peek(), self.stats[key])

        # `key` only reached to a deeper NestedDict -> Have to convert to dict.
        if isinstance(ret, NestedDict):
            return ret.asdict()

        # Otherwise, return the reduced Stats' (peek) value.
        return ret

    def set_value(
        self,
        key: Union[str, Tuple[str]],
        value: Any,
        *,
        reduce: Optional[str] = "mean",
        window: Optional[Union[int, float]] = None,
        ema_coeff: Optional[float] = None,
        clear_on_reduce: bool = False,
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
        """
        # Key already in self -> Erase internal values list with [`value`].
        if key in self.stats:
            self.stats[key].values = [value]
        # Key cannot be found in `self` -> Simply log as a (new) value.
        else:
            self.log_value(
                key,
                value,
                reduce=reduce,
                window=window,
                ema_coeff=ema_coeff,
                clear_on_reduce=clear_on_reduce,
            )

    def reduce(
        self,
        key: Optional[Union[str, Tuple[str]]] = None,
        *,
        return_stats_obj: bool = True,
    ) -> Dict:
        """Reduces all logged values based on their settings and returns a result dict.

        The returned result dict has the exact same structure as the logged keys (or
        nested key sequences) combined. At the leafs of the returned structure are
        either `Stats` objects (return_stats_obj=True, which is the default) or
        primitive (non-Stats) values. In case of `return_stats_obj=True`, the returned
        dict with Stats at the leafs can conveniently be re-used downstream for further
        logging and reduction operations.

        For example, imagine component A (e.g. an Algorithm) containing a MetricsLogger
        and n remote components (e.g. EnvRunner  workers), each with their own
        MetricsLogger object. Component A now calls its n remote components, each of
        which returns an equivalent, reduced dict with `Stats` as leafs.
        Component A can then further log these n result dicts via its own MetricsLogger:
        `logger.log_n_dicts([n returned result dicts from the remote components])`.

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
            downstream_logger.log_n_dicts([result1, result2])
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
        # Create a shallow copy of `self.stats` in case we need to reset some of our
        # stats due to this `reduce()` call (and the Stat having self.clear_on_reduce
        # set to True).
        if key is not None:
            stats_to_return = self.stats[key].copy()
        else:
            stats_to_return = self.stats.copy()

        # Reduce all stats according to each of their reduce-settings.
        for sub_key, stat in stats_to_return.items():
            # In case we clear the Stats upon `reduce`, we get returned a new empty
            # `Stats` object from `stat.reduce()` with the same settings as existing one
            # and can now re-assign it to `self.stats[key]` (while we return from this
            # method the properly reduced, but not cleared/emptied new `Stats`).
            if key is not None:
                self.stats[key][sub_key] = stat.reduce()
            else:
                self.stats[sub_key] = stat.reduce()

        # Return reduced values as dict (not NestedDict).
        stats_to_return = stats_to_return.asdict()

        if return_stats_obj:
            return stats_to_return
        else:
            return tree.map_structure(lambda s: s.peek(), stats_to_return)

    def get_state(self) -> Dict[str, Any]:
        """Returns the current state of `self` as a dict.

        Note that the state is merely the combination of all states of the individual
        `Stats` objects stored under `self.stats`.
        """
        return {
            "stats": {key: stat.get_state() for key, stat in self.stats.items()},
        }

    def set_state(self, state: Dict[str, Any]) -> None:
        """Sets the state of `self` to the given `state`.

        Args:
            state: The state to set `self` to.
        """
        self.stats = NestedDict(
            {
                key: Stats.from_state(stat_state)
                for key, stat_state in state["stats"].items()
            }
        )

    def _check_tensor(self, key, value) -> None:
        # `value` is a tensor -> Log it in our keys set.
        if self.tensor_mode and (
            (torch and torch.is_tensor(value)) or (tf and tf.is_tensor(value))
        ):
            self._tensor_keys.add(key)
