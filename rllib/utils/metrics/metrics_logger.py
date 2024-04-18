import copy
from typing import Any, Dict, Optional, Tuple, Union

import tree  # pip install dm_tree

from ray.rllib.utils import force_tuple
from ray.rllib.utils.metrics.stats import Stats
from ray.rllib.utils.nested_dict import NestedDict


class MetricsLogger:
    """TODO (sven)"""

    def __init__(self):
        """Initializes a MetricsLogger instance."""
        self.stats = NestedDict()

    def log_value(
        self,
        key: Union[str, Tuple[str]],
        value: Any,
        reduce: Optional[str] = "mean",
        window: Optional[int] = None,
        ema_coeff: Optional[float] = None,
        reset_on_reduce: bool = False,
    ) -> None:
        """Logs a new value under a (possibly nested) key to the logger.

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
            reset_on_reduce: If True, all values under `key` will be emptied after
                `self.reduce()` is called. Setting this to True is useful for cases,
                in which the internal values list would otherwise grow indefinitely,
                for example if reduce is None and there is no `window` provided.
        """
        # No reduction (continue appending to list) AND no window.
        # -> We'll force-reset our values upon `reduce()`.
        if reduce is None and window is None:
            reset_on_reduce = True

        if key not in self.stats:
            self.stats[key] = Stats(
                value,
                reduce=reduce,
                window=window,
                ema_coeff=ema_coeff,
                reset_on_reduce=reset_on_reduce,
            )
        else:
            self.stats[key].push(value)

    def log_dict(
        self,
        stats_dict,
        key: Optional[Union[str, Tuple[str]]] = None,
        reduce: Optional[str] = "mean",
        window: Optional[int] = None,
        ema_coeff: Optional[float] = None,
        reset_on_reduce: bool = False,
    ) -> None:
        """Logs the `Stats` (or simple value) leafs of a (nested) dict to this logger.

        Traverses through all leafs of `stats_dict` and - if a path cannot be found in
        this logger's stats yet, will add the found `Stats` under that new key in `self.
        If a path already exists in `self`, will merge the found `Stats` with the ones
        in `self`. This way, `stats_dict` does NOT have to have an identical structure
        that what's already in `self`, but can also be used to log entirely new
        keys/paths to `self`.

        .. testcode::
            # TODO (sven)

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
            reset_on_reduce: If True, all values under `key` will be emptied after
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
            if reduce is None and window is None:
                reset_on_reduce = True

            if not isinstance(stat_or_value, Stats):
                stat_or_value = Stats(
                    stat_or_value,
                    reduce=reduce,
                    window=window,
                    ema_coeff=ema_coeff,
                    reset_on_reduce=reset_on_reduce,
                )

            if extended_key in self.stats:
                # Merge existing Stats with incoming one.
                self.stats[extended_key].merge(stat_or_value)
            else:
                # Make a copy to not mess with the incoming stats objects.
                self.stats[extended_key] = copy.deepcopy(stat_or_value)

    def log_n_dicts(
        self,
        stats_dicts,
        key: Optional[Union[str, Tuple[str]]] = None,
        reduce: Optional[str] = "mean",
        window: Optional[int] = None,
        ema_coeff: Optional[float] = None,
        reset_on_reduce: bool = False,
    ) -> None:
        """

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
            reset_on_reduce: If True, all values under `key` will be emptied after
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
            if reduce is None and window is None:
                reset_on_reduce = True

            available_stats = [s[key] for s in stats_dicts if key in s]
            for i, stat_or_value in enumerate(available_stats):
                if not isinstance(stat_or_value, Stats):
                    available_stats[i] = stat_or_value = Stats(
                        stat_or_value,
                        reduce=reduce,
                        window=window,
                        ema_coeff=ema_coeff,
                        reset_on_reduce=reset_on_reduce,
                    )
                if extended_key not in self.stats:
                    self.stats[extended_key] = Stats.similar_to(stat_or_value)
            self.stats[extended_key].merge(*available_stats)

    def log_time(
        self,
        key: Union[str, Tuple[str]],
        reduce: Optional[str] = "mean",
        window: Optional[int] = None,
        ema_coeff: Optional[float] = None,
        reset_on_reduce: bool = False,
        throughput_prefix: Optional[str] = None

    ) -> None:
        """

        Args:
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
            reset_on_reduce: If True, all values under `key` will be emptied after
                `self.reduce()` is called. Setting this to True is useful for cases,
                in which the internal values list would otherwise grow indefinitely,
                for example if reduce is None and there is no `window` provided.
        """
        # No reduction (continue appending to list) AND no window.
        # -> We'll force-reset our values upon `reduce()`.
        if reduce is None and window is None:
            reset_on_reduce = True

        if key not in self.stats:
            self.stats[key] = Stats(
                reduce=reduce,
                window=window,
                ema_coeff=ema_coeff,
                reset_on_reduce=reset_on_reduce,
            )

        # Return the Stats object, so a `with` clause can enter and exit it.
        return self.stats[key]

    def get(self, *key) -> Any:
        """Returns the (reduced) value(s) found under the given key or key sequence.

        Note that if `key` only reaches to a nested dicts deeper in `self`, that
        sub-dictionary's entire values are returned as a (nested) dict.

        Note that calling `self.get()` does NOT cause an actual underlying value list
        reduction, even though reduced values are being returned. It'll keep all
        internal structures as-is.

        .. testcode::
            # TODO (sven)

        Args:
            key: The key/key sequence of the sub-structure of `self`, whose values
                (already reduced) to return.

        Returns:
            The (reduced) values of the (possibly nested) sub-structure found under
            the given `key` or key sequence.
        """
        ret = tree.map_structure(lambda s: s.peek(), self.stats[*key])
        if isinstance(ret, NestedDict):
            return ret.asdict()
        return ret

    def reduce(self) -> Dict:
        """

        Returns:

        """
        # Create a shallow copy of `self.stats` in case we need to reset some of our
        # stats due to this `reduce()` call (and the Stat having self.reset_on_reduce
        # set to True).
        stats_to_return = self.stats.copy()

        # Reduce all stats according to each of their reduce-settings.
        for key, stat in stats_to_return.items():
            # In case we reset the Stats upon `reduce`, we get returned a new empty
            # Stats object here (same settings as existing one) and can now re-assign
            # it to `self.stats[key]` (while we return from this method the properly
            # reduced, but not emptied/reset new Stats).
            self.stats[key] = stat.reduce()

        # Return reduced values as dict (not NestedDict).
        # TODO (sven): Maybe we want to change that to NestedDict, but we would like to
        #  asses to what extend we need to expose NestedDict to the user.
        return stats_to_return.asdict()

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
        self.stats = NestedDict({
            key: Stats.from_state(stat_state)
            for key, stat_state in state["stats"].items()
        })
