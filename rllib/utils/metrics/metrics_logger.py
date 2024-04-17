import copy
from typing import Any, Dict

import tree  # pip install dm_tree 

from ray.rllib.utils import force_list, force_tuple
from ray.rllib.utils.metrics.stats import Stats
from ray.rllib.utils.nested_dict import NestedDict
from ray.util.timer import _Timer


class MetricsLogger:

    def __init__(self):
        self.stats = NestedDict()
        # Which keys' Stats objects do we need to reset after calling `self.reduce()`?
        self._keys_to_reset_on_reduce = set()

    def log_value(self, key, value, reduce="mean", window=None, ema_coeff=None, reset_on_reduce=False):
        # No reduction (continue appending to list) or reset explicitly requested
        # -> We have to reset out values at any `reduce()`.
        if reduce is None or reset_on_reduce:
            self._keys_to_reset_on_reduce.add(key)

        if key not in self.stats:
            self.stats[key] = Stats(value, reduce=reduce, window=window, ema_coeff=ema_coeff)
        else:
            self.stats[key].push(value)

    def log_dict(self, stats_dict, key=None, reduce="mean", window=None, ema_coeff=None, reset_on_reduce=False):
        stats_dict = NestedDict(stats_dict)
        prefix_key = force_tuple(key)

        for key, stat_or_value in stats_dict.items():
            extended_key = prefix_key + key
            # No reduction (continue appending to list) or reset explicitly requested
            # -> We have to reset out values at any `reduce()`.
            if reduce is None or reset_on_reduce:
                self._keys_to_reset_on_reduce.add(extended_key)

            if not isinstance(stat_or_value, Stats):
                stat_or_value = Stats(stat_or_value, reduce=reduce, window=window, ema_coeff=ema_coeff)

            if extended_key in self.stats:
                # Merge existing Stats with incoming one.
                self.stats[extended_key].merge(stat_or_value)
            else:
                # Make a copy to not mess with the incoming stats objects.
                self.stats[extended_key] = copy.deepcopy(stat_or_value)

    def log_n_dicts(self, stats_dicts, key=None, reduce="mean", window=None, ema_coeff=None, reset_on_reduce=False):
        stats_dicts = [NestedDict(s) for s in stats_dicts]
        prefix_key = force_tuple(key)

        all_keys = set()
        for s in stats_dicts:
            all_keys |= set(s.keys())

        for key in all_keys:
            extended_key = prefix_key + key

            # No reduction (continue appending to list) or reset explicitly requested
            # -> We have to reset out values at any `reduce()`.
            if reduce is None or reset_on_reduce:
                extended_key = prefix_key + key
                self._keys_to_reset_on_reduce.add()

            available_stats = [s[key] for s in stats_dicts if key in s]
            for i, stat_or_value in enumerate(available_stats):
                if not isinstance(stat_or_value, Stats):
                    available_stats[i] = stat_or_value = Stats(
                        stat_or_value,
                        reduce=reduce,
                        window=window,
                        ema_coeff=ema_coeff,
                    )
                if extended_key not in self.stats:
                    self.stats[extended_key] = Stats(
                        reduce=stat_or_value._reduce_method,
                        window=stat_or_value._window,
                        ema_coeff=stat_or_value._ema_coeff,
                    )
            self.stats[extended_key].merge(*available_stats)

    def log_time(self, key, reduce="mean", window=None, ema_coeff=None, reset_on_reduce=False):
        # No reduction (continue appending to list) or reset explicitly requested
        # -> We have to reset out values at any `reduce()`.
        if reduce is None or reset_on_reduce:
            self._keys_to_reset_on_reduce.add(key)

        if key not in self.stats:
            self.stats[key] = Stats(reduce=reduce, window=window, ema_coeff=ema_coeff)
        # Return the Stats object, so a `with` clause can enter and exit it.
        return self.stats[key]

    #def log_video(self, key, video):
    #    """Convenience method for logging videos."""
    #    self.log_value(key, video, reduce=None)

    def get(self, *key):
        ret = tree.map_structure(lambda s: s.peek(), self.stats[*key])
        if isinstance(ret, NestedDict):
            return ret.asdict()
        return ret

    def reduce(self):
        # Create a shallow copy of `self.stats` in case we need to reset some of our
        # stats due to this `reduce()` call (and the key being in
        # `self._keys_to_reset_on_reduce`).
        stats_to_return = self.stats.copy()

        # Reduce all stats according to each of their reduce-settings.
        for key, stat in stats_to_return.items():
            stat.reduce()
            # Do we have to reset this key? If yes, create a new (empty) Stats object
            # under this key.
            if key in self._keys_to_reset_on_reduce:
                self.stats[key] = Stats(
                    reduce=stat._reduce_method,
                    window=stat._window,
                    ema_coeff=stat._ema_coeff,
                )

        # Return reduced values as dict (not NestedDict).
        return stats_to_return.asdict()

    def get_state(self) -> Dict[str, Any]:
        return {
            "stats": {key: stat.get_state() for key, stat in self.stats.items()},
            "_keys_to_reset_on_reduce": self._keys_to_reset_on_reduce,
        }

    def set_state(self, state: Dict[str, Any]) -> None:
        self.stats = {
            key: Stats.from_state(stat_state)
            for key, stat_state in state["stats"].items()
        }
        self._keys_to_reset_on_reduce = state["_keys_to_reset_on_reduce"]
