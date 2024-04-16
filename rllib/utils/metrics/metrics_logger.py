import copy

from ray.rllib.utils import force_list
from ray.rllib.utils.metrics.stats import Stats
from ray.rllib.utils.nested_dict import NestedDict
from ray.util.timer import _Timer


class MetricsLogger:

    def __init__(self):
        self.stats = NestedDict()
        # Which keys' Stats objects do we need to reset after calling `self.reduce()`?
        self._keys_to_reset_on_reduce = set()

    def log_value(self, key, value, reduce="mean", window=None, ema_coeff=None, reset_on_reduce=False):
        if reset_on_reduce:
            self._keys_to_reset_on_reduce.add(key)

        if key not in self.stats:
            self.stats[key] = Stats(value, reduce=reduce, window=window, ema_coeff=ema_coeff)
        else:
            self.stats[key].push(value)

    def log_dict(self, stats_dict, reduce="mean", window=None, ema_coeff=None, reset_on_reduce=False):
        stats_dict = NestedDict(stats_dict)
        for key, stat_or_value in stats_dict.items():
            if reset_on_reduce:
                self._keys_to_reset_on_reduce.add(key)

            if not isinstance(stat_or_value, Stats):
                stat_or_value = Stats(stat_or_value, reduce=reduce, window=window, ema_coeff=ema_coeff)

            if key in self.stats:
                # Merge existing Stats with incoming one.
                self.stats[key].merge(stat_or_value)
            else:
                # Make a copy to not mess with the incoming stats objects.
                self.stats[key] = copy.deepcopy(stat_or_value)

    def log_n_dicts(self, stats_dicts, reduce="mean", window=None, ema_coeff=None, reset_on_reduce=False):
        all_keys = set()
        for s in stats_dicts:
            all_keys |= set(s.keys())
        for key in all_keys:
            if reset_on_reduce:
                self._keys_to_reset_on_reduce.add(key)

            available_stats_under_key = [s for s in stats_dicts if key in s]
            # Make a copy to not mess with the incoming stats objects.
            stat_or_value = copy.deepcopy(available_stats_under_key[0])
            if not isinstance(stat_or_value, Stats):
                stat_or_value = Stats(
                    stat_or_value,
                    reduce=reduce,
                    window=window,
                    ema_coeff=ema_coeff,
                )
            # More than one Stat available, merge all of them together.
            if len(available_stats_under_key) > 1:
                stat_or_value.merge(*available_stats_under_key[1:])
            self.stats[key] = stat_or_value

    def log_time(self, key, reduce="mean", window=None, ema_coeff=None, reset_on_reduce=False):
        if reset_on_reduce:
            self._keys_to_reset_on_reduce.add(key)
        if key not in self.stats:
            self.stats[key] = Stats(reduce=reduce, window=window, ema_coeff=ema_coeff)
        # Return the Stats object, so a `with` clause can enter and exit it.
        return self.stats[key]

    def log_video(self, key, video):
        """Convenience me"""
        self.log_value(key, video, reduce=None)

    def get(self, *key):
        return self.stats[*key].peek()

    def reduce(self):
        # Create a shallow copy of `self.stats` in case we need to reset some of our
        # stats due to this `reduce()` call.
        stats_to_return = self.stats.copy()
        # Reduce all stats according to each of their reduce-settings.
        for key, stat in stats_to_return.items():
            stat.reduce()
            if key in self._keys_to_reset_on_reduce:
                #stats_to_return[key] = stat
                self.stats[key] = Stats(
                    reduce=stat._reduce_method,
                    window=stat._window,
                    ema_coeff=stat._ema_coeff,
                )

        # Return scalar values as dict (not NestedDict).
        return stats_to_return.asdict()

    def to_dict(self):
        result_stats_dict = self.reduce()
        result_stats_dict
