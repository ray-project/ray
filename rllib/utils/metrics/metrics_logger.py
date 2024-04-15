from ray.rllib.utils import force_list
from ray.rllib.utils.metrics.stats import Stats
from ray.rllib.utils.nested_dict import NestedDict


class MetricsLogger:

    def __init__(self):
        self.stats = NestedDict()

    def log_scalar(self, keys, value, reduce="mean", window=None, ema_coeff=None):
        for red in force_list(reduce):
            key = force_list(keys)
            key[-1] = reduce + "_" + key[-1]
            if key not in self.stats:
                self.stats[key] = Stats(value, reduce=red, window=window, ema_coeff=ema_coeff)
            else:
                self.stats[key].push(value)

    def log_video(self, key, video):
        if key not in self.stats:
            self.stats[key] = Stats(video, reduce=None)
        else:
            self.stats[key].push(video)

    def reduce(self):
        # Reduce all stats according to each of their reduce-settings.
        for stat in self.stats.values():
            stat.reduce()
        # Return scalar values as dict (not NestedDict).
        return self.stats.asdict()

    def to_dict(self):
        result_stats_dict = self.reduce()
        result_stats_dict


if __name__ == "__main__":
    import numpy as np

    logger = MetricsLogger()
    logger.log_scalar("a", 1.0)
    logger.log_scalar("a", 2.0)
    logger.log_scalar("a", 3.0)
    logger.log_scalar(["b", "c"], -1.0)
    logger.log_scalar(["b", "d"], -2.0)
    print(logger.scalar_values)

    # Test logging videos.


    reduced = logger.reduce()
    print(f"reduced a={reduced['mean_a']}")
