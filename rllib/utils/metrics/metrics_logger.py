from ray.rllib.utils.metrics.stats import Stats
from ray.rllib.utils.nested_dict import NestedDict


class MetricsLogger:

    def __init__(self):
        self.scalar_values = NestedDict()

    def log_scalar(self, keys, value, reduction="mean", ttl="until_reduce"):
        assert ttl in ["until_reduce", ""] or isinstance(ttl, int)
        if keys not in self.scalar_values:
            self.scalar_values[keys] = Stats(value, reduction=reduction)
        else:
            self.scalar_values[keys].push(value)

    def reduce(self):
        ret = NestedDict()
        for key, stat in self.scalar_values.items():
            reduced = stat.reduce()
            for k, v in reduced.items():
                ret[key[:-1] + (key[-1] + "_" + k,)] = v
        return ret.asdict()



if __name__ == "__main__":
    logger = MetricsLogger()
    logger.log_scalar("a", 1.0)
    logger.log_scalar("a", 2.0)
    logger.log_scalar("a", 3.0)
    logger.log_scalar(["b", "c"], -1.0)
    logger.log_scalar(["b", "d"], -2.0)
    print(logger.scalar_values)
    print(logger.reduce())
