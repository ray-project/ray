from ray.rllib.utils import force_list
from ray.rllib.utils.metrics.stats import Stats
from ray.rllib.utils.nested_dict import NestedDict


class MetricsLogger:

    def __init__(self):
        self.scalar_values = NestedDict()

    def log_scalar(self, keys, value, reduce="mean", ttl="until_reduce"):
        for red in force_list(reduce):
            key = force_list(keys)
            key[-1] = reduce + "_" + key[-1]
            if key not in self.scalar_values:
                self.scalar_values[key] = Stats(value, reduce=red)
            else:
                self.scalar_values[key].push(value)

    def reduce(self):
        ret = NestedDict()
        for key, stat in self.scalar_values.items():
            reduced = stat.reduce()
            ret[key] = reduced
        return ret.asdict()

    def to_dict(self):



if __name__ == "__main__":
    logger = MetricsLogger()
    logger.log_scalar("a", 1.0)
    logger.log_scalar("a", 2.0)
    logger.log_scalar("a", 3.0)
    logger.log_scalar(["b", "c"], -1.0)
    logger.log_scalar(["b", "d"], -2.0)
    print(logger.scalar_values)
    reduced = logger.reduce()
    print(f"reduced a={reduced['mean_a']}")
