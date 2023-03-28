# flake8: noqa

# __rllib-adv_api_counter_begin__
import ray


@ray.remote
class Counter:
    def __init__(self):
        self.count = 0

    def inc(self, n):
        self.count += n

    def get(self):
        return self.count


# on the driver
counter = Counter.options(name="global_counter").remote()
print(ray.get(counter.get.remote()))  # get the latest count

# in your envs
counter = ray.get_actor("global_counter")
counter.inc.remote(1)  # async call to increment the global count
# __rllib-adv_api_counter_end__

# __rllib-adv_api_explore_begin__
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

config = AlgorithmConfig().exploration(
    exploration_config={
        # Special `type` key provides class information
        "type": "StochasticSampling",
        # Add any needed constructor args here.
        "constructor_arg": "value",
    }
)
# __rllib-adv_api_explore_end__
