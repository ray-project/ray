import ray
from ray import rllib


ray.init(address="auto", runtime_env={"ray_libraries": [rllib]})


# rllib.__some_attribute__ = new_thing
