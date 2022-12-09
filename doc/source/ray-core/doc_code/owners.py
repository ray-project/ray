# flake8: noqa
# __owners_begin__
import ray
import numpy as np


@ray.remote
def large_array():
    return np.zeros(int(1e5))


x = ray.put(1)  # The driver owns x and also creates the value of x.

y = large_array.remote()
# The driver is the owner of y, even though the value may be stored somewhere else.
# If the node that stores the value of y dies, Ray will automatically recover
# it by re-executing the large_array task.
# If the driver dies, anyone still using y will receive an OwnerDiedError.
# __owners_end__
