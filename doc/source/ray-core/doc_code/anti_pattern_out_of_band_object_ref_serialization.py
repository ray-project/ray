import numpy
import pickle

import ray
from ray._private.internal_api import memory_summary


class Wrapper:
    def __init__(self, obj_ref):
        self.obj_ref = obj_ref


@ray.remote(num_cpus=0.1)
def f():
    obj_ref = ray.put(1)
    print(f"object reference to be leaked! {obj_ref}")
    arr = numpy.array([Wrapper(obj_ref)], dtype=numpy.object_)
    batch = {"data": [arr]}
    # object_ref is serialized from user code.
    return pickle.dumps(batch)


ray.get([f.remote() for _ in range(1)])
# You can see the object is leaked.
memory_summary()
