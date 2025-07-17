import ray
import time
from ray.data import from_items


# Initialize Ray
ray.init()


def sleep(batch):
   time.sleep(10)
   return batch


# Create two datasets
dataset01 = from_items([{"id": 1, "value": "a"}, {"id": 2, "value": "b"}])
dataset02 = from_items([{"id": 3, "value": "c"}, {"id": 4, "value": "d"}])


for i in range(1):
   union01 = dataset01.union(dataset02, dataset02)
   union02 = union01.union(dataset01)
   union02.materialize()
