import numpy as np
import ray
from ray.util import ActorPool
import math
import time
 
with ray.init(object_store_memory=786432009.5, _memory=1.5):
  # large = [2] * math.ceil(1000000000)
  large = np.zeros((5000, 5000))
  lref = ray.put(large)

  @ray.remote(memory=1)
  class Actor:
    def __init__(self, val):
      self.val = val
    
  # with ray.init():
  actor_pool = ActorPool([Actor.remote(lref) for i in range(2)])
  
  time.sleep(100)
  