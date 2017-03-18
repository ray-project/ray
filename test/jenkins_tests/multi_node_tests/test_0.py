import os
import time

import ray

@ray.remote
def f():
  time.sleep(0.1)
  return ray.services.get_node_ip_address()

if __name__ == "__main__":
  ray.init(redis_address=os.environ["RAY_REDIS_ADDRESS"])
  ip_addresses = ray.get([f.remote() for i in range(1000)])
  distinct_addresses = set(ip_addresses)
  counts = [ip_addresses.count(address) for address in distinct_addresses]
  print("Counts are {}".format(counts))
  assert len(counts) == 5
