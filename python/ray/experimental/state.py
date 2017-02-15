from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray.worker

def get_local_schedulers():
  local_schedulers = []
  for client in ray.worker.global_worker.redis_client.keys("CL:*"):
    client_type, ray_client_id = ray.worker.global_worker.redis_client.hmget(client, "client_type", "ray_client_id")
    if client_type == b"photon":
      local_schedulers.append(ray_client_id)
  return local_schedulers
