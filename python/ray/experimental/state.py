from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

def get_local_schedulers(worker):
  local_schedulers = []
  for client in worker.redis_client.keys("CL:*"):
    client_info = worker.redis_client.hgetall(client)
    if client_info[b"client_type"] == b"local_scheduler":
      local_schedulers.append(client_info)
  return local_schedulers
