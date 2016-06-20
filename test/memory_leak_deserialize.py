# This code reproduces a memory leak we had in the past

import os
import numpy as np
import ray
import ray.worker
import ray.services as services

worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_worker.py")
services.start_singlenode_cluster(return_drivers=False, num_workers_per_objstore=1, worker_path=worker_path)

d = {"w": np.zeros(1000000)}

obj_capsule, contained_objrefs = ray.lib.serialize_object(ray.worker.global_worker.handle, d)

while True:
  ray.lib.deserialize_object(ray.worker.global_worker.handle, obj_capsule)
