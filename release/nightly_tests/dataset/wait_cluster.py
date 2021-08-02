import ray
import time

ray.init()

while ray.cluster_resources().get("GPU", 0) != 2:
    print("Waiting for GPUs {}/2".format(ray.cluster_resources().get(
        "GPU", 400)))
    time.sleep(5)
