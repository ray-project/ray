import time
from ray.cluster_utils import Cluster

cluster = Cluster()

cluster.add_node(num_cpus=16)

time.sleep(20)
print("Scaling up.")
cluster.add_node(num_cpus=16, num_gpus=1)

print("Scaled up. Waiting for 1000 seconds until done.")
time.sleep(1000)
