import ray
import time

ray.init(address="auto")

curr_nodes = 0
while not curr_nodes > 8:
    print("Waiting for more nodes to come up: {}/{}".format(curr_nodes, 8))
    curr_nodes = len(ray.nodes())
    time.sleep(5)
