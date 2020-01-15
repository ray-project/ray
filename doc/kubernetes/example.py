from collections import Counter
import os
import sys
import time
import ray


@ray.remote
def gethostname(x):
    import time
    import socket
    time.sleep(0.01)
    return x + (socket.gethostname(), )


def wait_for_nodes(expected):
    # Wait for all nodes to join the cluster.
    while True:
        num_nodes = len(ray.nodes())
        if num_nodes < expected:
            print("{} nodes have joined so far, waiting for {} more.".format(
                num_nodes, expected - num_nodes))
            sys.stdout.flush()
            time.sleep(1)
        else:
            break


def main():
    wait_for_nodes(4)

    # Check that objects can be transferred from each node to each other node.
    for i in range(10):
        print("Iteration {}".format(i))
        results = [
            gethostname.remote(gethostname.remote(())) for _ in range(100)
        ]
        print(Counter(ray.get(results)))
        sys.stdout.flush()

    print("Success!")
    sys.stdout.flush()


if __name__ == "__main__":
    # NOTE: If you know you're running this on the head node, you can just
    # use "localhost" here.
    # redis_host = "localhost"
    if ("RAY_HEAD_SERVICE_HOST" not in os.environ
            or os.environ["RAY_HEAD_SERVICE_HOST"] == ""):
        raise ValueError("RAY_HEAD_SERVICE_HOST environment variable empty."
                         "Is there a ray cluster running?")
    redis_host = os.environ["RAY_HEAD_SERVICE_HOST"]
    ray.init(address=redis_host + ":6379")
    main()
