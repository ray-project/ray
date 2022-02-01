from collections import Counter
import sys
import time
import ray


@ray.remote
def get_host_name(x):
    import platform
    import time

    time.sleep(0.01)
    return x + (platform.node(),)


def wait_for_nodes(expected):
    # Wait for all nodes to join the cluster.
    while True:
        num_nodes = len(ray.nodes())
        if num_nodes < expected:
            print(
                "{} nodes have joined so far, waiting for {} more.".format(
                    num_nodes, expected - num_nodes
                )
            )
            sys.stdout.flush()
            time.sleep(1)
        else:
            break


def main():
    wait_for_nodes(4)

    # Check that objects can be transferred from each node to each other node.
    for i in range(10):
        print("Iteration {}".format(i))
        results = [get_host_name.remote(get_host_name.remote(())) for _ in range(100)]
        print(Counter(ray.get(results)))
        sys.stdout.flush()

    print("Success!")
    sys.stdout.flush()
    time.sleep(20)


if __name__ == "__main__":
    ray.init(address="localhost:6379")
    main()
