from collections import Counter
import os
import sys
import time
import ray

""" This script is meant to be run from a pod in the same Kubernetes namespace
as your Ray cluster.

Just below are the environment variables used to access Ray client via a
service targetting the Ray cluster's head node pod.
These environment variables are set by Kubernetes.
See https://kubernetes.io/docs/concepts/services-networking/service/#environment-variables
In the documentation examples, the head service has
"example-cluster-ray-head" and the relevant port is named "client".
Modify the environment variables as needed to match the name of the service
and port.

Note: The default head service set up by the Ray Kubernetes operator is named
<cluster-name>-ray-head,
where <cluster-name> is the metadata.name field you set in the RayCluster
custom resource.
"""  # noqa
HEAD_SERVICE_IP_ENV = "EXAMPLE_CLUSTER_RAY_HEAD_SERVICE_HOST"
HEAD_SERVICE_CLIENT_PORT_ENV = "EXAMPLE_CLUSTER_RAY_HEAD_SERVICE_PORT_CLIENT"


@ray.remote
def gethostname(x):
    import platform
    import time
    time.sleep(0.01)
    return x + (platform.node(), )


def wait_for_nodes(expected):
    # Wait for all nodes to join the cluster.
    while True:
        resources = ray.cluster_resources()
        node_keys = [key for key in resources if "node" in key]
        num_nodes = sum(resources[node_key] for node_key in node_keys)
        if num_nodes < expected:
            print("{} nodes have joined so far, waiting for {} more.".format(
                num_nodes, expected - num_nodes))
            sys.stdout.flush()
            time.sleep(1)
        else:
            break


def main():
    wait_for_nodes(3)

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
    head_service_ip = os.environ[HEAD_SERVICE_IP_ENV]
    client_port = os.environ[HEAD_SERVICE_CLIENT_PORT_ENV]
    ray.util.connect(f"{head_service_ip}:{client_port}")
    main()
