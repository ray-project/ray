import ray
from ray.autoscaler._private.providers import _get_node_provider
from ray.autoscaler._private.kuberay.autoscaling_config import _generate_provider_config


@ray.remote
def count_non_terminated_nodes() -> int:
    """Get the count of non terminated nodes for the Ray cluster raycluster-complete
    in namespace default.
    """
    provider_config = _generate_provider_config(ray_cluster_namespace="default")
    kuberay_node_provider = _get_node_provider(
        provider_config=provider_config, cluster_name="raycluster-complete"
    )
    nodes = kuberay_node_provider.non_terminated_nodes({})
    return len(nodes)


def main() -> int:
    return ray.get(count_non_terminated_nodes.remote())


if __name__ == "__main__":
    ray.init("auto")
    out = main()
    print(out)
