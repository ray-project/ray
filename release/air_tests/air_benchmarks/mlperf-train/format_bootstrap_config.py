import json


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", type=str, required=True)
    parser.add_argument("--num-cpu-nodes", type=int, required=True)
    args = parser.parse_args()
    assert args.num_cpu_nodes >= 0

    with open(args.config_path, "r") as f:
        yaml = json.load(f)

    node_types = yaml["available_node_types"]
    assert (
        "worker-node-type-0" in node_types
    ), "Must specify exactly one worker node type using the cluster UI"

    node_types["worker-node-type-0"]["min_workers"] = args.num_cpu_nodes
    node_types["worker-node-type-0"]["max_workers"] = args.num_cpu_nodes
    # Cluster-level max workers includes 1 node for the head node.
    yaml["max_workers"] = args.num_cpu_nodes + 1

    with open(args.config_path, "w") as f:
        json.dump(yaml, f)
