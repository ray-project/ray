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
    worker_label = None
    for label in node_types:
        if "worker" in label:
            assert (
                worker_label is None
            ), "More than one worker node type specified in config"
            worker_label = label

    node_types[worker_label]["min_workers"] = args.num_cpu_nodes
    node_types[worker_label]["max_workers"] = args.num_cpu_nodes
    yaml["max_workers"] = args.num_cpu_nodes

    with open(args.config_path, "w") as f:
        json.dump(yaml, f)
