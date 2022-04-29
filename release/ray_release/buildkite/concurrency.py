import csv
import os
from collections import namedtuple
from typing import Tuple, Optional, Dict

from ray_release.config import Test, RELEASE_PACKAGE_DIR, load_test_cluster_compute
from ray_release.logger import logger

# Keep 10% for the buffer.
limit = int(15784 * 0.9)


CONCURRENY_GROUPS = {
    "tiny": 32,  # <= 1k vCPU
    "small": 16,  # <= 2k vCPU
    "medium": 6,  # <= 3k vCPU
    "large": 8,  # <= 8k vCPU
    "enormous": 1,  # <= 4k vCPU (?)
    "small-gpu": 8,
    "large-gpu": 4,
}


Condition = namedtuple(
    "Condition", ["min_gpu", "max_gpu", "min_cpu", "max_cpu", "group"]
)

gpu_cpu_to_concurrency_groups = [
    Condition(min_gpu=9, max_gpu=-1, min_cpu=0, max_cpu=-1, group="large-gpu"),
    Condition(min_gpu=1, max_gpu=9, min_cpu=0, max_cpu=-128, group="small-gpu"),
    Condition(min_gpu=0, max_gpu=0, min_cpu=1025, max_cpu=-1, group="enormous"),
    Condition(min_gpu=0, max_gpu=0, min_cpu=513, max_cpu=1024, group="large"),
    Condition(min_gpu=0, max_gpu=0, min_cpu=129, max_cpu=512, group="medium"),
    Condition(min_gpu=0, max_gpu=0, min_cpu=0, max_cpu=32, group="tiny"),
    # Make sure "small" is the last in the list, because it is the fallback.
    Condition(min_gpu=0, max_gpu=0, min_cpu=0, max_cpu=128, group="small"),
]


# Obtained from https://cloud.google.com/compute/docs/accelerator-optimized-machines
gcp_gpu_instances = {
    "a2-highgpu-1g": (12, 1),
    "a2-highgpu-2g": (24, 2),
    "a2-highgpu-4g": (48, 4),
    "a2-highgpu-8g": (96, 8),
    "a2-megagpu-16g": (96, 16),
}


def load_instance_types(path: Optional[str] = None) -> Dict[str, Tuple[int, int]]:
    path = path or os.path.join(
        RELEASE_PACKAGE_DIR, "ray_release", "buildkite", "aws_instance_types.csv"
    )

    instance_to_resources = {}
    with open(path, "rt") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            instance_to_resources[row["instance"]] = (
                int(row["cpus"]),
                int(row["gpus"]),
            )

    return instance_to_resources


def parse_instance_resources(instance: str) -> Tuple[int, int]:
    """Parse (GCP) instance strings to resources"""
    # Assumes that GPU instances have already been parsed
    num_cpus = int(instance.split("-")[-1])
    num_gpus = 0
    return num_cpus, num_gpus


def parse_condition(cond: int, limit: float = float("inf")) -> float:
    return cond if cond > -1 else limit


def get_concurrency_group(test: Test) -> Tuple[str, int]:
    try:
        test_cpus, test_gpus = get_test_resources(test)
    except Exception as e:
        logger.warning(f"Couldn't get test resources for test {test['name']}: {e}")
        return "small", CONCURRENY_GROUPS["small"]

    for condition in gpu_cpu_to_concurrency_groups:
        min_gpu = parse_condition(condition.min_gpu, float("-inf"))
        max_gpu = parse_condition(condition.max_gpu, float("inf"))
        min_cpu = parse_condition(condition.min_cpu, float("-inf"))
        max_cpu = parse_condition(condition.max_cpu, float("inf"))

        if min_cpu <= test_cpus <= max_cpu and min_gpu <= test_gpus <= max_gpu:
            group = condition.group
            return group, CONCURRENY_GROUPS[group]

    # Return default
    logger.warning(
        f"Could not find concurrency group for test {test['name']} "
        f"based on used resources."
    )
    return "small", CONCURRENY_GROUPS["small"]


def get_test_resources(test: Test) -> Tuple[int, int]:
    cluster_compute = load_test_cluster_compute(test)
    return get_test_resources_from_cluster_compute(cluster_compute)


def get_test_resources_from_cluster_compute(cluster_compute: Dict) -> Tuple[int, int]:
    instances = []

    # Add head node instance
    instances.append((cluster_compute["head_node_type"]["instance_type"], 1))

    # Add worker node instances
    instances.extend(
        (w["instance_type"], w.get("max_workers", w.get("min_workers", 1)))
        for w in cluster_compute["worker_node_types"]
    )

    aws_instance_types = load_instance_types()
    total_cpus = 0
    total_gpus = 0

    for instance, count in instances:
        if instance in aws_instance_types:
            instance_cpus, instance_gpus = aws_instance_types[instance]
        elif instance in gcp_gpu_instances:
            instance_cpus, instance_gpus = gcp_gpu_instances[instance]
        else:
            instance_cpus, instance_gpus = parse_instance_resources(instance)

        total_cpus += instance_cpus * count
        total_gpus += instance_gpus * count

    return total_cpus, total_gpus
