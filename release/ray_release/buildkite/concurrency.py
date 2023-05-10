import csv
from collections import namedtuple
from typing import Tuple, Optional, Dict

from ray_release.bazel import bazel_runfile
from ray_release.config import Test
from ray_release.template import load_test_cluster_compute
from ray_release.logger import logger

# Keep 10% for the buffer.
limit = int(15784 * 0.9)


Condition = namedtuple(
    "Condition", ["min_gpu", "max_gpu", "min_cpu", "max_cpu", "group", "limit"]
)

aws_gpu_cpu_to_concurrency_groups = [
    Condition(min_gpu=9, max_gpu=-1, min_cpu=0, max_cpu=-1, group="large-gpu", limit=4),
    Condition(
        min_gpu=1, max_gpu=9, min_cpu=0, max_cpu=-128, group="small-gpu", limit=8
    ),
    Condition(
        min_gpu=0, max_gpu=0, min_cpu=1025, max_cpu=-1, group="enormous", limit=1
    ),
    Condition(min_gpu=0, max_gpu=0, min_cpu=513, max_cpu=1024, group="large", limit=8),
    Condition(min_gpu=0, max_gpu=0, min_cpu=129, max_cpu=512, group="medium", limit=6),
    Condition(min_gpu=0, max_gpu=0, min_cpu=0, max_cpu=32, group="tiny", limit=32),
    # Make sure "small" is the last in the list, because it is the fallback.
    Condition(min_gpu=0, max_gpu=0, min_cpu=0, max_cpu=128, group="small", limit=16),
]

gce_gpu_cpu_to_concurrent_groups = [
    Condition(min_gpu=8, max_gpu=-1, min_cpu=0, max_cpu=-1, group="gpu-gce", limit=4),
    Condition(min_gpu=4, max_gpu=-1, min_cpu=0, max_cpu=-1, group="gpu-gce", limit=8),
    Condition(min_gpu=2, max_gpu=-1, min_cpu=0, max_cpu=-1, group="gpu-gce", limit=16),
    Condition(min_gpu=1, max_gpu=-1, min_cpu=0, max_cpu=-1, group="gpu-gce", limit=32),
    Condition(
        min_gpu=0, max_gpu=0, min_cpu=1025, max_cpu=-1, group="enormous-gce", limit=1
    ),
    Condition(
        min_gpu=0, max_gpu=0, min_cpu=513, max_cpu=1024, group="large-gce", limit=8
    ),
    Condition(
        min_gpu=0, max_gpu=0, min_cpu=129, max_cpu=512, group="medium-gce", limit=6
    ),
    Condition(min_gpu=0, max_gpu=0, min_cpu=0, max_cpu=32, group="tiny-gce", limit=32),
    Condition(
        min_gpu=0, max_gpu=0, min_cpu=0, max_cpu=128, group="small-gce", limit=16
    ),
]


# Obtained from https://cloud.google.com/compute/docs/accelerator-optimized-machines
gcp_gpu_instances = {
    "a2-highgpu-1g": (12, 1),
    "a2-highgpu-2g": (24, 2),
    "a2-highgpu-4g": (48, 4),
    "a2-highgpu-8g": (96, 8),
    "a2-megagpu-16g": (96, 16),
    "n1-standard-16-nvidia-tesla-t4-1": (16, 1),
    "n1-standard-64-nvidia-tesla-t4-4": (64, 4),
    "n1-standard-32-nvidia-tesla-t4-2": (32, 2),
    "n1-highmem-64-nvidia-tesla-v100-8": {64, 8},
    "n1-highmem-96-nvidia-tesla-v100-8": {96, 8},
}


def load_instance_types(path: Optional[str] = None) -> Dict[str, Tuple[int, int]]:
    if not path:
        path = bazel_runfile(
            "release/ray_release/buildkite/aws_instance_types.csv",
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
    if test.get("env", None) == "gce":
        concurrent_group = gce_gpu_cpu_to_concurrent_groups
    else:
        concurrent_group = aws_gpu_cpu_to_concurrency_groups
    default_concurrent = concurrent_group[-1]
    try:
        test_cpus, test_gpus = get_test_resources(test)
    except Exception as e:
        logger.warning(f"Couldn't get test resources for test {test['name']}: {e}")
        return default_concurrent.group, default_concurrent.limit

    for condition in concurrent_group:
        min_gpu = parse_condition(condition.min_gpu, float("-inf"))
        max_gpu = parse_condition(condition.max_gpu, float("inf"))
        min_cpu = parse_condition(condition.min_cpu, float("-inf"))
        max_cpu = parse_condition(condition.max_cpu, float("inf"))

        if min_cpu <= test_cpus <= max_cpu and min_gpu <= test_gpus <= max_gpu:
            return condition.group, condition.limit

    # Return default
    logger.warning(
        f"Could not find concurrency group for test {test['name']} "
        f"based on used resources."
    )
    return default_concurrent.group, default_concurrent.limit


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
