from ray.core.generated.node_manager_pb2 import TaskOrActorResourceUsage
from typing import Dict, List

ResourceSet = Dict[str, float]
ResourceSetList = List[Dict]


def aggregate_resource_usage_for_task(
    task: str,
    resource_set: ResourceSet,
    task_resource_count_mapping: Dict[str, ResourceSetList],
):
    if task in task_resource_count_mapping:
        _add_to_resources_list(
            resource_set,
            task_resource_count_mapping[task]["resource_set_list"],
        )
    else:
        task_resource_count_mapping[task] = {
            "task_name": task,
            "resource_set_list": [{"resource_set": resource_set, "count": 1}],
        }


def _add_to_resources_list(
    resource_set: ResourceSet,
    resource_count_list: ResourceSetList,
):
    found = False
    for idx, resource_count in enumerate(resource_count_list):
        if resource_count["resource_set"] == resource_set:
            resource_count_list[idx] = {
                "resource_set": resource_set,
                "count": resource_count["count"] + 1,
            }
            found = True
            break
    if not found:
        resource_count_list.append({"resource_set": resource_set, "count": 1})


def get_task_name(task: TaskOrActorResourceUsage):
    fd = task.function_descriptor
    if hasattr(fd, "python_function_descriptor"):
        fd = fd.python_function_descriptor
    elif hasattr(fd, "java_function_descriptor"):
        fd = fd.java_function_descriptor
    elif hasattr(fd, "cpp_function_descriptor"):
        fd = fd.cpp_function_descriptor
    else:
        return None
    if task.is_actor:
        task_name = fd.class_name
    else:
        task_name = fd.function_name
    return task_name
