from typing import Dict, List, Tuple

ResourceSet = Dict[str, float]
ResourceCountList = List[Tuple[ResourceSet, int]]


def add_resource_usage_to_task(
    task: str,
    resource_set: ResourceSet,
    task_resource_count_mapping: Dict[str, ResourceCountList],
):
    if task in task_resource_count_mapping:
        task_resource_count_mapping[task] = _add_to_resources_list(
            resource_set,
            task_resource_count_mapping[task],
        )
    else:
        task_resource_count_mapping[task] = [[resource_set, 1]]


def _add_to_resources_list(
    resource_set: ResourceSet,
    resources_list: ResourceCountList,
):
    found = False
    for idx, resource in enumerate(resources_list):
        if resource[0] == resource_set:
            resources_list[idx] = [resource[0], resource[1] + 1]
            found = True
            break
    if not found:
        resources_list.append([resource_set, 1])
