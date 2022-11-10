import ray

# __specifying_node_resources_start__
# This will start a Ray node with 3 logical cpus, 4 logical gpus,
# 1 special_hardware resource and 1 custom_label resource.
ray.init(num_cpus=3, num_gpus=4, resources={"special_hardware": 1, "custom_label": 1})
# __specifying_node_resources_end__


# __specifying_resource_requirements_start__
# Specify the default resource requirements for this remote function.
@ray.remote(num_cpus=4, num_gpus=2, resources={"special_hardware": 1})
def func():
    return 1


# You can override the default resource requirements.
func.options(num_cpus=5, num_gpus=3, resources={"custom_label": 0.001}).remote()


# Ray also supports fractional resource requirements.
@ray.remote(num_cpus=0, num_gpus=0.5)
class Actor:
    pass


# You can override the default resource requirements for actors as well.
actor = Actor.options(num_cpus=0, num_gpus=0.6).remote()
# __specifying_resource_requirements_end__
