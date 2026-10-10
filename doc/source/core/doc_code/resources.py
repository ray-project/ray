import ray

# __specifying_node_resources_start__
# This will start a Ray node with 3 logical cpus, 4 logical gpus,
# 1 special_hardware resource and 1 custom_label resource.
ray.init(num_cpus=3, num_gpus=4, resources={"special_hardware": 1, "custom_label": 1})
# __specifying_node_resources_end__


# __specifying_resource_requirements_start__
# Specify the default resource requirements for this remote function.
@ray.remote(num_cpus=2, num_gpus=2, resources={"special_hardware": 1})
def func():
    return 1


# You can override the default resource requirements.
func.options(num_cpus=3, num_gpus=1, resources={"special_hardware": 0}).remote()


@ray.remote(num_cpus=0, num_gpus=1)
class Actor:
    pass


# You can override the default resource requirements for actors as well.
actor = Actor.options(num_cpus=1, num_gpus=0).remote()
# __specifying_resource_requirements_end__


# __specifying_fractional_resource_requirements_start__
@ray.remote(num_cpus=0.5)
def io_bound_task():
    import time

    time.sleep(1)
    return 2


io_bound_task.remote()


@ray.remote(num_gpus=0.5)
class IOActor:
    def ping(self):
        import os

        print(f"CUDA_VISIBLE_DEVICES: {os.environ['CUDA_VISIBLE_DEVICES']}")


# Two actors can share the same GPU.
io_actor1 = IOActor.remote()
io_actor2 = IOActor.remote()
ray.get(io_actor1.ping.remote())
ray.get(io_actor2.ping.remote())
# Output:
# (IOActor pid=96328) CUDA_VISIBLE_DEVICES: 1
# (IOActor pid=96329) CUDA_VISIBLE_DEVICES: 1
# __specifying_fractional_resource_requirements_end__
