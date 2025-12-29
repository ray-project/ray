import ray

# __max_replicas_per_node_start__
from ray import serve


@serve.deployment(num_replicas=6, max_replicas_per_node=2, ray_actor_options={"num_cpus": 0.1})
class MyDeployment:
    def __call__(self, request):
        return "Hello!"


app = MyDeployment.bind()
# __max_replicas_per_node_end__

if __name__ == "__main__":
    ray.init()
    # doing this because cluster has only one node
    serve.run(MyDeployment.options(max_replicas_per_node=6).bind())
    serve.shutdown()
    ray.shutdown()


# __placement_group_start__
from ray import serve


@serve.deployment(
    ray_actor_options={"num_cpus": 0.1},
    placement_group_bundles=[{"CPU": 0.1}, {"CPU": 0.1}],
    placement_group_strategy="STRICT_PACK",
)
class MultiCPUModel:
    def __call__(self, request):
        return "Processed with 2 CPUs"


multi_cpu_app = MultiCPUModel.bind()
# __placement_group_end__

if __name__ == "__main__":
    ray.init()
    serve.run(multi_cpu_app)
    serve.shutdown()
    ray.shutdown()


# __custom_resources_start__
from ray import serve


# Schedule only on nodes with A100 GPUs
@serve.deployment(ray_actor_options={"resources": {"A100": 1}})
class A100Model:
    def __call__(self, request):
        return "Running on A100"


# Schedule only on nodes with T4 GPUs
@serve.deployment(ray_actor_options={"resources": {"T4": 1}})
class T4Model:
    def __call__(self, request):
        return "Running on T4"


a100_app = A100Model.bind()
t4_app = T4Model.bind()
# __custom_resources_end__


# __custom_resources_main_start__
if __name__ == "__main__":
    ray.init(
        resources={
            "A100": 1,
            "T4": 1,
        }
    )
    serve.run(a100_app, name="a100", route_prefix="/a100")
    serve.run(t4_app, name="t4", route_prefix="/t4")
    serve.shutdown()
    ray.shutdown()
# __custom_resources_main_end__
