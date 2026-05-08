import ray

# __max_replicas_per_node_start__
from ray import serve


@serve.deployment(num_replicas=6, max_replicas_per_node=2, ray_actor_options={"num_cpus": 0.1})
class MyDeployment:
    def __call__(self, request):
        return "Hello!"


app = MyDeployment.bind()
# __max_replicas_per_node_end__

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

# __placement_group_labels_start__
@serve.deployment(
    ray_actor_options={"num_cpus": 0.1},
    placement_group_bundles=[{"CPU": 0.1, "GPU": 1}],
    placement_group_bundle_label_selector=[
        {"ray.io/accelerator-type": "A100"}
    ]
)
def PlacementGroupBundleLabelSelector(request):
    return "Running in PG on A100"

pg_label_app = PlacementGroupBundleLabelSelector.bind()
# __placement_group_labels_end__

# __label_selectors_start__
from ray import serve


# Schedule only on nodes with A100 GPUs
@serve.deployment(ray_actor_options={"label_selector": {"ray.io/accelerator-type": "A100"}})
class A100Model:
    def __call__(self, request):
        return "Running on A100"


# Schedule only on nodes with T4 GPUs
@serve.deployment(ray_actor_options={"label_selector": {"ray.io/accelerator-type": "T4"}})
class T4Model:
    def __call__(self, request):
        return "Running on T4"


a100_app = A100Model.bind()
t4_app = T4Model.bind()
# __label_selectors_end__

# __fallback_strategy_start__
@serve.deployment(
    ray_actor_options={
        "label_selector": {"zone": "us-west-2a"},
        "fallback_strategy": [{"label_selector": {"zone": "us-west-2b"}}]
    }
)
class SoftAffinityDeployment:
    def __call__(self, request):
        return "Scheduling to a zone with soft constraints!"

soft_affinity_app = SoftAffinityDeployment.bind()
# __fallback_strategy_end__

# __label_selector_main_start__
if __name__ == "__main__":
    # RayCluster with resources to run example tests.
    ray.init(
        labels={
            "ray.io/accelerator-type": "A100",
            "zone": "us-west-2b",
        },
        num_cpus=16,
        num_gpus=1,
        resources={"my_custom_resource": 10},
    )

    serve.run(a100_app, name="a100", route_prefix="/a100")
# __label_selector_main_end__

    # Run remaining doc code.
    serve.run(MyDeployment.options(max_replicas_per_node=6).bind(), name="max_replicas", route_prefix="/max_replicas")

    serve.run(multi_cpu_app, name="multi_cpu", route_prefix="/multi_cpu")

    serve.run(pg_label_app, name="pg_label", route_prefix="/pg_label")

    serve.run(soft_affinity_app, name="soft_affinity", route_prefix="/soft_affinity")

    serve.shutdown()
    ray.shutdown()
