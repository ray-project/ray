from ray import serve

# __basic_gang_start__
from ray import serve
from ray.serve.config import GangSchedulingConfig


@serve.deployment(
    num_replicas=8,
    ray_actor_options={"num_cpus": 0.25},
    gang_scheduling_config=GangSchedulingConfig(gang_size=4),
)
class Gang:
    def __call__(self, request):
        return "Hello!"


app = Gang.bind()
# __basic_gang_end__

# __gang_context_start__
@serve.deployment(
    num_replicas=4,
    ray_actor_options={"num_cpus": 0.25},
    gang_scheduling_config=GangSchedulingConfig(gang_size=2),
)
class GangWithContext:
    def __init__(self):
        ctx = serve.get_replica_context()
        gc = ctx.gang_context
        self.rank = gc.rank
        self.world_size = gc.world_size
        self.gang_id = gc.gang_id
        self.member_ids = gc.member_replica_ids

    def __call__(self, request):
        return {
            "gang_id": self.gang_id,
            "rank": self.rank,
            "world_size": self.world_size,
        }


gang_context_app = GangWithContext.bind()
# __gang_context_end__

# __pack_strategy_start__
from ray import serve
from ray.serve.config import GangPlacementStrategy, GangSchedulingConfig


@serve.deployment(
    num_replicas=4,
    ray_actor_options={"num_cpus": 0.25},
    gang_scheduling_config=GangSchedulingConfig(
        gang_size=4,
        gang_placement_strategy=GangPlacementStrategy.PACK,
    ),
)
class PackedGang:
    def __call__(self, request):
        return "Packed on same node"


packed_app = PackedGang.bind()
# __pack_strategy_end__

# __spread_strategy_start__
@serve.deployment(
    num_replicas=4,
    ray_actor_options={"num_cpus": 0.25},
    gang_scheduling_config=GangSchedulingConfig(
        gang_size=2,
        gang_placement_strategy=GangPlacementStrategy.SPREAD,
    ),
)
class SpreadGang:
    def __call__(self, request):
        return "Spread across nodes"


spread_app = SpreadGang.bind()
# __spread_strategy_end__

# __options_start__
@serve.deployment
class BaseGang:
    def __call__(self, request):
        return "Hello!"


app_with_gang = BaseGang.options(
    num_replicas=8,
    ray_actor_options={"num_cpus": 0.25},
    gang_scheduling_config=GangSchedulingConfig(gang_size=4),
).bind()
# __options_end__

# __autoscaling_start__
@serve.deployment(
    autoscaling_config={
        "min_replicas": 4,
        "max_replicas": 16,
        "initial_replicas": 8,
        "target_ongoing_requests": 5,
    },
    ray_actor_options={"num_cpus": 0.25},
    gang_scheduling_config=GangSchedulingConfig(gang_size=4),
)
class AutoscaledGang:
    def __call__(self, request):
        return "Hello!"


autoscaled_app = AutoscaledGang.bind()
# __autoscaling_end__

# __fault_tolerance_start__
from ray import serve
from ray.serve.config import GangRuntimeFailurePolicy, GangSchedulingConfig


@serve.deployment(
    num_replicas=8,
    ray_actor_options={"num_cpus": 0.25},
    gang_scheduling_config=GangSchedulingConfig(
        gang_size=4,
        runtime_failure_policy=GangRuntimeFailurePolicy.RESTART_GANG,
    ),
)
class FaultTolerantGang:
    def __call__(self, request):
        return "Hello!"


fault_tolerant_app = FaultTolerantGang.bind()
# __fault_tolerance_end__

# __placement_group_bundles_start__
@serve.deployment(
    num_replicas=4,
    ray_actor_options={"num_cpus": 0},
    placement_group_bundles=[{"CPU": 1, "GPU": 1}],
    gang_scheduling_config=GangSchedulingConfig(gang_size=2),
)
class GangWithSingleBundleReplica:
    def __call__(self, request):
        return "Running on reserved GPUs"


gang_single_bundle_replica_app = GangWithSingleBundleReplica.bind()
# __placement_group_bundles_end__

# __multi_placement_group_bundles_start__
@serve.deployment(
    num_replicas=4,
    ray_actor_options={"num_cpus": 1},
    placement_group_bundles=[{"CPU": 1, "GPU": 1}, {"GPU": 1}],
    gang_scheduling_config=GangSchedulingConfig(gang_size=2),
)
class GangWithMultiBundlesReplica:
    def __call__(self, request):
        return "Running on reserved GPUs"


gang_multi_bundles_replica_app = GangWithMultiBundlesReplica.bind()
# __multi_placement_group_bundles_end__

# __label_selector_start__
@serve.deployment(
    num_replicas=4,
    ray_actor_options={"num_cpus": 0},
    placement_group_bundles=[{"CPU": 1, "GPU": 1}],
    placement_group_bundle_label_selector=[{"ray.io/accelerator-type": "A100"}],
    gang_scheduling_config=GangSchedulingConfig(gang_size=2),
)
class GangOnA100:
    def __call__(self, request):
        return "Running on A100"


gang_a100_app = GangOnA100.bind()
# __label_selector_end__
