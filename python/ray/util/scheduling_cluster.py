import ray
from ray.util.placement_group import placement_group


class SchedulingCluster:
    def __init__(self, pg):
        self.pg = pg

    def __enter__(self):
        self._previous_scheduling_cluster = (
            ray.get_runtime_context()._get_scheduling_cluster()
        )
        ray.get_runtime_context()._set_scheduling_cluster(self)

    def __exit__(self, *args):
        ray.get_runtime_context()._set_scheduling_cluster(
            self._previous_scheduling_cluster
        )

    def _rewrite_resource_requirements(self, resources):
        rewritten_resources = {}
        for name, quantity in resources.items():
            if name == "bundle":
                rewritten_resources[name] = quantity
            else:
                rewritten_resources[f"{name}_group_{self.pg.id.hex()}"] = quantity
        return rewritten_resources

    def _rewrite_placement_group_bundles(self, bundles):
        rewritten_bundles = []
        for bundle in bundles:
            rewritten_bundles.append(self._rewrite_placement_group_bundle(bundle))
        return rewritten_bundles

    def _rewrite_placement_group_bundle(self, bundle):
        return self._rewrite_resource_requirements(bundle)


def scheduling_cluster(bundles, strategy):
    pg = placement_group(bundles, strategy, is_scheduling_cluster=True)
    return SchedulingCluster(pg)
