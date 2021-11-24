from ray.util.annotations import PublicAPI


@PublicAPI(stability="beta")
class SchedulingStrategy(object):
    pass


@PublicAPI(stability="beta")
class PlacementGroupSchedulingStrategy(SchedulingStrategy):
    def __init__(self,
                 placement_group,
                 placement_group_bundle_index=-1,
                 placement_group_capture_child_tasks=None):
        self.placement_group = placement_group
        self.placement_group_bundle_index = placement_group_bundle_index
        self.placement_group_capture_child_tasks = \
            placement_group_capture_child_tasks


@PublicAPI(stability="beta")
class SpreadSchedulingStrategy(SchedulingStrategy):
    pass
