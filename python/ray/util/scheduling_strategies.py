from ray.util.annotations import PublicAPI
from ray.util.placement_group import PlacementGroup


@PublicAPI(stability="beta")
class SchedulingStrategy(object):
    @staticmethod
    def from_dict(ss_dict: dict) -> "SchedulingStrategy":
        if ss_dict["__class__"] == PlacementGroupSchedulingStrategy.__name__:
            return PlacementGroupSchedulingStrategy.from_dict(ss_dict)
        else:
            return SpreadSchedulingStrategy.from_dict(ss_dict)


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

    def to_dict(self) -> dict:
        return {
            "__class__": self.__class__.__name__,
            "placement_group": self.placement_group.to_dict()
            if isinstance(self.placement_group,
                          PlacementGroup) else self.placement_group,
            "placement_group_bundle_index": self.placement_group_bundle_index,
            "placement_group_capture_child_tasks": self.
            placement_group_capture_child_tasks,
        }

    @staticmethod
    def from_dict(ss_dict: dict) -> "PlacementGroupSchedulingStrategy":
        assert ss_dict[
            "__class__"] == PlacementGroupSchedulingStrategy.__name__
        placement_group = ss_dict["placement_group"]
        return PlacementGroupSchedulingStrategy(
            placement_group=PlacementGroup.from_dict(placement_group)
            if isinstance(placement_group, dict) else placement_group,
            placement_group_bundle_index=ss_dict[
                "placement_group_bundle_index"],
            placement_group_capture_child_tasks=ss_dict[
                "placement_group_capture_child_tasks"],
        )


@PublicAPI(stability="beta")
class SpreadSchedulingStrategy(SchedulingStrategy):
    def to_dict(self) -> dict:
        return {
            "__class__": self.__class__.__name__,
        }

    @staticmethod
    def from_dict(ss_dict: dict) -> "SpreadSchedulingStrategy":
        assert ss_dict["__class__"] == SpreadSchedulingStrategy.__name__
        return SpreadSchedulingStrategy()
