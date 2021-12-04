from typing import Union, Optional
from ray.util.annotations import PublicAPI
from ray.util.placement_group import PlacementGroup


@PublicAPI(stability="beta")
class SchedulingStrategy(object):
    """Base class for all scheduling strategies.
    """

    def to_dict(self) -> dict:
        """Convert this scheduling strategy into a dict for purposes of json
        serialization.

        Used when passing a scheduling strategy as an option to a
        Ray client remote function.
        See set_task_options in util/client/common.py.

        Return:
            Json-serializable dictionary representing the scheduling strategy.
        """
        raise NotImplementedError("to_dict() has to be implemented")

    @staticmethod
    def from_dict(ss_dict: dict) -> "SchedulingStrategy":
        """Instantiate and return a scheduling strategy
        from its json-serializable dict representation.

        Used by Ray Client on server-side to deserialize scheduling strategy
        option. See decode_options in util/client/server/server.py.

        Args:
            ss_dict: Dictionary representing a scheduling strategy.
        Return:
            A scheduling strategy made from the data in the input dict.
        """
        if ss_dict["__class__"] == PlacementGroupSchedulingStrategy.__name__:
            return PlacementGroupSchedulingStrategy.from_dict(ss_dict)
        else:
            return DefaultSchedulingStrategy.from_dict(ss_dict)


@PublicAPI(stability="beta")
class DefaultSchedulingStrategy(SchedulingStrategy):
    """The default hybrid scheduling strategy.
    """

    def to_dict(self) -> dict:
        return {
            "__class__": self.__class__.__name__,
        }

    @staticmethod
    def from_dict(ss_dict: dict) -> "DefaultSchedulingStrategy":
        assert ss_dict["__class__"] == DefaultSchedulingStrategy.__name__
        return DefaultSchedulingStrategy()


@PublicAPI(stability="beta")
class PlacementGroupSchedulingStrategy(SchedulingStrategy):
    """Placement group based scheduling strategy.

    Attributes:
        placement_group: the placement group this actor belongs to,
            or None if it doesn't belong to any group.
        placement_group_bundle_index: the index of the bundle
            if the actor belongs to a placement group, which may be -1 to
            specify any available bundle.
        placement_group_capture_child_tasks: Whether or not children tasks
            of this actor should implicitly use the same placement group
            as its parent. It is False by default.
    """

    def __init__(self,
                 placement_group: PlacementGroup,
                 placement_group_bundle_index: int = -1,
                 placement_group_capture_child_tasks: Optional[bool] = None):
        if placement_group is None:
            raise ValueError("placement_group needs to be an instance "
                             "of PlacementGroup")

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
        return PlacementGroupSchedulingStrategy(
            placement_group=PlacementGroup.from_dict(
                ss_dict["placement_group"]),
            placement_group_bundle_index=ss_dict[
                "placement_group_bundle_index"],
            placement_group_capture_child_tasks=ss_dict[
                "placement_group_capture_child_tasks"],
        )


SchedulingStrategyT = Union[None, str,  # Literal["DEFAULT"]
                            DefaultSchedulingStrategy,
                            PlacementGroupSchedulingStrategy]
