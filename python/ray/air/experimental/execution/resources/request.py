from typing import List, Type

from dataclasses import dataclass

from ray.tune import PlacementGroupFactory


class ResourceRequest(PlacementGroupFactory):
    """Request for bundles of resources.

    This class is used to request resources, e.g. to trigger autoscaling.
    """

    pass


@dataclass
class AllocatedResource:
    """Base class for available resources.

    Internally this can point e.g. to a placement group, a placement
    group bundle index, or just raw resources.

    The main interaction is the `annotate_remote_objects` method. Parameters
    other than the `request` should be private.
    """

    resource_request: ResourceRequest

    def annotate_remote_objects(self, actor_classes: List[Type]) -> List[Type]:
        """Return actor class with options set to use the available resources"""
        raise NotImplementedError
