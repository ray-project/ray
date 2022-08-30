from typing import List, Type, Dict

from dataclasses import dataclass


@dataclass
class ResourceRequest:
    """Request for bundles of resources.

    This class is used to request resources, e.g. to trigger autoscaling.
    """

    bundles: List[Dict[str, float]]


@dataclass
class ReadyResource:
    """Base class for available resources.

    Internally this can point e.g. to a placement group, a placement
    group bundle index, or just raw resources.

    The main interaction is the `annotate_actor_classes` method. Parameters
    other than the `request` should be private.
    """

    request: ResourceRequest

    def annotate_actor_classes(self, actor_classes: List[Type]) -> List[Type]:
        """Return actor class with options set to use the available resources"""
        raise NotImplementedError
