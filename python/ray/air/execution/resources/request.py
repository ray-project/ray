from typing import List, Type, Dict

from dataclasses import dataclass


@dataclass
class ResourceRequest:
    """Request for bundles of resources.

    This class is used to request resources, e.g. to trigger autoscaling.
    """

    bundles: List[Dict[str, float]]
    strategy: str = "PACK"

    def __hash__(self):
        return hash(
            tuple(frozenset(bundle.items()) for bundle in self.bundles)
            + (self.strategy,)
        )

    @property
    def head_bundle_is_empty(self):
        """Returns True if head bundle is empty while child bundles
        need resources.

        This is considered an internal API within Ray AIR.
        """
        return not bool(sum(self.bundles[0].values()))

    def __post_init__(self):
        if self.head_bundle_is_empty and not self.bundles[1:]:
            raise ValueError(
                "Cannot initialize a ResourceRequest with an empty head "
                "and zero worker bundles."
            )


@dataclass
class ReadyResource:
    """Base class for available resources.

    Internally this can point e.g. to a placement group, a placement
    group bundle index, or just raw resources.

    The main interaction is the `annotate_remote_objects` method. Parameters
    other than the `request` should be private.
    """

    request: ResourceRequest

    def annotate_remote_objects(self, actor_classes: List[Type]) -> List[Type]:
        """Return actor class with options set to use the available resources"""
        raise NotImplementedError
