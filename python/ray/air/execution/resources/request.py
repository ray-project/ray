import abc
import json
from copy import deepcopy
from inspect import signature
from typing import List, Type, Dict, Union

from dataclasses import dataclass

from ray.util import placement_group
from ray.util.annotations import DeveloperAPI


def _sum_bundles(bundles: List[Dict[str, float]]) -> Dict[str, float]:
    """Sum all resources in a list of resource bundles.

    Args:
        bundles: List of resource bundles.

    Returns: Dict containing all resources summed up.
    """
    resources = {}
    for bundle in bundles:
        for k, v in bundle.items():
            resources[k] = resources.get(k, 0) + v
    return resources


@DeveloperAPI
class ResourceRequest:
    """Request for resources.

    This class is used to define a resource request. A resource request comprises one
    or more bundles of resources and instructions on the scheduling behavior.

    The resource request can be submitted to a resource manager, which will
    schedule the resources. Depending on the resource backend, this may instruct
    Ray to scale up (autoscaling).

    Args:
        bundles: A list of bundles which represent the resources requirements.
        strategy: The scheduling strategy to acquire the bundles. This is only
            relevant if the resources are requested as placement groups.

         - "PACK": Packs Bundles into as few nodes as possible.
         - "SPREAD": Places Bundles across distinct nodes as even as possible.
         - "STRICT_PACK": Packs Bundles into one node. The group is
           not allowed to span multiple nodes.
         - "STRICT_SPREAD": Packs Bundles across distinct nodes.
        *args: Passed to the call of ``placement_group()``, if applicable.
        **kwargs: Passed to the call of ``placement_group()``, if applicable.

    """

    def __init__(
        self,
        bundles: List[Dict[str, Union[int, float]]],
        strategy: str = "PACK",
        *args,
        **kwargs,
    ):
        if not bundles:
            raise ValueError("Cannot initialize a ResourceRequest with zero bundles.")

        # Remove empty resource keys
        self._bundles = [
            {k: float(v) for k, v in bundle.items() if v != 0} for bundle in bundles
        ]

        # Check if the head bundle is empty (no resources defined or all resources
        # are 0 (and thus removed in the previous step)
        if not self._bundles[0]:
            # This is when trainable itself doesn't need resources.
            self._head_bundle_is_empty = True
            self._bundles.pop(0)

            if not self._bundles:
                raise ValueError(
                    "Cannot initialize a ResourceRequest with an empty head "
                    "and zero worker bundles."
                )
        else:
            self._head_bundle_is_empty = False

        self._strategy = strategy
        self._args = args
        self._kwargs = kwargs

        self._hash = None
        self._bound = None

        self._bind()

    @property
    def head_bundle_is_empty(self):
        """Returns True if head bundle is empty while child bundles
        need resources.

        This is considered an internal API within Tune.
        """
        return self._head_bundle_is_empty

    @property
    @DeveloperAPI
    def head_cpus(self) -> float:
        return 0.0 if self._head_bundle_is_empty else self._bundles[0].get("CPU", 0.0)

    @property
    @DeveloperAPI
    def bundles(self) -> List[Dict[str, float]]:
        """Returns a deep copy of resource bundles"""
        return deepcopy(self._bundles)

    @property
    def required_resources(self) -> Dict[str, float]:
        """Returns a dict containing the sums of all resources"""
        return _sum_bundles(self._bundles)

    @property
    @DeveloperAPI
    def strategy(self) -> str:
        """Returns the placement strategy"""
        return self._strategy

    def _bind(self):
        """Bind the args and kwargs to the `placement_group()` signature.

        We bind the args and kwargs, so we can compare equality of two resource
        requests. The main reason for this is that the `placement_group()` API
        can evolve independently from the ResourceRequest API (e.g. adding new
        arguments). Then, `ResourceRequest(bundles, strategy, arg=arg)` should
        be the same as `ResourceRequest(bundles, strategy, arg)`.
        """
        sig = signature(placement_group)
        try:
            self._bound = sig.bind(
                self._bundles, self._strategy, *self._args, **self._kwargs
            )
        except Exception as exc:
            raise RuntimeError(
                "Invalid definition for resource request. Please check "
                "that you passed valid arguments to the ResourceRequest "
                "object."
            ) from exc

    def to_placement_group(self):
        return placement_group(*self._bound.args, **self._bound.kwargs)

    def __eq__(self, other: "ResourceRequest"):
        return (
            self._bound == other._bound
            and self.head_bundle_is_empty == other.head_bundle_is_empty
        )

    def __hash__(self):
        if not self._hash:
            # Cache hash
            self._hash = hash(
                json.dumps(
                    {"args": self._bound.args, "kwargs": self._bound.kwargs},
                    sort_keys=True,
                    indent=0,
                    ensure_ascii=True,
                )
            )
        return self._hash

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop("_hash", None)
        state.pop("_bound", None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._hash = None
        self._bound = None
        self._bind()

    def __repr__(self) -> str:
        return (
            f"<ResourceRequest (_bound={self._bound}, "
            f"head_bundle_is_empty={self.head_bundle_is_empty})>"
        )


@DeveloperAPI
@dataclass
class AllocatedResource(abc.ABC):
    """Base class for available resources.

    Internally this can point e.g. to a placement group, a placement
    group bundle index, or just raw resources.

    The main API is the `annotate_remote_objects` method. This will associate
    remote Ray objects (tasks and actors) with the allocated resources.

    Parameters other than the `request` should be private.
    """

    resource_request: ResourceRequest

    def annotate_remote_objects(self, objects: List[Type]) -> List[Type]:
        """Return remote ray objects with options set to use the allocated resources.

        The first object will be associated with the first bundle, the second
        object will be associated with the second bundle, etc.

        Args:
            object: Remote Ray objects to allocate resources to.
        """
        raise NotImplementedError
