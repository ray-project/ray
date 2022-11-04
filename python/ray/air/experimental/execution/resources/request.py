import json
from copy import deepcopy
from inspect import signature
from typing import List, Type, Dict, Union

from dataclasses import dataclass

from ray.util import placement_group, PublicAPI
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


@PublicAPI(stability="beta")
class ResourceRequest:
    """Wrapper class that creates placement groups for trials.

    This function should be used to define resource requests for Ray Tune
    trials. It holds the parameters to create placement groups.
    At a minimum, this will hold at least one bundle specifying the
    resource requirements for each trial:

    .. code-block:: python

        from ray import tune

        tuner = tune.Tuner(
            tune.with_resources(
                train,
                resources=tune.PlacementGroupFactory([
                    {"CPU": 1, "GPU": 0.5, "custom_resource": 2}
                ])
            )
        )
        tuner.fit()

    If the trial itself schedules further remote workers, the resource
    requirements should be specified in additional bundles. You can also
    pass the placement strategy for these bundles, e.g. to enforce
    co-located placement:

    .. code-block:: python

        from ray import tune

        tuner = tune.Tuner(
            tune.with_resources(
                train,
                resources=tune.PlacementGroupFactory([
                    {"CPU": 1, "GPU": 0.5, "custom_resource": 2},
                    {"CPU": 2},
                    {"CPU": 2},
                ], strategy="PACK")
            )
        )
        tuner.fit()

    The example above will reserve 1 CPU, 0.5 GPUs and 2 custom_resources
    for the trainable itself, and reserve another 2 bundles of 2 CPUs each.
    The trial will only start when all these resources are available. This
    could be used e.g. if you had one learner running in the main trainable
    that schedules two remote workers that need access to 2 CPUs each.

    If the trainable itself doesn't require resources.
    You can specify it as:

    .. code-block:: python

        from ray import tune

        tuner = tune.Tuner(
            tune.with_resources(
                train,
                resources=tune.PlacementGroupFactory([
                    {},
                    {"CPU": 2},
                    {"CPU": 2},
                ], strategy="PACK")
            )
        )
        tuner.fit()

    Args:
        bundles: A list of bundles which
            represent the resources requirements.
        strategy: The strategy to create the placement group.

         - "PACK": Packs Bundles into as few nodes as possible.
         - "SPREAD": Places Bundles across distinct nodes as even as possible.
         - "STRICT_PACK": Packs Bundles into one node. The group is
           not allowed to span multiple nodes.
         - "STRICT_SPREAD": Packs Bundles across distinct nodes.
        *args: Passed to the call of ``placement_group()``
        **kwargs: Passed to the call of ``placement_group()``

    """

    def __init__(
        self,
        bundles: List[Dict[str, Union[int, float]]],
        strategy: str = "PACK",
        *args,
        **kwargs,
    ):
        if not bundles:
            raise ValueError(
                "Cannot initialize a PlacementGroupFactory with zero bundles."
            )

        self._bundles = [
            {k: float(v) for k, v in bundle.items() if v != 0} for bundle in bundles
        ]

        if not self._bundles[0]:
            # This is when trainable itself doesn't need resources.
            self._head_bundle_is_empty = True
            self._bundles.pop(0)

            if not self._bundles:
                raise ValueError(
                    "Cannot initialize a PlacementGroupFactory with an empty head "
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
        sig = signature(placement_group)
        try:
            self._bound = sig.bind(
                self._bundles, self._strategy, *self._args, **self._kwargs
            )
        except Exception as exc:
            raise RuntimeError(
                "Invalid definition for placement group factory. Please check "
                "that you passed valid arguments to the PlacementGroupFactory "
                "object."
            ) from exc

    def __call__(self, *args, **kwargs):
        kwargs.update(self._bound.kwargs)
        # Call with bounded *args and **kwargs
        return placement_group(*self._bound.args, **kwargs)

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
            f"<PlacementGroupFactory (_bound={self._bound}, "
            f"head_bundle_is_empty={self.head_bundle_is_empty})>"
        )


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
