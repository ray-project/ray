import warnings
from typing import Dict, Optional

from ray.air.execution.resources.request import ResourceRequest
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.util.placement_group import placement_group


@PublicAPI(stability="beta")
class PlacementGroupFactory(ResourceRequest):
    """Wrapper class that creates placement groups for trials.

    This function should be used to define resource requests for Ray Tune
    trials. It holds the parameters to create
    :ref:`placement groups <ray-placement-group-doc-ref>`.
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

    def __call__(self, *args, **kwargs):
        warnings.warn(
            "Calling PlacementGroupFactory objects is deprecated. Use "
            "`to_placement_group()` instead.",
            DeprecationWarning,
        )
        kwargs.update(self._bound.kwargs)
        # Call with bounded *args and **kwargs
        return placement_group(*self._bound.args, **kwargs)


@DeveloperAPI
def resource_dict_to_pg_factory(spec: Optional[Dict[str, float]] = None):
    """Translates resource dict into PlacementGroupFactory."""
    spec = spec or {"cpu": 1}

    spec = spec.copy()

    cpus = spec.pop("cpu", spec.pop("CPU", 0.0))
    gpus = spec.pop("gpu", spec.pop("GPU", 0.0))
    memory = spec.pop("memory", 0.0)

    # If there is a custom_resources key, use as base for bundle
    bundle = dict(spec.pop("custom_resources", {}))

    # Otherwise, consider all other keys as custom resources
    if not bundle:
        bundle = spec

    bundle.update(
        {
            "CPU": cpus,
            "GPU": gpus,
            "memory": memory,
        }
    )

    return PlacementGroupFactory([bundle])
