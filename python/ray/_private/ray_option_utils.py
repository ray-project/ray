"""Manage, parse and validate options for Ray tasks, actors and actor methods."""
import warnings
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, Union

import logging
import ray
from ray._private import ray_constants
from ray._private.utils import get_ray_doc_version
from ray.util.accelerators import accelerators
from ray.util.placement_group import PlacementGroup
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
    PlacementGroupSchedulingStrategy,
    NodeLabelSchedulingStrategy,
)


@dataclass
class Option:
    # Type constraint of an option.
    type_constraint: Optional[Union[type, Tuple[type]]] = None
    # Value constraint of an option.
    # The callable should return None if there is no error.
    # Otherwise, return the error message.
    value_constraint: Optional[Callable[[Any], Optional[str]]] = None
    # Default value.
    default_value: Any = None

    def validate(self, keyword: str, value: Any):
        """Validate the option."""
        if self.type_constraint is not None:
            if not isinstance(value, self.type_constraint):
                raise TypeError(
                    f"The type of keyword '{keyword}' must be {self.type_constraint}, "
                    f"but received type {type(value)}"
                )
        if self.value_constraint is not None:
            possible_error_message = self.value_constraint(value)
            if possible_error_message:
                raise ValueError(possible_error_message)


def _counting_option(name: str, infinite: bool = True, default_value: Any = None):
    """This is used for positive and discrete options.

    Args:
        name: The name of the option keyword.
        infinite: If True, user could use -1 to represent infinity.
        default_value: The default value for this option.
    """
    if infinite:
        return Option(
            (int, type(None)),
            lambda x: None
            if (x is None or x >= -1)
            else f"The keyword '{name}' only accepts None, 0, -1"
            " or a positive integer, where -1 represents infinity.",
            default_value=default_value,
        )
    return Option(
        (int, type(None)),
        lambda x: None
        if (x is None or x >= 0)
        else f"The keyword '{name}' only accepts None, 0 or a positive integer.",
        default_value=default_value,
    )


def _validate_resource_quantity(name, quantity):
    if quantity < 0:
        return f"The quantity of resource {name} cannot be negative"
    if (
        isinstance(quantity, float)
        and quantity != 0.0
        and int(quantity * ray._raylet.RESOURCE_UNIT_SCALING) == 0
    ):
        return (
            f"The precision of the fractional quantity of resource {name}"
            " cannot go beyond 0.0001"
        )
    return None


def _resource_option(name: str, default_value: Any = None):
    """This is used for resource related options."""
    return Option(
        (float, int, type(None)),
        lambda x: None if (x is None) else _validate_resource_quantity(name, x),
        default_value=default_value,
    )


def _validate_resources(resources: Optional[Dict[str, float]]) -> Optional[str]:
    if resources is None:
        return None

    if "CPU" in resources or "GPU" in resources:
        return (
            "Use the 'num_cpus' and 'num_gpus' keyword instead of 'CPU' and 'GPU' "
            "in 'resources' keyword"
        )

    for name, quantity in resources.items():
        possible_error_message = _validate_resource_quantity(name, quantity)
        if possible_error_message:
            return possible_error_message

    return None


def _maybe_warn_misconfigured_tpu_chips(resources: Dict[str, Any]):
    """Possibly warn against misconfigured TPU chip configuration."""
    num_tpus = resources.get(ray_constants.TPU, 0)
    if num_tpus not in ray_constants.TPU_VALID_CHIP_OPTIONS:
        logging.warning(
            f"The number of requested 'TPU' was set to {num_tpus} which "
            "is not a supported chip configuration. Supported configs: "
            f"{ray_constants.TPU_VALID_CHIP_OPTIONS}"
        )


def _validate_accelerators(options: Dict[str, Any]):
    """Validate options for accelerators - support only one out of multiple options.

    GPUs, NeuronCore accelerators (neuron_cores), and TPUs are valid options, but
    individual nodes do not support heterogeneous accelerators. This function
    guards against this setting.

    The control flow is as follows:
        - For each accelerator, determine if the options indicate usage of
          a particular accelerator, captured by booleans.
            - For GPUs, this is set if num_gpus > 0.
            - For custom resources, this is set if the
              resource name ("neuron_cores" or "TPU") or
              accelerator_type ("aws-neuron-core", "TPU-V2", "TPU-V3", etc.)
              is requested.
        - If we identify that >1 resource type is requested,
          we raise an error indicating that heterogeneous raylets
          are not supported.

    Args:
        options: The options to be validated.

    Raises:
        ValueError: If the options are invalid.
    """
    num_gpus = options.get("num_gpus", None)
    non_zero_gpus = num_gpus is not None and num_gpus > 0
    resources = options["resources"] if "resources" in options else None

    def non_zero_custom_resource(
        resource_id: str, accelerator_ids: Iterable[str]
    ) -> bool:
        """Return whether or not the options includes >0 of a custom resource."""
        result = False
        accelerator_type_value: str = options.get("accelerator_type", "")
        if resources is not None:
            num_resources: int = resources.get(resource_id, 0)
            if num_resources > 0:
                result = True
        if accelerator_type_value in accelerator_ids:
            result = True
        return result

    non_zero_neuron_cores = non_zero_custom_resource(
        resource_id=ray_constants.NEURON_CORES,
        accelerator_ids=[accelerators.AWS_NEURON_CORE],
    )
    non_zero_tpus = non_zero_custom_resource(
        resource_id=ray_constants.TPU,
        accelerator_ids=[
            accelerators.GOOGLE_TPU_V2,
            accelerators.GOOGLE_TPU_V3,
            accelerators.GOOGLE_TPU_V4,
        ],
    )

    if non_zero_tpus:
        _maybe_warn_misconfigured_tpu_chips(resources)

    num_configured_accelerators = sum(
        [non_zero_gpus, non_zero_tpus, non_zero_neuron_cores]
    )
    if num_configured_accelerators > 1:
        hardware_requested = []
        if non_zero_gpus:
            hardware_requested.append("GPU")
        if non_zero_neuron_cores:
            hardware_requested.append("neuron_cores")
        if non_zero_tpus:
            hardware_requested.append("TPU")
        hardware_str = ",".join(hardware_requested)
        raise ValueError(
            "Only one of 'num_gpus', 'neuron_cores/accelerator_type:aws-neuron-core' "
            "and 'TPU/accelerator_type:TPU-V*' can be set. "
            f"Detected {num_configured_accelerators} "
            f"options were configured: {hardware_str}."
        )


_common_options = {
    "accelerator_type": Option((str, type(None))),
    "memory": _resource_option("memory"),
    "name": Option((str, type(None))),
    "num_cpus": _resource_option("num_cpus"),
    "num_gpus": _resource_option("num_gpus"),
    "object_store_memory": _counting_option("object_store_memory", False),
    # TODO(suquark): "placement_group", "placement_group_bundle_index"
    # and "placement_group_capture_child_tasks" are deprecated,
    # use "scheduling_strategy" instead.
    "placement_group": Option(
        (type(None), str, PlacementGroup), default_value="default"
    ),
    "placement_group_bundle_index": Option(int, default_value=-1),
    "placement_group_capture_child_tasks": Option((bool, type(None))),
    "resources": Option((dict, type(None)), lambda x: _validate_resources(x)),
    "runtime_env": Option((dict, type(None))),
    "scheduling_strategy": Option(
        (
            type(None),
            str,
            PlacementGroupSchedulingStrategy,
            NodeAffinitySchedulingStrategy,
            NodeLabelSchedulingStrategy,
        )
    ),
    "_metadata": Option((dict, type(None))),
}


def issubclass_safe(obj: Any, cls_: type) -> bool:
    try:
        return issubclass(obj, cls_)
    except TypeError:
        return False


_task_only_options = {
    "max_calls": _counting_option("max_calls", False, default_value=0),
    # Normal tasks may be retried on failure this many times.
    # TODO(swang): Allow this to be set globally for an application.
    "max_retries": _counting_option(
        "max_retries", default_value=ray_constants.DEFAULT_TASK_MAX_RETRIES
    ),
    # override "_common_options"
    "num_cpus": _resource_option("num_cpus", default_value=1),
    "num_returns": Option(
        (int, str, type(None)),
        lambda x: None
        if (x is None or x == "dynamic" or x == "streaming" or x >= 0)
        else "The keyword 'num_returns' only accepts None, a non-negative integer, or "
        '"dynamic" (for generators)',
        default_value=1,
    ),
    "object_store_memory": Option(  # override "_common_options"
        (int, type(None)),
        lambda x: None
        if (x is None)
        else "Setting 'object_store_memory' is not implemented for tasks",
    ),
    "retry_exceptions": Option(
        (bool, list, tuple),
        lambda x: None
        if (
            isinstance(x, bool)
            or (
                isinstance(x, (list, tuple))
                and all(issubclass_safe(x_, Exception) for x_ in x)
            )
        )
        else "retry_exceptions must be either a boolean or a list of exceptions",
        default_value=False,
    ),
}

_actor_only_options = {
    "concurrency_groups": Option((list, dict, type(None))),
    "lifetime": Option(
        (str, type(None)),
        lambda x: None
        if x in (None, "detached", "non_detached")
        else "actor `lifetime` argument must be one of 'detached', "
        "'non_detached' and 'None'.",
    ),
    "max_concurrency": _counting_option("max_concurrency", False),
    "max_restarts": _counting_option("max_restarts", default_value=0),
    "max_task_retries": _counting_option("max_task_retries", default_value=0),
    "max_pending_calls": _counting_option("max_pending_calls", default_value=-1),
    "namespace": Option((str, type(None))),
    "get_if_exists": Option(bool, default_value=False),
}

# Priority is important here because during dictionary update, same key with higher
# priority overrides the same key with lower priority. We make use of priority
# to set the correct default value for tasks / actors.

# priority: _common_options > _actor_only_options > _task_only_options
valid_options: Dict[str, Option] = {
    **_task_only_options,
    **_actor_only_options,
    **_common_options,
}
# priority: _task_only_options > _common_options
task_options: Dict[str, Option] = {**_common_options, **_task_only_options}
# priority: _actor_only_options > _common_options
actor_options: Dict[str, Option] = {**_common_options, **_actor_only_options}

remote_args_error_string = (
    "The @ray.remote decorator must be applied either with no arguments and no "
    "parentheses, for example '@ray.remote', or it must be applied using some of "
    f"the arguments in the list {list(valid_options.keys())}, for example "
    "'@ray.remote(num_returns=2, resources={\"CustomResource\": 1})'."
)


def _check_deprecate_placement_group(options: Dict[str, Any]):
    """Check if deprecated placement group option exists."""
    placement_group = options.get("placement_group", "default")
    scheduling_strategy = options.get("scheduling_strategy")
    # TODO(suquark): @ray.remote(placement_group=None) is used in
    # "python/ray.data._internal/remote_fn.py" and many other places,
    # while "ray.data.read_api.read_datasource" set "scheduling_strategy=SPREAD".
    # This might be a bug, but it is also ok to allow them co-exist.
    if (placement_group not in ("default", None)) and (scheduling_strategy is not None):
        raise ValueError(
            "Placement groups should be specified via the "
            "scheduling_strategy option. "
            "The placement_group option is deprecated."
        )


def _warn_if_using_deprecated_placement_group(
    options: Dict[str, Any], caller_stacklevel: int
):
    placement_group = options["placement_group"]
    placement_group_bundle_index = options["placement_group_bundle_index"]
    placement_group_capture_child_tasks = options["placement_group_capture_child_tasks"]
    if placement_group != "default":
        warnings.warn(
            "placement_group parameter is deprecated. Use "
            "scheduling_strategy=PlacementGroupSchedulingStrategy(...) "
            "instead, see the usage at "
            f"https://docs.ray.io/en/{get_ray_doc_version()}/ray-core/package-ref.html#ray-remote.",  # noqa: E501
            DeprecationWarning,
            stacklevel=caller_stacklevel + 1,
        )
    if placement_group_bundle_index != -1:
        warnings.warn(
            "placement_group_bundle_index parameter is deprecated. Use "
            "scheduling_strategy=PlacementGroupSchedulingStrategy(...) "
            "instead, see the usage at "
            f"https://docs.ray.io/en/{get_ray_doc_version()}/ray-core/package-ref.html#ray-remote.",  # noqa: E501
            DeprecationWarning,
            stacklevel=caller_stacklevel + 1,
        )
    if placement_group_capture_child_tasks:
        warnings.warn(
            "placement_group_capture_child_tasks parameter is deprecated. Use "
            "scheduling_strategy=PlacementGroupSchedulingStrategy(...) "
            "instead, see the usage at "
            f"https://docs.ray.io/en/{get_ray_doc_version()}/ray-core/package-ref.html#ray-remote.",  # noqa: E501
            DeprecationWarning,
            stacklevel=caller_stacklevel + 1,
        )


def validate_task_options(options: Dict[str, Any], in_options: bool):
    """Options check for Ray tasks.

    Args:
        options: Options for Ray tasks.
        in_options: If True, we are checking the options under the context of
            ".options()".
    """
    for k, v in options.items():
        if k not in task_options:
            raise ValueError(
                f"Invalid option keyword {k} for remote functions. "
                f"Valid ones are {list(task_options)}."
            )
        task_options[k].validate(k, v)
    if in_options and "max_calls" in options:
        raise ValueError("Setting 'max_calls' is not supported in '.options()'.")
    _check_deprecate_placement_group(options)
    _validate_accelerators(options)


def validate_actor_options(options: Dict[str, Any], in_options: bool):
    """Options check for Ray actors.

    Args:
        options: Options for Ray actors.
        in_options: If True, we are checking the options under the context of
            ".options()".
    """
    for k, v in options.items():
        if k not in actor_options:
            raise ValueError(
                f"Invalid option keyword {k} for actors. "
                f"Valid ones are {list(actor_options)}."
            )
        actor_options[k].validate(k, v)

    if in_options and "concurrency_groups" in options:
        raise ValueError(
            "Setting 'concurrency_groups' is not supported in '.options()'."
        )

    if options.get("max_restarts", 0) == 0 and options.get("max_task_retries", 0) != 0:
        raise ValueError(
            "'max_task_retries' cannot be set if 'max_restarts' "
            "is 0 or if 'max_restarts' is not set."
        )

    if options.get("get_if_exists") and not options.get("name"):
        raise ValueError("The actor name must be specified to use `get_if_exists`.")

    if "object_store_memory" in options:
        warnings.warn(
            "Setting 'object_store_memory'"
            " for actors is deprecated since it doesn't actually"
            " reserve the required object store memory."
            f" Use object spilling that's enabled by default (https://docs.ray.io/en/{get_ray_doc_version()}/ray-core/objects/object-spilling.html) "  # noqa: E501
            "instead to bypass the object store memory size limitation.",
            DeprecationWarning,
            stacklevel=1,
        )

    _check_deprecate_placement_group(options)
    _validate_accelerators(options)


def update_options(
    original_options: Dict[str, Any], new_options: Dict[str, Any]
) -> Dict[str, Any]:
    """Update original options with new options and return.
    The returned updated options contain shallow copy of original options.
    """

    updated_options = {**original_options, **new_options}
    # Ensure we update each namespace in "_metadata" independently.
    # "_metadata" is a dict like {namespace1: config1, namespace2: config2}
    if (
        original_options.get("_metadata") is not None
        and new_options.get("_metadata") is not None
    ):
        # make a shallow copy to avoid messing up the metadata dict in
        # the original options.
        metadata = original_options["_metadata"].copy()
        for namespace, config in new_options["_metadata"].items():
            metadata[namespace] = {**metadata.get(namespace, {}), **config}

        updated_options["_metadata"] = metadata

    return updated_options
