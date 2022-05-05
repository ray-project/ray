"""Manage, parse and validate options for Ray tasks, actors and actor methods."""
from typing import Dict, Any, Callable, Tuple, Union, Optional
from dataclasses import dataclass
from ray.util.placement_group import PlacementGroup
from ray.util.scheduling_strategies import (
    PlacementGroupSchedulingStrategy,
    NodeAffinitySchedulingStrategy,
)


@dataclass
class Option:
    # Type constraint of an option.
    type_constraint: Optional[Union[type, Tuple[type]]] = None
    # Value constraint of an option.
    value_constraint: Optional[Callable[[Any], bool]] = None
    # Error message for value constraint.
    error_message_for_value_constraint: Optional[str] = None
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
            if not self.value_constraint(value):
                raise ValueError(self.error_message_for_value_constraint)


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
            lambda x: x is None or x >= -1,
            f"The keyword '{name}' only accepts None, 0, -1 or a positive integer, "
            "where -1 represents infinity.",
            default_value=default_value,
        )
    return Option(
        (int, type(None)),
        lambda x: x is None or x >= 0,
        f"The keyword '{name}' only accepts None, 0 or a positive integer.",
        default_value=default_value,
    )


def _resource_option(name: str, default_value: Any = None):
    """This is used for non-negative options, typically for defining resources."""
    return Option(
        (float, int, type(None)),
        lambda x: x is None or x >= 0,
        f"The keyword '{name}' only accepts None, 0 or a positive number",
        default_value=default_value,
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
    "resources": Option(
        (dict, type(None)),
        lambda x: x is None or ("CPU" not in x and "GPU" not in x),
        "Use the 'num_cpus' and 'num_gpus' keyword instead of 'CPU' and 'GPU' "
        "in 'resources' keyword",
    ),
    "runtime_env": Option((dict, type(None))),
    "scheduling_strategy": Option(
        (
            type(None),
            str,
            PlacementGroupSchedulingStrategy,
            NodeAffinitySchedulingStrategy,
        )
    ),
    "_metadata": Option((dict, type(None))),
}


_task_only_options = {
    "max_calls": _counting_option("max_calls", False, default_value=0),
    # Normal tasks may be retried on failure this many times.
    # TODO(swang): Allow this to be set globally for an application.
    "max_retries": _counting_option("max_retries", default_value=3),
    # override "_common_options"
    "num_cpus": _resource_option("num_cpus", default_value=1),
    "num_returns": _counting_option("num_returns", False, default_value=1),
    "object_store_memory": Option(  # override "_common_options"
        (int, type(None)),
        lambda x: x is None,
        "Setting 'object_store_memory' is not implemented for tasks",
    ),
    "retry_exceptions": Option(bool, default_value=False),
}

_actor_only_options = {
    "concurrency_groups": Option((list, dict, type(None))),
    "lifetime": Option(
        (str, type(None)),
        lambda x: x in (None, "detached", "non_detached"),
        "actor `lifetime` argument must be one of 'detached', "
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
    # "python/ray/data/impl/remote_fn.py" and many other places,
    # while "ray.data.read_api.read_datasource" set "scheduling_strategy=SPREAD".
    # This might be a bug, but it is also ok to allow them co-exist.
    if (placement_group not in ("default", None)) and (scheduling_strategy is not None):
        raise ValueError(
            "Placement groups should be specified via the "
            "scheduling_strategy option. "
            "The placement_group option is deprecated."
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
    _check_deprecate_placement_group(options)


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
