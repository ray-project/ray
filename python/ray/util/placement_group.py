import warnings
from typing import Dict, List, Optional, Union

import ray
from ray._common.utils import PLACEMENT_GROUP_BUNDLE_RESOURCE_NAME, hex_to_binary
from ray._private.auto_init_hook import auto_init_ray
from ray._private.client_mode_hook import client_mode_should_convert, client_mode_wrap
from ray._private.label_utils import validate_label_selector
from ray._private.utils import get_ray_doc_version
from ray._raylet import PlacementGroupID
from ray.util.annotations import DeveloperAPI, PublicAPI

VALID_PLACEMENT_GROUP_STRATEGIES = {
    "PACK",
    "SPREAD",
    "STRICT_PACK",
    "STRICT_SPREAD",
}

VALID_PLACEMENT_GROUP_FALLBACK_OPTIONS = {"bundles", "bundle_label_selector"}


@PublicAPI
class PlacementGroup:
    """A handle to a placement group."""

    @staticmethod
    def empty() -> "PlacementGroup":
        return PlacementGroup(PlacementGroupID.nil())

    def __init__(
        self,
        id: "ray._raylet.PlacementGroupID",
        bundle_cache: Optional[List[Dict]] = None,
    ):
        self.id = id
        self.bundle_cache = bundle_cache
        self._scheduling_options_cache = None

    @property
    def is_empty(self):
        return self.id.is_nil()

    def ready(self) -> "ray._raylet.ObjectRef":
        """Returns an ObjectRef to check ready status.

        This API returns an ObjectRef that becomes ready when the placement group
        is created. It is compatible with ray.get, ray.wait, and await.

        Example:
            .. testcode::

                import ray

                pg = ray.util.placement_group([{"CPU": 1}])
                ray.get(pg.ready())

                pg = ray.util.placement_group([{"CPU": 1}])
                ray.wait([pg.ready()])

        """
        if self.is_empty:
            return ray.put(self)

        return _call_placement_group_ready_async(self)

    def wait(self, timeout_seconds: Union[float, int] = 30) -> bool:
        """Wait for the placement group to be ready within the specified time.
        Args:
             timeout_seconds(float|int): Timeout in seconds.
        Return:
             True if the placement group is created. False otherwise.
        """
        return _call_placement_group_ready(self.id, timeout_seconds)

    @property
    def bundle_specs(self) -> List[Dict]:
        """List[Dict]: Return bundles belonging to this placement group.

        This returns the primary resource requirements specified at creation.
        """
        self._fill_bundle_cache_if_needed()
        return self.bundle_cache or []

    @property
    def bundle_count(self) -> int:
        """Returns the number of bundles in this placement group."""
        return len(self.bundle_specs)

    def _get_scheduling_options_bundles(self) -> List[List[Dict]]:
        """Return all possible bundles across primary and fallback options
        as a nested list of strategies.
        """
        # If we haven't fetched the static scheduling options yet, fetch them once.
        self._fill_scheduling_options_cache_if_needed()
        return self._scheduling_options_cache or []

    def _fill_bundle_cache_if_needed(self) -> None:
        if self.bundle_cache is None:
            self.bundle_cache = _get_bundle_cache(self.id)

    def _fill_scheduling_options_cache_if_needed(self) -> None:
        if self._scheduling_options_cache is None:
            self._scheduling_options_cache = _get_all_scheduling_options(self.id)

    def __eq__(self, other):
        if not isinstance(other, PlacementGroup):
            return False
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)


@client_mode_wrap
def _call_placement_group_ready_async(pg: PlacementGroup) -> "ray._raylet.ObjectRef":
    worker = ray._private.worker.global_worker
    worker.check_connected()
    # Serialize pg so that ray.get() returns the PlacementGroup
    serialized = worker.get_serialization_context().serialize(pg)
    return worker.core_worker.async_wait_placement_group_ready(pg.id, serialized)


@client_mode_wrap
def _call_placement_group_ready(pg_id: PlacementGroupID, timeout_seconds: int) -> bool:
    worker = ray._private.worker.global_worker
    worker.check_connected()

    return worker.core_worker.wait_placement_group_ready(pg_id, timeout_seconds)


@client_mode_wrap
def _get_bundle_cache(pg_id: PlacementGroupID) -> List[Dict]:
    worker = ray._private.worker.global_worker
    worker.check_connected()

    table = ray._private.state.state.placement_group_table(pg_id)

    # TODO(ryanaoleary): Add a new API for active_bundle_specs to return
    # `table.get("bundles")`, which are the bundles actually scheduled for the PG.
    scheduling_options = table.get("scheduling_options", [])
    if scheduling_options:
        return scheduling_options[0].get("bundles", [])
    return []


@client_mode_wrap
def _get_all_scheduling_options(pg_id: PlacementGroupID) -> List[List[Dict]]:
    worker = ray._private.worker.global_worker
    worker.check_connected()

    table = ray._private.state.state.placement_group_table(pg_id)

    all_options = []
    for strategy in table.get("scheduling_options", []):
        all_options.append(strategy.get("bundles", []))

    return all_options


@PublicAPI
@client_mode_wrap
def placement_group(
    bundles: List[Dict[str, float]],
    strategy: str = "PACK",
    name: str = "",
    lifetime: Optional[str] = None,
    _soft_target_node_id: Optional[str] = None,
    bundle_label_selector: List[Dict[str, str]] = None,
    fallback_strategy: Optional[List[Dict]] = None,
) -> PlacementGroup:
    """Asynchronously creates a PlacementGroup.

    Args:
        bundles: A list of bundles which
            represent the resources requirements.
        strategy: The strategy to create the placement group.

         - "PACK": Packs Bundles into as few nodes as possible.
         - "SPREAD": Places Bundles across distinct nodes as even as possible.
         - "STRICT_PACK": Packs Bundles into one node. The group is
           not allowed to span multiple nodes.
         - "STRICT_SPREAD": Packs Bundles across distinct nodes.

        name: The name of the placement group.
        lifetime: Either `None`, which defaults to the placement group
            will fate share with its creator and will be deleted once its
            creator is dead, or "detached", which means the placement group
            will live as a global object independent of the creator.
        _soft_target_node_id: (Private, Experimental) Soft hint where bundles of
            this placement group should be placed.
            The target node is specified by it's hex ID.
            If the target node has no available resources or died,
            bundles can be placed elsewhere.
            This currently only works with STRICT_PACK pg.
        bundle_label_selector: A list of label selectors to apply to a
            placement group on a per-bundle level.
        fallback_strategy: A list of scheduling option dicts that define the fallback
            options to use when attempting to schedule this placement group. Supported
            options are the bundles and bundle_label_selector to attempt to schedule.

    Raises:
        ValueError: if bundle type is not a list.
        ValueError: if empty bundle or empty resource bundles are given.
        ValueError: if the wrong lifetime arguments are given.

    Return:
        PlacementGroup: Placement group object.
    """
    worker = ray._private.worker.global_worker
    worker.check_connected()

    validate_placement_group(
        bundles=bundles,
        strategy=strategy,
        lifetime=lifetime,
        _soft_target_node_id=_soft_target_node_id,
        bundle_label_selector=bundle_label_selector,
        fallback_strategy=fallback_strategy,
    )

    if bundle_label_selector is None:
        bundle_label_selector = []

    if fallback_strategy is None:
        fallback_strategy = []

    if lifetime == "detached":
        detached = True
    else:
        detached = False

    placement_group_id = worker.core_worker.create_placement_group(
        name,
        bundles,
        strategy,
        detached,
        _soft_target_node_id,
        bundle_label_selector,
        fallback_strategy,
    )

    return PlacementGroup(placement_group_id)


@PublicAPI
@client_mode_wrap
def remove_placement_group(placement_group: PlacementGroup) -> None:
    """Asynchronously remove placement group.

    Args:
        placement_group: The placement group to delete.
    """
    assert placement_group is not None
    worker = ray._private.worker.global_worker
    worker.check_connected()

    worker.core_worker.remove_placement_group(placement_group.id)


@PublicAPI
@client_mode_wrap
def get_placement_group(placement_group_name: str) -> PlacementGroup:
    """Get a placement group object with a global name.

    Returns:
        None if can't find a placement group with the given name.
        The placement group object otherwise.
    """
    if not placement_group_name:
        raise ValueError("Please supply a non-empty value to get_placement_group")
    worker = ray._private.worker.global_worker
    worker.check_connected()
    placement_group_info = ray._private.state.state.get_placement_group_by_name(
        placement_group_name, worker.namespace
    )
    if placement_group_info is None:
        raise ValueError(
            f"Failed to look up placement group with name: {placement_group_name}"
        )
    else:
        return PlacementGroup(
            PlacementGroupID(hex_to_binary(placement_group_info["placement_group_id"]))
        )


@DeveloperAPI
@client_mode_wrap
def placement_group_table(placement_group: PlacementGroup = None) -> dict:
    """Get the state of the placement group from GCS.

    Args:
        placement_group: placement group to see
            states.
    """
    worker = ray._private.worker.global_worker
    worker.check_connected()
    placement_group_id = placement_group.id if (placement_group is not None) else None
    return ray._private.state.state.placement_group_table(placement_group_id)


@PublicAPI
def get_current_placement_group() -> Optional[PlacementGroup]:
    """Get the current placement group which a task or actor is using.

    It returns None if there's no current placement group for the worker.
    For example, if you call this method in your driver, it returns None
    (because drivers never belong to any placement group).

    Examples:
        .. testcode::

            import ray
            from ray.util.placement_group import get_current_placement_group
            from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

            @ray.remote
            def f():
                # This returns the placement group the task f belongs to.
                # It means this pg is identical to the pg created below.
                return get_current_placement_group()

            pg = ray.util.placement_group([{"CPU": 2}])
            assert ray.get(f.options(
                    scheduling_strategy=PlacementGroupSchedulingStrategy(
                        placement_group=pg)).remote()) == pg

            # Driver doesn't belong to any placement group,
            # so it returns None.
            assert get_current_placement_group() is None

    Return:
        PlacementGroup: Placement group object.
            None if the current task or actor wasn't
            created with any placement group.
    """
    auto_init_ray()
    if client_mode_should_convert():
        # Client mode is only a driver.
        return None
    worker = ray._private.worker.global_worker
    worker.check_connected()
    pg_id = worker.placement_group_id
    if pg_id.is_nil():
        return None
    return PlacementGroup(pg_id)


def check_placement_group_index(
    placement_group: PlacementGroup, bundle_index: int
) -> None:
    assert placement_group is not None
    if placement_group.id.is_nil():
        if bundle_index != -1:
            raise ValueError(
                "If placement group is not set, "
                "the value of bundle index must be -1."
            )
    else:
        # Fetch the updated scheduling options first.
        strategies = placement_group._get_scheduling_options_bundles()

        valid_count = max((len(strategy) for strategy in strategies), default=0)

        if bundle_index >= valid_count or bundle_index < -1:
            raise ValueError(
                f"placement group bundle index {bundle_index} "
                f"is invalid. Valid placement group indexes: "
                f"0 to {valid_count - 1}."
            )


def validate_placement_group(
    bundles: List[Dict[str, float]],
    strategy: str = "PACK",
    lifetime: Optional[str] = None,
    _soft_target_node_id: Optional[str] = None,
    bundle_label_selector: List[Dict[str, str]] = None,
    fallback_strategy: Optional[List[Dict]] = None,
) -> bool:
    """Validates inputs for placement_group.

    Raises ValueError if inputs are invalid.
    """
    if _soft_target_node_id and strategy != "STRICT_PACK":
        raise ValueError(
            "_soft_target_node_id currently only works "
            f"with STRICT_PACK but got {strategy}"
        )

    if _soft_target_node_id and ray.NodeID.from_hex(_soft_target_node_id).is_nil():
        raise ValueError(
            f"Invalid hex ID of _soft_target_node_id, got {_soft_target_node_id}"
        )

    _validate_bundles(bundles)

    if bundle_label_selector is not None:
        if len(bundles) != len(bundle_label_selector):
            raise ValueError(
                f"Invalid bundle label selector {bundle_label_selector}. "
                f"The length of `bundle_label_selector` should equal the length of `bundles`."
            )
        _validate_bundle_label_selector(bundle_label_selector)

    if fallback_strategy is not None:
        _validate_fallback_strategy(fallback_strategy)

    if strategy not in VALID_PLACEMENT_GROUP_STRATEGIES:
        raise ValueError(
            f"Invalid placement group strategy {strategy}. "
            f"Supported strategies are: {VALID_PLACEMENT_GROUP_STRATEGIES}."
        )

    if lifetime not in [None, "detached"]:
        raise ValueError(
            "Placement group `lifetime` argument must be either `None` or "
            f"'detached'. Got {lifetime}."
        )


def _validate_bundles(bundles: List[Dict[str, float]]):
    """Validates each bundle and raises a ValueError if any bundle is invalid."""

    if not isinstance(bundles, list):
        raise ValueError(
            "Placement group bundles must be a list, " f"got {type(bundles)}."
        )

    if len(bundles) == 0:
        raise ValueError(
            "Bundles must be a non-empty list of resource "
            'dictionaries. For example: `[{"CPU": 1.0}, {"GPU": 1.0}]`. '
            "Got empty list instead."
        )

    for bundle in bundles:
        if (
            not isinstance(bundle, dict)
            or not all(isinstance(k, str) for k in bundle.keys())
            or not all(isinstance(v, (int, float)) for v in bundle.values())
        ):
            raise ValueError(
                "Bundles must be a non-empty list of "
                "resource dictionaries. For example: "
                '`[{"CPU": 1.0}, {"GPU": 1.0}]`.'
            )

        if len(bundle) == 0 or all(
            resource_value == 0 for resource_value in bundle.values()
        ):
            raise ValueError(
                "Bundles cannot be an empty dictionary or "
                f"resources with only 0 values. Bundles: {bundles}"
            )

        if "object_store_memory" in bundle.keys():
            warnings.warn(
                "Setting 'object_store_memory' for"
                " bundles is deprecated since it doesn't actually"
                " reserve the required object store memory."
                f" Use object spilling that's enabled by default (https://docs.ray.io/en/{get_ray_doc_version()}/ray-core/objects/object-spilling.html) "  # noqa: E501
                "instead to bypass the object store memory size limitation.",
                DeprecationWarning,
                stacklevel=1,
            )


def _validate_bundle_label_selector(bundle_label_selector: List[Dict[str, str]]):
    """Validates each label selector and raises a ValueError if any label selector is invalid."""

    if not isinstance(bundle_label_selector, list):
        raise ValueError(
            "Placement group bundle_label_selector must be a list, "
            f"got {type(bundle_label_selector)}."
        )

    if len(bundle_label_selector) == 0:
        # No label selectors provided, no-op.
        return

    for label_selector in bundle_label_selector:
        if (
            not isinstance(label_selector, dict)
            or not all(isinstance(k, str) for k in label_selector.keys())
            or not all(isinstance(v, str) for v in label_selector.values())
        ):
            raise ValueError(
                "Bundle label selector must be a list of string dictionary"
                " label selectors. For example: "
                '`[{ray.io/market_type": "spot"}, {"ray.io/accelerator-type": "A100"}]`.'
            )
        # Call helper function to validate label selector key-value syntax.
        error_message = validate_label_selector(label_selector)
        if error_message:
            raise ValueError(
                f"Invalid label selector provided in bundle_label_selector list."
                f" Detailed error: '{error_message}'"
            )


def _validate_fallback_strategy(fallback_strategy: List[Dict]):
    """Validates the placement group fallback strategy."""
    if not isinstance(fallback_strategy, list):
        raise ValueError(
            f"fallback_strategy must be a list, got {type(fallback_strategy)}."
        )

    for i, option in enumerate(fallback_strategy):
        if not isinstance(option, dict):
            raise ValueError(
                f"fallback_strategy[{i}] must be a dict, got {type(option)}."
            )

        # Validate placement group fallback options.
        if "bundles" not in option:
            raise ValueError(f"fallback_strategy[{i}] must contain 'bundles'.")

        _validate_bundles(option["bundles"])

        if "bundle_label_selector" in option:
            fallback_labels = option["bundle_label_selector"]

            if len(option["bundles"]) != len(fallback_labels):
                raise ValueError(
                    f"In fallback_strategy[{i}], length of `bundle_label_selector` "
                    f"must equal length of `bundles`."
                )

            _validate_bundle_label_selector(fallback_labels)

        # Check that fallback strategy only specifies supported options.
        invalid_options = set(option.keys()) - VALID_PLACEMENT_GROUP_FALLBACK_OPTIONS
        if invalid_options:
            raise ValueError(
                f"fallback_strategy[{i}] contains invalid options: {invalid_options}. "
                f"Supported options are: {VALID_PLACEMENT_GROUP_FALLBACK_OPTIONS}"
            )


def _valid_resource_shape(resources, bundle_specs):
    """
    If the resource shape cannot fit into every
    bundle spec, return False
    """
    for bundle in bundle_specs:
        fit_in_bundle = True
        for resource, requested_val in resources.items():
            # Skip "bundle" resource as it is automatically added
            # to all nodes with bundles by the placement group.
            if resource == PLACEMENT_GROUP_BUNDLE_RESOURCE_NAME:
                continue
            if bundle.get(resource, 0) < requested_val:
                fit_in_bundle = False
                break
        if fit_in_bundle:
            # If resource request fits in any bundle, it is valid.
            return True
    return False


def _validate_resource_shape(
    placement_group, resources, placement_resources, task_or_actor_repr
):
    strategies = placement_group._get_scheduling_options_bundles()
    resources_valid = any(
        _valid_resource_shape(resources, strategy) for strategy in strategies
    )
    placement_resources_valid = any(
        _valid_resource_shape(placement_resources, strategy) for strategy in strategies
    )

    if not resources_valid:
        raise ValueError(
            f"Cannot schedule {task_or_actor_repr} with "
            "the placement group because the resource request "
            f"{resources} cannot fit into any bundles across all scheduling strategies for "
            f"the placement group, {strategies}."
        )
    if not placement_resources_valid:
        # Happens for the default actor case.
        # placement_resources is not an exposed concept to users,
        # so we should write more specialized error messages.
        raise ValueError(
            f"Cannot schedule {task_or_actor_repr} with "
            "the placement group because the actor requires "
            f"{placement_resources.get('CPU', 0)} CPU for "
            "creation, but it cannot "
            f"fit into any bundles across all scheduling strategies for the placement group, "
            f"{strategies}. Consider "
            "creating a placement group with CPU resources."
        )


def _configure_placement_group_based_on_context(
    placement_group_capture_child_tasks: bool,
    bundle_index: int,
    resources: Dict,
    placement_resources: Dict,
    task_or_actor_repr: str,
    placement_group: Union[PlacementGroup, str, None] = "default",
) -> PlacementGroup:
    """Configure the placement group based on the given context.

    Based on the given context, this API returns the placement group instance
    for task/actor scheduling.

    Params:
        placement_group_capture_child_tasks: Whether or not the
            placement group needs to be captured from the global
            context.
        bundle_index: The bundle index for tasks/actor scheduling.
        resources: The scheduling resources.
        placement_resources: The scheduling placement resources for
            actors.
        task_or_actor_repr: The repr of task or actor
            function/class descriptor.
        placement_group: The placement group instance.
            - "default": Default placement group argument. Currently,
                the default behavior is to capture the parent task'
                placement group if placement_group_capture_child_tasks
                is set.
            - None: means placement group is explicitly not configured.
            - Placement group instance: In this case, do nothing.

    Returns:
        Placement group instance based on the given context.

    Raises:
        ValueError: If the bundle index is invalid for the placement group
            or the requested resources shape doesn't fit to any
            bundles.
    """
    # Validate inputs.
    assert placement_group_capture_child_tasks is not None
    assert resources is not None

    # Validate and get the PlacementGroup instance.
    # Placement group could be None, default, or placement group.
    # Default behavior is "do not capture child tasks".
    if placement_group != "default":
        if not placement_group:
            placement_group = PlacementGroup.empty()
    elif placement_group == "default":
        if placement_group_capture_child_tasks:
            placement_group = get_current_placement_group()
        else:
            placement_group = PlacementGroup.empty()

    if not placement_group:
        placement_group = PlacementGroup.empty()
    assert isinstance(placement_group, PlacementGroup)

    # Validate the index.
    check_placement_group_index(placement_group, bundle_index)

    # Validate the shape.
    if not placement_group.is_empty:
        _validate_resource_shape(
            placement_group, resources, placement_resources, task_or_actor_repr
        )
    return placement_group
