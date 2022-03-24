from typing import Dict, Union, List, Optional

import ray
from ray._raylet import ObjectRef
from ray._raylet import PlacementGroupID
from ray._private.utils import hex_to_binary
from ray.util.annotations import PublicAPI, DeveloperAPI
from ray.ray_constants import to_memory_units
from ray._private.client_mode_hook import client_mode_should_convert
from ray._private.client_mode_hook import client_mode_wrap

bundle_reservation_check = None
BUNDLE_RESOURCE_LABEL = "bundle"


# We need to import this method to use for ready API.
# But ray.remote is only available in runtime, and
# if we define this method inside ready method, this function is
# exported whenever ready is called, which can impact performance,
# https://github.com/ray-project/ray/issues/6240.
def _export_bundle_reservation_check_method_if_needed():
    global bundle_reservation_check
    if bundle_reservation_check:
        return

    @ray.remote(num_cpus=0)
    def bundle_reservation_check_func(placement_group):
        return placement_group

    bundle_reservation_check = bundle_reservation_check_func


@PublicAPI
class PlacementGroup:
    """A handle to a placement group."""

    @staticmethod
    def empty() -> "PlacementGroup":
        return PlacementGroup(PlacementGroupID.nil())

    def __init__(self, id: PlacementGroupID, bundle_cache: Optional[List[Dict]] = None):
        self.id = id
        self.bundle_cache = bundle_cache

    @property
    def is_empty(self):
        return self.id.is_nil()

    def ready(self) -> ObjectRef:
        """Returns an ObjectRef to check ready status.

        This API runs a small dummy task to wait for placement group creation.
        It is compatible to ray.get and ray.wait.

        Example:

        >>> pg = placement_group([{"CPU": 1}])
            ray.get(pg.ready())

        >>> pg = placement_group([{"CPU": 1}])
            ray.wait([pg.ready()], timeout=0)
        """
        self._fill_bundle_cache_if_needed()

        _export_bundle_reservation_check_method_if_needed()

        assert len(self.bundle_cache) != 0, (
            "ready() cannot be called on placement group object with a "
            "bundle length == 0, current bundle length: "
            f"{len(self.bundle_cache)}"
        )

        return bundle_reservation_check.options(
            placement_group=self, resources={BUNDLE_RESOURCE_LABEL: 0.001}
        ).remote(self)

    def wait(self, timeout_seconds: Union[float, int]) -> bool:
        """Wait for the placement group to be ready within the specified time.
        Args:
             timeout_seconds(float|int): Timeout in seconds.
        Return:
             True if the placement group is created. False otherwise.
        """
        return _call_placement_group_ready(self.id, timeout_seconds)

    @property
    def bundle_specs(self) -> List[Dict]:
        """List[Dict]: Return bundles belonging to this placement group."""
        self._fill_bundle_cache_if_needed()
        return self.bundle_cache

    @property
    def bundle_count(self) -> int:
        self._fill_bundle_cache_if_needed()
        return len(self.bundle_cache)

    def _fill_bundle_cache_if_needed(self) -> None:
        if not self.bundle_cache:
            self.bundle_cache = _get_bundle_cache(self.id)


@client_mode_wrap
def _call_placement_group_ready(pg_id: PlacementGroupID, timeout_seconds: int) -> bool:
    worker = ray.worker.global_worker
    worker.check_connected()

    return worker.core_worker.wait_placement_group_ready(pg_id, timeout_seconds)


@client_mode_wrap
def _get_bundle_cache(pg_id: PlacementGroupID) -> List[Dict]:
    worker = ray.worker.global_worker
    worker.check_connected()

    return list(ray.state.state.placement_group_table(pg_id)["bundles"].values())


@PublicAPI
@client_mode_wrap
def placement_group(
    bundles: List[Dict[str, float]],
    strategy: str = "PACK",
    name: str = "",
    lifetime=None,
) -> PlacementGroup:
    """Asynchronously creates a PlacementGroup.

    Args:
        bundles(List[Dict]): A list of bundles which
            represent the resources requirements.
        strategy(str): The strategy to create the placement group.

         - "PACK": Packs Bundles into as few nodes as possible.
         - "SPREAD": Places Bundles across distinct nodes as even as possible.
         - "STRICT_PACK": Packs Bundles into one node. The group is
           not allowed to span multiple nodes.
         - "STRICT_SPREAD": Packs Bundles across distinct nodes.

        name(str): The name of the placement group.
        lifetime(str): Either `None`, which defaults to the placement group
            will fate share with its creator and will be deleted once its
            creator is dead, or "detached", which means the placement group
            will live as a global object independent of the creator.

    Raises:
        ValueError if bundle type is not a list.
        ValueError if empty bundle or empty resource bundles are given.
        ValueError if the wrong lifetime arguments are given.

    Return:
        PlacementGroup: Placement group object.
    """
    worker = ray.worker.global_worker
    worker.check_connected()

    if not isinstance(bundles, list):
        raise ValueError("The type of bundles must be list, got {}".format(bundles))

    # Validate bundles
    for bundle in bundles:
        if len(bundle) == 0 or all(
            resource_value == 0 for resource_value in bundle.values()
        ):
            raise ValueError(
                "Bundles cannot be an empty dictionary or "
                f"resources with only 0 values. Bundles: {bundles}"
            )

        if "memory" in bundle.keys() and bundle["memory"] > 0:
            # Make sure the memory resource can be
            # transformed to memory unit.
            to_memory_units(bundle["memory"], True)

    if lifetime is None:
        detached = False
    elif lifetime == "detached":
        detached = True
    else:
        raise ValueError(
            "placement group `lifetime` argument must be either `None` or 'detached'"
        )

    placement_group_id = worker.core_worker.create_placement_group(
        name, bundles, strategy, detached
    )

    return PlacementGroup(placement_group_id)


@PublicAPI
@client_mode_wrap
def remove_placement_group(placement_group: PlacementGroup) -> None:
    """Asynchronously remove placement group.

    Args:
        placement_group (PlacementGroup): The placement group to delete.
    """
    assert placement_group is not None
    worker = ray.worker.global_worker
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
    worker = ray.worker.global_worker
    worker.check_connected()
    placement_group_info = ray.state.state.get_placement_group_by_name(
        placement_group_name, worker.namespace
    )
    if placement_group_info is None:
        raise ValueError(f"Failed to look up actor with name: {placement_group_name}")
    else:
        return PlacementGroup(
            PlacementGroupID(hex_to_binary(placement_group_info["placement_group_id"]))
        )


@DeveloperAPI
@client_mode_wrap
def placement_group_table(placement_group: PlacementGroup = None) -> dict:
    """Get the state of the placement group from GCS.

    Args:
        placement_group (PlacementGroup): placement group to see
            states.
    """
    worker = ray.worker.global_worker
    worker.check_connected()
    placement_group_id = placement_group.id if (placement_group is not None) else None
    return ray.state.state.placement_group_table(placement_group_id)


@PublicAPI
def get_current_placement_group() -> Optional[PlacementGroup]:
    """Get the current placement group which a task or actor is using.

    It returns None if there's no current placement group for the worker.
    For example, if you call this method in your driver, it returns None
    (because drivers never belong to any placement group).

    Examples:

        >>> @ray.remote
        >>> def f():
        >>>     # This will return the placement group the task f belongs to.
        >>>     # It means this pg will be identical to the pg created below.
        >>>     pg = get_current_placement_group()
        >>> pg = placement_group([{"CPU": 2}])
        >>> f.options(placement_group=pg).remote()

        >>> # New script.
        >>> ray.init()
        >>> # New script doesn't belong to any placement group,
        >>> # so it returns None.
        >>> assert get_current_placement_group() is None

    Return:
        PlacementGroup: Placement group object.
            None if the current task or actor wasn't
            created with any placement group.
    """
    if client_mode_should_convert(auto_init=True):
        # Client mode is only a driver.
        return None
    worker = ray.worker.global_worker
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
    elif bundle_index >= placement_group.bundle_count or bundle_index < -1:
        raise ValueError(
            f"placement group bundle index {bundle_index} "
            f"is invalid. Valid placement group indexes: "
            f"0-{placement_group.bundle_count}"
        )


def _validate_resource_shape(
    placement_group, resources, placement_resources, task_or_actor_repr
):
    def valid_resource_shape(resources, bundle_specs):
        """
        If the resource shape cannot fit into every
        bundle spec, return False
        """
        for bundle in bundle_specs:
            fit_in_bundle = True
            for resource, requested_val in resources.items():
                # Skip "bundle" resource as it is automatically added
                # to all nodes with bundles by the placement group.
                if resource == BUNDLE_RESOURCE_LABEL:
                    continue
                if bundle.get(resource, 0) < requested_val:
                    fit_in_bundle = False
                    break
            if fit_in_bundle:
                # If resource request fits in any bundle, it is valid.
                return True
        return False

    bundles = placement_group.bundle_specs
    resources_valid = valid_resource_shape(resources, bundles)
    placement_resources_valid = valid_resource_shape(placement_resources, bundles)

    if not resources_valid:
        raise ValueError(
            f"Cannot schedule {task_or_actor_repr} with "
            "the placement group because the resource request "
            f"{resources} cannot fit into any bundles for "
            f"the placement group, {bundles}."
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
            f"fit into any bundles for the placement group, "
            f"{bundles}. Consider "
            "creating a placement group with CPU resources."
        )


def configure_placement_group_based_on_context(
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
