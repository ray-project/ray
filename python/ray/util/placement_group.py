import time

from typing import (List, Dict, Optional, Union)

import ray
from ray._raylet import PlacementGroupID, ObjectRef

bundle_reservation_check = None


# We need to import this method to use for ready API.
# But ray.remote is only available in runtime, and
# if we define this method inside ready method, this function is
# exported whenever ready is called, which can impact performance,
# https://github.com/ray-project/ray/issues/6240.
def _export_bundle_reservation_check_method_if_needed():
    global bundle_reservation_check
    if bundle_reservation_check:
        return

    @ray.remote(num_cpus=0, max_calls=0)
    def bundle_reservation_check_func(placement_group):
        return placement_group

    bundle_reservation_check = bundle_reservation_check_func


class PlacementGroup:
    """A handle to a placement group."""

    @staticmethod
    def empty():
        return PlacementGroup(PlacementGroupID.nil())

    def __init__(self, id: PlacementGroupID):
        self.id = id
        self.bundle_cache = None

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
            f"{len(self.bundle_cache)}")

        # Select the first bundle to schedule a dummy task.
        # Since the placement group creation will be atomic, it is sufficient
        # to schedule a single task.
        bundle_index = 0
        bundle = self.bundle_cache[bundle_index]

        resource_name, value = self._get_none_zero_resource(bundle)
        num_cpus = 0
        num_gpus = 0
        resources = {}
        if resource_name == "CPU":
            num_cpus = value
        elif resource_name == "GPU":
            num_gpus = value
        else:
            resources[resource_name] = value

        return bundle_reservation_check.options(
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            placement_group=self,
            placement_group_bundle_index=bundle_index,
            resources=resources).remote(self)

    def wait(self, timeout_seconds: Union[float, int]) -> bool:
        """Wait for the placement group to be ready within the specified time.
        Args:
             timeout_seconds(float|int): Timeout in seconds.
        Return:
             True if the placement group is created. False otherwise.
        """
        worker = ray.worker.global_worker
        worker.check_connected()

        return worker.core_worker.wait_placement_group_ready(
            self.id, timeout_seconds)

    @property
    def bundle_specs(self) -> List[Dict]:
        """List[Dict]: Return bundles belonging to this placement group."""
        self._fill_bundle_cache_if_needed()
        return self.bundle_cache

    @property
    def bundle_count(self):
        self._fill_bundle_cache_if_needed()
        return len(self.bundle_cache)

    def _get_none_zero_resource(self, bundle: List[Dict]):
        # This number shouldn't be changed.
        # When it is specified, node manager won't warn about infeasible
        # tasks.
        INFEASIBLE_TASK_SUPPRESS_MAGIC_NUMBER = 0.0101
        for key, value in bundle.items():
            if value > 0:
                value = INFEASIBLE_TASK_SUPPRESS_MAGIC_NUMBER
                return key, value
        assert False, "This code should be unreachable."

    def _fill_bundle_cache_if_needed(self):
        if not self.bundle_cache:
            # Since creating placement group is async, it is
            # possible table is not ready yet. To avoid the
            # problem, we should keep trying with timeout.
            TIMEOUT_SECOND = 30
            WAIT_INTERVAL = 0.05
            timeout_cnt = 0
            worker = ray.worker.global_worker
            worker.check_connected()

            while timeout_cnt < int(TIMEOUT_SECOND / WAIT_INTERVAL):
                pg_info = ray.state.state.placement_group_table(self.id)
                if pg_info:
                    self.bundle_cache = list(pg_info["bundles"].values())
                    return
                time.sleep(WAIT_INTERVAL)
                timeout_cnt += 1

            raise RuntimeError(
                "Couldn't get the bundle information of placement group id "
                f"{self.id} in {TIMEOUT_SECOND} seconds. It is likely "
                "because GCS server is too busy.")


def placement_group(bundles: List[Dict[str, float]],
                    strategy: str = "PACK",
                    name: str = "unnamed_group") -> PlacementGroup:
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

    Return:
        PlacementGroup: Placement group object.
    """
    worker = ray.worker.global_worker
    worker.check_connected()

    if not isinstance(bundles, list):
        raise ValueError(
            "The type of bundles must be list, got {}".format(bundles))

    # Validate bundles
    for bundle in bundles:
        if (len(bundle) == 0 or all(resource_value == 0
                                    for resource_value in bundle.values())):
            raise ValueError(
                "Bundles cannot be an empty dictionary or "
                f"resources with only 0 values. Bundles: {bundles}")

    placement_group_id = worker.core_worker.create_placement_group(
        name, bundles, strategy)

    return PlacementGroup(placement_group_id)


def remove_placement_group(placement_group: PlacementGroup):
    """Asynchronously remove placement group.

    Args:
        placement_group (PlacementGroup): The placement group to delete.
    """
    assert placement_group is not None
    worker = ray.worker.global_worker
    worker.check_connected()

    worker.core_worker.remove_placement_group(placement_group.id)


def placement_group_table(placement_group: PlacementGroup = None) -> list:
    """Get the state of the placement group from GCS.

    Args:
        placement_group (PlacementGroup): placement group to see
            states.
    """
    worker = ray.worker.global_worker
    worker.check_connected()
    placement_group_id = placement_group.id if (placement_group is
                                                not None) else None
    return ray.state.state.placement_group_table(placement_group_id)


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
    worker = ray.worker.global_worker
    worker.check_connected()
    pg_id = worker.placement_group_id
    if pg_id.is_nil():
        return None
    return PlacementGroup(pg_id)


def check_placement_group_index(placement_group: PlacementGroup,
                                bundle_index: int):
    assert placement_group is not None
    if placement_group.id.is_nil():
        if bundle_index != -1:
            raise ValueError("If placement group is not set, "
                             "the value of bundle index must be -1.")
    elif bundle_index >= placement_group.bundle_count \
            or bundle_index < -1:
        raise ValueError(f"placement group bundle index {bundle_index} "
                         f"is invalid. Valid placement group indexes: "
                         f"0-{placement_group.bundle_count}")
