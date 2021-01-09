import time

from typing import (List, Dict, Optional)

class ClientPlacementGroup:
    """A handle to a placement group."""

    @staticmethod
    def empty():
        return ClientPlacementGroup(b"")

    def __init__(self, id: bytes):
        self.id = id
        self.bundle_cache = None

    def ready(self) -> ClientObjectRef:
        """Returns an ObjectRef to check ready status.

        This API runs a small dummy task to wait for placement group creation.
        It is compatible to ray.get and ray.wait.

        Example:

        >>> pg = placement_group([{"CPU": 1}])
            ray.get(pg.ready())

        >>> pg = placement_group([{"CPU": 1}])
            ray.wait([pg.ready()], timeout=0)
        """
        # XXX Stub: run the same dummy task on the server side
        pass

    def wait(self, timeout_seconds: int) -> bool:
        """Wait for the placement group to be ready within the specified time.
        Args:
             timeout_seconds(str): Timeout in seconds.
        Return:
             True if the placement group is created. False otherwise.
        """
        # XXX Stub: Wrap and call based on ID
        pass

    @property
    def bundle_specs(self) -> List[Dict]:
        """Return bundles belonging to this placement group."""
        self._fill_bundle_cache_if_needed()
        return self.bundle_cache

    @property
    def bundle_count(self) -> int:
        self._fill_bundle_cache_if_needed()
        return len(self.bundle_cache)

    def _fill_bundle_cache_if_needed(self):
        if not self.bundle_cache:
            # Since creating placement group is async, it is
            # possible table is not ready yet. To avoid the
            # problem, we should keep trying with timeout.

            TIMEOUT_SECOND = 30
            WAIT_INTERVAL = 0.05
            timeout_cnt = 0

            while timeout_cnt < int(TIMEOUT_SECOND / WAIT_INTERVAL):
                # XXX STUB: Repeatedly call the metadata call
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
    # XXX Stub: Remote function create by ID
    # placement_group_id = worker.core_worker.create_placement_group(
        # name, bundles, strategy)
    # return PlacementGroup(placement_group_id)
    return ClientPlacementGroup()


def remove_placement_group(placement_group: ClientPlacementGroup):
    """Asynchronously remove placement group.

    Args:
        placement_group (PlacementGroup): The placement group to delete.
    """
    assert placement_group is not None
    # XXX Stub call remotely
    pass


def placement_group_table(placement_group: PlacementGroup = None) -> list:
    """Get the state of the placement group from GCS.

    Args:
        placement_group (PlacementGroup): placement group to see
            states.
    """
    # XXX Add to the metadata RPC
    pass


# TODO(barakmich): Hook the real get_current_placement_group to check for client mode explicitly.
from ray.util.placement_group import get_current_placement_group

# XXX Stub: Break into a helpers file and call the upstream implementation from remote.options
from ray.util.placement_group import check_placement_group_index
