import json
import logging
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import ray
from ray._raylet import PlacementGroupID
from ray.experimental.internal_kv import (
    _internal_kv_del,
    _internal_kv_get,
    _internal_kv_list,
    _internal_kv_put,
)
from ray.train.v2._internal.state.util import is_actor_alive
from ray.util.placement_group import PlacementGroup, remove_placement_group

logger = logging.getLogger(__name__)


PLACEMENT_GROUP_CLEANER_NAME = "train_v2_placement_group_cleaner_v1"
PLACEMENT_GROUP_CLEANER_NAMESPACE = "_train_placement_group_cleaner"
_CONTROLLER_STATE_KV_NAMESPACE = b"train_v2_placement_group_cleaner_v1"
_CONTROLLER_STATE_KEY_PREFIX = b"controller:"
_MISSING_CONTROLLER_CHECK_THRESHOLD = 3


@dataclass
class _ControllerState:
    placement_groups: Dict[str, PlacementGroup] = field(default_factory=dict)
    cleaning: bool = False


class PlacementGroupCleaner:
    """Shared detached helper that cleans up PGs for failed Train controllers.

    One instance is shared by all Train v2 controllers in a cluster. Controllers
    register their placement groups and unregister themselves during graceful
    shutdown. A background thread removes the placement groups belonging to any
    controller that exits ungracefully.

    This actor should be created with lifetime="detached" so it is not
    fate-shared with any individual Train controller.
    """

    def __init__(
        self,
        check_interval_s: float,
        get_actor_timeout_s: float,
        stop_timeout: Optional[float],
    ):
        self._check_interval_s = check_interval_s
        self._get_actor_timeout_s = get_actor_timeout_s
        self._stop_timeout = stop_timeout
        # State API auto-discovery is ambiguous when multiple clusters are active.
        self._gcs_address = ray.get_runtime_context().gcs_address
        self._controller_states: Dict[
            str, _ControllerState
        ] = self._load_controller_states()
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None
        self._missing_controller_checks: Dict[str, int] = {}
        self._exiting = False
        if self._controller_states:
            self.start_monitoring()

    def register_controller(self, controller_actor_id: str) -> bool:
        """Register a controller. Returns False if it is already known to be dead."""
        with self._lock:
            state = self._controller_states.get(controller_actor_id)
            if state is not None and state.cleaning:
                return False
            if state is None:
                state = _ControllerState()
                self._controller_states[controller_actor_id] = state
                try:
                    self._persist_controller_state(controller_actor_id, state)
                except Exception:
                    self._controller_states.pop(controller_actor_id, None)
                    raise
        return True

    def unregister_controller(self, controller_actor_id: str):
        """Forget a controller without cleaning its placement groups."""
        with self._lock:
            self._delete_controller_state(controller_actor_id)
            self._controller_states.pop(controller_actor_id, None)

    def unregister_placement_group(
        self, controller_actor_id: str, placement_group: PlacementGroup
    ):
        """Forget a placement group after its normal shutdown completes."""
        with self._lock:
            state = self._controller_states.get(controller_actor_id)
            if state is None:
                self._missing_controller_checks.pop(controller_actor_id, None)
                return
            placement_group_id = placement_group.id.hex()
            removed_placement_group = state.placement_groups.pop(
                placement_group_id, None
            )
            if removed_placement_group is None:
                return
            try:
                self._persist_controller_state(controller_actor_id, state)
            except Exception:
                state.placement_groups[placement_group_id] = removed_placement_group
                raise

    def register_placement_group(
        self, controller_actor_id: str, placement_group: PlacementGroup
    ) -> bool:
        """Register a placement group owned by a controller.

        Returns False if the controller has already been observed dead. The
        cleaner retains responsibility for removing a rejected placement group.
        """
        with self._lock:
            state = self._controller_states.get(controller_actor_id)
            controller_is_dead = state is None or state.cleaning
            created_state = state is None
            if state is None:
                state = _ControllerState(cleaning=True)
                self._controller_states[controller_actor_id] = state
            placement_group_id = placement_group.id.hex()
            previous = state.placement_groups.get(placement_group_id)
            state.placement_groups[placement_group_id] = placement_group
            try:
                self._persist_controller_state(controller_actor_id, state)
            except Exception:
                if created_state:
                    self._controller_states.pop(controller_actor_id, None)
                elif previous is None:
                    state.placement_groups.pop(placement_group_id, None)
                else:
                    state.placement_groups[placement_group_id] = previous
                raise

        logger.debug(
            "PlacementGroupCleaner registered placement group %s for controller %s",
            placement_group.id,
            controller_actor_id,
        )
        return not controller_is_dead

    def start_monitoring(self):
        """Start the shared controller-monitoring thread idempotently."""
        if self._monitor_thread is not None and self._monitor_thread.is_alive():
            logger.debug("Monitor thread already running")
            return True

        self._stop_event.clear()
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name="PlacementGroupCleanerMonitor",
            daemon=True,
        )
        self._monitor_thread.start()
        logger.debug("PlacementGroupCleaner started monitoring in background thread")
        return True

    def _monitor_loop(self):
        """Monitor all registered controllers until stop() is called."""
        while not self._stop_event.wait(self._check_interval_s):
            with self._lock:
                controller_states = [
                    (controller_actor_id, state.cleaning)
                    for controller_actor_id, state in self._controller_states.items()
                ]

            for controller_actor_id, cleaning in controller_states:
                if self._stop_event.is_set():
                    break
                if not cleaning:
                    try:
                        alive = is_actor_alive(
                            actor_id=controller_actor_id,
                            timeout=self._get_actor_timeout_s,
                            address=self._gcs_address,
                        )
                    except (
                        ray.util.state.exception.RayStateApiException,
                        ConnectionError,
                    ):
                        logger.warning(
                            "Failed to query Ray Train Controller actor %s state. "
                            "State API may be temporarily unavailable. Continuing "
                            "to monitor other controllers.",
                            controller_actor_id,
                        )
                        continue
                    except Exception:
                        logger.exception(
                            "Unexpected error while querying Ray Train Controller "
                            "%s state. Continuing to monitor other controllers.",
                            controller_actor_id,
                        )
                        continue
                    if alive is None:
                        missing_checks = (
                            self._missing_controller_checks.get(controller_actor_id, 0)
                            + 1
                        )
                        self._missing_controller_checks[
                            controller_actor_id
                        ] = missing_checks
                        if missing_checks < _MISSING_CONTROLLER_CHECK_THRESHOLD:
                            logger.warning(
                                "Controller %s is missing from the State API "
                                "(%s/%s checks). Retrying before cleanup.",
                                controller_actor_id,
                                missing_checks,
                                _MISSING_CONTROLLER_CHECK_THRESHOLD,
                            )
                            continue
                        logger.warning(
                            "Controller %s is missing from the State API for "
                            "%s consecutive checks. Treating it as dead.",
                            controller_actor_id,
                            missing_checks,
                        )
                    else:
                        self._missing_controller_checks.pop(controller_actor_id, None)
                    if alive:
                        continue

                try:
                    placement_groups = self._mark_controller_dead(controller_actor_id)
                except Exception:
                    logger.exception(
                        "Failed to persist cleanup state for controller %s. "
                        "Will retry without affecting other controllers.",
                        controller_actor_id,
                    )
                    continue

                cleaned_placement_groups = []
                for placement_group in placement_groups:
                    if self._cleanup_placement_group(
                        controller_actor_id, placement_group
                    ):
                        cleaned_placement_groups.append(placement_group)
                if cleaned_placement_groups or not placement_groups:
                    try:
                        self._finish_controller_cleanup(
                            controller_actor_id, cleaned_placement_groups
                        )
                    except Exception:
                        logger.exception(
                            "Failed to update persisted cleanup state for controller "
                            "%s. Will retry without affecting other controllers.",
                            controller_actor_id,
                        )

        self._monitor_thread = None

    def _mark_controller_dead(self, controller_actor_id: str) -> List[PlacementGroup]:
        """Durably mark a controller for cleanup and return its placement groups."""
        with self._lock:
            state = self._controller_states.get(controller_actor_id)
            if state is None:
                return []
            if not state.cleaning:
                state.cleaning = True
                try:
                    self._persist_controller_state(controller_actor_id, state)
                except Exception:
                    state.cleaning = False
                    raise
            return list(state.placement_groups.values())

    def _finish_controller_cleanup(
        self,
        controller_actor_id: str,
        cleaned_placement_groups: List[PlacementGroup],
    ):
        with self._lock:
            state = self._controller_states.get(controller_actor_id)
            if state is None:
                return
            remaining_placement_groups = state.placement_groups.copy()
            for placement_group in cleaned_placement_groups:
                remaining_placement_groups.pop(placement_group.id.hex(), None)
            if remaining_placement_groups:
                updated_state = _ControllerState(
                    placement_groups=remaining_placement_groups,
                    cleaning=True,
                )
                self._persist_controller_state(controller_actor_id, updated_state)
                self._controller_states[controller_actor_id] = updated_state
            else:
                self._delete_controller_state(controller_actor_id)
                self._controller_states.pop(controller_actor_id, None)
                self._missing_controller_checks.pop(controller_actor_id, None)

    def _cleanup_placement_group(
        self, controller_actor_id: str, placement_group: PlacementGroup
    ) -> bool:
        """Clean up a placement group if it has not already been removed."""
        if self._is_placement_group_removed(placement_group):
            logger.debug(
                "Controller actor died but placement group already removed; "
                "skipping cleanup."
            )
            return True

        logger.warning(
            "Detected that Ray Train controller actor %s is dead. Cleaning up "
            "placement group [%s] created by this run.",
            controller_actor_id,
            placement_group.id,
        )
        try:
            remove_placement_group(placement_group)
        except Exception as e:
            logger.warning("Failed to clean up placement group: %s", e)
            return False

        logger.debug("Placement group [%s] cleaned up successfully", placement_group.id)
        return True

    def _controller_state_key(self, controller_actor_id: str) -> bytes:
        return _CONTROLLER_STATE_KEY_PREFIX + controller_actor_id.encode()

    def _persist_controller_state(
        self, controller_actor_id: str, state: _ControllerState
    ):
        value = json.dumps(
            {
                "cleaning": state.cleaning,
                "placement_group_ids": list(state.placement_groups),
            }
        )
        _internal_kv_put(
            self._controller_state_key(controller_actor_id),
            value,
            overwrite=True,
            namespace=_CONTROLLER_STATE_KV_NAMESPACE,
        )

    def _delete_controller_state(self, controller_actor_id: str):
        _internal_kv_del(
            self._controller_state_key(controller_actor_id),
            namespace=_CONTROLLER_STATE_KV_NAMESPACE,
        )

    def _load_controller_states(self) -> Dict[str, _ControllerState]:
        states = {}
        keys = _internal_kv_list(
            _CONTROLLER_STATE_KEY_PREFIX,
            namespace=_CONTROLLER_STATE_KV_NAMESPACE,
        )
        for key in keys:
            value = _internal_kv_get(key, namespace=_CONTROLLER_STATE_KV_NAMESPACE)
            if value is None:
                continue
            data = json.loads(value)
            controller_actor_id = key[len(_CONTROLLER_STATE_KEY_PREFIX) :].decode()
            states[controller_actor_id] = _ControllerState(
                placement_groups={
                    placement_group_id: PlacementGroup(
                        PlacementGroupID.from_hex(placement_group_id)
                    )
                    for placement_group_id in data["placement_group_ids"]
                },
                cleaning=data["cleaning"],
            )
        return states

    def _stop_monitor_thread(self) -> bool:
        """Stop the monitor thread. Returns whether an active thread was stopped."""
        monitor_thread = self._monitor_thread
        if monitor_thread is None or not monitor_thread.is_alive():
            return False

        self._stop_event.set()
        monitor_thread.join(timeout=self._stop_timeout)
        if monitor_thread.is_alive():
            logger.warning(
                "Monitor thread did not exit within %.2f seconds", self._stop_timeout
            )
            return False

        if self._monitor_thread is monitor_thread:
            self._monitor_thread = None
        return True

    def stop(self):
        """Stop monitoring and exit the shared actor (primarily for tests)."""
        self._stop_monitor_thread()
        self._exit()

    def _is_placement_group_removed(self, placement_group: PlacementGroup) -> bool:
        try:
            table = ray.util.placement_group_table(placement_group)
        except Exception as e:
            logger.warning(
                "Failed to query placement group table: %s. "
                "Assuming placement group is not removed.",
                e,
            )
            return False
        if "state" not in table:
            return True
        return table["state"] == "REMOVED"

    def _exit(self):
        if self._exiting:
            return
        self._exiting = True
        try:
            ray.actor.exit_actor()
        except Exception as e:
            logger.warning("Failed to exit actor: %s", e)
