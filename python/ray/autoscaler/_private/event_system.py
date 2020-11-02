from enum import Enum, auto
from typing import Any, Callable, Dict


class CreateClusterEvent(Enum):
    up_started = auto()
    ssh_keypair_downloaded = auto()
    cluster_booting_started = auto()
    acquiring_new_head_node = auto()
    head_node_acquired = auto()
    ssh_control_acquired = auto()
    run_initialization_cmd = auto()
    run_setup_cmd = auto()
    start_ray_runtime = auto()
    start_ray_runtime_completed = auto()
    cluster_booting_completed = auto()


class _EventSystem:
    def __init__(self):
        self.callback_map = {}

    def add_callback_handler(self, event, callback: Callable[[Dict], None]):
        if event in self.callback_map:
            self.callback_map[event].append(callback)
        else:
            self.callback_map[event] = [callback]

    def execute_callback(self, event, event_data: Dict[str, Any]):
        event_data["event_name"] = event
        if event in self.callback_map:
            for callback in self.callback_map[event]:
                callback(event_data)


global_event_system = _EventSystem()
