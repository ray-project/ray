from enum import Enum, auto
from typing import Any, Callable, Dict


class CreateClusterEvent(Enum):
    """Events to track in ray.autoscaler.sdk.create_or_update_cluster.

    Attributes:
        up_started : Callback called at beginning of create_or_update_cluster.
        ssh_keypair_downloaded : Callback called when ssh keypair downloaded.
        cluster_booting_started : Callback called when cluster booting starts.
        acquiring_new_head_node : Callback called before head node acquired.
        head_node_acquired : Callback called after head node acquired.
        ssh_control_acquired : Callback called when node is being updated.
        run_initialization_cmd : Callback called before all initialization
            commands are called and again before each initialization command.
        run_setup_cmd : Callback called before all setup
            commands are called and again before each setup command.
        start_ray_runtime : Callback called before ray start commands are run.
        start_ray_runtime_completed : Callback called after ray start commands
            are run.
        cluster_booting_completed : Callback called after cluster booting
            is completed.
    """

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
    """Event system that handles storing and calling callbacks for events.

    Attributes:
        callback_map (Dict[str, List[Callable]]) : Stores list of callbacks
            for events when registered.
    """

    def __init__(self):
        self.callback_map = {}

    def add_callback_handler(self, event, callback: Callable[[Dict], None]):
        if event in self.callback_map:
            self.callback_map[event].append(callback)
        else:
            self.callback_map[event] = [callback]

    def execute_callback(self, event, event_data: Dict[str, Any] = {}):
        event_data["event_name"] = event
        if event in self.callback_map:
            for callback in self.callback_map[event]:
                callback(event_data)


global_event_system = _EventSystem()
