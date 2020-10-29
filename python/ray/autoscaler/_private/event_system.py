from enum import auto, Enum

class CreateClusterEvent(Enum):
	up_started = auto()
	ssh_keypair_downloaded = auto()
	cluster_booting_started = auto()
	run_initialization_cmd = auto()
	run_setup_cmd = auto()
	start_ray_runtime = auto()
	start_ray_runtime_completed = auto()
	cluster_booting_completed = auto()

class _EventSystem:
    def __init__(self):
        self.callback_map = {}

    def add_callback_handler(self, event, callback):
        self.callback_map[event] = callback

    def execute_callback(self, event, event_data):
        if event in self.callback_map:
            event_data["event_name"] = event
            self.callback_map[event](event_data)

global_event_system = _EventSystem()