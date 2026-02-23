from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ray.train.backend import BackendConfig


class ControllerLifecycleMixin:
    def on_controller_start(self, backend_config: "BackendConfig") -> None:
        pass
