from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ray.train.backend import BackendConfig


class BeforeWorkerGroupStartMixin:
    def before_worker_group_start(self, backend_config: "BackendConfig") -> None:
        pass
