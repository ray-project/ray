from typing import Optional, Dict, Callable, Any

from ray.air.execution.impl.train.train_controller import TrainController
from ray.air.execution.resources.fixed import FixedResourceManager
from ray.air.execution.resources.resource_manager import ResourceManager
from ray.train import BackendConfig
from ray.train._internal.utils import construct_train_func


def train_run(
    train_func: Callable,
    config: Dict[str, Any],
    num_workers: int = 2,
    backend_config: Optional[BackendConfig] = None,
    resource_manager: Optional[ResourceManager] = None,
):
    wrapped_train_func = construct_train_func(train_func, config)

    resource_manager = resource_manager or FixedResourceManager()
    backend_config = backend_config or BackendConfig()

    train_controller = TrainController(
        train_fn=wrapped_train_func,
        backend_config=backend_config,
        resource_manager=resource_manager,
        num_workers=num_workers,
    )
    while not train_controller.is_finished():
        train_controller.step()

    return train_controller.last_result
