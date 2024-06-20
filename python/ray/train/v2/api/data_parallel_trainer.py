import logging
from typing import Any, Callable, Dict, Optional, Union

from ray.train import BackendConfig, Checkpoint
from ray.train._internal.data_config import DataConfig
from ray.train.base_trainer import GenDataset
from ray.train.v2._internal.accelerators import AcceleratorSetupCallback
from ray.train.v2._internal.constants import _UNSUPPORTED
from ray.train.v2._internal.execution.controller import TrainController
from ray.train.v2._internal.execution.failure_handling import DefaultFailurePolicy
from ray.train.v2._internal.execution.scaling_policy import FixedScalingPolicy
from ray.train.v2._internal.util import construct_train_func
from ray.train.v2.api.backend_setup import BackendSetupCallback
from ray.train.v2.api.config import RunConfig, ScalingConfig

logger = logging.getLogger(__name__)


class DataParallelTrainer:
    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        backend_config: Optional[BackendConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
        datasets: Optional[Dict[str, GenDataset]] = _UNSUPPORTED,
        dataset_config: Optional[DataConfig] = _UNSUPPORTED,
        metadata: Optional[Dict[str, Any]] = _UNSUPPORTED,
    ):
        self.train_loop_per_worker = train_loop_per_worker
        self.train_loop_config = train_loop_config
        self.scaling_config = scaling_config
        self.backend_config = backend_config or BackendConfig()
        self.run_config = run_config or RunConfig()
        self.resume_from_checkpoint = resume_from_checkpoint

        # TODO: No support for below
        error_msg = "The '{}' argument is not supported yet."

        if datasets != _UNSUPPORTED:
            raise NotImplementedError(error_msg.format("datasets"))
        if dataset_config != _UNSUPPORTED:
            raise NotImplementedError(error_msg.format("dataset_config"))
        if metadata != _UNSUPPORTED:
            raise NotImplementedError(error_msg.format("metadata"))

    def fit(self):
        train_fn = construct_train_func(
            self.train_loop_per_worker,
            config=self.train_loop_config,
            train_func_context=self.backend_config.train_func_context,
            fn_arg_name="train_loop_per_worker",
        )

        accelerator_setup_callback = AcceleratorSetupCallback(
            self.backend_config, self.scaling_config
        )
        backend_setup_callback = BackendSetupCallback(self.backend_config)
        callbacks = [accelerator_setup_callback, backend_setup_callback]

        controller = TrainController(
            train_fn=train_fn,
            scaling_policy=FixedScalingPolicy(self.scaling_config),
            failure_policy=DefaultFailurePolicy(self.run_config.failure_config),
            run_config=self.run_config,
            callbacks=callbacks,
        )
        controller.run()

        return controller.get_result()

    @classmethod
    def restore(cls, *args, **kwargs):
        raise NotImplementedError
