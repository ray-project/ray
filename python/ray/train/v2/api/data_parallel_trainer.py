import logging
from typing import Any, Callable, Dict, Optional, Union

from ray._private.ray_constants import env_bool
from ray.train import BackendConfig, Checkpoint
from ray.train._internal.data_config import DataConfig
from ray.train.base_trainer import GenDataset
from ray.train.constants import RAY_CHDIR_TO_TRIAL_DIR
from ray.train.v2._internal.callbacks import (
    AcceleratorSetupCallback,
    BackendSetupCallback,
    WorkingDirectorySetupCallback,
)
from ray.train.v2._internal.constants import _UNSUPPORTED
from ray.train.v2._internal.execution.controller import TrainController
from ray.train.v2._internal.execution.failure_handling import DefaultFailurePolicy
from ray.train.v2._internal.execution.scaling_policy import create_scaling_policy
from ray.train.v2._internal.util import construct_train_func
from ray.train.v2.api.config import RunConfig, ScalingConfig

logger = logging.getLogger(__name__)


TRAINER_RESTORE_DEPRECATION_WARNING = """
The `Trainer.restore` API is deprecated and will be removed in a future release.

This API previously accepted a Train run directory path and loaded state such as the
training code and configurations from a pkl file, which was hard to use.
Now, Ray Train attempts to load the snapshot of reported checkpoints if it exists
in the run directory, which makes `ray.train.get_checkpoint` available as long as
you're pointing to the same run directory (i.e. the same `storage_path` and `name`).

If you want to start a new training run without any prior checkpoint history, please
specify a new, unique `RunConfig(name)`.

Job-level restoration can still be achieved, as shown below:

Before
-------

def train_fn_per_worker(config):
    checkpoint = ray.train.get_checkpoint()
    # Perform your training-specific checkpoint recovery here...

storage_path = "s3://bucket/"
name = "<unique_job_identifier>"
run_path = f"{storage_path}/{name}"

if TorchTrainer.can_restore(run_path):
    # Some parameters are optionally re-specified.
    trainer = TorchTrainer.restore(run_path, datasets={...})
    result = trainer.fit()
else:
    trainer = TorchTrainer(
        train_fn_per_worker,
        datasets={...},
        scaling_config=train.ScalingConfig(num_workers=2),
        run_config=train.RunConfig(storage_path=storage_path, name=name),
    )
    result = trainer.fit()

After
-----

def train_fn_per_worker(config):
    # `ray.train.get_checkpoint` will be populated as long as your run
    # is pointing to the same directory.
    checkpoint = ray.train.get_checkpoint()
    # Perform your training-specific checkpoint recovery here...

storage_path = "s3://bucket/"
name = "<unique_job_identifier>"

trainer = TorchTrainer(
    train_fn_per_worker,
    datasets={...},
    scaling_config=train.ScalingConfig(num_workers=2),
    run_config=train.RunConfig(storage_path=storage_path, name=name),
)
result = trainer.fit()
"""

RESUME_FROM_CHECKPOINT_DEPRECATION_WARNING = """
`resume_from_checkpoint` is deprecated and will be removed in an upcoming
release, since it is conceptually confusing and can be replaced very easily.
For example:

Before
------

def train_fn_per_worker(config: dict):
    # This is the checkpoint passed to `resume_from_checkpoint`
    # if no other checkpoints have been saved.
    # Otherwise this is the latest reported checkpoint.
    checkpoint = ray.train.get_checkpoint()

trainer = TorchTrainer(
    train_fn_per_worker,
    ...,
    resume_from_checkpoint=ray.train.Checkpoint(...)
)

After
-----

def train_fn_per_worker(config: dict):
    # Equivalent behavior that is explicit and more flexible.
    checkpoint = (
        ray.train.get_checkpoint()
        or config.get("resume_from_checkpoint")
    )

trainer = TorchTrainer(
    train_fn_per_worker,
    train_loop_config={"resume_from_checkpoint": ray.train.Checkpoint(...)},
)
"""


class DataParallelTrainer:
    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        backend_config: Optional[BackendConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        # TODO: [Deprecated] Remove in future release
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
        if resume_from_checkpoint:
            logger.warning(RESUME_FROM_CHECKPOINT_DEPRECATION_WARNING)
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
        callbacks = [
            accelerator_setup_callback,
            backend_setup_callback,
        ]
        if env_bool(RAY_CHDIR_TO_TRIAL_DIR, True):
            working_directory_setup_callback = WorkingDirectorySetupCallback()
            callbacks.append(working_directory_setup_callback)

        # TODO: Add support for user-defined callbacks

        controller = TrainController(
            train_fn=train_fn,
            scaling_policy=create_scaling_policy(self.scaling_config),
            failure_policy=DefaultFailurePolicy(self.run_config.failure_config),
            run_config=self.run_config,
            callbacks=callbacks,
            resume_from_checkpoint=self.resume_from_checkpoint,
        )
        controller.run()

        return controller.get_result()

    @classmethod
    def restore(cls, *args, **kwargs):
        raise DeprecationWarning(TRAINER_RESTORE_DEPRECATION_WARNING)

    @classmethod
    def can_restore(cls, *args, **kwargs):
        raise DeprecationWarning(
            "This API is deprecated and will be removed in a future release. "
            "The trainer will be always restored automatically when an existing "
            "training snapshot is detected in the run configuration path."
        )
