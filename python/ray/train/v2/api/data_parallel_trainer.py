import logging
from typing import Any, Callable, Dict, Optional, Union

import ray
from ray._private.ray_constants import env_bool
from ray.train import BackendConfig, Checkpoint
from ray.train._internal.data_config import DataConfig
from ray.train.constants import RAY_CHDIR_TO_TRIAL_DIR
from ray.train.v2._internal.callbacks import (
    AcceleratorSetupCallback,
    BackendSetupCallback,
    DatasetsSetupCallback,
    WorkingDirectorySetupCallback,
)
from ray.train.v2._internal.callbacks.datasets import GenDataset
from ray.train.v2._internal.callbacks.metrics import (
    ControllerMetricsCallback,
    WorkerMetricsCallback,
)
from ray.train.v2._internal.constants import (
    METRICS_ENABLED_ENV_VAR,
    get_env_vars_to_propagate,
)
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.controller import TrainController
from ray.train.v2._internal.execution.failure_handling import DefaultFailurePolicy
from ray.train.v2._internal.execution.scaling_policy import create_scaling_policy
from ray.train.v2._internal.util import construct_train_func
from ray.train.v2.api.config import RunConfig, ScalingConfig
from ray.train.v2.api.result import Result
from ray.util.annotations import Deprecated, DeveloperAPI
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)


_TRAINER_RESTORE_DEPRECATION_WARNING = """
The `restore` and `can_restore` APIs are deprecated and
will be removed in a future release.

This API previously accepted a Train run directory path and
loaded state (including user training code) partially from .pkl file and
partially from new arguments passed to the constructor.
This was confusing and error-prone.

Now, trainers only have a single constructor codepath, which takes in
all arguments needed to construct the trainer (with no more brittle
serialization/deserialization logic).
Ray Train will auto-detect if an existing run snapshot exists
at the path configured by `RunConfig(storage_path, name)` and will populate
`ray.train.get_checkpoint` with the latest checkpoint, accessible
by all Train workers.

If you want to start a brand new training run without any prior checkpoint history,
please specify a new, unique `RunConfig(storage_path, name)`.

Trainer-level restoration can still be achieved, as shown below:

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

# The second run will automatically find the snapshot saved by the first run
# at (storage_path, name).
trainer = TorchTrainer(
    train_fn_per_worker,
    datasets={...},
    scaling_config=train.ScalingConfig(num_workers=2),
    run_config=train.RunConfig(storage_path=storage_path, name=name),
)
result = trainer.fit()
"""

_RESUME_FROM_CHECKPOINT_DEPRECATION_WARNING = """
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


@DeveloperAPI
class DataParallelTrainer:
    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        backend_config: Optional[BackendConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        dataset_config: Optional[DataConfig] = None,
        # TODO: [Deprecated] Remove in future release
        resume_from_checkpoint: Optional[Checkpoint] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.run_config = run_config or RunConfig()
        self.train_run_context = TrainRunContext(self.run_config)
        self.train_loop_per_worker = train_loop_per_worker
        self.train_loop_config = train_loop_config
        self.scaling_config = scaling_config
        self.backend_config = backend_config or BackendConfig()
        self.datasets = datasets or {}
        self.data_config = dataset_config or DataConfig()

        if resume_from_checkpoint is not None:
            raise DeprecationWarning(_RESUME_FROM_CHECKPOINT_DEPRECATION_WARNING)

        if metadata is not None:
            raise DeprecationWarning()

    def fit(self) -> Result:
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
        datasets_setup_callback = DatasetsSetupCallback(
            datasets=self.datasets,
            data_config=self.data_config,
            scaling_config=self.scaling_config,
        )
        callbacks = [
            accelerator_setup_callback,
            backend_setup_callback,
            datasets_setup_callback,
        ]
        if env_bool(RAY_CHDIR_TO_TRIAL_DIR, True):
            working_directory_setup_callback = WorkingDirectorySetupCallback()
            callbacks.append(working_directory_setup_callback)

        if env_bool(METRICS_ENABLED_ENV_VAR, True):
            callbacks.append(ControllerMetricsCallback(self.train_run_context))
            callbacks.append(WorkerMetricsCallback(self.train_run_context))

        # TODO: Add support for user-defined callbacks

        # By default, attach the controller to the node running the driver script.
        controller_actor_cls = ray.remote(
            num_cpus=0,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=ray.get_runtime_context().get_node_id(), soft=False
            ),
            runtime_env={"env_vars": get_env_vars_to_propagate()},
        )(TrainController)

        controller = controller_actor_cls.remote(
            train_fn=train_fn,
            scaling_policy=create_scaling_policy(self.scaling_config),
            failure_policy=DefaultFailurePolicy(self.run_config.failure_config),
            train_run_context=self.train_run_context,
            callbacks=callbacks,
        )
        ray.get(controller.run.remote())

        result = ray.get(controller.get_result.remote())
        if result.error:
            # NOTE: If the training run errored out, raise an error back to the
            # user's driver script.
            # For example, if the Train `FailurePolicy` runs out of retries,
            # and one of the workers errors. The controller will exit, and
            # the error will be raised here.
            raise result.error

        return result

    @Deprecated
    @classmethod
    def restore(cls, *args, **kwargs):
        raise DeprecationWarning(_TRAINER_RESTORE_DEPRECATION_WARNING)

    @Deprecated
    @classmethod
    def can_restore(cls, *args, **kwargs):
        raise DeprecationWarning(_TRAINER_RESTORE_DEPRECATION_WARNING)
