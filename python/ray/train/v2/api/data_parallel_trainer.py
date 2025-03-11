import logging
from typing import Any, Callable, Dict, List, Optional, Union

import ray
from ray._private.ray_constants import env_bool
from ray.air._internal.usage import tag_train_v2_trainer
from ray.train import (
    BackendConfig,
    Checkpoint,
    DataConfig,
    ScalingConfig,
    RunConfig,
    Result,
)
from ray.train.base_trainer import (
    _RESUME_FROM_CHECKPOINT_DEPRECATION_WARNING,
    _TRAINER_RESTORE_DEPRECATION_WARNING,
)
from ray.train.constants import RAY_CHDIR_TO_TRIAL_DIR, RAY_TRAIN_ENABLE_STATE_TRACKING
from ray.train.context import _GET_METADATA_DEPRECATION_MESSAGE
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
from ray.train.v2._internal.callbacks.state_manager import StateManagerCallback
from ray.train.v2._internal.callbacks.user_callback import UserCallbackHandler
from ray.train.v2._internal.constants import (
    DEFAULT_RUN_CONTROLLER_AS_ACTOR,
    METRICS_ENABLED_ENV_VAR,
    RUN_CONTROLLER_AS_ACTOR_ENV_VAR,
    get_env_vars_to_propagate,
)
from ray.train.v2._internal.execution.callback import RayTrainCallback
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.controller import TrainController
from ray.train.v2._internal.execution.failure_handling import create_failure_policy
from ray.train.v2._internal.execution.scaling_policy import create_scaling_policy
from ray.train.v2._internal.util import construct_train_func
from ray.train.v2.api.callback import UserCallback
from ray.util.annotations import Deprecated, DeveloperAPI
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)


@DeveloperAPI
class DataParallelTrainer:
    """Base class for distributed data parallel training on Ray.

    This class supports the SPMD parallelization pattern, where a single
    training function is executed in parallel across multiple workers,
    and different shards of data are processed by each worker.
    """

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
        self.train_loop_per_worker = train_loop_per_worker
        self.train_loop_config = train_loop_config
        self.scaling_config = scaling_config or ScalingConfig()
        self.backend_config = backend_config or BackendConfig()
        self.datasets = datasets or {}
        self.data_config = dataset_config or DataConfig()

        self.train_run_context = TrainRunContext(self.run_config)

        if resume_from_checkpoint is not None:
            raise DeprecationWarning(_RESUME_FROM_CHECKPOINT_DEPRECATION_WARNING)

        if metadata is not None:
            raise DeprecationWarning(_GET_METADATA_DEPRECATION_MESSAGE)

        tag_train_v2_trainer(self)

    def fit(self) -> Result:
        """Launches the Ray Train controller to run training on workers.

        Returns:
            A Result object containing the training result.

        Raises:
            ray.train.v2.api.exceptions.TrainingFailedError: If any failures occur
                during training and the number of retries configured in
                `FailureConfig` is exhausted.
        """
        train_fn = construct_train_func(
            self.train_loop_per_worker,
            config=self.train_loop_config,
            train_func_context=self.backend_config.train_func_context,
            fn_arg_name="train_loop_per_worker",
        )

        result = self._initialize_and_run_controller(
            train_fn=train_fn,
            scaling_policy=create_scaling_policy(self.scaling_config),
            failure_policy=create_failure_policy(self.run_config.failure_config),
            train_run_context=self.train_run_context,
            callbacks=self._create_default_callbacks(),
        )

        if result.error:
            # NOTE: If the training run errored out, raise an error back to the
            # user's driver script.
            # For example, if the Train `FailurePolicy` runs out of retries,
            # and one of the workers errors. The controller will exit, and
            # the error will be raised here.
            raise result.error

        return result

    def _create_default_callbacks(self) -> List[RayTrainCallback]:
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

        if env_bool(RAY_TRAIN_ENABLE_STATE_TRACKING, False):
            callbacks.append(StateManagerCallback(self.train_run_context))

        # Add internal callback that invokes all user-defined callbacks.
        user_callbacks = [
            cb for cb in self.run_config.callbacks if isinstance(cb, UserCallback)
        ]
        callbacks.append(
            UserCallbackHandler(
                user_callbacks=user_callbacks, train_run_context=self.train_run_context
            )
        )

        # Append all other callbacks to the full list. This allows custom workarounds
        # built on top of internal callbacks to work.
        callbacks.extend(
            [cb for cb in self.run_config.callbacks if not isinstance(cb, UserCallback)]
        )
        return callbacks

    def _initialize_and_run_controller(self, **controller_init_kwargs) -> Result:
        run_controller_as_actor = env_bool(
            RUN_CONTROLLER_AS_ACTOR_ENV_VAR, DEFAULT_RUN_CONTROLLER_AS_ACTOR
        )
        if run_controller_as_actor:
            # Attach the controller to the node running the driver script.
            controller_actor_cls = ray.remote(
                num_cpus=0,
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=ray.get_runtime_context().get_node_id(), soft=False
                ),
                runtime_env={"env_vars": get_env_vars_to_propagate()},
            )(TrainController)

            controller = controller_actor_cls.remote(**controller_init_kwargs)
            ray.get(controller.run.remote())
            return ray.get(controller.get_result.remote())
        else:
            controller = TrainController(**controller_init_kwargs)
            controller.run()
            return controller.get_result()

    @classmethod
    @Deprecated
    def restore(cls, *args, **kwargs):
        """[Deprecated] Restores a Train experiment from a previously
        interrupted/failed run.

        This method is deprecated and will be removed in a future release.
        """
        raise DeprecationWarning(_TRAINER_RESTORE_DEPRECATION_WARNING)

    @classmethod
    @Deprecated
    def can_restore(cls, *args, **kwargs):
        """[Deprecated] Checks if a Train experiment can be restored from
        a previously interrupted/failed run.

        This method is deprecated and will be removed in a future release.
        """
        raise DeprecationWarning(_TRAINER_RESTORE_DEPRECATION_WARNING)
