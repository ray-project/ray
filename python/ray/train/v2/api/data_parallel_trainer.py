import logging
import signal
import sys
import threading
from typing import Any, Callable, Dict, List, Optional, Union

import ray
from ray._common.constants import RAY_WARN_BLOCKING_GET_INSIDE_ASYNC_ENV_VAR
from ray._common.usage import usage_lib
from ray._private.ray_constants import env_bool
from ray.actor import ActorHandle
from ray.air._internal.usage import tag_train_v2_trainer
from ray.train import (
    BackendConfig,
    Checkpoint,
    DataConfig,
    Result,
    RunConfig,
    ScalingConfig,
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
    TPUReservationCallback,
    WorkingDirectorySetupCallback,
)
from ray.train.v2._internal.callbacks.env_callback import _initialize_env_callbacks
from ray.train.v2._internal.callbacks.metrics import (
    ControllerMetricsCallback,
    WorkerMetricsCallback,
)
from ray.train.v2._internal.callbacks.placement_group_callback import (
    PlacementGroupCleanerCallback,
)
from ray.train.v2._internal.callbacks.state_manager import StateManagerCallback
from ray.train.v2._internal.callbacks.user_callback import UserCallbackHandler
from ray.train.v2._internal.constants import (
    DEFAULT_RAY_WARN_BLOCKING_GET_INSIDE_ASYNC_VALUE,
    METRICS_ENABLED_ENV_VAR,
    V2_ENABLED_ENV_VAR,
    get_env_vars_to_propagate,
    is_v2_enabled,
)
from ray.train.v2._internal.data_integration.interfaces import GenDataset
from ray.train.v2._internal.execution.callback import RayTrainCallback
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.controller import TrainController
from ray.train.v2._internal.execution.failure_handling import create_failure_policy
from ray.train.v2._internal.execution.local_mode.utils import LocalController
from ray.train.v2._internal.execution.scaling_policy import create_scaling_policy
from ray.train.v2._internal.util import ObjectRefWrapper, construct_train_func
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

        self.running_in_local_mode = self.scaling_config.num_workers == 0

        self.train_run_context = TrainRunContext(
            run_config=self.run_config,
            train_loop_config=self.train_loop_config,
            scaling_config=self.scaling_config,
            backend_config=self.backend_config,
            datasets=self.datasets,
            dataset_config=self.data_config,
        )

        if resume_from_checkpoint is not None:
            raise DeprecationWarning(_RESUME_FROM_CHECKPOINT_DEPRECATION_WARNING)

        if metadata is not None:
            raise DeprecationWarning(_GET_METADATA_DEPRECATION_MESSAGE)

        self._validate_configs()

        usage_lib.record_library_usage("train")
        tag_train_v2_trainer(self)

    def _validate_configs(self):
        if not is_v2_enabled():
            raise ValueError(
                f"Ray Train V2 must be enabled with `{V2_ENABLED_ENV_VAR}=1` "
                "when using this V2 Trainer API."
            )

        from ray.train.v2.api.config import (
            RunConfig as RunConfigV2,
            ScalingConfig as ScalingConfigV2,
        )

        if not isinstance(self.run_config, RunConfigV2):
            raise ValueError(
                f"Invalid `RunConfig` type: {self.run_config.__class__}. "
                "Use `ray.train.RunConfig` instead. "
                "See this issue for more context: "
                "https://github.com/ray-project/ray/issues/49454"
            )

        if not isinstance(self.scaling_config, ScalingConfigV2):
            raise ValueError(
                f"Invalid `ScalingConfig` type: {self.scaling_config.__class__}. "
                "Use `ray.train.ScalingConfig` instead. "
                "See this issue for more context: "
                "https://github.com/ray-project/ray/issues/49454"
            )

    def _get_train_func(self) -> Callable[[], None]:
        return construct_train_func(
            self.train_loop_per_worker,
            config=self.train_loop_config,
            train_func_context=self.backend_config.train_func_context,
            fn_arg_name="train_loop_per_worker",
        )

    def fit(self) -> Result:
        """Launches the Ray Train controller to run training on workers.

        Returns:
            A Result object containing the training result.

        Raises:
            ray.train.TrainingFailedError: This is a union of the ControllerError and WorkerGroupError.
                This returns a :class:`ray.train.ControllerError` if internal Ray Train controller logic
                encounters a non-retryable error or reaches the controller failure limit configured in `FailureConfig`.
                This returns a :class:`ray.train.WorkerGroupError` if one or more workers fail during
                training and reaches the worker group failure limit configured in `FailureConfig(max_failures)`.
        """
        train_fn = self._get_train_func()
        if self.running_in_local_mode:
            return self._initialize_and_run_local_controller(train_fn)
        else:
            train_fn_ref = ObjectRefWrapper(train_fn)

            result = self._initialize_and_run_controller(
                train_fn_ref=train_fn_ref,
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

    def _get_local_controller(self) -> LocalController:
        return LocalController(
            experiment_name=self.run_config.name,
            datasets=self.datasets,
        )

    def _create_default_callbacks(self) -> List[RayTrainCallback]:
        # Initialize callbacks from environment variable
        callbacks = _initialize_env_callbacks()

        accelerator_setup_callback = AcceleratorSetupCallback(
            self.backend_config, self.scaling_config
        )
        backend_setup_callback = BackendSetupCallback(self.backend_config)
        datasets_callback = DatasetsSetupCallback(
            train_run_context=self.train_run_context
        )
        tpu_reservation_setup_callback = TPUReservationCallback()
        placement_group_cleaner_callback = PlacementGroupCleanerCallback()
        callbacks.extend(
            [
                accelerator_setup_callback,
                tpu_reservation_setup_callback,
                backend_setup_callback,
                placement_group_cleaner_callback,
                datasets_callback,
            ]
        )
        if env_bool(RAY_CHDIR_TO_TRIAL_DIR, True):
            working_directory_setup_callback = WorkingDirectorySetupCallback()
            callbacks.append(working_directory_setup_callback)

        if env_bool(METRICS_ENABLED_ENV_VAR, True):
            callbacks.append(ControllerMetricsCallback())
            callbacks.append(WorkerMetricsCallback(self.train_run_context))

        if env_bool(RAY_TRAIN_ENABLE_STATE_TRACKING, False):
            callbacks.append(StateManagerCallback())

        run_config_callbacks = (
            self.run_config.callbacks if self.run_config.callbacks is not None else []
        )

        # Add internal callback that invokes all user-defined callbacks.
        user_callbacks = [
            cb for cb in run_config_callbacks if isinstance(cb, UserCallback)
        ]
        callbacks.append(
            UserCallbackHandler(
                user_callbacks=user_callbacks, train_run_context=self.train_run_context
            )
        )

        # Append all other callbacks to the full list. This allows custom workarounds
        # built on top of internal callbacks to work.
        callbacks.extend(
            [cb for cb in run_config_callbacks if not isinstance(cb, UserCallback)]
        )
        return callbacks

    def _initialize_and_run_local_controller(
        self, train_func: Callable[[], None]
    ) -> Result:
        return self._get_local_controller().run(train_func)

    def _initialize_and_run_controller(self, **controller_init_kwargs) -> Result:
        env_vars = get_env_vars_to_propagate()
        env_vars.setdefault(
            RAY_WARN_BLOCKING_GET_INSIDE_ASYNC_ENV_VAR,
            DEFAULT_RAY_WARN_BLOCKING_GET_INSIDE_ASYNC_VALUE,
        )

        # Attach the controller to the node running the driver script.
        controller_actor_cls = ray.remote(
            num_cpus=0,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=ray.get_runtime_context().get_node_id(), soft=False
            ),
            # TODO: Extract env variables that affect controller behavior
            # and pass them as explicit args
            runtime_env={"env_vars": env_vars},
        )(TrainController)

        controller = controller_actor_cls.remote(**controller_init_kwargs)

        # If this is not the main thread - as is the case when running in Tune -
        # registering the SIGINT handler raises an exception.
        if threading.current_thread() is threading.main_thread():
            self._register_sigint_handler(controller)

        ray.get(controller.run.remote())
        return ray.get(controller.get_result.remote())

    def _register_sigint_handler(self, controller: ActorHandle[TrainController]):
        """Register SIGINT handler so user Ctrl C gracefully aborts run."""
        sigint_count = 0

        def sigint_handler(signum, frame):
            logger.info(
                "Received SIGINT. Gracefully aborting the training run — this "
                "may take a few seconds. To forcefully abort immediately, you "
                "can send a different signal, such as SIGKILL."
            )
            nonlocal sigint_count
            sigint_count += 1
            if sigint_count >= 3:
                logger.info(
                    "Received SIGINT at least 3 times. "
                    "Forcefully aborting the training run."
                )
                sys.exit(0)
            if sigint_count <= 1:
                try:
                    ray.get(controller.abort.remote())
                except ray.exceptions.ActorDiedError:
                    # We catch the error and exit 0 to indicate graceful termination.
                    # However, for some reason the process still exits with 1.
                    sys.exit(0)

        signal.signal(signal.SIGINT, sigint_handler)

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
