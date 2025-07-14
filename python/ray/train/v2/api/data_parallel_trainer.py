import asyncio
import logging
import os
import signal
import sys
from typing import Any, Callable, Dict, List, Optional, Union

import torch
import torch.distributed as torch_dist

import ray
from ray._private.ray_constants import env_bool
from ray._private.usage import usage_lib
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
    WorkingDirectorySetupCallback,
)
from ray.train.v2._internal.callbacks.datasets import GenDataset
from ray.train.v2._internal.callbacks.env_callback import _initialize_env_callbacks
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
from ray.train.v2._internal.execution.context import TrainRunContext, set_train_context
from ray.train.v2._internal.execution.controller import TrainController
from ray.train.v2._internal.execution.failure_handling import create_failure_policy
from ray.train.v2._internal.execution.scaling_policy import create_scaling_policy
from ray.train.v2._internal.util import ObjectRefWrapper, construct_train_func
from ray.train.v2.api.callback import UserCallback
from ray.train.v2.api.local_testing_context import LocalTestingContext
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
        local_test_mode: bool = False,
    ):
        self.run_config = run_config or RunConfig()
        self.train_loop_per_worker = train_loop_per_worker
        self.train_loop_config = train_loop_config
        self.scaling_config = scaling_config or ScalingConfig()
        self.backend_config = backend_config or BackendConfig()
        self.datasets = datasets or {}
        self.data_config = dataset_config or DataConfig()
        self.local_test_mode = local_test_mode

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

        usage_lib.record_library_usage("train")
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
        if self.local_test_mode:
            self._set_local_testing_train_context()
            train_fn()
            return Result(checkpoint=None, error=None, metrics={}, path=None)
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

    def _set_local_testing_train_context(self) -> None:
        # TODO: warning that all configs might be overridden by default values
        def launched_by_torchrun() -> bool:
            """Return True if this process looks like it came from `torchrun`."""
            env_markers = {
                "LOCAL_RANK",
                "LOCAL_WORLD_SIZE",
                "WORLD_SIZE",
                "TORCHELASTIC_RUN_ID",
            }  # torchrun ≥1.10
            argv_markers = (
                "--local-rank",
                "--local_rank",
            )  # torchrun always passes one of these

            # Any of the env vars *or* the CLI flag counts as evidence
            return bool(
                (env_markers & os.environ.keys())
                or any(a.startswith(argv_markers) for a in sys.argv)
            )

        assert self.local_test_mode
        if launched_by_torchrun():
            torch_dist.init_process_group(
                backend="nccl" if torch.cuda.is_available() else "gloo"
            )
            world_size = torch_dist.get_world_size()
            world_rank = torch_dist.get_rank()
            dataset_shards = DataConfig().configure(
                datasets=self.datasets,
                world_size=world_size,
                worker_handles=None,
                worker_node_ids=None,
            )
            # We are only using 1 node for local testing, so world_size == local_world_size
            set_train_context(
                LocalTestingContext(
                    dataset_shards=dataset_shards[world_rank],
                    world_size=world_size,
                    world_rank=world_rank,
                    local_rank=world_rank,
                    local_world_size=world_size,
                )
            )
        else:
            set_train_context(LocalTestingContext(dataset_shards=self.datasets))

    def _create_default_callbacks(self) -> List[RayTrainCallback]:
        # Initialize callbacks from environment variable
        callbacks = _initialize_env_callbacks()

        accelerator_setup_callback = AcceleratorSetupCallback(
            self.backend_config, self.scaling_config
        )
        backend_setup_callback = BackendSetupCallback(self.backend_config)
        datasets_setup_callback = DatasetsSetupCallback(
            datasets=self.datasets,
            data_config=self.data_config,
            scaling_config=self.scaling_config,
        )
        callbacks.extend(
            [
                accelerator_setup_callback,
                backend_setup_callback,
                datasets_setup_callback,
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
                # TODO: Extract env variables that affect controller behavior
                # and pass them as explicit args
                runtime_env={"env_vars": get_env_vars_to_propagate()},
            )(TrainController)

            controller = controller_actor_cls.remote(**controller_init_kwargs)

            self._register_sigint_handler(controller)

            ray.get(controller.run.remote())
            return ray.get(controller.get_result.remote())
        else:
            controller = TrainController(**controller_init_kwargs)
            asyncio.run(controller.run())
            return controller.get_result()

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
