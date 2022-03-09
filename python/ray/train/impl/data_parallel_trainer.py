import logging
import os
from typing import Dict, Callable, Optional, Type, TYPE_CHECKING

import ray
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.config import ScalingConfig, RunConfig, _DataParallelScalingConfig
from ray.train.backend import BackendExecutor
from ray.train.checkpoint import TuneCheckpointManagerV2
from ray.train.trainer import BACKEND_ENV_VARS
from ray.train.ml_trainer import Trainer
from ray.train import BackendConfig, TrainingIterator
from ray.train.constants import TRAIN_DATASET_KEY
from ray.train.utils import construct_train_func
from ray import tune
from ray.tune import PlacementGroupFactory
from ray.tune.function_runner import wrap_function
from ray.tune.trainable import Trainable
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import ray.data.Dataset

logger = logging.getLogger(__name__)


@DeveloperAPI
class DataParallelFunctionTrainer(Trainer):
    """
    A Trainer for data parallel training.

    This Trainer runs the provided function on multiple Ray Actors. The
    provided datasets are automatically sharded to each Ray actor running
    the training function.

    Args:
        train_func (Callable): The training function to execute.
            This can either take in no arguments or a ``config`` dict.
        train_func_config (Optional[Dict]): Configurations to pass into
            ``train_func`` if it accepts an argument.
        backend_config (Optional[BackendConfig]): Used to specify which
            backend to setup on the workers enable distributed
            communication, for example torch or horovod. If no Backend
            should be set up, then set this to None.
        scaling_config (Optional[ScalingConfig]): Configuration
            for how to scale data parallel training.
        run_config (Optional[RunConfig]): Configuration for the execution of
            the training run.
        train_dataset (Optional[GenDataset]): Either a distributed Ray
            :ref:`Dataset <dataset-api>` or :ref:`DatasetPipeline
            <dataset-pipeline-api>`, or a Callbable that returns a Dataset, to
            use for training. If a ``preprocessor`` is also provided,
            it will be fit on this dataset.
        additional_datasets (Optional[GenDataset]): Any additional Ray
            Datasets (such as validation or test datasets) to use for
            training. If a ``preprocessor`` is provided, it will only
            transform these datasets.
        preprocessor (Optional[Preprocessor]): A preprocessor to preprocess
            the provided datasets.
        resume_from_checkpoint (Optional[Checkpoint]): A checkpoint to
            resume training from.

    """

    def __init__(
        self,
        train_func: Callable,
        train_func_config: Optional[Dict] = None,
        backend_config: Optional[BackendConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        train_dataset: Optional["ray.data.Dataset"] = None,
        additional_datasets: Optional[Dict[str, "ray.data.Dataset"]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        if not ray.is_initialized():
            ray.init()

        if (
            "GPU" in ray.available_resources()
            and scaling_config.num_gpus_per_worker <= 0
        ):
            logger.info(
                "GPUs are detected in your Ray cluster, but GPU "
                "training is not enabled for Ray Train. To enable "
                "GPU training, make sure to set `use_gpu` to True "
                "when instantiating your Trainer."
            )

        self.train_func = train_func
        self.train_func_config = train_func_config

        super(DataParallelFunctionTrainer, self).__init__(
            scaling_config=scaling_config,
            run_config=run_config,
            train_dataset=train_dataset,
            additional_datasets=additional_datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

        backend_config = backend_config if backend_config else BackendConfig()
        self.backend_config = backend_config

    def as_trainable(self) -> Type[Trainable]:

        train_env_var_values = {
            var_name: os.environ[var_name]
            for var_name in BACKEND_ENV_VARS
            if var_name in os.environ
        }

        def tune_function(config, checkpoint_dir=None):
            leftover_config = self._override_attributes_with_config(config)

            scaling_config_dataclass = _DataParallelScalingConfig(**self.scaling_config)

            # If there are any leftover key-value pairs in config,
            # then propagate them directly to the training function.
            if self.train_func_config and leftover_config:
                self.train_func_config.update(leftover_config)

            train_func = construct_train_func(self.train_func, self.train_func_config)

            runtime_env = {"env_vars": train_env_var_values}

            remote_executor = ray.remote(num_cpus=0)(BackendExecutor)
            additional_resources_per_worker = (
                scaling_config_dataclass.additional_resources_per_worker
            )
            backend_executor_actor = remote_executor.options(
                runtime_env=runtime_env
            ).remote(
                backend_config=self.backend_config,
                num_workers=scaling_config_dataclass.num_workers,
                num_cpus_per_worker=scaling_config_dataclass.num_cpus_per_worker,
                num_gpus_per_worker=scaling_config_dataclass.num_gpus_per_worker,
                additional_resources_per_worker=additional_resources_per_worker,
                max_retries=scaling_config_dataclass.max_retries,
            )

            checkpoint_manager = TuneCheckpointManagerV2()
            checkpoint_manager.on_init(preprocessor=self.preprocessor)

            # Start the remote actors.
            ray.get(backend_executor_actor.start.remote(initialization_hook=None))

            if self.train_dataset:
                if self.preprocessor:
                    self.train_dataset = self.preprocessor.fit_transform(
                        self.train_dataset
                    )
                # Combine datasets into a single dict.
                dataset_dict = {TRAIN_DATASET_KEY: self.train_dataset}
            else:
                dataset_dict = None

            if dataset_dict and self.additional_datasets:
                dataset_dict = {**dataset_dict, **self.additional_datasets}

            # Transform all the other datasets concurrently in remote tasks.
            task_dict = {}
            if dataset_dict and self.preprocessor:
                for key, dataset in dataset_dict.items():
                    if key != TRAIN_DATASET_KEY:
                        remote_transform = ray.remote(num_cpus=0)(
                            lambda: self.preprocessor.transform(dataset)
                        ).remote()
                        task_dict[key] = remote_transform

            ray.get(list(task_dict.values()))

            for key, transformed_dataset in task_dict.items():
                dataset_dict[key] = ray.get(transformed_dataset)

            if checkpoint_dir:
                checkpoint = Checkpoint.from_directory(checkpoint_dir)
            else:
                checkpoint = self.checkpoint_to_resume_from

            if checkpoint:
                checkpoint = checkpoint.to_dict()

            training_iterator = TrainingIterator(
                backend_executor_actor=backend_executor_actor,
                backend_config=self.backend_config,
                train_func=train_func,
                dataset=dataset_dict,
                checkpoint_manager=checkpoint_manager,
                checkpoint=checkpoint,
                checkpoint_strategy=None,
            )

            for results in training_iterator:
                first_worker_results = results[0]

                tune.report(**first_worker_results)

            # Shutdown workers.
            ray.get(backend_executor_actor.shutdown.remote())

        trainable_cls = wrap_function(tune_function)

        class TrainTrainable(trainable_cls):
            """Add default resources to the Trainable."""

            @classmethod
            def default_resource_request(cls, config: Dict) -> PlacementGroupFactory:
                self._override_attributes_with_config(config)
                scaling_config_dataclass = _DataParallelScalingConfig(
                    **self.scaling_config
                )
                return scaling_config_dataclass.get_placement_group_factory()

        return TrainTrainable
