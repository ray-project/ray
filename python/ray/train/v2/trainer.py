import abc
import dataclasses
import logging
import os
from typing import Dict, Union, Callable, Optional, Type

import ray
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.config import ScalingConfig, RunConfig, DataParallelScalingConfig
from ray.ml.result import Result
from ray.train.backend import BackendExecutor
from ray.train.checkpoint import TuneCheckpointManagerV2
from ray.train.trainer import BACKEND_ENV_VARS
from ray.train.utils import RayDataset
from ray.train import BackendConfig, TrainingIterator
from ray.train.constants import TRAIN_DATASET_KEY
from ray.train.utils import construct_train_func
from ray import tune
from ray.tune import PlacementGroupFactory
from ray.tune.function_runner import wrap_function
from ray.tune.trainable import ConvertibleToTrainable, Trainable
from ray.tune.utils import deep_update
from ray.util.annotations import DeveloperAPI

GenDataset = Union[RayDataset, Callable[[], RayDataset]]


logger = logging.getLogger(__name__)


# TODO: support conditional hyperparameter tuning of Trainer __init__ args.
class Trainer(ConvertibleToTrainable, abc.ABC):
    """
    Defines interface for distributed training on Ray.

    Args:
        scaling_config (Optional[ScalingConfig]): Configuration for how to
            scale training.
        run_config (Optional[RunConfig]): Configuration for the execution of
            the training run.
        train_dataset (Optional[GenDataset]): Either a distributed Ray
            :ref:`Dataset <dataset-api>` or :ref:`DatasetPipeline
            <dataset-pipeline-api>`, or a Callbable that returns a Dataset, to
            use for training. If a ``preprocessor`` is also provided,
            it will be fit on this dataset and this dataset will be
            transfor,ed..
        additional_datasets (Optional[GenDataset]): Any additional Ray
            Datasets (such as validation or test datasets) to use for
            training. If a ``preprocessor`` is provided, the datasets
            specified here will only be transformed, and not fit on.
        preprocessor (Optional[Preprocessor]): A preprocessor to preprocess
            the provided datasets.
        resume_from_checkpoint (Optional[Checkpoint]): A checkpoint to
            resume training from.
    """

    def __init__(
        self,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        train_dataset: Optional[GenDataset] = None,
        additional_datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        self.scaling_config = scaling_config
        self.run_config = run_config if run_config else {}
        self.train_dataset = train_dataset
        self.additional_datasets = additional_datasets
        self.preprocessor = preprocessor
        self.checkpoint_to_resume_from = resume_from_checkpoint

    def fit(self) -> Result:
        """Runs training.

        Returns:
            A Result object containing the training result.
        """
        trainable = self.as_trainable()

        # Copied from initial prototyping.
        # TODO: Replace with Tuner.
        analysis = tune.run(run_or_experiment=trainable, **self.run_config)

        assert len(analysis.trials) == 1

        trial = analysis.trials[0]

        result = Result(
            metrics=trial.last_result,
            checkpoint=Checkpoint.from_directory(trial.checkpoint.value)
            if trial.checkpoint.value
            else None,
        )

        return result

    def _override_attributes_with_config(self, config: Dict):
        """Overrides attributes of the Trainer with values in ``config``.

        This is needed to allow attributes of Trainer to be tuned as
        hyperparameters.

        This method does a deep update of dictionaries and dataclasses

        Args:
            config (Dict): A dictionary to update attributes with.
        """
        for key in list(config.keys()):
            if hasattr(self, key):
                current_attribute = getattr(self, key)
                if isinstance(current_attribute, dict):
                    # Do a deep update of the dictionary.
                    deep_update(current_attribute, config[key], new_keys_allowed=True)
                elif dataclasses.is_dataclass(current_attribute):
                    if type(config[key]) == type(current_attribute):
                        # If the hyperparam is already passed in as a dataclass
                        # Then just directly set it.
                        setattr(self, key, config[key])
                    elif isinstance(config[key], dict):
                        # If the hyperparam is passed in as a dict, then update
                        # each attribute of the dataclass directly.
                        for inner_key, inner_value in config[key].items():
                            if hasattr(current_attribute, inner_key):
                                dataclass_field = getattr(current_attribute, inner_key)
                                if isinstance(dataclass_field, dict):
                                    # Do a deep update.
                                    deep_update(
                                        dataclass_field,
                                        inner_value,
                                        new_keys_allowed=True,
                                    )
                                else:
                                    setattr(current_attribute, inner_key, inner_value)
                else:
                    # Don't do a deep update and directly set the attribute
                    # to the value in config.
                    setattr(self, key, config[key])
                # Remove the key from the dict.
                del config[key]


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
        scaling_config (Optional[DataParallelScalingConfig]): Configuration
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
        scaling_config: Optional[DataParallelScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        train_dataset: Optional[GenDataset] = None,
        additional_datasets: Optional[Dict[str, GenDataset]] = None,
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
            self._override_attributes_with_config(config)

            # If there are any leftover key-value pairs in config,
            # then propagate them directly to the training function.
            if self.train_func_config and config:
                self.train_func_config.update(config)

            train_func = construct_train_func(self.train_func, self.train_func_config)

            runtime_env = {"env_vars": train_env_var_values}

            remote_executor = ray.remote(num_cpus=0)(BackendExecutor)
            additional_resources_per_worker = (
                self.scaling_config.additional_resources_per_worker
            )
            backend_executor_actor = remote_executor.options(
                runtime_env=runtime_env
            ).remote(
                backend_config=self.backend_config,
                num_workers=self.scaling_config.num_workers,
                num_cpus_per_worker=self.scaling_config.num_cpus_per_worker,
                num_gpus_per_worker=self.scaling_config.num_gpus_per_worker,
                additional_resources_per_worker=additional_resources_per_worker,
                max_retries=self.scaling_config.max_retries,
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
                return self.scaling_config.get_placement_group_factory()

        return TrainTrainable
