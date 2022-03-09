import abc
import copy
import logging
from typing import Dict, Union, Callable, Optional, TYPE_CHECKING, Any

import ray
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.config import ScalingConfig, RunConfig
from ray.ml.result import Result
from ray import tune
from ray.tune import PlacementGroupFactory
from ray.tune.function_runner import wrap_function
from ray.tune.trainable import ConvertibleToTrainable
from ray.tune.utils import deep_update

if TYPE_CHECKING:
    from ray.data import Dataset

GenDataset = Union["Dataset", Callable[[], "Dataset"]]


logger = logging.getLogger(__name__)


class Trainer(ConvertibleToTrainable, abc.ABC):
    """Defines interface for distributed training on Ray.

    Args:
        scaling_config: Configuration for how to
            scale training.
        run_config: Configuration for the execution of
            the training run.
        train_dataset: Either a distributed Ray
            :ref:`Dataset <dataset-api>` or a Callable that returns a Dataset,
            to use for training. If a ``preprocessor`` is also provided,
            it will be fit on this dataset and this dataset will be
            transformed.
        extra_datasets: Any extra Datasets (such as validation or test
            datasets) to use for training. If a ``preprocessor`` is
            provided, the datasets specified here will only be transformed,
            and not fit on.
        preprocessor: A preprocessor to preprocess the provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    def __init__(
        self,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        train_dataset: Optional[GenDataset] = None,
        extra_datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        self.scaling_config = scaling_config
        self.run_config = run_config if run_config else {}
        self.train_dataset = train_dataset
        self.extra_datasets = extra_datasets
        self.preprocessor = preprocessor
        self.checkpoint_to_resume_from = resume_from_checkpoint

    def fit(self) -> Result:
        """Runs training.

        Returns:
            A Result object containing the training result.
        """
        trainable = self.as_trainable()

        # Copied from initial prototyping.
        # TODO(amog): Replace with Tuner.
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

    def _override_attributes_with_config(self, config: Dict) -> Dict:
        """Overrides attributes of the Trainer with values in ``config``.

        This is needed to allow attributes of Trainer to be tuned as
        hyperparameters.

        This method does a deep update of class attributes that are dicts.

        Args:
            config: A dictionary to update attributes with.

        Returns:
            A dict containing any remaining key-value pairs from ``config``
            that don't override any attributes. This leftover config can be
            used for any downstream tasks (such as as passing to a training
            function).
        """
        config = copy.deepcopy(config)
        for key in list(config.keys()):
            if hasattr(self, key):
                current_attribute = getattr(self, key)
                if isinstance(current_attribute, dict):
                    # Do a deep update of the dictionary.
                    deep_update(current_attribute, config[key], new_keys_allowed=True)
                else:
                    # Don't do a deep update and directly set the attribute
                    # to the value in config.
                    setattr(self, key, config[key])
                # Remove the key from the dict if it's been set to an
                # attribute.
                del config[key]
        # Return whatever is leftover in config.
        return config


class FunctionTrainer(Trainer):
    def __init__(self, *args, **kwargs):
        super(FunctionTrainer, self).__init__(*args, **kwargs)

    def training_function(
        self,
        config: Dict[str, Any],
        checkpoint: Optional[Checkpoint] = None
    ):
        """Function to define the training logic.

        Implement this the same way you would implement a training function
        for Tune.

        Dataset attributes have already been preprocessed.

        Args:
            config: Configurations for training logic specific implementation.
            checkpoint: A checkpoint to resume from, if applicable.
        """
        raise NotImplementedError

    def resource_func(
        self,
        scaling_config: Dict,
    ) -> PlacementGroupFactory:
        """Converts `scaling_config` to `PlacementGroupFactory`.

        Args:
            scaling_config: The scaling config to convert.
        """
        raise NotImplementedError

    def as_trainable(self):
        def tune_function(config, checkpoint_dir=None):
            leftover_config = self._override_attributes_with_config(config)

            if self.train_dataset:
                if self.preprocessor:
                    self.train_dataset = self.preprocessor.fit_transform(
                        self.train_dataset
                    )

            # Transform all the other datasets concurrently in remote tasks.
            task_dict = {}
            if self.extra_datasets and self.preprocessor:
                for key, dataset in self.extra_datasets.items():
                    remote_transform = ray.remote(num_cpus=0)(
                        lambda: self.preprocessor.transform(dataset)
                    ).remote()
                    task_dict[key] = remote_transform

            ray.get(list(task_dict.values()))

            for key, transformed_dataset in task_dict.items():
                self.extra_datasets[key] = ray.get(transformed_dataset)

            if checkpoint_dir:
                checkpoint = Checkpoint.from_directory(checkpoint_dir)
            else:
                checkpoint = self.checkpoint_to_resume_from

            self.training_function(
                config=leftover_config,
                checkpoint=checkpoint
            )

        trainable_cls = wrap_function(tune_function)

        class TrainTrainable(trainable_cls):
            """Add default resources to the Trainable."""

            @classmethod
            def default_resource_request(cls,
                                         config: Dict) -> PlacementGroupFactory:
                self._override_attributes_with_config(config)
                try:
                    placement_group_factory = self.resource_func(self.scaling_config)
                except NotImplementedError:
                    # resource_fn is not implemented. Default to super class
                    # resource request.
                    placement_group_factory = \
                        trainable_cls.default_resource_request(config)
                return placement_group_factory

        return TrainTrainable
