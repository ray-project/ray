import abc
import dataclasses
import logging
from typing import Dict, Union, Callable, Optional

from ray.data import Dataset
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.config import ScalingConfig, RunConfig
from ray.ml.result import Result
from ray import tune
from ray.tune.trainable import ConvertibleToTrainable
from ray.tune.utils import deep_update

GenDataset = Union[Dataset, Callable[[], Dataset]]


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
            :ref:`Dataset <dataset-api>` or a Callbable that returns a Dataset,
            to use for training. If a ``preprocessor`` is also provided,
            it will be fit on this dataset and this dataset will be
            transformed.
        additional_datasets (Optional[GenDataset]): Any additional
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
