import abc
import logging
from typing import Dict, Union, Callable, Optional, TYPE_CHECKING, Type

from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.result import Result
from ray.ml.config import RunConfig, ScalingConfig
from ray.tune import Trainable
from ray.util import PublicAPI
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.data import Dataset

# A type representing either a ray.data.Dataset or a function that returns a
# ray.data.Dataset and accepts no arguments.
GenDataset = Union["Dataset", Callable[[], "Dataset"]]


logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class TrainingFailedError(RuntimeError):
    """An error indicating that training has failed."""

    pass


@PublicAPI(stability="alpha")
class Trainer(abc.ABC):
    """Defines interface for distributed training on Ray.

    Note: The base ``Trainer`` class cannot be instantiated directly. Only
    one of its subclasses can be used.

    How does a trainer work?
        - First, initialize the Trainer. The initialization runs locally,
        so heavyweight setup should not be done in __init__
        - Then, when you call ``trainer.fit()``, the Trainer is serialized
        and copied to a remote Ray actor. The following methods are then
        called in sequence on the remote actor.
            - ``trainer.setup()``: Any heavyweight Trainer setup should be
            specified here.
            - ``trainer.preprocess_datasets()``: The provided
            ray.data.Dataset are preprocessed with the provided
            ray.ml.preprocessor.
            - ``trainer.train_loop()``: Executes the main training logic.
        - Calling ``trainer.fit()`` will return a ``ray.result.Result``
        object where you can access metrics from your training run, as well
        as any checkpoints that may have been saved.

    How do I create a new ``Trainer``?

    Subclass ``ray.train.Trainer``, and override the ``training_loop``
    method, and optionally ``setup``.

        Example:

            .. code-block:: python

                import torch

                from ray.ml.trainer import Trainer
                from ray import tune


                class MyPytorchTrainer(Trainer):
                    def setup(self):
                        self.model = torch.nn.Linear(1, 1)
                        self.optimizer = torch.optim.SGD(
                            self.model.parameters(), lr=0.1)

                    def training_loop(self):
                        # You can access any Trainer attributes directly in this method.
                        # self.train_dataset has already been preprocessed by
                        # self.preprocessor
                        dataset = self.train_dataset

                        torch_ds = dataset.to_torch()

                        for epoch_idx in range(10):
                            loss = 0
                            num_batches = 0
                            for X, y in iter(torch_ds):
                                # Compute prediction error
                                pred = self.model(X)
                                batch_loss = torch.nn.MSELoss(pred, y)

                                # Backpropagation
                                self.optimizer.zero_grad()
                                batch_loss.backward()
                                self.optimizer.step()

                                loss += batch_loss.item()
                                num_batches += 1
                            loss /= num_batches

                            # Use Tune functions to report intermediate
                            # results.
                            tune.report(loss=loss, epoch=epoch_idx)


    How do I use an existing ``Trainer`` or one of my custom Trainers?

    Initialize the Trainer, and call Trainer.fit()

        Example:
              .. code-block:: python

                import ray

                train_dataset = ray.data.from_items([1, 2, 3])
                my_trainer = MyPytorchTrainer(train_dataset=train_dataset)
                result = my_trainer.fit()

    Args:
        scaling_config: Configuration for how to scale training.
        run_config: Configuration for the execution of the training run.
        train_dataset: Either a distributed Ray :ref:`Dataset <dataset-api>`
            or a Callable that returns a Dataset, to use for training. If a
            ``preprocessor`` is also provided, it will be fit on this
            dataset and this dataset will be transformed.
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

        raise NotImplementedError

    @DeveloperAPI
    def setup(self) -> None:
        """Perform initial setup on the Trainer.

        Note: this method is run on a remote process.

        This method will not be called on the driver, so any expensive setup
        operations should be placed here and not in ``__init__``.

        This method is called prior to ``_preprocess_datasets`` and
        ``training_loop``.
        """
        raise NotImplementedError

    def _preprocess_datasets(self) -> None:
        """Preprocess dataset attributes with provided preprocessor.

        Note: This method is run on a remote process.

        This method is called prior to entering the training_loop.

        If the ``Trainer`` has both a train_dataset and
        preprocessor, and the preprocessor has not yet been fit, then it
        will be fit on the train_dataset.

        Then, the Trainer's train_dataset and any extra_datasets
        will be transformed by its preprocessor.

        The transformed datasets will be set back in the
        ``self.train_dataset`` and ``self.extra_datasets`` attributes to be
        used when overriding ``training_loop``.
        """
        raise NotImplementedError

    @DeveloperAPI
    @abc.abstractmethod
    def training_loop(self) -> None:
        """Loop for distributed training and result reporting to Tune.

        Note: this method runs on a remote process.

        `self.train_dataset` and the Dataset values in `self.extra_datasets`
        have already been preprocessed by `self.preprocessor`.'

        You can use the :ref:`Tune Function API functions <tune-function-docstring>`
        (``tune.report()`` and ``tune.save_checkpoint()``) inside
        this training loop.

        Example:
            .. code-block: python

                from ray.ml.trainer import Trainer

                class MyTrainer(Trainer):
                    def training_loop(self):
                        for epoch_idx in range(5):
                            ...
                            tune.report(epoch=epoch_idx)

        """
        raise NotImplementedError

    @PublicAPI(stability="alpha")
    def fit(self) -> Result:
        """Runs training.

        Returns:
            A Result object containing the training result.

        Raises:
            TrainingFailedError: If any failures during the execution of
            ``self.as_trainable()``.
        """
        raise NotImplementedError

    @PublicAPI(stability="alpha")
    def as_trainable(self) -> Type[Trainable]:
        """Convert self to a ``tune.Trainable`` class."""
        raise NotImplementedError
