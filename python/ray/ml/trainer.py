import abc
import inspect
import logging
from typing import Dict, Union, Callable, Optional, TYPE_CHECKING, Type

from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.result import Result
from ray.ml.config import RunConfig, ScalingConfig, ScalingConfigDataClass
from ray.ml.constants import TRAIN_DATASET_KEY
from ray.tune import Trainable
from ray.tune.error import TuneError
from ray.tune.function_runner import wrap_function
from ray.util import PublicAPI
from ray.util.annotations import DeveloperAPI
from ray.util.ml_utils.dict import merge_dicts

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


@DeveloperAPI
class Trainer(abc.ABC):
    """Defines interface for distributed training on Ray.

    Note: The base ``Trainer`` class cannot be instantiated directly. Only
    one of its subclasses can be used.

    How does a trainer work?

        - First, initialize the Trainer. The initialization runs locally,
          so heavyweight setup should not be done in __init__.
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

    **How do I create a new Trainer?**

    Subclass ``ray.train.Trainer``, and override the ``training_loop``
    method, and optionally ``setup``.

    .. code-block:: python

        import torch

        from ray.ml.train import Trainer
        from ray import tune


        class MyPytorchTrainer(Trainer):
            def setup(self):
                self.model = torch.nn.Linear(1, 1)
                self.optimizer = torch.optim.SGD(
                    self.model.parameters(), lr=0.1)

            def training_loop(self):
                # You can access any Trainer attributes directly in this method.
                # self.datasets["train"] has already been
                # preprocessed by self.preprocessor
                dataset = self.datasets["train"]

                torch_ds = dataset.to_torch(label_column="y")
                loss_fn = torch.nn.MSELoss()

                for epoch_idx in range(10):
                    loss = 0
                    num_batches = 0
                    for X, y in iter(torch_ds):
                        # Compute prediction error
                        pred = self.model(X)
                        batch_loss = loss_fn(pred, y.float())

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

    **How do I use an existing Trainer or one of my custom Trainers?**

    Initialize the Trainer, and call Trainer.fit()

    .. code-block:: python

        import ray
        train_dataset = ray.data.from_items(
            [{"x": i, "y": i} for i in range(3)])
        my_trainer = MyPytorchTrainer(datasets={"train": train_dataset})
        result = my_trainer.fit()


    Args:
        scaling_config: Configuration for how to scale training.
        run_config: Configuration for the execution of the training run.
        datasets: Any Ray Datasets to use for training. Use the key "train"
            to denote which dataset is the training
            dataset. If a ``preprocessor`` is provided and has not already been fit,
            it will be fit on the training dataset. All datasets will be transformed
            by the ``preprocessor`` if one is provided.
        preprocessor: A preprocessor to preprocess the provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    def __init__(
        self,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):

        self.scaling_config = scaling_config if scaling_config else {}
        self.run_config = run_config if run_config else RunConfig()
        self.datasets = datasets if datasets else {}
        self.preprocessor = preprocessor
        self.resume_from_checkpoint = resume_from_checkpoint

    def __new__(cls, *args, **kwargs):
        """Store the init args as attributes so this can be merged with Tune hparams."""
        trainer = super(Trainer, cls).__new__(cls)
        parameters = inspect.signature(cls.__init__).parameters
        parameters = list(parameters.keys())
        # Remove self.
        parameters = parameters[1:]
        arg_dict = dict(zip(parameters, args))
        trainer._param_dict = {**arg_dict, **kwargs}
        return trainer

    def setup(self) -> None:
        """Called during fit() to perform initial setup on the Trainer.

        Note: this method is run on a remote process.

        This method will not be called on the driver, so any expensive setup
        operations should be placed here and not in ``__init__``.

        This method is called prior to ``preprocess_datasets`` and
        ``training_loop``.
        """
        pass

    def preprocess_datasets(self) -> None:
        """Called during fit() to preprocess dataset attributes with preprocessor.

        Note: This method is run on a remote process.

        This method is called prior to entering the training_loop.

        If the ``Trainer`` has both a datasets dict and
        a preprocessor, the datasets dict contains a training dataset (denoted by
        the "train" key), and the preprocessor has not yet
        been fit, then it will be fit on the train.

        Then, the Trainer's datasets will be transformed by the preprocessor.

        The transformed datasets will be set back in the ``self.datasets`` attribute
        of the Trainer to be used when overriding ``training_loop``.
        """
        # Evaluate all datasets.
        self.datasets = {k: d() if callable(d) else d for k, d in self.datasets.items()}

        if self.preprocessor:
            train_dataset = self.datasets.get(TRAIN_DATASET_KEY, None)
            if train_dataset and not self.preprocessor.check_is_fitted():
                self.preprocessor.fit(train_dataset)

            # Execute dataset transformations serially for now.
            # Cannot execute them in remote tasks due to dataset ownership model:
            # if datasets are created on a remote node, then if that node fails,
            # we cannot recover the dataset.
            new_datasets = {}
            for key, dataset in self.datasets.items():
                new_datasets[key] = self.preprocessor.transform(dataset)

            self.datasets = new_datasets

    @abc.abstractmethod
    def training_loop(self) -> None:
        """Loop called by fit() to run training and report results to Tune.

        Note: this method runs on a remote process.

        ``self.datasets`` have already been preprocessed by ``self.preprocessor``.

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
        from ray.tune.tuner import Tuner

        trainable = self.as_trainable()

        tuner = Tuner(trainable=trainable, run_config=self.run_config)
        result_grid = tuner.fit()
        assert len(result_grid) == 1
        try:
            result = result_grid[0]
            if result.error:
                raise result.error
        except TuneError:
            raise TrainingFailedError
        return result

    def as_trainable(self) -> Type[Trainable]:
        """Convert self to a ``tune.Trainable`` class."""

        base_config = self._param_dict
        trainer_cls = self.__class__
        scaling_config = self.scaling_config

        def train_func(config, checkpoint_dir=None):
            # config already contains merged values.
            # Instantiate new Trainer in Trainable.
            trainer = trainer_cls(**config)

            if checkpoint_dir:
                trainer.resume_from_checkpoint = Checkpoint.from_directory(
                    checkpoint_dir
                )

            trainer.setup()
            trainer.preprocess_datasets()
            trainer.training_loop()

        trainable_cls = wrap_function(train_func)

        class TrainTrainable(trainable_cls):
            """Add default resources to the Trainable."""

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)

                # Create a new config by merging the dicts.
                self._merged_config = merge_dicts(base_config, self.config)

            def _trainable_func(self, config, reporter, checkpoint_dir):
                # We ignore the config passed by Tune and instead use the merged
                # config which includes the initial Trainer args.
                super()._trainable_func(self._merged_config, reporter, checkpoint_dir)

            @classmethod
            def default_resource_request(cls, config):
                updated_scaling_config = config.get("scaling_config", scaling_config)
                scaling_config_dataclass = ScalingConfigDataClass(
                    **updated_scaling_config
                )
                return scaling_config_dataclass.as_placement_group_factory()

        return TrainTrainable
