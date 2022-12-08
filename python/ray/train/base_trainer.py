import abc
import inspect
import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type, Union

import ray
from ray.air._internal.config import ensure_only_allowed_dataclass_keys_updated
from ray.air.checkpoint import Checkpoint
from ray.air.config import RunConfig, ScalingConfig
from ray.air.result import Result
from ray.train.constants import TRAIN_DATASET_KEY
from ray.util import PublicAPI
from ray.util.annotations import DeveloperAPI
from ray._private.dict import merge_dicts

if TYPE_CHECKING:
    from ray.data import Dataset
    from ray.data.preprocessor import Preprocessor

    from ray.tune import Trainable

# A type representing either a ray.data.Dataset or a function that returns a
# ray.data.Dataset and accepts no arguments.
GenDataset = Union["Dataset", Callable[[], "Dataset"]]


logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
class TrainingFailedError(RuntimeError):
    """An error indicating that training has failed."""

    pass


@DeveloperAPI
class BaseTrainer(abc.ABC):
    """Defines interface for distributed training on Ray.

    Note: The base ``BaseTrainer`` class cannot be instantiated directly. Only
    one of its subclasses can be used.

    **How does a trainer work?**

    - First, initialize the Trainer. The initialization runs locally,
      so heavyweight setup should not be done in ``__init__``.
    - Then, when you call ``trainer.fit()``, the Trainer is serialized
      and copied to a remote Ray actor. The following methods are then
      called in sequence on the remote actor.
    - ``trainer.setup()``: Any heavyweight Trainer setup should be
      specified here.
    - ``trainer.preprocess_datasets()``: The provided
      ray.data.Dataset are preprocessed with the provided
      ray.data.Preprocessor.
    - ``trainer.train_loop()``: Executes the main training logic.
    - Calling ``trainer.fit()`` will return a ``ray.result.Result``
      object where you can access metrics from your training run, as well
      as any checkpoints that may have been saved.

    **How do I create a new Trainer?**

    Subclass ``ray.train.trainer.BaseTrainer``, and override the ``training_loop``
    method, and optionally ``setup``.

    .. code-block:: python

        import torch

        from ray.train.trainer import BaseTrainer
        from ray import tune
        from ray.air import session


        class MyPytorchTrainer(BaseTrainer):
            def setup(self):
                self.model = torch.nn.Linear(1, 1)
                self.optimizer = torch.optim.SGD(
                    self.model.parameters(), lr=0.1)

            def training_loop(self):
                # You can access any Trainer attributes directly in this method.
                # self.datasets["train"] has already been
                # preprocessed by self.preprocessor
                dataset = self.datasets["train"]

                torch_ds = dataset.iter_torch_batches(dtypes=torch.float)
                loss_fn = torch.nn.MSELoss()

                for epoch_idx in range(10):
                    loss = 0
                    num_batches = 0
                    for batch in torch_ds:
                        X, y = torch.unsqueeze(batch["x"], 1), batch["y"]
                        # Compute prediction error
                        pred = self.model(X)
                        batch_loss = loss_fn(pred, y)

                        # Backpropagation
                        self.optimizer.zero_grad()
                        batch_loss.backward()
                        self.optimizer.step()

                        loss += batch_loss.item()
                        num_batches += 1
                    loss /= num_batches

                    # Use Tune functions to report intermediate
                    # results.
                    session.report({"loss": loss, "epoch": epoch_idx})

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

    _scaling_config_allowed_keys: List[str] = [
        "trainer_resources",
        "_max_cpu_fraction_per_node",
    ]
    _handles_checkpoint_freq: bool = False
    _handles_checkpoint_at_end: bool = False

    def __init__(
        self,
        *,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional["Preprocessor"] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):

        self.scaling_config = (
            scaling_config if scaling_config is not None else ScalingConfig()
        )
        self.run_config = run_config if run_config is not None else RunConfig()
        self.datasets = datasets if datasets is not None else {}
        self.preprocessor = preprocessor
        self.resume_from_checkpoint = resume_from_checkpoint

        self._validate_attributes()

    def __repr__(self):
        # A dictionary that maps parameters to their default values.
        default_values: Dict[str, Any] = {
            "scaling_config": ScalingConfig(),
            "run_config": RunConfig(),
            "datasets": {},
            "preprocessor": None,
            "resume_from_checkpoint": None,
        }

        non_default_arguments = []
        for parameter, default_value in default_values.items():
            value = getattr(self, parameter)
            if value != default_value:
                non_default_arguments.append(f"{parameter}={value!r}")

        if non_default_arguments:
            return f"<{self.__class__.__name__} {' '.join(non_default_arguments)}>"

        return f"<{self.__class__.__name__}>"

    def __new__(cls, *args, **kwargs):
        # Store the init args as attributes so this can be merged with Tune hparams.
        trainer = super(BaseTrainer, cls).__new__(cls)
        parameters = inspect.signature(cls.__init__).parameters
        parameters = list(parameters.keys())
        # Remove self.
        parameters = parameters[1:]
        arg_dict = dict(zip(parameters, args))
        trainer._param_dict = {**arg_dict, **kwargs}
        return trainer

    def _validate_attributes(self):
        """Called on __init()__ to validate trainer attributes."""
        # Run config
        if not isinstance(self.run_config, RunConfig):
            raise ValueError(
                f"`run_config` should be an instance of `ray.air.RunConfig`, "
                f"found {type(self.run_config)} with value `{self.run_config}`."
            )
        # Scaling config
        if not isinstance(self.scaling_config, ScalingConfig):
            raise ValueError(
                "`scaling_config` should be an instance of `ScalingConfig`, "
                f"found {type(self.scaling_config)} with value `{self.scaling_config}`."
            )
        # Datasets
        if not isinstance(self.datasets, dict):
            raise ValueError(
                f"`datasets` should be a dict mapping from a string to "
                f"`ray.data.Dataset` objects, "
                f"found {type(self.datasets)} with value `{self.datasets}`."
            )
        else:
            for key, dataset in self.datasets.items():
                if isinstance(dataset, ray.data.DatasetPipeline):
                    raise ValueError(
                        f"The Dataset under '{key}' key is a "
                        f"`ray.data.DatasetPipeline`. Only `ray.data.Dataset` are "
                        f"allowed to be passed in.  Pipelined/streaming ingest can be "
                        f"configured via the `dataset_config` arg. See "
                        "https://docs.ray.io/en/latest/ray-air/check-ingest.html#enabling-streaming-ingest"  # noqa: E501
                        "for an example."
                    )
                elif not isinstance(dataset, ray.data.Dataset) and not callable(
                    dataset
                ):
                    raise ValueError(
                        f"The Dataset under '{key}' key is not a `ray.data.Dataset`. "
                        f"Received {dataset} instead."
                    )

        # Preprocessor
        if self.preprocessor is not None and not isinstance(
            self.preprocessor, ray.data.Preprocessor
        ):
            raise ValueError(
                f"`preprocessor` should be an instance of `ray.data.Preprocessor`, "
                f"found {type(self.preprocessor)} with value `{self.preprocessor}`."
            )

        if self.resume_from_checkpoint is not None and not isinstance(
            self.resume_from_checkpoint, ray.air.Checkpoint
        ):
            raise ValueError(
                f"`resume_from_checkpoint` should be an instance of "
                f"`ray.air.Checkpoint`, found {type(self.resume_from_checkpoint)} "
                f"with value `{self.resume_from_checkpoint}`."
            )

    @classmethod
    def _validate_scaling_config(cls, scaling_config: ScalingConfig) -> ScalingConfig:
        """Return scaling config dataclass after validating updated keys."""
        ensure_only_allowed_dataclass_keys_updated(
            dataclass=scaling_config,
            allowed_keys=cls._scaling_config_allowed_keys,
        )
        return scaling_config

    def setup(self) -> None:
        """Called during fit() to perform initial setup on the Trainer.

        .. note:: This method is run on a remote process.

        This method will not be called on the driver, so any expensive setup
        operations should be placed here and not in ``__init__``.

        This method is called prior to ``preprocess_datasets`` and
        ``training_loop``.
        """
        pass

    def preprocess_datasets(self) -> None:
        """Called during fit() to preprocess dataset attributes with preprocessor.

        .. note:: This method is run on a remote process.

        This method is called prior to entering the training_loop.

        If the ``Trainer`` has both a datasets dict and
        a preprocessor, the datasets dict contains a training dataset (denoted by
        the "train" key), and the preprocessor has not yet
        been fit, then it will be fit on the train dataset.

        Then, all Trainer's datasets will be transformed by the preprocessor.

        The transformed datasets will be set back in the ``self.datasets`` attribute
        of the Trainer to be used when overriding ``training_loop``.
        """
        # Evaluate all datasets.
        self.datasets = {k: d() if callable(d) else d for k, d in self.datasets.items()}

        if self.preprocessor:
            train_dataset = self.datasets.get(TRAIN_DATASET_KEY, None)
            if train_dataset:
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

        .. note:: This method runs on a remote process.

        ``self.datasets`` have already been preprocessed by ``self.preprocessor``.

        You can use the :ref:`Tune Function API functions <tune-function-docstring>`
        (``session.report()`` and ``session.get_checkpoint()``) inside
        this training loop.

        Example:

        .. code-block:: python

            from ray.train.trainer import BaseTrainer

            class MyTrainer(BaseTrainer):
                def training_loop(self):
                    for epoch_idx in range(5):
                        ...
                        session.report({"epoch": epoch_idx})

        """
        raise NotImplementedError

    @PublicAPI(stability="beta")
    def fit(self) -> Result:
        """Runs training.

        Returns:
            A Result object containing the training result.

        Raises:
            TrainingFailedError: If any failures during the execution of
            ``self.as_trainable()``.
        """
        from ray.tune.tuner import Tuner
        from ray.tune.error import TuneError

        trainable = self.as_trainable()

        tuner = Tuner(trainable=trainable, run_config=self.run_config)
        result_grid = tuner.fit()
        assert len(result_grid) == 1
        try:
            result = result_grid[0]
            if result.error:
                raise result.error
        except TuneError as e:
            raise TrainingFailedError from e
        return result

    def _generate_trainable_cls(self) -> Type["Trainable"]:
        """Generate the base Trainable class.

        Returns:
            A Trainable class to use for training.
        """

        from ray.tune.execution.placement_groups import PlacementGroupFactory
        from ray.tune.trainable import wrap_function

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

        # Change the name of the training function to match the name of the Trainer
        # class. This will mean the Tune trial name will match the name of Trainer on
        # stdout messages and the results directory.
        train_func.__name__ = trainer_cls.__name__

        trainable_cls = wrap_function(train_func, warn=False)
        has_base_dataset = bool(self.datasets)
        if has_base_dataset:
            from ray.data.context import DatasetContext

            dataset_context = DatasetContext.get_current()
        else:
            dataset_context = None

        class TrainTrainable(trainable_cls):
            """Add default resources to the Trainable."""

            _handles_checkpoint_freq = trainer_cls._handles_checkpoint_freq
            _handles_checkpoint_at_end = trainer_cls._handles_checkpoint_at_end

            @classmethod
            def has_base_dataset(cls) -> bool:
                """Whether a dataset is provided through the Trainer."""
                return has_base_dataset

            @classmethod
            def base_scaling_config(cls) -> ScalingConfig:
                """Returns the unchanged scaling config provided through the Trainer."""
                return scaling_config

            def setup(self, config, **kwargs):
                base_config = dict(kwargs)
                # Create a new config by merging the dicts.
                # run_config is not a tunable hyperparameter so it does not need to be
                # merged.
                run_config = base_config.pop("run_config", None)
                self._merged_config = merge_dicts(base_config, self.config)
                self._merged_config["run_config"] = run_config
                merged_scaling_config = self._merged_config.get("scaling_config")
                if isinstance(merged_scaling_config, dict):
                    merged_scaling_config = ScalingConfig(**merged_scaling_config)
                self._merged_config[
                    "scaling_config"
                ] = self._reconcile_scaling_config_with_trial_resources(
                    merged_scaling_config
                )
                if self.has_base_dataset():
                    # Set the DatasetContext on the Trainer actor to the DatasetContext
                    # specified on the driver.
                    DatasetContext._set_current(dataset_context)
                super(TrainTrainable, self).setup(config)

            def _reconcile_scaling_config_with_trial_resources(
                self, scaling_config: ScalingConfig
            ) -> ScalingConfig:
                """
                ResourceChangingScheduler workaround.

                Ensures that the scaling config matches trial resources.

                This should be replaced with RCS returning a ScalingConfig
                in the future.
                """

                trial_resources = self.trial_resources
                # This will be false if the resources are default
                if not isinstance(trial_resources, PlacementGroupFactory):
                    return scaling_config

                if scaling_config:
                    scaling_config = trainer_cls._validate_scaling_config(
                        scaling_config
                    )
                scaling_config_from_trial_resources = (
                    ScalingConfig.from_placement_group_factory(trial_resources)
                )

                # This check should always pass if ResourceChangingScheduler is not
                # used.
                if scaling_config_from_trial_resources != scaling_config:
                    scaling_config = trainer_cls._validate_scaling_config(
                        scaling_config_from_trial_resources
                    )
                return scaling_config

            def _trainable_func(self, config, reporter, checkpoint_dir):
                # We ignore the config passed by Tune and instead use the merged
                # config which includes the initial Trainer args.
                super()._trainable_func(self._merged_config, reporter, checkpoint_dir)

            @classmethod
            def default_resource_request(cls, config):
                # `config["scaling_config"] is a dataclass when passed via the
                # `scaling_config` argument in `Trainer` and is a dict when passed
                # via the `scaling_config` key of `param_spec`.

                # Conversion logic must be duplicated in `TrainTrainable.__init__`
                # because this is a class method.
                updated_scaling_config = config.get("scaling_config", scaling_config)
                if isinstance(updated_scaling_config, dict):
                    updated_scaling_config = ScalingConfig(**updated_scaling_config)
                validated_scaling_config = trainer_cls._validate_scaling_config(
                    updated_scaling_config
                )
                return validated_scaling_config.as_placement_group_factory()

        return TrainTrainable

    def as_trainable(self) -> Type["Trainable"]:
        """Convert self to a ``tune.Trainable`` class."""
        from ray import tune

        base_config = self._param_dict
        trainable_cls = self._generate_trainable_cls()

        # Wrap with `tune.with_parameters` to handle very large values in base_config
        return tune.with_parameters(trainable_cls, **base_config)
