import abc
import copy
import inspect
import json
import logging
import os
from pathlib import Path
import tempfile
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type, Union
import warnings

import pyarrow.fs

import ray
import ray.cloudpickle as pickle
from ray.air._internal.config import ensure_only_allowed_dataclass_keys_updated
from ray.air._internal.remote_storage import (
    download_from_uri,
    is_non_local_path_uri,
    list_at_uri,
)
from ray.air._internal import usage as air_usage
from ray.air._internal.uri_utils import URI
from ray.air._internal.usage import AirEntrypoint
from ray.air.config import RunConfig, ScalingConfig
from ray.air.result import Result
from ray.train._internal import session
from ray.train._internal.storage import (
    _exists_at_fs_path,
    _use_storage_context,
    get_fs_and_path,
)

from ray.train import Checkpoint
from ray.train.constants import TRAIN_DATASET_KEY
from ray.util import PublicAPI
from ray.util.annotations import DeveloperAPI
from ray._private.dict import merge_dicts


if TYPE_CHECKING:
    from ray.data import Dataset
    from ray.data.preprocessor import Preprocessor

    from ray.tune import Trainable

_TRAINER_PKL = "trainer.pkl"

# A type representing either a ray.data.Dataset or a function that returns a
# ray.data.Dataset and accepts no arguments.
GenDataset = Union["Dataset", Callable[[], "Dataset"]]


logger = logging.getLogger(__name__)

PREPROCESSOR_DEPRECATION_MESSAGE = (
    "The `preprocessor` argument to Trainers is deprecated as of Ray 2.7. "
    "Instead, use the Preprocessor `fit` and `transform` APIs directly on the Ray "
    "Dataset. For any state that needs to be saved to the trained checkpoint, pass it "
    "in using the `metadata` argument of the `Trainer`. "
    "For a full example, see "
    "https://docs.ray.io/en/master/train/user-guides/data-loading-preprocessing.html#preprocessing-structured-data "  # noqa:E501
)


@PublicAPI(stability="beta")
class TrainingFailedError(RuntimeError):
    """An error indicating that training has failed."""

    _RESTORE_MSG = (
        "The Ray Train run failed. Please inspect the previous error messages for a "
        "cause. After fixing the issue (assuming that the error is not caused by "
        "your own application logic, but rather an error such as OOM), you can restart "
        "the run from scratch or continue this run.\n"
        "To continue this run, you can use: "
        '`trainer = {trainer_cls_name}.restore("{path}")`.'
    )

    _FAILURE_CONFIG_MSG = (
        "To start a new run that will retry on training failures, set "
        "`train.RunConfig(failure_config=train.FailureConfig(max_failures))` "
        "in the Trainer's `run_config` with `max_failures > 0`, or `max_failures = -1` "
        "for unlimited retries."
    )


@DeveloperAPI
class BaseTrainer(abc.ABC):
    """Defines interface for distributed training on Ray.

    Note: The base ``BaseTrainer`` class cannot be instantiated directly. Only
    one of its subclasses can be used.

    Note to developers: If a new trainer is added, please update
    `air/_internal/usage.py`.

    **How does a trainer work?**

    - First, initialize the Trainer. The initialization runs locally,
      so heavyweight setup should not be done in ``__init__``.
    - Then, when you call ``trainer.fit()``, the Trainer is serialized
      and copied to a remote Ray actor. The following methods are then
      called in sequence on the remote actor.
    - ``trainer.setup()``: Any heavyweight Trainer setup should be
      specified here.
    - ``trainer.preprocess_datasets()``: The datasets passed to the Trainer will be
      setup here.
    - ``trainer.train_loop()``: Executes the main training logic.
    - Calling ``trainer.fit()`` will return a ``ray.result.Result``
      object where you can access metrics from your training run, as well
      as any checkpoints that may have been saved.

    **How do I create a new Trainer?**

    Subclass ``ray.train.trainer.BaseTrainer``, and override the ``training_loop``
    method, and optionally ``setup``.

    .. testcode::

        import torch

        from ray.train.trainer import BaseTrainer
        from ray import train, tune


        class MyPytorchTrainer(BaseTrainer):
            def setup(self):
                self.model = torch.nn.Linear(1, 1)
                self.optimizer = torch.optim.SGD(
                    self.model.parameters(), lr=0.1)

            def training_loop(self):
                # You can access any Trainer attributes directly in this method.
                # self.datasets["train"] has already been
                dataset = self.datasets["train"]

                torch_ds = dataset.iter_torch_batches(dtypes=torch.float)
                loss_fn = torch.nn.MSELoss()

                for epoch_idx in range(10):
                    loss = 0
                    num_batches = 0
                    torch_ds = dataset.iter_torch_batches(
                        dtypes=torch.float, batch_size=2
                    )
                    for batch in torch_ds:
                        X = torch.unsqueeze(batch["x"], 1)
                        y = torch.unsqueeze(batch["y"], 1)
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
                    train.report({"loss": loss, "epoch": epoch_idx})


        # Initialize the Trainer, and call Trainer.fit()
        import ray
        train_dataset = ray.data.from_items(
            [{"x": i, "y": i} for i in range(10)])
        my_trainer = MyPytorchTrainer(datasets={"train": train_dataset})
        result = my_trainer.fit()

    .. testoutput::
            :hide:

            ...

    Args:
        scaling_config: Configuration for how to scale training.
        run_config: Configuration for the execution of the training run.
        datasets: Any Datasets to use for training. Use the key "train"
            to denote which dataset is the training dataset.
        metadata: Dict that should be made available via
            `train.get_context().get_metadata()` and in `checkpoint.get_metadata()`
            for checkpoints saved from this Trainer. Must be JSON-serializable.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    _scaling_config_allowed_keys: List[str] = [
        "trainer_resources",
    ]
    _handles_checkpoint_freq: bool = False
    _handles_checkpoint_at_end: bool = False

    # fields to propagate to Tuner param_space.
    # See `BaseTrainer._extract_fields_for_tuner_param_space` for more details.
    _fields_for_tuner_param_space = []

    def __init__(
        self,
        *,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
        # Deprecated.
        preprocessor: Optional["Preprocessor"] = None,
    ):
        self.scaling_config = (
            scaling_config if scaling_config is not None else ScalingConfig()
        )
        self.run_config = run_config if run_config is not None else RunConfig()
        self.metadata = metadata
        self.datasets = datasets if datasets is not None else {}
        self.preprocessor = preprocessor
        self.starting_checkpoint = resume_from_checkpoint

        # These attributes should only be set through `BaseTrainer.restore`
        self._restore_path = None
        self._restore_storage_filesystem = None

        self._validate_attributes()

        air_usage.tag_air_trainer(self)

        if preprocessor is not None:
            raise DeprecationWarning(PREPROCESSOR_DEPRECATION_MESSAGE)

    @PublicAPI(stability="alpha")
    @classmethod
    def restore(
        cls: Type["BaseTrainer"],
        path: Union[str, os.PathLike],
        storage_filesystem: Optional[pyarrow.fs.FileSystem] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional["Preprocessor"] = None,
        scaling_config: Optional[ScalingConfig] = None,
        **kwargs,
    ) -> "BaseTrainer":
        """Restores a Train experiment from a previously interrupted/failed run.

        Restore should be used for experiment-level fault tolerance in the event
        that the head node crashes (e.g., OOM or some other runtime error) or the
        entire cluster goes down (e.g., network error affecting all nodes).

        The following example can be paired with implementing job retry using
        :ref:`Ray Jobs <jobs-overview>` to produce a Train experiment that will
        attempt to resume on both experiment-level and trial-level failures:

        .. testcode::

            import os
            import ray
            from ray import train
            from ray.train.trainer import BaseTrainer

            experiment_name = "unique_experiment_name"
            storage_path = os.path.expanduser("~/ray_results")
            experiment_dir = os.path.join(storage_path, experiment_name)

            # Define some dummy inputs for demonstration purposes
            datasets = {"train": ray.data.from_items([{"a": i} for i in range(10)])}

            class CustomTrainer(BaseTrainer):
                def training_loop(self):
                    pass

            if CustomTrainer.can_restore(experiment_dir):
                trainer = CustomTrainer.restore(
                    experiment_dir, datasets=datasets
                )
            else:
                trainer = CustomTrainer(
                    datasets=datasets,
                    run_config=train.RunConfig(
                        name=experiment_name,
                        storage_path=storage_path,
                        # Tip: You can also enable retries on failure for
                        # worker-level fault tolerance
                        failure_config=train.FailureConfig(max_failures=3),
                    ),
                )

            result = trainer.fit()

        .. testoutput::
            :hide:

            ...

        Args:
            path: The path to the experiment directory of the training run to restore.
                This can be a local path or a remote URI if the experiment was
                uploaded to the cloud.
            datasets: Re-specified datasets used in the original training run.
                This must include all the datasets that were passed in the
                original trainer constructor.
            scaling_config: Optionally re-specified scaling config. This can be
                modified to be different from the original spec.
            **kwargs: Other optionally re-specified arguments, passed in by subclasses.

        Raises:
            ValueError: If all datasets were not re-supplied on restore.

        Returns:
            BaseTrainer: A restored instance of the class that is calling this method.
        """
        if not cls.can_restore(path, storage_filesystem):
            raise ValueError(
                f"Invalid restore path: {path}. Make sure that this path exists and "
                "is the experiment directory that results from a call to "
                "`trainer.fit()`."
            )
        if _use_storage_context():
            fs, fs_path = get_fs_and_path(path, storage_filesystem)
            with fs.open_input_file(os.path.join(fs_path, _TRAINER_PKL)) as f:
                trainer_cls, param_dict = pickle.loads(f.readall())
        else:
            trainer_state_path = cls._maybe_sync_down_trainer_state(path)
            assert trainer_state_path.exists()

            with open(trainer_state_path, "rb") as fp:
                trainer_cls, param_dict = pickle.load(fp)

        if trainer_cls is not cls:
            warnings.warn(
                f"Invalid trainer type. You are attempting to restore a trainer of type"
                f" {trainer_cls} with `{cls.__name__}.restore`, "
                "which will most likely fail. "
                f"Use `{trainer_cls.__name__}.restore` instead."
            )

        original_datasets = param_dict.pop("datasets", {})
        if original_datasets and not datasets:
            raise ValueError(
                "The following datasets need to be provided again on restore: "
                f"{list(original_datasets.keys())}\n"
                f"Use {cls.__name__}.restore(..., datasets=datasets) "
                "with the datasets that were provided to the original trainer."
            )
        datasets = datasets or {}
        if set(original_datasets) != set(datasets):
            raise ValueError(
                "The provided datasets don't match the original dataset keys.\n"
                f"  Expected datasets for the keys: {list(original_datasets.keys())}\n"
                f"  Actual datasets provided: {list(datasets.keys())}"
            )
        param_dict["datasets"] = datasets

        # If no preprocessor is re-specified, then it will be set to None
        # here and loaded from the latest checkpoint
        param_dict["preprocessor"] = preprocessor

        if scaling_config:
            param_dict["scaling_config"] = scaling_config

        for param_name, val in kwargs.items():
            # Overwrite the old value if something is passed into restore
            if val is not None:
                param_dict[param_name] = val

        try:
            trainer = cls(**param_dict)
        except Exception as e:
            raise ValueError(
                "Trainer restoration failed (see above for the stack trace). "
                "Make sure that you use the right trainer class to restore: "
                f"`{cls.__name__}.restore`\n"
            ) from e
        trainer._restore_path = path
        trainer._restore_storage_filesystem = storage_filesystem
        return trainer

    @PublicAPI(stability="alpha")
    @classmethod
    def can_restore(
        cls: Type["BaseTrainer"],
        path: Union[str, os.PathLike],
        storage_filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> bool:
        """Checks whether a given directory contains a restorable Train experiment.

        Args:
            path: The path to the experiment directory of the Train experiment.
                This can be either a local directory (e.g., ~/ray_results/exp_name)
                or a remote URI (e.g., s3://bucket/exp_name).

        Returns:
            bool: Whether this path exists and contains the trainer state to resume from
        """
        if _use_storage_context():
            fs, fs_path = get_fs_and_path(path, storage_filesystem)
            return _exists_at_fs_path(fs, os.path.join(fs_path, _TRAINER_PKL))

        return _TRAINER_PKL in list_at_uri(str(path))

    def __repr__(self):
        # A dictionary that maps parameters to their default values.
        default_values: Dict[str, Any] = {
            "scaling_config": ScalingConfig(),
            "run_config": RunConfig(),
            "datasets": {},
            "starting_checkpoint": None,
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
                f"`run_config` should be an instance of `ray.train.RunConfig`, "
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
                        f"The Dataset under '{key}' key is not a "
                        "`ray.data.Dataset`. "
                        f"Received {dataset} instead."
                    )
        # Metadata.
        self.metadata = self.metadata or {}
        if not isinstance(self.metadata, dict):
            raise TypeError(
                f"The provided metadata must be a dict, was {type(self.metadata)}."
            )
        try:
            self.metadata = json.loads(json.dumps(self.metadata))
        except Exception as e:
            raise ValueError(
                "The provided metadata must be JSON-serializable: "
                f"{self.metadata}: {e}"
            )

        # Preprocessor
        if self.preprocessor is not None and not isinstance(
            self.preprocessor, ray.data.Preprocessor
        ):
            raise ValueError(
                f"`preprocessor` should be an instance of `ray.data.Preprocessor`, "
                f"found {type(self.preprocessor)} with value `{self.preprocessor}`."
            )

        if self.starting_checkpoint is not None and not isinstance(
            self.starting_checkpoint, Checkpoint
        ):
            raise ValueError(
                f"`resume_from_checkpoint` should be an instance of "
                f"`ray.train.Checkpoint`, found {type(self.starting_checkpoint)} "
                f"with value `{self.starting_checkpoint}`."
            )

    @classmethod
    def _validate_scaling_config(cls, scaling_config: ScalingConfig) -> ScalingConfig:
        """Returns scaling config dataclass after validating updated keys."""
        ensure_only_allowed_dataclass_keys_updated(
            dataclass=scaling_config,
            allowed_keys=cls._scaling_config_allowed_keys,
        )
        return scaling_config

    @classmethod
    def _maybe_sync_down_trainer_state(cls, restore_path: str) -> Path:
        """Syncs down trainer state from remote storage.

        Returns:
            str: Local directory containing the trainer state
        """
        if not is_non_local_path_uri(restore_path):
            return Path(os.path.expanduser(restore_path)) / _TRAINER_PKL

        tempdir = Path(tempfile.mkdtemp("tmp_experiment_dir"))

        uri = URI(restore_path)
        download_from_uri(str(uri / _TRAINER_PKL), str(tempdir / _TRAINER_PKL))
        return tempdir / _TRAINER_PKL

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
            if train_dataset and self.preprocessor.fit_status() in (
                ray.data.Preprocessor.FitStatus.NOT_FITTED,
                ray.data.Preprocessor.FitStatus.PARTIALLY_FITTED,
            ):
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
        (``train.report()`` and ``train.get_checkpoint()``) inside
        this training loop.

        Example:

        .. testcode::

            from ray.train.trainer import BaseTrainer
            from ray import train

            class MyTrainer(BaseTrainer):
                def training_loop(self):
                    for epoch_idx in range(5):
                        ...
                        train.report({"epoch": epoch_idx})

        """
        raise NotImplementedError

    @PublicAPI(stability="beta")
    def fit(self) -> Result:
        """Runs training.

        Returns:
            A Result object containing the training result.

        Raises:
            TrainingFailedError: If any failures during the execution of
            ``self.as_trainable()``, or during the Tune execution loop.
        """
        from ray.tune.tuner import Tuner, TunerInternal
        from ray.tune import TuneError

        trainable = self.as_trainable()
        param_space = self._extract_fields_for_tuner_param_space()

        if self._restore_path:
            tuner = Tuner.restore(
                path=self._restore_path,
                trainable=trainable,
                param_space=param_space,
                resume_unfinished=True,
                resume_errored=True,
                storage_filesystem=self._restore_storage_filesystem,
            )
        else:
            tuner = Tuner(
                trainable=trainable,
                param_space=param_space,
                run_config=self.run_config,
                _entrypoint=AirEntrypoint.TRAINER,
            )

        experiment_local_path, _ = TunerInternal.setup_create_experiment_checkpoint_dir(
            trainable, self.run_config
        )

        experiment_local_path = Path(experiment_local_path)
        self._save(experiment_local_path)

        restore_msg = TrainingFailedError._RESTORE_MSG.format(
            trainer_cls_name=self.__class__.__name__,
            path=str(experiment_local_path),
        )

        try:
            result_grid = tuner.fit()
        except TuneError as e:
            # Catch any `TuneError`s raised by the `Tuner.fit` call.
            # Unwrap the `TuneError` if needed.
            parent_error = e.__cause__ or e

            # Raise it to the user as a `TrainingFailedError` with a message to restore.
            raise TrainingFailedError(restore_msg) from parent_error
        # Other exceptions get passed through directly (ex: on `fail_fast='raise'`)

        assert len(result_grid) == 1
        result = result_grid[0]
        if result.error:
            # Raise trainable errors to the user with a message to restore
            # or configure `FailureConfig` in a new run.
            raise TrainingFailedError(
                "\n".join([restore_msg, TrainingFailedError._FAILURE_CONFIG_MSG])
            ) from result.error
        return result

    def _save(self, experiment_path: Union[str, Path]):
        """Saves the current trainer's class along with the `param_dict` of
        parameters passed to this trainer's constructor.

        This is used to recreate the trainer on restore.
        Unless a parameter is re-specified during restoration (only a subset
        of parameters can be passed in again), that parameter will be loaded
        from the saved copy.

        Datasets should not be saved as part of the state. Instead, we save the
        keys and replace the dataset values with dummy functions that will
        raise an error if invoked. The error only serves as a guardrail for
        misuse (e.g., manually unpickling and constructing the Trainer again)
        and is not typically surfaced, since datasets must be re-specified
        upon restoration.
        """
        param_dict = self._param_dict.copy()
        datasets = param_dict.pop("datasets", {})

        def raise_fn():
            raise RuntimeError

        if datasets:
            param_dict["datasets"] = {
                dataset_name: raise_fn for dataset_name in datasets
            }

        cls_and_param_dict = (self.__class__, param_dict)

        experiment_path = Path(experiment_path)
        with open(experiment_path / _TRAINER_PKL, "wb") as fp:
            pickle.dump(cls_and_param_dict, fp)

    def _extract_fields_for_tuner_param_space(self) -> Dict:
        """Extracts fields to be included in `Tuner.param_space`.

        This is needed to leverage the full logging/integration offerings from Tune.
        For example, `param_space` is logged automatically to wandb integration.

        Currently only done for `train_loop_config`.

        Returns:
            A dictionary that should be passed to Tuner.param_space.
        """
        result = {}
        for key in self._fields_for_tuner_param_space:
            if key in self._param_dict.keys():
                result[key] = copy.deepcopy(self._param_dict[key])
        return result

    def _generate_trainable_cls(self) -> Type["Trainable"]:
        """Generates the base Trainable class.

        Returns:
            A Trainable class to use for training.
        """

        from ray.tune.execution.placement_groups import PlacementGroupFactory
        from ray.tune.trainable import wrap_function

        trainer_cls = self.__class__
        scaling_config = self.scaling_config
        restored = bool(self._restore_path)
        metadata = self.metadata

        def train_func(config):
            assert metadata is not None, metadata
            # Propagate user metadata from the Trainer constructor.
            session._get_session().metadata = metadata

            # config already contains merged values.
            # Instantiate new Trainer in Trainable.
            trainer = trainer_cls(**config)

            # Get the checkpoint from Tune and pass it to workers later on.
            checkpoint = session.get_checkpoint()
            if checkpoint:
                # Set `starting_checkpoint` for auto-recovery fault-tolerance
                # as well as manual restoration.
                trainer.starting_checkpoint = checkpoint

                # TODO(justinvyu): Remove this when Preprocessor is removed from Trainer
                if not _use_storage_context():
                    # Always load the preprocessor from an available checkpoint
                    # Unless we are restoring the experiment and have explicitly
                    # passed in a new preprocessor
                    if not (restored and trainer.preprocessor):
                        trainer.preprocessor = checkpoint.get_preprocessor()
            # Else: Train will restore from the user-provided
            # `resume_from_checkpoint` == `starting_checkpoint`.

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
            from ray.data.context import DataContext

            dataset_context = DataContext.get_current()
        else:
            dataset_context = None

        class TrainTrainable(trainable_cls):
            """Adds default resources to the Trainable."""

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
                    # Set the DataContext on the Trainer actor to the DataContext
                    # specified on the driver.
                    DataContext._set_current(dataset_context)
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

            def _trainable_func(self, config):
                # We ignore the config passed by Tune and instead use the merged
                # config which includes the initial Trainer args.
                super()._trainable_func(self._merged_config)

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
        """Converts self to a ``tune.Trainable`` class."""
        from ray import tune

        base_config = self._param_dict
        trainable_cls = self._generate_trainable_cls()

        # Wrap with `tune.with_parameters` to handle very large values in base_config
        return tune.with_parameters(trainable_cls, **base_config)
