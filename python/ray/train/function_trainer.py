import inspect
from typing import Any, Optional, Callable, Type, Dict

from ray.air._internal.checkpointing import save_preprocessor_to_dir
from ray.air.checkpoint import Checkpoint
from ray.air.config import RunConfig, ScalingConfig
from ray.data import Preprocessor
from ray.train.base_trainer import GenDataset
from ray.train.trainer import BaseTrainer
from ray.tune.trainable import Trainable, wrap_function, TrainableUtil
from ray.tune.utils import detect_checkpoint_function, detect_config_single
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class FunctionTrainer(BaseTrainer):
    """Trainer for generic (non-distributed) training functions.

    This trainer can be used to wrap a generic function trainable. In contrast
    to e.g. the DataParallelTrainer, this trainer will only start up one worker
    (the trainer). Starting up potential distributed workers is then up
    to the custom training function.

    The trainer accepts a ``train_fn``, which can be any Ray Tune-compatible
    function trainable.

    If a scaling config is given, resources will be requested for all workers.
    Starting potential distributed workers within the current placement group
    is up to the user.

    If a checkpoint to resume from is given, it will be passed as the
    ``checkpoint_dir`` argument of the training function, if supported.
    Alternatively, ``ray.air.session.get_checkpoint()`` can be used.

    If datasets are passed, they will be injected into the ``config`` argument
    passed to the ``train_fn`` as ``config["datasets"]``. If a preprocessor
    is passed, the datasets will be preprocessed first.

    Args:
        train_fn: Callable training function taking a ``config`` argument.
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        datasets: Any Ray Datasets to use for training. Use
            the key "train" to denote which dataset is the training
            dataset. If a ``preprocessor`` is provided and has not already been fit,
            it will be fit on the training dataset. All datasets will be transformed
            by the ``preprocessor`` if one is provided.
        preprocessor: A ray.data.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.

    Example:

        .. code-block:: python

            from ray.air.config import ScalingConfig
            from ray.train.function_trainer import FunctionTrainer

            def train_fn(config):
                # ...
                return {"metric": 5}

            trainer = FunctionTrainer(
                train_fn,
                scaling_config=ScalingConfig(
                    trainer_resources={"CPU": 4}
                )
            )
            trainer.fit()

    """

    _scaling_config_allowed_keys = BaseTrainer._scaling_config_allowed_keys + [
        "num_workers",
        "resources_per_worker",
        "use_gpu",
        "placement_strategy",
    ]
    _handles_checkpoint_freq = False
    _handles_checkpoint_at_end = False

    def __init__(
        self,
        train_fn: Callable[[dict, Any], Any],
        *,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional["Preprocessor"] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        self.train_fn = train_fn
        super().__init__(
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    def _validate_attributes(self):
        super()._validate_attributes()

        use_checkpoint = detect_checkpoint_function(self.train_fn)
        use_config_single = detect_config_single(self.train_fn)

        if not any([use_checkpoint, use_config_single]):
            func_args = inspect.getfullargspec(self.train_fn).args

            raise ValueError(
                f"Unknown argument found in the Trainable function. "
                f"The function args must include a 'config' positional "
                f"parameter. Any other args must be 'checkpoint_dir'. "
                f"Found: {func_args}"
            )

    def _get_base_trainable(self) -> Type[Trainable]:
        use_checkpoint = detect_checkpoint_function(self.train_fn)

        if use_checkpoint and (self.resume_from_checkpoint or self.datasets):

            def wrap_fn(config, checkpoint_dir=None):
                if self.resume_from_checkpoint and not checkpoint_dir:
                    checkpoint_dir = self.resume_from_checkpoint.to_directory()
                if self.datasets:
                    self.preprocess_datasets()
                    config["datasets"] = self.datasets

                return self.train_fn(config, checkpoint_dir=checkpoint_dir)

            train_fn = wrap_fn
        elif self.datasets:

            def wrap_fn(config):
                if self.datasets:
                    self.preprocess_datasets()
                    config["datasets"] = self.datasets

                return self.train_fn(config)

            train_fn = wrap_fn
        else:
            train_fn = self.train_fn

        return wrap_function(train_fn, warn=False)

    def as_trainable(self) -> Type[Trainable]:
        trainable_cls = super().as_trainable()

        class _FunctionTrainer(trainable_cls):
            # Workaround for actor name not being logged correctly
            # if __repr__ is not directly defined in a class.
            def __repr__(self):
                return super().__repr__()

            def save_checkpoint(self, tmp_checkpoint_dir: str = ""):
                checkpoint_path = super().save_checkpoint()
                parent_dir = TrainableUtil.find_checkpoint_dir(checkpoint_path)

                preprocessor = self._merged_config.get("preprocessor", None)
                if parent_dir and preprocessor:
                    save_preprocessor_to_dir(preprocessor, parent_dir)
                return checkpoint_path

        return _FunctionTrainer

    def training_loop(self) -> None:
        raise NotImplementedError
