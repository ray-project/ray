import logging
from functools import partial
from typing import Any, Callable, Dict, Optional, Union, List

import xgboost
from packaging.version import Version

import ray.train
from ray.train import Checkpoint
from ray.train.constants import TRAIN_DATASET_KEY
from ray.train.trainer import GenDataset
from ray.train.xgboost import RayTrainReportCallback, XGBoostConfig
from ray.train.xgboost.v2 import XGBoostTrainer as SimpleXGBoostTrainer
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


LEGACY_XGBOOST_TRAINER_DEPRECATION_MESSAGE = (
    "Passing in `xgboost.train` kwargs such as `params`, `num_boost_round`, "
    "`label_column`, etc. to `XGBoostTrainer` is deprecated "
    "in favor of the new API which accepts a ``train_loop_per_worker`` argument, "
    "similar to the other DataParallelTrainer APIs (ex: TorchTrainer). "
    "See this issue for more context: "
    "https://github.com/ray-project/ray/issues/50042"
)


def _xgboost_train_fn_per_worker(
    config: dict,
    label_column: str,
    num_boost_round: int,
    dataset_keys: set,
    xgboost_train_kwargs: dict,
    use_external_memory: bool = False,
    external_memory_cache_dir: Optional[str] = None,
    external_memory_device: str = "cpu",
    external_memory_batch_size: Optional[int] = None,
):
    checkpoint = ray.train.get_checkpoint()
    starting_model = None
    remaining_iters = num_boost_round
    if checkpoint:
        starting_model = RayTrainReportCallback.get_model(checkpoint)
        starting_iter = starting_model.num_boosted_rounds()
        remaining_iters = num_boost_round - starting_iter
        logger.info(
            f"Model loaded from checkpoint will train for "
            f"additional {remaining_iters} iterations (trees) in order "
            "to achieve the target number of iterations "
            f"({num_boost_round=})."
        )

    train_ds_iter = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)

    # External memory configuration is now passed directly as parameters
    # No need to extract from config

    if use_external_memory:
        # Use external memory DMatrix
        from ray.train.xgboost._external_memory_utils import (
            create_external_memory_dmatrix,
        )

        dtrain = create_external_memory_dmatrix(
            dataset_shard=train_ds_iter,
            label_column=label_column,
            batch_size=external_memory_batch_size,
            cache_dir=external_memory_cache_dir,
            device=external_memory_device,
        )
    else:
        # Use standard DMatrix (load entire dataset into memory)
        train_df = train_ds_iter.materialize().to_pandas()
        train_X, train_y = train_df.drop(label_column, axis=1), train_df[label_column]
        dtrain = xgboost.DMatrix(train_X, label=train_y)

    eval_ds_iters = {
        k: ray.train.get_dataset_shard(k)
        for k in dataset_keys
        if k != TRAIN_DATASET_KEY
    }

    # NOTE: Include the training dataset in the evaluation datasets.
    # This allows `train-*` metrics to be calculated and reported.
    evals = [(dtrain, TRAIN_DATASET_KEY)]

    for eval_name, eval_ds_iter in eval_ds_iters.items():
        if use_external_memory:
            # Use external memory DMatrix for evaluation
            deval = create_external_memory_dmatrix(
                dataset_shard=eval_ds_iter,
                label_column=label_column,
                batch_size=external_memory_batch_size,
                cache_dir=external_memory_cache_dir,
                device=external_memory_device,
            )
        else:
            # Use standard DMatrix for evaluation
            eval_df = eval_ds_iter.materialize().to_pandas()
            eval_X, eval_y = eval_df.drop(label_column, axis=1), eval_df[label_column]
            deval = xgboost.DMatrix(eval_X, label=eval_y)

        evals.append((deval, eval_name))

    evals_result = {}

    # Filter out external memory parameters from config before passing to xgboost.train
    xgboost_params = {
        k: v
        for k, v in config.items()
        if not k.startswith("external_memory_") and k != "use_external_memory"
    }

    xgboost.train(
        xgboost_params,
        dtrain=dtrain,
        evals=evals,
        evals_result=evals_result,
        num_boost_round=remaining_iters,
        xgb_model=starting_model,
        **xgboost_train_kwargs,
    )


@PublicAPI(stability="beta")
class XGBoostTrainer(SimpleXGBoostTrainer):
    """A Trainer for distributed data-parallel XGBoost training.

    Example
    -------

    .. testcode::

        import xgboost

        import ray.data
        import ray.train
        from ray.train.xgboost import RayTrainReportCallback, XGBoostTrainer

        def train_fn_per_worker(config: dict):
            # (Optional) Add logic to resume training state from a checkpoint.
            # ray.train.get_checkpoint()

            # 1. Get the dataset shard for the worker and convert to a `xgboost.DMatrix`
            train_ds_iter, eval_ds_iter = (
                ray.train.get_dataset_shard("train"),
                ray.train.get_dataset_shard("validation"),
            )
            train_ds, eval_ds = train_ds_iter.materialize(), eval_ds_iter.materialize()

            train_df, eval_df = train_ds.to_pandas(), eval_ds.to_pandas()
            train_X, train_y = train_df.drop("y", axis=1), train_df["y"]
            eval_X, eval_y = eval_df.drop("y", axis=1), eval_df["y"]

            dtrain = xgboost.DMatrix(train_X, label=train_y)
            deval = xgboost.DMatrix(eval_X, label=eval_y)

            params = {
                "tree_method": "approx",
                "objective": "reg:squarederror",
                "eta": 1e-4,
                "subsample": 0.5,
                "max_depth": 2,
            }

            # 2. Do distributed data-parallel training.
            # Ray Train sets up the necessary coordinator processes and
            # environment variables for your workers to communicate with each other.
            bst = xgboost.train(
                params,
                dtrain=dtrain,
                evals=[(deval, "validation")],
                num_boost_round=10,
                callbacks=[RayTrainReportCallback()],
            )

        train_ds = ray.data.from_items([{"x": x, "y": x + 1} for x in range(32)])
        eval_ds = ray.data.from_items([{"x": x, "y": x + 1} for x in range(16)])
        trainer = XGBoostTrainer(
            train_fn_per_worker,
            datasets={"train": train_ds, "validation": eval_ds},
            scaling_config=ray.train.ScalingConfig(num_workers=4),
        )
        result = trainer.fit()
        booster = RayTrainReportCallback.get_model(result.checkpoint)

    .. testoutput::
        :hide:

        ...

    Args:
        train_loop_per_worker: The training function to execute on each worker.
            This function can either take in zero arguments or a single ``Dict``
            argument which is set by defining ``train_loop_config``.
            Within this function you can use any of the
            :ref:`Ray Train Loop utilities <train-loop-api>`.
        train_loop_config: A configuration ``Dict`` to pass in as an argument to
            ``train_loop_per_worker``.
            This is typically used for specifying hyperparameters.
        xgboost_config: The configuration for setting up the distributed xgboost
            backend. Defaults to using the "rabit" backend.
            See :class:`~ray.train.xgboost.XGBoostConfig` for more info.
        datasets: The Ray Datasets to use for training and validation.
        dataset_config: The configuration for ingesting the input ``datasets``.
            By default, all the Ray Datasets are split equally across workers.
            See :class:`~ray.train.DataConfig` for more details.
        scaling_config: The configuration for how to scale data parallel training.
            ``num_workers`` determines how many Python processes are used for training,
            and ``use_gpu`` determines whether or not each process should use GPUs.
            See :class:`~ray.train.ScalingConfig` for more info.
        run_config: The configuration for the execution of the training run.
            See :class:`~ray.train.RunConfig` for more info.
        resume_from_checkpoint: A checkpoint to resume training from.
            This checkpoint can be accessed from within ``train_loop_per_worker``
            by calling ``ray.train.get_checkpoint()``.
        metadata: Dict that should be made available via
            `ray.train.get_context().get_metadata()` and in `checkpoint.get_metadata()`
            for checkpoints saved from this Trainer. Must be JSON-serializable.
        label_column: [Deprecated] Name of the label column. A column with this name
            must be present in the training dataset.
        params: [Deprecated] XGBoost training parameters.
            Refer to `XGBoost documentation <https://xgboost.readthedocs.io/>`_
            for a list of possible parameters.
        num_boost_round: [Deprecated] Target number of boosting iterations
            (trees in the model). Note that unlike in ``xgboost.train``, this is
            the target number of trees, meaning that if you set
            ``num_boost_round=10`` and pass a model that has already been
            trained for 5 iterations, it will be trained for 5 iterations more,
            instead of 10 more.
        **train_kwargs: [Deprecated] Additional kwargs passed to
            ``xgboost.train()`` function.
        use_external_memory: Whether to use external memory for training on
            large datasets. When enabled, datasets are automatically converted
            to use XGBoost's external memory API, allowing training on datasets
            larger than available RAM.
        external_memory_cache_dir: Directory for caching external memory files.
            If None, a temporary directory will be used.
        external_memory_device: Device to use for external memory training
            ("cpu" or "cuda").
        external_memory_batch_size: Batch size for external memory iteration.
            If None, an optimal batch size will be automatically determined.
    """

    _handles_checkpoint_freq = True
    _handles_checkpoint_at_end = True

    def __init__(
        self,
        train_loop_per_worker: Optional[
            Union[Callable[[], None], Callable[[Dict], None]]
        ] = None,
        *,
        train_loop_config: Optional[Dict] = None,
        xgboost_config: Optional[XGBoostConfig] = None,
        scaling_config: Optional[ray.train.ScalingConfig] = None,
        run_config: Optional[ray.train.RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        dataset_config: Optional[ray.train.DataConfig] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
        metadata: Optional[Dict[str, Any]] = None,
        # External memory configuration
        use_external_memory: bool = False,
        external_memory_cache_dir: Optional[str] = None,
        external_memory_device: str = "cpu",
        external_memory_batch_size: Optional[int] = None,
        # TODO(justinvyu): [Deprecated] Legacy XGBoostTrainer API
        label_column: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        num_boost_round: Optional[int] = None,
        **train_kwargs,
    ):
        if Version(xgboost.__version__) < Version("1.7.0"):
            raise ImportError(
                "`XGBoostTrainer` requires the `xgboost` version to be >= 1.7.0. "
                'Upgrade with: `pip install -U "xgboost>=1.7"`'
            )

        # TODO(justinvyu): [Deprecated] Legacy XGBoostTrainer API
        legacy_api = train_loop_per_worker is None
        if legacy_api:
            train_loop_per_worker = self._get_legacy_train_fn_per_worker(
                xgboost_train_kwargs=train_kwargs,
                run_config=run_config,
                label_column=label_column,
                num_boost_round=num_boost_round,
                datasets=datasets,
                # Pass external memory configuration separately for legacy API
                use_external_memory=use_external_memory,
                external_memory_cache_dir=external_memory_cache_dir,
                external_memory_device=external_memory_device,
                external_memory_batch_size=external_memory_batch_size,
            )
            train_loop_config = params or {}

            # Note: For legacy API, external memory configuration is handled in
            # the training function itself, not through train_loop_config
        # TODO(justinvyu): [Deprecated] Legacy XGBoostTrainer API
        # elif train_kwargs:
        #     _log_deprecation_warning(
        #         "Passing `xgboost.train` kwargs to `XGBoostTrainer` is "
        #         "deprecated. Please pass in a `train_loop_per_worker` function "
        #         "instead, which has full flexibility on the call to "
        #         "`xgboost.train(**kwargs)`. "
        #         f"{LEGACY_XGBOOST_TRAINER_DEPRECATION_MESSAGE}"
        #     )

        super(XGBoostTrainer, self).__init__(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
            xgboost_config=xgboost_config,
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            dataset_config=dataset_config,
            resume_from_checkpoint=resume_from_checkpoint,
            metadata=metadata,
        )

    def _get_legacy_train_fn_per_worker(
        self,
        xgboost_train_kwargs: Dict,
        run_config: Optional[ray.train.RunConfig],
        datasets: Optional[Dict[str, GenDataset]],
        label_column: Optional[str],
        num_boost_round: Optional[int],
        use_external_memory: bool,
        external_memory_cache_dir: Optional[str],
        external_memory_device: str,
        external_memory_batch_size: Optional[int],
    ) -> Callable[[Dict], None]:
        """Get the training function for the legacy XGBoostTrainer API."""

        datasets = datasets or {}
        if not datasets.get(TRAIN_DATASET_KEY):
            raise ValueError(
                "`datasets` must be provided for the XGBoostTrainer API "
                "if `train_loop_per_worker` is not provided. "
                "This dict must contain the training dataset under the "
                f"key: '{TRAIN_DATASET_KEY}'. "
                f"Got keys: {list(datasets.keys())}"
            )
        if not label_column:
            raise ValueError(
                "`label_column` must be provided for the XGBoostTrainer API "
                "if `train_loop_per_worker` is not provided. "
                "This is the column name of the label in the dataset."
            )

        num_boost_round = num_boost_round or 10

        # TODO(justinvyu): [Deprecated] Legacy XGBoostTrainer API
        # _log_deprecation_warning(LEGACY_XGBOOST_TRAINER_DEPRECATION_MESSAGE)

        # Initialize a default Ray Train metrics/checkpoint reporting callback if needed
        callbacks = xgboost_train_kwargs.get("callbacks", [])
        user_supplied_callback = any(
            isinstance(callback, RayTrainReportCallback) for callback in callbacks
        )
        callback_kwargs = {}
        if run_config:
            checkpoint_frequency = run_config.checkpoint_config.checkpoint_frequency
            checkpoint_at_end = run_config.checkpoint_config.checkpoint_at_end

            callback_kwargs["frequency"] = checkpoint_frequency
            # Default `checkpoint_at_end=True` unless the user explicitly sets it.
            callback_kwargs["checkpoint_at_end"] = (
                checkpoint_at_end if checkpoint_at_end is not None else True
            )

        if not user_supplied_callback:
            callbacks.append(RayTrainReportCallback(**callback_kwargs))
        xgboost_train_kwargs["callbacks"] = callbacks

        train_fn_per_worker = partial(
            _xgboost_train_fn_per_worker,
            label_column=label_column,
            num_boost_round=num_boost_round,
            dataset_keys=set(datasets),
            xgboost_train_kwargs=xgboost_train_kwargs,
            # Pass external memory configuration to the training function
            use_external_memory=use_external_memory,
            external_memory_cache_dir=external_memory_cache_dir,
            external_memory_device=external_memory_device,
            external_memory_batch_size=external_memory_batch_size,
        )

        # Store external memory configuration in the trainer instance for later use
        self._external_memory_config = {
            "use_external_memory": use_external_memory,
            "external_memory_cache_dir": external_memory_cache_dir,
            "external_memory_device": external_memory_device,
            "external_memory_batch_size": external_memory_batch_size,
        }

        return train_fn_per_worker

    @classmethod
    def get_model(
        cls,
        checkpoint: Checkpoint,
    ) -> xgboost.Booster:
        """Retrieve the XGBoost model stored in this checkpoint."""
        return RayTrainReportCallback.get_model(checkpoint)

    def create_dmatrix(
        self,
        dataset_shard,
        label_column: Union[str, List[str]],
        feature_columns: Optional[List[str]] = None,
        **kwargs,
    ) -> "xgboost.DMatrix":
        """Automatically create the appropriate XGBoost DMatrix based on trainer
        configuration.

        This method automatically chooses between standard DMatrix and external memory
        DMatrix based on the `use_external_memory` parameter passed to the trainer.
        When external memory is enabled, it automatically converts the dataset to use
        XGBoost's external memory API.

        Args:
            dataset_shard: Ray dataset shard to convert.
            label_column: Name(s) of the label column(s).
            feature_columns: Names of feature columns. If None, all non-label
                columns are used.
            **kwargs: Additional arguments passed to DMatrix constructor.

        Returns:
            XGBoost DMatrix object (either standard or external memory).

        Example:
            .. testcode::

                def train_fn_per_worker(config: dict):
                    train_ds_iter = ray.train.get_dataset_shard("train")

                    # Automatically use external memory if enabled in trainer config
                    dtrain = trainer.create_dmatrix(
                        train_ds_iter,
                        label_column="target"
                    )

                    # Train as usual
                    bst = xgboost.train(params, dtrain=dtrain, ...)
        """
        from ray.train.xgboost._external_memory_utils import create_auto_dmatrix

        # Get external memory configuration from train_loop_config
        config = self.train_loop_config or {}
        use_external_memory = config.get("use_external_memory", False)
        external_memory_cache_dir = config.get("external_memory_cache_dir")
        external_memory_device = config.get("external_memory_device", "cpu")
        external_memory_batch_size = config.get("external_memory_batch_size")

        return create_auto_dmatrix(
            dataset_shard=dataset_shard,
            label_column=label_column,
            feature_columns=feature_columns,
            use_external_memory=use_external_memory,
            external_memory_cache_dir=external_memory_cache_dir,
            external_memory_device=external_memory_device,
            external_memory_batch_size=external_memory_batch_size,
            **kwargs,
        )
