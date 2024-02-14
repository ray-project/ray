from functools import partial
import logging
from typing import Any, Callable, Dict, Optional, Union

import xgboost
from packaging.version import Version

import ray.train
from ray.train import Checkpoint
from ray.train.constants import _DEPRECATED_VALUE, TRAIN_DATASET_KEY
from ray.train.trainer import GenDataset
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.gbdt_trainer import GBDTTrainer
from ray.train.xgboost import RayTrainReportCallback, XGBoostConfig
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
class LegacyXGBoostTrainer(GBDTTrainer):
    """A Trainer for data parallel XGBoost training.

    This Trainer runs the XGBoost training loop in a distributed manner
    using multiple Ray Actors.

    .. note::
        ``XGBoostTrainer`` does not modify or otherwise alter the working
        of the XGBoost distributed training algorithm.
        Ray only provides orchestration, data ingest and fault tolerance.
        For more information on XGBoost distributed training, refer to
        `XGBoost documentation <https://xgboost.readthedocs.io>`__.

    Example:
        .. testcode::

            import ray

            from ray.train.xgboost import XGBoostTrainer
            from ray.train import ScalingConfig

            train_dataset = ray.data.from_items(
                [{"x": x, "y": x + 1} for x in range(32)])
            trainer = XGBoostTrainer(
                label_column="y",
                params={"objective": "reg:squarederror"},
                scaling_config=ScalingConfig(num_workers=3),
                datasets={"train": train_dataset}
            )
            result = trainer.fit()

        .. testoutput::
            :hide:

            ...

    Args:
        datasets: The Ray Datasets to use for training and validation. Must include a
            "train" key denoting the training dataset. All non-training datasets will
            be used as separate validation sets, each reporting a separate metric.
        label_column: Name of the label column. A column with this name
            must be present in the training dataset.
        params: XGBoost training parameters.
            Refer to `XGBoost documentation <https://xgboost.readthedocs.io/>`_
            for a list of possible parameters.
        dmatrix_params: Dict of ``dataset name:dict of kwargs`` passed to respective
            :class:`xgboost_ray.RayDMatrix` initializations, which in turn are passed
            to ``xgboost.DMatrix`` objects created on each worker. For example, this can
            be used to add sample weights with the ``weight`` parameter.
        num_boost_round: Target number of boosting iterations (trees in the model).
            Note that unlike in ``xgboost.train``, this is the target number
            of trees, meaning that if you set ``num_boost_round=10`` and pass a model
            that has already been trained for 5 iterations, it will be trained for 5
            iterations more, instead of 10 more.
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        resume_from_checkpoint: A checkpoint to resume training from.
        metadata: Dict that should be made available in `checkpoint.get_metadata()`
            for checkpoints saved from this Trainer. Must be JSON-serializable.
        **train_kwargs: Additional kwargs passed to ``xgboost.train()`` function.
    """

    _default_ray_params: Dict[str, Any] = {
        "num_actors": 1,
        "cpus_per_actor": 1,
        "gpus_per_actor": 0,
    }
    _init_model_arg_name: str = "xgb_model"

    def __init__(self, *args, **kwargs):
        # TODO(justinvyu): Fix circular import by moving xgboost_ray into ray
        import xgboost_ray

        self._dmatrix_cls: type = xgboost_ray.RayDMatrix
        self._ray_params_cls: type = xgboost_ray.RayParams
        self._tune_callback_checkpoint_cls: type = (
            xgboost_ray.tune.TuneReportCheckpointCallback
        )
        super().__init__(*args, **kwargs)

    @classmethod
    def get_model(
        cls,
        checkpoint: Checkpoint,
    ) -> xgboost.Booster:
        """Retrieve the XGBoost model stored in this checkpoint."""
        return RayTrainReportCallback.get_model(checkpoint)

    def _train(self, **kwargs):
        import xgboost_ray

        return xgboost_ray.train(**kwargs)

    def _model_iteration(self, model: xgboost.Booster) -> int:
        if not hasattr(model, "num_boosted_rounds"):
            # Compatibility with XGBoost < 1.4
            return len(model.get_dump())
        return model.num_boosted_rounds()

    def preprocess_datasets(self) -> None:
        super().preprocess_datasets()

        # XGBoost/LightGBM-Ray requires each dataset to have at least as many
        # blocks as there are workers.
        # This is only applicable for xgboost-ray<0.1.16
        import xgboost_ray

        if Version(xgboost_ray.__version__) < Version("0.1.16"):
            self._repartition_datasets_to_match_num_actors()


class SimpleXGBoostTrainer(DataParallelTrainer):
    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        xgboost_config: Optional[XGBoostConfig] = None,
        scaling_config: Optional[ray.train.ScalingConfig] = None,
        run_config: Optional[ray.train.RunConfig] = None,
        datasets: Optional[Dict[str, Any]] = None,
        dataset_config: Optional[ray.train.DataConfig] = None,
        metadata: Optional[Dict[str, Any]] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        if not xgboost_config:
            xgboost_config = XGBoostConfig()

        dataset_config = dataset_config or ray.train.DataConfig()
        dataset_config._convert_to_data_iterator = False

        super(SimpleXGBoostTrainer, self).__init__(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
            backend_config=xgboost_config,
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            resume_from_checkpoint=resume_from_checkpoint,
            metadata=metadata,
        )


def _xgboost_train_fn_per_worker(
    config: dict, label_column: str, num_boost_round: int, xgboost_train_kwargs: dict
):
    from xgboost.collective import CommunicatorContext
    from ray.train._internal.session import get_session

    checkpoint = ray.train.get_checkpoint()
    init_model = None
    remaining_iters = num_boost_round
    if checkpoint:
        init_model = RayTrainReportCallback.get_model(checkpoint)
        starting_iter = init_model.num_boosted_rounds()
        remaining_iters = num_boost_round - starting_iter
        logger.warning(
            f"Model loaded from checkpoint will train for "
            f"additional {remaining_iters} iterations (trees) in order "
            "to achieve the target number of iterations "
            f"({num_boost_round=})."
        )

    train_dataset = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
    train_df = train_dataset.to_pandas()

    eval_datasets = {
        k: d for k, d in get_session().dataset_shard.items() if k != TRAIN_DATASET_KEY
    }
    eval_dfs = {k: d.to_pandas() for k, d in eval_datasets.items()}

    train_X, train_y = train_df.drop(label_column, axis=1), train_df[label_column]
    dtrain = xgboost.DMatrix(train_X, label=train_y)
    evals = []
    for eval_name, eval_df in eval_dfs.items():
        eval_X, eval_y = eval_df.drop(label_column, axis=1), eval_df[label_column]
        evals.append((xgboost.DMatrix(eval_X, label=eval_y), eval_name))

    with CommunicatorContext():
        evals_result = {}
        xgboost.train(
            config,
            dtrain=dtrain,
            evals=evals,
            evals_result=evals_result,
            num_boost_round=remaining_iters,
            **xgboost_train_kwargs,
        )


@PublicAPI(stability="beta")
class XGBoostTrainer(SimpleXGBoostTrainer):
    """A Trainer for data parallel XGBoost training.

    This Trainer runs the XGBoost training loop in a distributed manner
    using multiple Ray Actors.

    .. note::
        ``XGBoostTrainer`` does not modify or otherwise alter the working
        of the XGBoost distributed training algorithm.
        Ray only provides orchestration, data ingest and fault tolerance.
        For more information on XGBoost distributed training, refer to
        `XGBoost documentation <https://xgboost.readthedocs.io>`__.

    Example:
        .. testcode::

            import ray

            from ray.train.xgboost import XGBoostTrainer
            from ray.train import ScalingConfig

            train_dataset = ray.data.from_items(
                [{"x": x, "y": x + 1} for x in range(32)])
            trainer = XGBoostTrainer(
                label_column="y",
                params={"objective": "reg:squarederror"},
                scaling_config=ScalingConfig(num_workers=3),
                datasets={"train": train_dataset}
            )
            result = trainer.fit()

        .. testoutput::
            :hide:

            ...

    Args:
        datasets: The Ray Datasets to use for training and validation. Must include a
            "train" key denoting the training dataset. All non-training datasets will
            be used as separate validation sets, each reporting a separate metric.
        label_column: Name of the label column. A column with this name
            must be present in the training dataset.
        params: XGBoost training parameters.
            Refer to `XGBoost documentation <https://xgboost.readthedocs.io/>`_
            for a list of possible parameters.
        dmatrix_params: Dict of ``dataset name:dict of kwargs`` passed to respective
            :class:`xgboost_ray.RayDMatrix` initializations, which in turn are passed
            to ``xgboost.DMatrix`` objects created on each worker. For example, this can
            be used to add sample weights with the ``weight`` parameter.
        num_boost_round: Target number of boosting iterations (trees in the model).
            Note that unlike in ``xgboost.train``, this is the target number
            of trees, meaning that if you set ``num_boost_round=10`` and pass a model
            that has already been trained for 5 iterations, it will be trained for 5
            iterations more, instead of 10 more.
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        resume_from_checkpoint: A checkpoint to resume training from.
        metadata: Dict that should be made available in `checkpoint.get_metadata()`
            for checkpoints saved from this Trainer. Must be JSON-serializable.
        **train_kwargs: Additional kwargs passed to ``xgboost.train()`` function.
    """

    def __init__(
        self,
        *,
        datasets: Dict[str, GenDataset],
        label_column: str,
        params: Dict[str, Any],
        dmatrix_params: Optional[Dict[str, Dict[str, Any]]] = _DEPRECATED_VALUE,
        num_boost_round: int = 10,
        scaling_config: Optional[ray.train.ScalingConfig] = None,
        run_config: Optional[ray.train.RunConfig] = None,
        preprocessor=None,  # Deprecated
        resume_from_checkpoint: Optional[Checkpoint] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **train_kwargs,
    ):
        # TODO(justinvyu): [Deprecated] Remove in 2.11
        if dmatrix_params != _DEPRECATED_VALUE:
            raise DeprecationWarning(
                "`dmatrix_params` is deprecated, since XGBoostTrainer no longer "
                "depends on the `xgboost_ray.RayDMatrix` utility."
            )

        # Initialize a default Ray Train metrics/checkpoint reporting callback if needed
        callbacks = train_kwargs.get("callbacks", [])
        user_supplied_callback = any(
            isinstance(callback, RayTrainReportCallback) for callback in callbacks
        )
        callback_kwargs = {}
        if run_config:
            callback_kwargs["frequency"] = (
                run_config.checkpoint_config.checkpoint_frequency
            )
            callback_kwargs["checkpoint_at_end"] = (
                run_config.checkpoint_config.checkpoint_frequency or 0
            )

        if not user_supplied_callback:
            callbacks.append(RayTrainReportCallback(**callback_kwargs))
        train_kwargs["callbacks"] = callbacks

        train_fn_per_worker = partial(
            _xgboost_train_fn_per_worker,
            label_column=label_column,
            num_boost_round=num_boost_round,
            xgboost_train_kwargs=train_kwargs,
        )

        super(XGBoostTrainer, self).__init__(
            train_loop_per_worker=train_fn_per_worker,
            train_loop_config=params,
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            resume_from_checkpoint=resume_from_checkpoint,
            metadata=metadata,
        )


def simple_trainer_example():
    from contextlib import contextmanager

    @contextmanager
    def ray_train_xgboost_setup():
        # env_vars = {
        #     "DMLC_NUM_WORKER",
        #     "DMLC_TASK_ID",
        #     "DMLC_TRACKER_URI",
        #     "DMLC_TRACKER_PORT",
        # }

        # rabit_args = {k: os.environ[k] for k in env_vars}

        from xgboost.collective import CommunicatorContext

        with CommunicatorContext():
            yield

    def train_fn(config):
        from xgboost.collective import CommunicatorContext

        with CommunicatorContext():
            print(
                f"worker start: {xgboost.collective.get_rank()=} "
                f"{xgboost.collective.get_world_size()=}"
            )

            params = {
                "tree_method": "approx",
                "objective": "reg:squarederror",
                "eta": 1e-4,
                "subsample": 0.5,
                "max_depth": 2,
            }

            train_ds = ray.train.get_dataset_shard("train")
            train_df = train_ds.to_pandas()
            X, y = train_df.drop("y", axis=1), train_df["y"]
            dtrain = xgboost.DMatrix(X, label=y)

            # User just calls xgboost.train as they would natively...
            bst = xgboost.train(
                params,
                dtrain,
            )

            print("done", bst)

    train_dataset = ray.data.from_items([{"x": x, "y": x + 1} for x in range(32)])
    trainer = SimpleXGBoostTrainer(
        train_fn,
        datasets={"train": train_dataset},
        scaling_config=ray.train.ScalingConfig(num_workers=4),
    )
    trainer.fit()


def original_api_example():
    import ray
    from ray.train import ScalingConfig
    from ray.train.xgboost import XGBoostTrainer

    dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")
    train_dataset, valid_dataset = dataset.train_test_split(test_size=0.3)

    trainer = XGBoostTrainer(
        scaling_config=ScalingConfig(
            num_workers=2,
            use_gpu=False,
        ),
        label_column="target",
        num_boost_round=20,
        params={
            "objective": "binary:logistic",
            "eval_metric": ["logloss", "error"],
        },
        datasets={"train": train_dataset, "valid": valid_dataset},
    )
    result = trainer.fit()
    print(result)


if __name__ == "__main__":
    original_api_example()
