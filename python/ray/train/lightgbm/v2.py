import logging
from typing import Any, Callable, Dict, Optional, Union

import ray.train
from ray.train import Checkpoint
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.lightgbm.config import NETWORK_PARAMS_KEY, LightGBMConfig
from ray.train.trainer import GenDataset

logger = logging.getLogger(__name__)


def get_network_params() -> dict:
    from ray.train._internal.session import get_session

    session = get_session()
    if not session:
        logger.warning(
            "`ray.train.lightgbm.get_network_params` was called outside the context "
            "of a `ray.train.lightgbm.LightGBMTrainer`. "
            "The current process has no knowledge of the distributed training group, "
            "so returning an empty dict. Please call this within the training loop "
            "of a `ray.train.lightgbm.LightGBMTrainer`."
        )
        return {}

    network_params = session.get_state(NETWORK_PARAMS_KEY)
    assert network_params is not None, (
        f"`LightGBMConfig.backend_cls` must set '{NETWORK_PARAMS_KEY}' "
        "in the session state in `on_training_start`. "
        "Please fix this if you provided a custom `LightGBMConfig` subclass."
        "Otherwise, please file a bug report to the Ray Team."
    )
    return network_params.copy()


class LightGBMTrainer(DataParallelTrainer):
    """A Trainer for distributed data-parallel LightGBM training.

    Example
    -------

    .. testcode::

        import lightgbm as lgb

        import ray.data
        import ray.train
        from ray.train.lightgbm import RayTrainReportCallback
        from ray.train.lightgbm.v2 import LightGBMTrainer


        def train_fn_per_worker(config: dict):
            # (Optional) Add logic to resume training state from a checkpoint.
            # ray.train.get_checkpoint()

            # 1. Get the dataset shard for the worker and convert to a `lgb.Dataset`
            train_ds_iter, eval_ds_iter = (
                ray.train.get_dataset_shard("train"),
                ray.train.get_dataset_shard("validation"),
            )
            train_ds, eval_ds = train_ds_iter.materialize(), eval_ds_iter.materialize()
            train_df, eval_df = train_ds.to_pandas(), eval_ds.to_pandas()
            train_X, train_y = train_df.drop("y", axis=1), train_df["y"]
            eval_X, eval_y = eval_df.drop("y", axis=1), eval_df["y"]

            train_set = lgb.Dataset(train_X, label=train_y)
            eval_set = lgb.Dataset(eval_X, label=eval_y)

            # 2. Run distributed data-parallel training.
            # `get_network_params` sets up the necessary configurations for LightGBM
            # to set up the data parallel training worker group on your Ray cluster.
            params = {
                "objective": "regression",
                # Adding the line below is the only change needed
                # for your `lgb.train` call!
                **ray.train.lightgbm.v2.get_network_params(),
            }
            lgb.train(
                params,
                train_set,
                valid_sets=[eval_set],
                valid_names=["eval"],
                callbacks=[RayTrainReportCallback()],
            )

        train_ds = ray.data.from_items([{"x": x, "y": x + 1} for x in range(32)])
        eval_ds = ray.data.from_items(
            [{"x": x, "y": x + 1} for x in range(32, 32 + 16)]
        )
        trainer = LightGBMTrainer(
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
        lightgbm_config: The configuration for setting up the distributed lightgbm
            backend. See :class:`~ray.train.lightgbm.LightGBMConfig` for more info.
        datasets: The Ray Datasets to use for training and validation.
        dataset_config: The configuration for ingesting the input ``datasets``.
            By default, all the Ray Dataset are split equally across workers.
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
    """

    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        lightgbm_config: Optional[LightGBMConfig] = None,
        scaling_config: Optional[ray.train.ScalingConfig] = None,
        run_config: Optional[ray.train.RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        dataset_config: Optional[ray.train.DataConfig] = None,
        metadata: Optional[Dict[str, Any]] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        super(LightGBMTrainer, self).__init__(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
            backend_config=lightgbm_config or LightGBMConfig(),
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            resume_from_checkpoint=resume_from_checkpoint,
            metadata=metadata,
        )
