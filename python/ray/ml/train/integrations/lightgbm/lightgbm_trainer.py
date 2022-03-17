from typing import Dict, Any
import os

from ray.ml.train.gbdt_trainer import GBDTTrainer
from ray.util.annotations import PublicAPI
from ray.ml.constants import MODEL_KEY

import lightgbm
import lightgbm_ray
from lightgbm_ray.tune import TuneReportCheckpointCallback


@PublicAPI(stability="alpha")
class LightGBMTrainer(GBDTTrainer):
    """A Trainer for data parallel LightGBM training.

    This Trainer runs the LightGBM training loop in a distributed manner
    using multiple Ray Actors.

    Example:
        .. code-block:: python

            import ray

            from ray.ml.train.integrations.lightgbm import LightGBMTrainer

            train_dataset = ray.data.from_items(
                [{"x": x, "y": x + 1} for x in range(32)])
            trainer = LightGBMTrainer(
                label_column="y",
                params={"objective": "regression"},
                scaling_config={"num_workers": 3},
                datasets={"train": train_dataset}
            )
            result = trainer.fit()

    Args:
        datasets: Ray Datasets to use for training and validation. Must include a
            "train" key denoting the training dataset. If a ``preprocessor``
            is provided and has not already been fit, it will be fit on the training
            dataset. All datasets will be transformed by the ``preprocessor`` if
            one is provided. All non-training datasets will be used as separate
            validation sets, each reporting a separate metric.
        label_column: Name of the label column. A column with this name
            must be present in the training dataset.
        params: LightGBM training parameters passed to ``lightgbm.train()``.
            Refer to `LightGBM documentation <https://lightgbm.readthedocs.io>`_
            for a list of possible parameters.
        dmatrix_params: Dict of ``dataset name:dict of kwargs`` passed to respective
            :class:`xgboost_ray.RayDMatrix` initializations, which in turn are passed
            to ``lightgbm.Dataset`` objects created on each worker. For example, this
            can be used to add sample weights with the ``weights`` parameter.
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        preprocessor: A ray.ml.preprocessor.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
        **train_kwargs: Additional kwargs passed to ``lightgbm.train()`` function.
    """

    # Currently, the RayDMatrix in lightgbm_ray is the same as in xgboost_ray
    # but it is explicitly set here for forward compatibility
    _dmatrix_cls: type = lightgbm_ray.RayDMatrix
    _ray_params_cls: type = lightgbm_ray.RayParams
    _tune_callback_cls: type = TuneReportCheckpointCallback
    _default_ray_params: Dict[str, Any] = {
        "checkpoint_frequency": 1,
        "allow_less_than_two_cpus": True,
    }
    _init_model_arg_name: str = "init_model"

    def _load_model_from_checkpoint(self):
        init_model_path = self.resume_from_checkpoint.to_directory()
        return lightgbm.Booster(model_file=os.path.join(init_model_path, MODEL_KEY))

    def _train(self, **kwargs):
        return lightgbm_ray.train(**kwargs)
