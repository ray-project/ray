from typing import Dict, List, Union

from ray import tune

import mxnet
from mxnet.model import save_checkpoint, BatchEndParam
import numpy as np

import os


class TuneCallback:
    """Base class for Tune's MXNet callbacks."""

    pass


class TuneReportCallback(TuneCallback):
    """MXNet to Ray Tune reporting callback

    Reports metrics to Ray Tune.

    This has to be passed to MXNet as the ``eval_end_callback``.

    Args:
        metrics (str|list|dict): Metrics to report to Tune. If this is a list,
            each item describes the metric key reported to MXNet,
            and it will reported under the same name to Tune. If this is a
            dict, each key will be the name reported to Tune and the respective
            value will be the metric key reported to MXNet.

    Example:

    .. code-block:: python

        from ray.tune.integration.mxnet import TuneReportCallback

        # mlp_model is a MXNet model
        mlp_model.fit(
            train_iter,
            # ...
            eval_metric="acc",
            eval_end_callback=TuneReportCallback({
                "mean_accuracy": "accuracy"
            }))

    """

    def __init__(self, metrics: Union[None, str, List[str], Dict[str, str]] = None):
        if isinstance(metrics, str):
            metrics = [metrics]
        self._metrics = metrics

    def __call__(self, param: BatchEndParam):
        if not param.eval_metric:
            return
        if not self._metrics:
            report_dict = dict(param.eval_metric.get_name_value())
        else:
            report_dict = {}
            lookup_dict = dict(param.eval_metric.get_name_value())
            for key in self._metrics:
                if isinstance(self._metrics, dict):
                    metric = self._metrics[key]
                else:
                    metric = key
                report_dict[key] = lookup_dict[metric]
        tune.report(**report_dict)


class TuneCheckpointCallback(TuneCallback):
    """MXNet checkpoint callback

    Saves checkpoints after each epoch.

    This has to be passed to the ``epoch_end_callback`` of the MXNet model.

    Checkpoint are currently not registered if no ``tune.report()`` call
    is made afterwards. You have to use this in conjunction with the
    ``TuneReportCallback`` to work!

    Args:
        filename (str): Filename of the checkpoint within the checkpoint
            directory. Defaults to "checkpoint".
        frequency (int): Integer indicating how often checkpoints should be
            saved.

    Example:

    .. code-block:: python


        from ray.tune.integration.mxnet import TuneReportCallback, \
            TuneCheckpointCallback

        # mlp_model is a MXNet model
        mlp_model.fit(
            train_iter,
            # ...
            eval_metric="acc",
            eval_end_callback=TuneReportCallback({
                "mean_accuracy": "accuracy"
            }),
            epoch_end_callback=TuneCheckpointCallback(
                filename="mxnet_cp",
                frequency=3
            ))

    """

    def __init__(self, filename: str = "checkpoint", frequency: int = 1):
        self._filename = filename
        self._frequency = frequency

    def __call__(
        self,
        epoch: int,
        sym: mxnet.symbol.Symbol,
        arg: Dict[str, np.ndarray],
        aux: Dict[str, np.ndarray],
    ):
        if epoch % self._frequency != 0:
            return
        with tune.checkpoint_dir(step=epoch) as checkpoint_dir:
            save_checkpoint(
                os.path.join(checkpoint_dir, self._filename), epoch, sym, arg, aux
            )
