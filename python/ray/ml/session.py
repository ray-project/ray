from typing import Optional

import logging
import os
import shutil

from ray import tune
from ray import train
from ray.ml.checkpoint import Checkpoint
from ray.tune.function_runner import StatusReporter

logger = logging.getLogger(__name__)

_LOCAL_PATH = "local_path"
_DATA_DICT = "data_dict"


def _get_session():
    # TODO(xwjiang): Currently the ml session only multiplexes between Tune and Train session.
    #  We should extract common functionalities if possible.
    from ray.tune.session import _session as tune_session
    from ray.train.session import _session as train_session
    if train_session and tune_session:
        logger.warning("Expected to be either in tune session or train session but not both.")
        return None
    if not (train_session or tune_session):
        logger.warning("In neither tune session nor train session!")
        return None
    return train_session or tune_session


def save(epoch: int, checkpoint: Optional[Checkpoint] = None, metrics: Optional[dict] = None):
    """Save checkpoint and report metrics for the given epoch.

    At least one of ``checkpoint`` and ``metrics`` should be provided.

    This is meant to replace barebone ``tune.report``, ``with tune.checkpoint_dir``,
    ``train.report`` and ``train.save_checkpoint``. Avoid mixing them together.

    Examples:
    1. use it in per worker training function in distributed training

    .. code-block:: python

        from ray.ml import session
        from ray.ml.checkpoint import Checkpoint
        from ray.ml.train.integrations.torch import TorchTrainer

        def train_loop_per_worker():
            checkpoint = Checkpoint.from_dict({"foo": 1})
            session.save(epoch=0, checkpoint=checkpoint)

        trainer = TorchTrainer(train_loop_per_worker=train_loop_per_worker, scaling_config={"num_workers": 2})
        trainer.fit()

    2. Or use it in the function trainable for tuning

    .. code-block:: python

        from ray.ml import session
        from ray.ml.checkpoint import Checkpoint
        from ray.tune.tuner import Tuner
        def training_func(config):
            checkpoint = Checkpoint.from_dict({"bar": 2})
            session.save(epoch=0, checkpoint=checkpoint)

        tuner = Tuner(trainable=training_func)
        tuner.fit()

    """
    is_dict = None
    if checkpoint:
        # This checkpoint can be dict checkpoint or a directory checkpoint.
        if not checkpoint.get_internal_representation()[0] in (_LOCAL_PATH, _DATA_DICT):
            logger.warning("Please provide the checkpoint through "
                           "``Checkpoint.from_directory`` or ``Checkpoint.from_dict``. Skipping..")
            return
        is_dict = checkpoint.get_internal_representation()[0] == _DATA_DICT
    session = _get_session()
    if not session:
        logger.warning("Can only call save inside of a session! Skipping..")
        return
    metrics = metrics or {}

    if isinstance(session, StatusReporter):

        if checkpoint:
            with tune.checkpoint_dir(step=epoch) as checkpoint_dir:
                path = os.path.join(checkpoint_dir, "checkpoint")
                if is_dict:
                    checkpoint.to_directory(checkpoint_dir)
                else:
                    shutil.move(checkpoint.to_directory(), path)
        tune.report(**metrics)
    else:
        if checkpoint:
            train.save_checkpoint(**checkpoint.to_dict())
        train.report(**metrics)
