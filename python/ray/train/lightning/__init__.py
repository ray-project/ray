import logging
from packaging.version import Version

# isort: off
try:
    import pytorch_lightning  # noqa: F401
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "PyTorch Lightning isn't installed. To install PyTorch Lightning, "
        "please run 'pip install pytorch-lightning'"
    )
# isort: on

from ray.train.lightning.lightning_checkpoint import LightningCheckpoint
from ray.train.lightning.lightning_predictor import LightningPredictor
from ray.train.lightning.lightning_trainer import (
    LightningTrainer,
    LightningConfigBuilder,
)

logger = logging.getLogger(__name__)
if Version(pytorch_lightning.__version__) <= Version("1.6.5"):
    logger.warning(
        f"You are using ``pytorch_lightning=={pytorch_lightning.__version__}``,"
        "but LightningTrainer only supports ``pytorch_lightning>=1.6.5``."
        "You may upgrade the PyTorch Lightning library to a newer version."
    )

__all__ = [
    "LightningTrainer",
    "LightningConfigBuilder",
    "LightningCheckpoint",
    "LightningPredictor",
]
