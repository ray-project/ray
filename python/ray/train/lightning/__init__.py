# isort: off
try:
    import pytorch_lightning  # noqa: F401
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "PyTorch Lightning isn't installed. To install PyTorch Lightning, "
        "please run 'pip install pytorch-lightning'"
    )
# isort: on

from ray.train.lightning.lightning_trainer import (
    LightningConfigBuilder,
    LightningTrainer,
)
from ray.train.lightning.lightning_checkpoint import LightningCheckpoint
from ray.train.lightning.lightning_predictor import LightningPredictor
from ray.train.lightning._lightning_utils import (
    prepare_trainer,
    RayDDPStrategy,
    RayFSDPStrategy,
    RayDeepSpeedStrategy,
    RayLightningEnvironment,
    RayTrainReportCallback,
)

from packaging.version import Version

LIGHTNIN_VERSION = Version(pytorch_lightning.__version__)
if LIGHTNIN_VERSION < "1.6.0":
    raise ImportError(
        "Ray Train requires pytorch_lightning >= 1.6.0,"
        f"but {LIGHTNIN_VERSION} is installed."
        "Please update by `pip install -U pytorch_lightning`."
    )

__all__ = [
    "LightningTrainer",
    "LightningConfigBuilder",
    "LightningCheckpoint",
    "LightningPredictor",
    "prepare_trainer",
    "RayDDPStrategy",
    "RayFSDPStrategy",
    "RayDeepSpeedStrategy",
    "RayLightningEnvironment",
    "RayTrainReportCallback",
]
