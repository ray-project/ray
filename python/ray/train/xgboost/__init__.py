# isort: off
try:
    import xgboost
except (ModuleNotFoundError, ImportError) as exc:
    raise ImportError(
        "`xgboost` must be installed to import `ray.train.xgboost`. "
        "Install with: `pip install xgboost>=1.7`"
    ) from exc

from packaging.version import Version

if Version(xgboost.__version__) < Version("1.7.0"):
    raise ImportError(
        "Ray Train's `XGBoostTrainer` requires `xgboost>=1.7.0`. "
        "Install with: `pip install -U xgboost>=1.7.0`"
    )
# isort: on

from ray.train.xgboost._xgboost_utils import RayTrainReportCallback
from ray.train.xgboost.config import XGBoostConfig
from ray.train.xgboost.xgboost_checkpoint import XGBoostCheckpoint
from ray.train.xgboost.xgboost_predictor import XGBoostPredictor
from ray.train.xgboost.xgboost_trainer import XGBoostTrainer

__all__ = [
    "RayTrainReportCallback",
    "XGBoostCheckpoint",
    "XGBoostConfig",
    "XGBoostPredictor",
    "XGBoostTrainer",
]
