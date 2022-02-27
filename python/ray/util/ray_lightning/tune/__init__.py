import logging

logger = logging.getLogger(__name__)

TuneReportCallback = None
TuneReportCheckpointCallback = None
get_tune_resources = None

try:
    from ray_lightning.tune import (
        TuneReportCallback,
        TuneReportCheckpointCallback,
        get_tune_resources,
    )
except ImportError:
    logger.info(
        "ray_lightning is not installed. Please run "
        "`pip install git+https://github.com/ray-project/"
        "ray_lightning#ray_lightning`."
    )

__all__ = [
    "TuneReportCallback",
    "TuneReportCheckpointCallback",
    "get_tune_resources",
]
