import logging

logger = logging.getLogger(__name__)

TuneSearchCV = None
TuneGridSearchCV = None

try:
    from tune_sklearn import TuneSearchCV, TuneGridSearchCV
except ImportError:
    logger.info("tune_sklearn is not installed. Please run "
                "`pip install tune-sklearn`.")

__all__ = ["TuneSearchCV", "TuneGridSearchCV"]
