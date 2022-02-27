import sys
import logging

logger = logging.getLogger(__name__)

TuneSearchCV = None
TuneGridSearchCV = None

try:
    from tune_sklearn import TuneSearchCV, TuneGridSearchCV
except ImportError as exc:
    # Changed in 1.5.0 -- Raises an exception instead of returning None.
    tb = sys.exc_info()[2]
    msg = (
        "Tune's Scikit-Learn bindings (tune-sklearn) is not installed. "
        "Please run `pip install tune-sklearn`."
    )
    raise type(exc)().with_traceback(tb) from None
__all__ = ["TuneSearchCV", "TuneGridSearchCV"]
