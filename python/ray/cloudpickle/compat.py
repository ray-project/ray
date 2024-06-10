import logging
import os

logger = logging.getLogger(__name__)

RAY_PICKLE_VERBOSE_DEBUG = os.environ.get("RAY_PICKLE_VERBOSE_DEBUG")
verbose_level = int(RAY_PICKLE_VERBOSE_DEBUG) if RAY_PICKLE_VERBOSE_DEBUG else 0

if verbose_level > 1:
    logger.warning(
        "Environmental variable RAY_PICKLE_VERBOSE_DEBUG is set to "
        f"'{verbose_level}', this enabled python-based serialization backend "
        f"instead of C-Pickle. Serialization would be very slow."
    )
    from ray.cloudpickle import py_pickle as pickle
    from ray.cloudpickle.py_pickle import Pickler
else:
    import pickle  # noqa: F401
    from _pickle import Pickler  # noqa: F401
