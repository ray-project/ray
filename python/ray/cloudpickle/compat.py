import logging
import os
import sys

logger = logging.getLogger(__name__)

RAY_PICKLE_VERBOSE_DEBUG = os.environ.get("RAY_PICKLE_VERBOSE_DEBUG")

if RAY_PICKLE_VERBOSE_DEBUG:
    level = int(RAY_PICKLE_VERBOSE_DEBUG)

if level > 1:
    logger.warning(
        "Environmental variable RAY_PICKLE_VERBOSE_DEBUG is set to "
        f"'{level}', this enabled python-based serialization backend "
        f"instead of C-Pickle. Serialization would be very slow."
    )
    from ray.cloudpickle import py_pickle as pickle
    from ray.cloudpickle.py_pickle import Pickler
elif sys.version_info < (3, 8):
    try:
        import pickle5 as pickle  # noqa: F401
        from pickle5 import Pickler  # noqa: F401
    except ImportError:
        import pickle  # noqa: F401
        from pickle import _Pickler as Pickler  # noqa: F401
else:
    import pickle  # noqa: F401
    from pickle import _Pickler as Pickler  # noqa: F401
