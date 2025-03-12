from typing import Optional

from packaging.version import Version
from packaging.version import parse as parse_version

_PYARROW_INSTALLED: Optional[bool] = None
_PYARROW_VERSION: Optional[Version] = None


def get_pyarrow_version() -> Optional[Version]:
    """Get the version of the pyarrow package or None if not installed."""
    global _PYARROW_INSTALLED, _PYARROW_VERSION
    if _PYARROW_INSTALLED is False:
        return None

    if _PYARROW_INSTALLED is None:
        try:
            import pyarrow

            _PYARROW_INSTALLED = True
            if hasattr(pyarrow, "__version__"):
                _PYARROW_VERSION = parse_version(pyarrow.__version__)
        except ModuleNotFoundError:
            _PYARROW_INSTALLED = False

    return _PYARROW_VERSION
