import logging
from typing import Union
from types import ModuleType

logger = logging.getLogger(__name__)

MIN_PYARROW_VERSION = (4, 0, 1)
_VERSION_VALIDATED = False


LazyModule = Union[None, bool, ModuleType]
_pyarrow_dataset: LazyModule = None


def _lazy_import_pyarrow_dataset() -> LazyModule:
    global _pyarrow_dataset
    if _pyarrow_dataset is None:
        try:
            from pyarrow import dataset as _pyarrow_dataset
        except ModuleNotFoundError:
            # If module is not found, set _pyarrow to False so we won't
            # keep trying to import it on every _lazy_import_pyarrow() call.
            _pyarrow_dataset = False
    return _pyarrow_dataset


def _check_pyarrow_version():
    global _VERSION_VALIDATED
    if not _VERSION_VALIDATED:
        import pkg_resources

        try:
            version_info = pkg_resources.require("pyarrow")
            version_str = version_info[0].version
            version = tuple(int(n) for n in version_str.split(".") if "dev" not in n)
            if version < MIN_PYARROW_VERSION:
                raise ImportError(
                    "Datasets requires pyarrow >= "
                    f"{'.'.join(str(n) for n in MIN_PYARROW_VERSION)}, "
                    f"but {version_str} is installed. Upgrade with "
                    "`pip install -U pyarrow`."
                )
        except pkg_resources.DistributionNotFound:
            logger.warning(
                "You are using the 'pyarrow' module, but "
                "the exact version is unknown (possibly carried as "
                "an internal component by another module). Please "
                "make sure you are using pyarrow >= "
                f"{'.'.join(str(n) for n in MIN_PYARROW_VERSION)} "
                "to ensure compatibility with Ray Datasets."
            )
        else:
            _VERSION_VALIDATED = True
