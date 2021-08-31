import logging

logger = logging.getLogger(__name__)

MIN_PYARROW_VERSION = (4, 0, 1)
_VERSION_VALIDATED = False


def _check_pyarrow_version():
    global _VERSION_VALIDATED
    if not _VERSION_VALIDATED:
        import pkg_resources
        try:
            version_info = pkg_resources.require("pyarrow")
            version_str = version_info[0].version
            version = tuple(int(n) for n in version_str.split("."))
            if version < MIN_PYARROW_VERSION:
                raise ImportError(
                    "Datasets requires pyarrow >= "
                    f"{'.'.join(str(n) for n in MIN_PYARROW_VERSION)}, "
                    f"but {version_str} is installed. Upgrade with "
                    "`pip install -U pyarrow`.")
        except pkg_resources.DistributionNotFound:
            logger.warning("You are using the 'pyarrow' module, but "
                           "the exact version is unknown (possibly carried as "
                           "an internal component by another module). Please "
                           "make sure you are using pyarrow >= "
                           f"{'.'.join(str(n) for n in MIN_PYARROW_VERSION)} "
                           "to ensure compatibility with Ray Datasets.")
        else:
            _VERSION_VALIDATED = True
