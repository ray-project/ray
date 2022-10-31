import importlib
import logging
import os
from typing import Union, Optional, TYPE_CHECKING
from types import ModuleType
import sys

import numpy as np

import ray
from ray.data.context import DatasetContext

if TYPE_CHECKING:
    from ray.data.datasource import Reader
    from ray.util.placement_group import PlacementGroup

logger = logging.getLogger(__name__)

# NOTE: Make sure that these lower and upper bounds stay in sync with version
# constraints given in python/setup.py.
# Inclusive minimum pyarrow version.
MIN_PYARROW_VERSION = "6.0.1"
# Exclusive maximum pyarrow version.
MAX_PYARROW_VERSION = "7.0.0"
RAY_DISABLE_PYARROW_VERSION_CHECK = "RAY_DISABLE_PYARROW_VERSION_CHECK"
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
    """Check that pyarrow's version is within the supported bounds."""
    global _VERSION_VALIDATED

    if not _VERSION_VALIDATED:
        if os.environ.get(RAY_DISABLE_PYARROW_VERSION_CHECK, "0") == "1":
            _VERSION_VALIDATED = True
            return

        try:
            import pyarrow
        except ModuleNotFoundError:
            # pyarrow not installed, short-circuit.
            return

        import pkg_resources

        if not hasattr(pyarrow, "__version__"):
            logger.warning(
                "You are using the 'pyarrow' module, but the exact version is unknown "
                "(possibly carried as an internal component by another module). Please "
                f"make sure you are using pyarrow >= {MIN_PYARROW_VERSION}, < "
                f"{MAX_PYARROW_VERSION} to ensure compatibility with Ray Datasets. "
                "If you want to disable this pyarrow version check, set the "
                f"environment variable {RAY_DISABLE_PYARROW_VERSION_CHECK}=1."
            )
        else:
            version = pyarrow.__version__
            if (
                pkg_resources.packaging.version.parse(version)
                < pkg_resources.packaging.version.parse(MIN_PYARROW_VERSION)
            ) or (
                pkg_resources.packaging.version.parse(version)
                >= pkg_resources.packaging.version.parse(MAX_PYARROW_VERSION)
            ):
                raise ImportError(
                    f"Datasets requires pyarrow >= {MIN_PYARROW_VERSION}, < "
                    f"{MAX_PYARROW_VERSION}, but {version} is installed. Reinstall "
                    f'with `pip install -U "pyarrow<{MAX_PYARROW_VERSION}"`. '
                    "If you want to disable this pyarrow version check, set the "
                    f"environment variable {RAY_DISABLE_PYARROW_VERSION_CHECK}=1."
                )
        _VERSION_VALIDATED = True


def _autodetect_parallelism(
    parallelism: int,
    cur_pg: Optional["PlacementGroup"],
    ctx: DatasetContext,
    reader: Optional["Reader"] = None,
    avail_cpus: Optional[int] = None,
) -> (int, int):
    """Returns parallelism to use and the min safe parallelism to avoid OOMs.

    This detects parallelism using the following heuristics, applied in order:

     1) We start with the default parallelism of 200.
     2) Min block size. If the parallelism would make blocks smaller than this
        threshold, the parallelism is reduced to avoid the overhead of tiny blocks.
     3) Max block size. If the parallelism would make blocks larger than this
        threshold, the parallelism is increased to avoid OOMs during processing.
     4) Available CPUs. If the parallelism cannot make use of all the available
        CPUs in the cluster, the parallelism is increased until it can.

    Args:
        parallelism: The user-requested parallelism, or -1 for auto-detection.
        cur_pg: The current placement group, to be used for avail cpu calculation.
        ctx: The current Dataset context to use for configs.
        reader: The datasource reader, to be used for data size estimation.
        avail_cpus: Override avail cpus detection (for testing only).

    Returns:
        Tuple of detected parallelism (only if -1 was specified), and the min safe
        parallelism (which can be used to generate warnings about large blocks).
    """
    min_safe_parallelism = 1
    max_reasonable_parallelism = sys.maxsize
    if reader:
        mem_size = reader.estimate_inmemory_data_size()
        if mem_size is not None and not np.isnan(mem_size):
            min_safe_parallelism = max(1, int(mem_size / ctx.target_max_block_size))
            max_reasonable_parallelism = max(
                1, int(mem_size / ctx.target_min_block_size)
            )
    else:
        mem_size = None
    if parallelism < 0:
        if parallelism != -1:
            raise ValueError("`parallelism` must either be -1 or a positive integer.")
        # Start with 2x the number of cores as a baseline, with a min floor.
        avail_cpus = avail_cpus or _estimate_avail_cpus(cur_pg)
        parallelism = max(
            min(ctx.min_parallelism, max_reasonable_parallelism),
            min_safe_parallelism,
            avail_cpus * 2,
        )
        logger.debug(
            f"Autodetected parallelism={parallelism} based on "
            f"estimated_available_cpus={avail_cpus} and "
            f"estimated_data_size={mem_size}."
        )
    return parallelism, min_safe_parallelism


def _estimate_avail_cpus(cur_pg: Optional["PlacementGroup"]) -> int:
    """Estimates the available CPU parallelism for this Dataset in the cluster.

    If we aren't in a placement group, this is trivially the number of CPUs in the
    cluster. Otherwise, we try to calculate how large the placement group is relative
    to the size of the cluster.

    Args:
        cur_pg: The current placement group, if any.
    """
    cluster_cpus = int(ray.cluster_resources().get("CPU", 1))
    cluster_gpus = int(ray.cluster_resources().get("GPU", 0))

    # If we're in a placement group, we shouldn't assume the entire cluster's
    # resources are available for us to use. Estimate an upper bound on what's
    # reasonable to assume is available for datasets to use.
    if cur_pg:
        pg_cpus = 0
        for bundle in cur_pg.bundle_specs:
            # Calculate the proportion of the cluster this placement group "takes up".
            # Then scale our cluster_cpus proportionally to avoid over-parallelizing
            # if there are many parallel Tune trials using the cluster.
            cpu_fraction = bundle.get("CPU", 0) / max(1, cluster_cpus)
            gpu_fraction = bundle.get("GPU", 0) / max(1, cluster_gpus)
            max_fraction = max(cpu_fraction, gpu_fraction)
            # Over-parallelize by up to a factor of 2, but no more than that. It's
            # preferrable to over-estimate than under-estimate.
            pg_cpus += 2 * int(max_fraction * cluster_cpus)

        return min(cluster_cpus, pg_cpus)

    return cluster_cpus


def _estimate_available_parallelism() -> int:
    """Estimates the available CPU parallelism for this Dataset in the cluster.
    If we are currently in a placement group, take that into account."""
    cur_pg = ray.util.get_current_placement_group()
    return _estimate_avail_cpus(cur_pg)


def _check_import(obj, *, module: str, package: str) -> None:
    """Check if a required dependency is installed.

    If `module` can't be imported, this function raises an `ImportError` instructing
    the user to install `package` from PyPI.

    Args:
        obj: The object that has a dependency.
        module: The name of the module to import.
        package: The name of the package on PyPI.
    """
    try:
        importlib.import_module(module)
    except ImportError:
        raise ImportError(
            f"`{obj.__class__.__name__}` depends on '{package}', but '{package}' "
            f"couldn't be imported. You can install '{package}' by running `pip "
            f"install {package}`."
        )
