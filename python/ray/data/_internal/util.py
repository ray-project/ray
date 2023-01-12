import importlib
import logging
import os
from typing import List, Union, Optional, TYPE_CHECKING
from types import ModuleType
import sys

import numpy as np

import ray
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.data.context import DatasetContext
from ray._private.utils import _get_pyarrow_version

if TYPE_CHECKING:
    from ray.data.datasource import Reader
    from ray.util.placement_group import PlacementGroup

logger = logging.getLogger(__name__)

# NOTE: Make sure that these lower and upper bounds stay in sync with version
# constraints given in python/setup.py.
# Inclusive minimum pyarrow version.
MIN_PYARROW_VERSION = "6.0.1"
RAY_DISABLE_PYARROW_VERSION_CHECK = "RAY_DISABLE_PYARROW_VERSION_CHECK"
_VERSION_VALIDATED = False
_LOCAL_SCHEME = "local"
_EXAMPLE_SCHEME = "example"


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

        version = _get_pyarrow_version()
        if version is not None:
            from pkg_resources._vendor.packaging.version import parse as parse_version

            if parse_version(version) < parse_version(MIN_PYARROW_VERSION):
                raise ImportError(
                    f"Datasets requires pyarrow >= {MIN_PYARROW_VERSION}, but "
                    f"{version} is installed. Reinstall with "
                    f'`pip install -U "pyarrow"`. '
                    "If you want to disable this pyarrow version check, set the "
                    f"environment variable {RAY_DISABLE_PYARROW_VERSION_CHECK}=1."
                )
        else:
            logger.warning(
                "You are using the 'pyarrow' module, but the exact version is unknown "
                "(possibly carried as an internal component by another module). Please "
                f"make sure you are using pyarrow >= {MIN_PYARROW_VERSION} to ensure "
                "compatibility with Ray Datasets. "
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


def _resolve_custom_scheme(path: str) -> str:
    """Returns the resolved path if the given path follows a Ray-specific custom
    scheme. Othewise, returns the path unchanged.

    The supported custom schemes are: "local", "example".
    """
    import pathlib
    import urllib.parse

    parsed_uri = urllib.parse.urlparse(path)
    if parsed_uri.scheme == _LOCAL_SCHEME:
        path = parsed_uri.netloc + parsed_uri.path
    elif parsed_uri.scheme == _EXAMPLE_SCHEME:
        example_data_path = pathlib.Path(__file__).parent.parent / "examples" / "data"
        path = example_data_path / (parsed_uri.netloc + parsed_uri.path)
        path = str(path.resolve())
    return path


def _is_local_scheme(paths: Union[str, List[str]]) -> bool:
    """Returns True if the given paths are in local scheme.
    Note: The paths must be in same scheme, i.e. it's invalid and
    will raise error if paths are mixed with different schemes.
    """
    import pathlib
    import urllib.parse

    if isinstance(paths, str):
        paths = [paths]
    if isinstance(paths, pathlib.Path):
        paths = [str(paths)]
    elif not isinstance(paths, list) or any(not isinstance(p, str) for p in paths):
        raise ValueError("paths must be a path string or a list of path strings.")
    elif len(paths) == 0:
        raise ValueError("Must provide at least one path.")
    num = sum(urllib.parse.urlparse(path).scheme == _LOCAL_SCHEME for path in paths)
    if num > 0 and num < len(paths):
        raise ValueError(
            "The paths must all be local-scheme or not local-scheme, "
            f"but found mixed {paths}"
        )
    return num == len(paths)


def _is_tensor_schema(column_names: List[str]):
    return column_names == [TENSOR_COLUMN_NAME]
