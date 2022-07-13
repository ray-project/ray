import logging
from typing import Union, Optional
from types import ModuleType

import ray
from ray.util.placement_group import PlacementGroup

logger = logging.getLogger(__name__)

MIN_PYARROW_VERSION = (6, 0, 1)
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


def _estimate_available_parallelism() -> int:
    cur_pg = ray.util.get_current_placement_group()
    return _estimate_avail_cpus(cur_pg)


def _estimate_avail_cpus(cur_pg: Optional[PlacementGroup]) -> int:
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
