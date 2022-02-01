import itertools
import logging
from typing import List, Dict, Any

from ray.remote_function import DEFAULT_REMOTE_FUNCTION_CPUS
import ray.ray_constants as ray_constants

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


def _get_spread_resources_iter(
    nodes: List[Dict[str, Any]],
    spread_resource_prefix: str,
    ray_remote_args: Dict[str, Any],
):
    """Returns a round-robin iterator over resources that match the given
    prefix and that coexist on nodes with the resource requests given in the
    provided remote args (along with the task resource request defaults).
    """
    # Extract the resource labels from the remote args.
    resource_request_labels = _get_resource_request_labels(ray_remote_args)
    # Filter on the prefix and the requested resources.
    spread_resource_labels = _filtered_resources(
        nodes,
        include_prefix=spread_resource_prefix,
        include_colocated_with=resource_request_labels,
    )
    if not spread_resource_labels:
        # No spreadable resource labels available, raise an error.
        raise ValueError(
            "No resources both match the provided prefix "
            f"{spread_resource_prefix} and are colocated with resources "
            f"{resource_request_labels}."
        )
    # Return a round-robin resource iterator over the spread labels.
    return itertools.cycle([{label: 0.001} for label in spread_resource_labels])


def _get_resource_request_labels(ray_remote_args: Dict[str, Any]):
    """Extracts the resource labels from the given remote args, filling in
    task resource request defaults.
    """
    resource_request_labels = set(ray_remote_args.get("resources", {}).keys())
    if ray_remote_args.get("num_cpus", DEFAULT_REMOTE_FUNCTION_CPUS) > 0:
        resource_request_labels.add("CPU")
    if "num_gpus" in ray_remote_args:
        resource_request_labels.add("GPU")
    try:
        accelerator_type = ray_remote_args["accelerator_type"]
    except KeyError:
        pass
    else:
        resource_request_labels.add(
            f"{ray_constants.RESOURCE_CONSTRAINT_PREFIX}" f"{accelerator_type}"
        )
    return resource_request_labels


def _filtered_resources(
    nodes: List[Dict[str, Any]], include_prefix: str, include_colocated_with: List[str]
):
    """Filters cluster resource labels based on the given prefix and the
    given resource colocation constraints.

    Returns a list of unique, sorted resource labels.
    """
    resources = [
        resource
        for node in nodes
        if set(include_colocated_with) <= set(node["Resources"].keys())
        for resource in node["Resources"].keys()
        if resource.startswith(include_prefix)
    ]
    # Ensure stable ordering of unique resources.
    return sorted(set(resources))
