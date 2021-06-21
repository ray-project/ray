import logging
from ray.autoscaler._private.util import DEBUG_AUTOSCALING_STATUS_LEGACY
from ray.experimental.internal_kv import _internal_kv_put, \
    _internal_kv_initialized
"""This file provides legacy support for the old info string in order to
ensure the dashboard's `api/cluster_status` does not break backwards
compatibilty.
"""

logger = logging.getLogger(__name__)


def legacy_log_info_string(autoscaler, nodes):
    tmp = "Cluster status: "
    tmp += info_string(autoscaler, nodes)
    tmp += "\n"
    tmp += autoscaler.load_metrics.info_string()
    tmp += "\n"
    tmp += autoscaler.resource_demand_scheduler.debug_string(
        nodes, autoscaler.pending_launches.breakdown(),
        autoscaler.load_metrics.get_resource_utilization())
    if _internal_kv_initialized():
        _internal_kv_put(DEBUG_AUTOSCALING_STATUS_LEGACY, tmp, overwrite=True)
    logger.debug(tmp)


def info_string(autoscaler, nodes):
    suffix = ""
    if autoscaler.updaters:
        suffix += " ({} updating)".format(len(autoscaler.updaters))
    if autoscaler.num_failed_updates:
        suffix += " ({} failed to update)".format(
            len(autoscaler.num_failed_updates))

    return "{} nodes{}".format(len(nodes), suffix)
