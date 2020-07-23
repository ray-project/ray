import logging

logger = logging.getLogger(__name__)


def bootstrap_local(config):
    # Copy max_workers to config["provider"] in on-prem auto-management mode.
    if "server_address" in config["provider"]:
        config["provider"]["max_workers"] = config["max_workers"]

        # Verify no autoscaling.
        valid_num_workers = (
            config["max_workers"] == config["min_workers"]
            and config["min_workers"] == config["initial_workers"]
        )
        if not valid_num_workers:
            logger.error(
                "Autoscaling is not supported for on prem clusters."
                + " Please make sure max_workers=min_workers=initial_workers."
            )
            logger.warning(
                "Setting min_workers and initial_workers to max_workers ..."
            )
    return config
