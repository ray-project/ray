import logging
from typing import Optional

import ray
from ray.rllib.utils.annotations import DeveloperAPI
from ray.exceptions import GetTimeoutError

logger = logging.getLogger(__name__)


@DeveloperAPI
class FilterManager:
    """Manages filters and coordination across remote evaluators that expose
    `get_filters` and `sync_filters`.
    """

    @staticmethod
    @DeveloperAPI
    def synchronize(
        local_filters,
        remotes,
        update_remote=True,
        timeout_seconds: Optional[float] = None,
    ):
        """Aggregates all filters from remote evaluators.

        Local copy is updated and then broadcasted to all remote evaluators.

        Args:
            local_filters: Filters to be synchronized.
            remotes: Remote evaluators with filters.
            update_remote: Whether to push updates to remote filters.
            timeout_seconds: How long to wait for filter to get or set filters
        """
        try:
            remote_filters = ray.get(
                [r.get_filters.remote(flush_after=True) for r in remotes],
                timeout=timeout_seconds,
            )
        except GetTimeoutError:
            logger.error(
                "Failed to get remote filters from a rollout worker in "
                "FilterManager. "
                "Filtered "
                "metrics may be computed, but filtered wrong."
            )

        for rf in remote_filters:
            for k in local_filters:
                local_filters[k].apply_changes(rf[k], with_buffer=False)
        if update_remote:
            copies = {k: v.as_serializable() for k, v in local_filters.items()}
            remote_copy = ray.put(copies)

            try:
                ray.get(
                    [r.sync_filters.remote(remote_copy) for r in remotes],
                    timeout=timeout_seconds,
                )
            except GetTimeoutError:
                logger.error(
                    "Failed to set remote filters to a rollout worker in "
                    "FilterManager. "
                    "Filtered "
                    "metrics may be computed, but filtered wrong."
                )
