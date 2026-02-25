import logging
from typing import Optional

import ray
from ray.rllib.utils.annotations import OldAPIStack

logger = logging.getLogger(__name__)


@OldAPIStack
class FilterManager:
    """Manages filters and coordination across remote evaluators that expose
    `get_filters` and `sync_filters`.
    """

    @staticmethod
    def synchronize(
        local_filters,
        worker_set,
        update_remote=True,
        timeout_seconds: Optional[float] = None,
        use_remote_data_for_update: bool = True,
    ):
        """Aggregates filters from remote workers (if use_remote_data_for_update=True).

        Local copy is updated and then broadcasted to all remote evaluators
        (if `update_remote` is True).

        Args:
            local_filters: Filters to be synchronized.
            worker_set: EnvRunnerGroup with remote EnvRunners with filters.
            update_remote: Whether to push updates from the local filters to the remote
                workers' filters.
            timeout_seconds: How long to wait for filter to get or set filters
            use_remote_data_for_update: Whether to use the `worker_set`'s remote workers
                to update the local filters. If False, stats from the remote workers
                will not be used and discarded.
        """
        # No sync/update required in either direction -> Early out.
        if not (update_remote or use_remote_data_for_update):
            return

        logger.debug(f"Synchronizing filters: {local_filters}")

        # Get the filters from the remote workers.
        remote_filters = worker_set.foreach_env_runner(
            func=lambda worker: worker.get_filters(flush_after=True),
            local_env_runner=False,
            timeout_seconds=timeout_seconds,
        )
        if len(remote_filters) != worker_set.num_healthy_remote_workers():
            logger.error(
                "Failed to get remote filters from a rollout worker in "
                "FilterManager! "
                "Filtered metrics may be computed, but filtered wrong."
            )

        # Should we utilize the remote workers' filter stats to update the local
        # filters?
        if use_remote_data_for_update:
            for rf in remote_filters:
                for k in local_filters:
                    local_filters[k].apply_changes(rf[k], with_buffer=False)

        # Should we update the remote workers' filters from the (now possibly synched)
        # local filters?
        if update_remote:
            copies = {k: v.as_serializable() for k, v in local_filters.items()}
            remote_copy = ray.put(copies)

            logger.debug("Updating remote filters ...")
            results = worker_set.foreach_env_runner(
                func=lambda worker: worker.sync_filters(ray.get(remote_copy)),
                local_env_runner=False,
                timeout_seconds=timeout_seconds,
            )
            if len(results) != worker_set.num_healthy_remote_workers():
                logger.error(
                    "Failed to set remote filters to a rollout worker in "
                    "FilterManager. "
                    "Filtered metrics may be computed, but filtered wrong."
                )
