from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray


class FilterManager(object):
    """Manages filters and coordination across remote evaluators that expose
        `get_filters` and `sync_filters`.
    """

    def __init__(self, **filters):
        self._filters = filters

    def synchronize(self, remotes):
        """Aggregates all filters from remote evaluators, updates local
            copy and broadcasts new copy of filters to all remote evaluators.

        Args:
            remotes: List of remote evaluators with filters.
        """
        remote_filters = ray.get(
            [r.get_filters.remote(flush_after=True) for r in remotes])
        for rf in remote_filters:
            for k in self._filters:
                self._filters[k].apply_changes(rf[k], with_buffer=False)
        copies = {k: v.lockless() if hasattr(v, "lockless") else v.copy()
                  for k, v in self._filters.items()}
        [r.sync_filters.remote(**copies) for r in remotes]
