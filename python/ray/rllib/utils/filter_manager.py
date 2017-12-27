from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray


class FilterManager(object):

    def __init__(self, **filters):
        self._filters = filters

    def synchronize(self, remotes):
        """Applies given filter's buffer to own filters.

        Args:
            remotes: List of remote actors that expose `get_filters` and
                `sync_filters`
        """
        remote_filters = ray.get(
            [r.get_filters.remote(flush_after=True) for r in remotes])
        for rf in remote_filters:
            for k in self._filters:
                self._filters[k].apply_changes(rf[k], with_buffer=False)
        copies = {k: v.lockless() if hasattr(v, "lockless") else v.copy()
                  for k, v in self._filters.items()}
        [r.sync_filters.remote(**copies) for r in remotes]
