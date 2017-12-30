from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray


class FilterManager(object):
    """Manages filters and coordination across remote evaluators that expose
        `get_filters` and `sync_filters`.
    """

    @staticmethod
    def synchronize(local_filters, remotes):
        """Aggregates all filters from remote evaluators.

        Local copy is updated and then broadcasted to all remote evaluators.

        Args:
            local_filters (dict): Filters to be synchronized.
            remotes (list): Remote evaluators with filters.
        """
        remote_filters = ray.get(
            [r.get_filters.remote(flush_after=True) for r in remotes])
        for rf in remote_filters:
            for k in local_filters:
                local_filters[k].apply_changes(rf[k], with_buffer=False)
        copies = {k: v.as_serializable() for k, v in local_filters.items()}
        remote_copy = ray.put(copies)
        [r.sync_filters.remote(remote_copy) for r in remotes]

    @staticmethod
    def update_filters(old_filters, new_filters):
        """Changes old filters to new and rebases any accumulated delta.

        Args:
            old_filters (dict): Filters to be synchronized.
            new_filters (dict): Filters with new state.
        """
        for name, current_f in old_filters.items():
            updated = new_filters[name]
            updated_copy = updated.copy()
            updated_copy.apply_changes(current_f, with_buffer=True)
            current_f.sync(updated_copy)
