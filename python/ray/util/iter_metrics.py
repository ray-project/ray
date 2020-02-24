import collections

from ray.util.timer import _Timer


class MetricsContext:
    """Metrics context object for a local iterator.

    This object is accessible by all operators of a local iterator. It can be
    used to store and retrieve global execution metrics for the iterator.
    It can be accessed by calling LocalIterator.get_metrics(), which is only
    allowable inside iterator functions.

    Attributes:
        counters (defaultdict): dict storing increasing metrics.
        timers (defaultdict): dict storing latency timers.
        info (dict): dict storing misc metric values.
        current_actor (ActorHandle): reference to the actor handle that
            produced the current iterator output. This is automatically set
            for gather_async().
        parent_metrics (list): list of other MetricsContexts that have been
            attached to this due to LocalIterator.union().
    """

    def __init__(self):
        self.counters = collections.defaultdict(int)
        self.timers = collections.defaultdict(_Timer)
        self.info = {}
        self.current_actor = None
        self.parent_metrics = []

    def save(self):
        """Return a serializable copy of this context."""
        return {
            "counters": dict(self.counters),
            "info": dict(self.info),
            "timers": None,  # TODO(ekl) consider persisting timers too
            "parent_state": [u.save() for u in self.parent_metrics],
        }

    def restore(self, values):
        """Restores state given the output of save()."""
        self.counters.clear()
        self.counters.update(values["counters"])
        self.timers.clear()
        self.info = values["info"]
        for u, state in zip(self.parent_metrics, values["parent_state"]):
            u.restore(state)
