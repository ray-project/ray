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
    """

    def __init__(self):
        self.counters = collections.defaultdict(int)
        self.timers = collections.defaultdict(_Timer)
        self.info = {}
        self.current_actor = None

    def save(self):
        """Return a serializable copy of this context."""
        return {
            "counters": dict(self.counters),
            "info": dict(self.info),
            "timers": None,  # TODO(ekl) consider persisting timers too
        }

    def restore(self, values):
        """Restores state given the output of save()."""
        self.counters.clear()
        self.counters.update(values["counters"])
        self.timers.clear()
        self.info = values["info"]


class SharedMetrics:
    """Holds a ref to a (shared) metrics context.

    This is used by LocalIterator.union() to point the metrics contexts of
    entirely separate iterator chains to the same underlying context."""

    def __init__(self, metrics: MetricsContext = None):
        self.metrics = metrics or MetricsContext()

    def set(self, metrics):
        self.metrics = metrics

    def get(self):
        return self.metrics
