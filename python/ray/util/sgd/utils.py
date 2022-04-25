import collections
from contextlib import contextmanager
import logging
import numpy as np
import time

import ray
from ray.exceptions import RayActorError

logger = logging.getLogger(__name__)

BATCH_COUNT = "batch_count"
NUM_SAMPLES = "num_samples"
BATCH_SIZE = "*batch_size"


class TimerStat:
    """A running stat for conveniently logging the duration of a code block.

    Note that this class is *not* thread-safe.

    Examples:
        Time a call to 'time.sleep'.

        >>> import time
        >>> from ray.util.sgd.utils import TimerStat
        >>> sleep_timer = TimerStat()
        >>> with sleep_timer:
        ...     time.sleep(1)
        >>> round(sleep_timer.mean) # doctest: +SKIP
        1
    """

    def __init__(self, window_size=10):
        self._window_size = window_size
        self._samples = []
        self._units_processed = []
        self._start_time = None
        self._total_time = 0.0
        self.count = 0

    def __enter__(self):
        assert self._start_time is None, "concurrent updates not supported"
        self._start_time = time.time()

    def __exit__(self, type, value, tb):
        assert self._start_time is not None
        time_delta = time.time() - self._start_time
        self.push(time_delta)
        self._start_time = None

    def push(self, time_delta):
        self._samples.append(time_delta)
        if len(self._samples) > self._window_size:
            self._samples.pop(0)
        self.count += 1
        self._total_time += time_delta

    def push_units_processed(self, n):
        self._units_processed.append(n)
        if len(self._units_processed) > self._window_size:
            self._units_processed.pop(0)

    @property
    def mean(self):
        return np.mean(self._samples)

    @property
    def median(self):
        return np.median(self._samples)

    @property
    def sum(self):
        return np.sum(self._samples)

    @property
    def max(self):
        return np.max(self._samples)

    @property
    def first(self):
        return self._samples[0] if self._samples else None

    @property
    def last(self):
        return self._samples[-1] if self._samples else None

    @property
    def size(self):
        return len(self._samples)

    @property
    def mean_units_processed(self):
        return float(np.mean(self._units_processed))

    @property
    def mean_throughput(self):
        time_total = sum(self._samples)
        if not time_total:
            return 0.0
        return sum(self._units_processed) / time_total

    def reset(self):
        self._samples = []
        self._units_processed = []
        self._start_time = None
        self._total_time = 0.0
        self.count = 0


@contextmanager
def _nullcontext(enter_result=None):
    """Used for mocking timer context."""
    yield enter_result


class TimerCollection:
    """A grouping of Timers."""

    def __init__(self):
        self._timers = collections.defaultdict(TimerStat)
        self._enabled = True

    def disable(self):
        self._enabled = False

    def enable(self):
        self._enabled = True

    def reset(self):
        for timer in self._timers.values():
            timer.reset()

    def record(self, key):
        if self._enabled:
            return self._timers[key]
        else:
            return _nullcontext()

    def stats(self, mean=True, last=False):
        aggregates = {}
        for k, t in self._timers.items():
            if t.count > 0:
                if mean:
                    aggregates[f"mean_{k}_s"] = t.mean
                if last:
                    aggregates[f"last_{k}_s"] = t.last
        return aggregates


class AverageMeter:
    """Utility for computing and storing the average and most recent value.

    Example:
        >>> from ray.util.sgd.utils import AverageMeter
        >>> meter = AverageMeter()
        >>> meter.update(5)
        >>> meter.val, meter.avg, meter.sum
        (5, 5.0, 5)
        >>> meter.update(10, n=4)
        >>> meter.val, meter.avg, meter.sum
        (10, 9.0, 45)
    """

    def __init__(self):
        self.reset()

    def reset(self):
        self.val = 0
        self.avg = 0
        self.sum = 0
        self.count = 0

    def update(self, val, n=1):
        """Update current value, total sum, and average."""
        self.val = val
        self.sum += val * n
        self.count += n
        self.avg = self.sum / self.count


class AverageMeterCollection:
    """A grouping of AverageMeters.

    This utility is used in TrainingOperator.train_epoch and
    TrainingOperator.validate to
    collect averages and most recent value across all batches. One
    AverageMeter object is used for each metric.

    Example:
        >>> from ray.util.sgd.utils import AverageMeterCollection
        >>> meter_collection = AverageMeterCollection()
        >>> meter_collection.update({"loss": 0.5, "acc": 0.5}, n=32)
        >>> meter_collection.summary() # doctest: +SKIP
        {'batch_count': 1, 'num_samples': 32, 'loss': 0.5,
        'last_loss': 0.5, 'acc': 0.5, 'last_acc': 0.5}
        >>> meter_collection.update({"loss": 0.1, "acc": 0.9}, n=32)
        >>> meter_collection.summary() # doctest: +SKIP
        {'batch_count': 2, 'num_samples': 64, 'loss': 0.3,
        'last_loss': 0.1, 'acc': 0.7, 'last_acc': 0.9}
    """

    def __init__(self):
        self._batch_count = 0
        self.n = 0
        self._meters = collections.defaultdict(AverageMeter)

    def update(self, metrics, n=1):
        """Does one batch of updates for the provided metrics."""
        self._batch_count += 1
        self.n += n
        for metric, value in metrics.items():
            self._meters[metric].update(value, n=n)

    def summary(self):
        """Returns a dict of average and most recent values for each metric."""
        stats = {BATCH_COUNT: self._batch_count, NUM_SAMPLES: self.n}
        for metric, meter in self._meters.items():
            stats[str(metric)] = meter.avg
            stats["last_" + str(metric)] = meter.val
        return stats


def check_for_failure(remote_values):
    """Checks remote values for any that returned and failed.

    Args:
        remote_values (list): List of object refs representing functions
            that may fail in the middle of execution. For example, running
            a SGD training loop in multiple parallel actor calls.

    Returns:
        Bool for success in executing given remote tasks.
    """
    unfinished = remote_values
    try:
        while len(unfinished) > 0:
            finished, unfinished = ray.wait(unfinished)
            finished = ray.get(finished)
        return True
    except RayActorError as exc:
        logger.exception(str(exc))
    return False


def override(interface_class):
    def overrider(method):
        assert method.__name__ in dir(interface_class)
        return method

    return overrider
