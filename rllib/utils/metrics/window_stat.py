import numpy as np

from ray.rllib.utils.annotations import OldAPIStack


@OldAPIStack
class WindowStat:
    """Handles/stores incoming dataset and provides window-based statistics.

    .. testcode::
        :skipif: True

        win_stats = WindowStat("level", 3)
        win_stats.push(5.0)
        win_stats.push(7.0)
        win_stats.push(7.0)
        win_stats.push(10.0)
        # Expect 8.0 as the mean of the last 3 values: (7+7+10)/3=8.0
        print(win_stats.mean())

    .. testoutput::

        8.0
    """

    def __init__(self, name: str, n: int):
        """Initializes a WindowStat instance.

        Args:
            name: The name of the stats to collect and return stats for.
            n: The window size. Statistics will be computed for the last n
                items received from the stream.
        """
        # The window-size.
        self.window_size = n
        # The name of the data (used for `self.stats()`).
        self.name = name
        # List of items to do calculations over (len=self.n).
        self.items = [None] * self.window_size
        # The current index to insert the next item into `self.items`.
        self.idx = 0
        # How many items have been added over the lifetime of this object.
        self.count = 0

    def push(self, obj) -> None:
        """Pushes a new value/object into the data buffer."""
        # Insert object at current index.
        self.items[self.idx] = obj
        # Increase insertion index by 1.
        self.idx += 1
        # Increase lifetime count by 1.
        self.count += 1
        # Fix index in case of rollover.
        self.idx %= len(self.items)

    def mean(self) -> float:
        """Returns the (NaN-)mean of the last `self.window_size` items."""
        return float(np.nanmean(self.items[: self.count]))

    def std(self) -> float:
        """Returns the (NaN)-stddev of the last `self.window_size` items."""
        return float(np.nanstd(self.items[: self.count]))

    def quantiles(self) -> np.ndarray:
        """Returns ndarray with 0, 10, 50, 90, and 100 percentiles."""
        if not self.count:
            return np.ndarray([], dtype=np.float32)
        else:
            return np.nanpercentile(
                self.items[: self.count], [0, 10, 50, 90, 100]
            ).tolist()

    def stats(self):
        return {
            self.name + "_count": int(self.count),
            self.name + "_mean": self.mean(),
            self.name + "_std": self.std(),
            self.name + "_quantiles": self.quantiles(),
        }
