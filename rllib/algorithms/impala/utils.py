from collections import defaultdict, deque

import numpy as np


class _SleepTimeController:
    def __init__(self):
        self.L = 0.0
        self.H = 0.4

        self._recompute_candidates()

        # Defaultdict mapping.
        self.results = defaultdict(lambda: deque(maxlen=3))

        self.iteration = 0

    def _recompute_candidates(self):
        self.center = (self.L + self.H) / 2
        self.low = (self.L + self.center) / 2
        self.high = (self.H + self.center) / 2

        # Expand a little if range becomes too narrow to avoid
        # overoptimization.
        if self.H - self.L < 0.00001:
            self.L = max(self.center - 0.1, 0.0)
            self.H = min(self.center + 0.1, 1.0)
            self._recompute_candidates()
            # Reduce results, just in case it has grown too much.
            c, l, h = (
                self.results[self.center],
                self.results[self.low],
                self.results[self.high],
            )
            self.results = defaultdict(lambda: deque(maxlen=3))
            self.results[self.center] = c
            self.results[self.low] = l
            self.results[self.high] = h

    @property
    def current(self):
        if len(self.results[self.center]) < 3:
            return self.center
        elif len(self.results[self.low]) < 3:
            return self.low
        else:
            return self.high

    def log_result(self, performance):
        self.iteration += 1

        # Skip first 2 iterations for ignoring warm-up effect.
        if self.iteration < 2:
            return

        self.results[self.current].append(performance)

        # If all candidates have at least 3 results logged, re-evaluate
        # and compute new L and H.
        center, low, high = self.center, self.low, self.high
        if (
            len(self.results[center]) == 3
            and len(self.results[low]) == 3
            and len(self.results[high]) == 3
        ):
            perf_center = np.mean(self.results[center])
            perf_low = np.mean(self.results[low])
            perf_high = np.mean(self.results[high])
            # Case: `center` is best.
            if perf_center > perf_low and perf_center > perf_high:
                self.L = low
                self.H = high
                # Erase low/high results: We'll not use these again.
                self.results.pop(low, None)
                self.results.pop(high, None)
            # Case: `low` is best.
            elif perf_low > perf_center and perf_low > perf_high:
                self.H = center
                # Erase center/high results: We'll not use these again.
                self.results.pop(center, None)
                self.results.pop(high, None)
            # Case: `high` is best.
            else:
                self.L = center
                # Erase center/low results: We'll not use these again.
                self.results.pop(center, None)
                self.results.pop(low, None)

            self._recompute_candidates()


if __name__ == "__main__":
    controller = _SleepTimeController()
    for _ in range(1000):
        performance = np.random.random()
        controller.log_result(performance)
