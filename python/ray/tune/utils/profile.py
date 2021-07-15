import csv
import itertools
import time


class TimeProfiler:
    def __init__(self, filename: str):
        self._filename = filename
        self._start = 0
        self._times = []
        self._current_times = {}

    def start(self):
        if self._current_times:
            self._times.append(self._current_times)

        self._start = time.perf_counter()
        self._current_times = {}

    def measure(self, key: str):
        now = time.perf_counter()
        taken = now - self._start

        self._current_times[key] = self._current_times.get(key, 0.) + taken
        self._start = now

    def stop(self):
        all_keys = set(
            itertools.chain.from_iterable(t.keys() for t in self._times))
        with open(self._filename, "wt") as fp:
            writer = csv.DictWriter(f=fp, fieldnames=sorted(all_keys))
            writer.writeheader()

            writer.writerows(self._times)
