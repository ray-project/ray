import threading
import time


def get_ray_spilled_and_restored_mb():
    import ray._private.internal_api as internal_api
    import re

    summary_str = internal_api.memory_summary(stats_only=True)

    match = re.search("Spilled (\d+) MiB", summary_str)
    spilled_mb = int(match.group(1)) if match else 0

    match = re.search("Restored (\d+) MiB", summary_str)
    restored_mb = int(match.group(1)) if match else 0

    return spilled_mb, restored_mb


class MaxMemoryUtilizationTracker:
    """
    Class that enables tracking of the maximum memory utilization on a
    system.

    This creates a thread which samples the available memory every sample_interval_s
    seconds. The "available" memory is reported directly from psutil.
    See https://psutil.readthedocs.io/en/latest/#psutil.virtual_memory for more
    information.
    """

    def __init__(self, sample_interval_s: float):
        self._results = {}
        self._stop_event = threading.Event()
        self._print_updates = False

        self._thread = threading.Thread(
            target=self._track_memory_utilization,
            args=(
                sample_interval_s,
                self._print_updates,
                self._results,
                self._stop_event,
            ),
        )

    @staticmethod
    def _track_memory_utilization(
        sample_interval_s: float,
        print_updates: bool,
        output_dict: dict,
        stop_event: threading.Event,
    ):
        import psutil

        min_available = float("inf")

        while not stop_event.is_set():
            memory_stats = psutil.virtual_memory()

            if memory_stats.available < min_available:
                if print_updates:
                    print(
                        "{before:.02f} -> {after:.02f}".format(
                            before=min_available / (1 << 30),
                            after=memory_stats.available / (1 << 30),
                        )
                    )
                min_available = memory_stats.available

            time.sleep(sample_interval_s)

        output_dict["min_available_bytes"] = min_available

    def start(self) -> None:
        assert (
            not self._stop_event.is_set()
        ), "Can't start a thread that has been stopped."
        self._thread.start()

    def stop(self) -> int:
        assert (
            not self._stop_event.is_set()
        ), "Can't stop a thread that has been stopped."
        self._stop_event.set()
        self._thread.join()
        return self._results["min_available_bytes"]


def determine_if_memory_monitor_is_enabled_in_latest_session():
    """
    Grep session_latest raylet logs to see if the memory monitor is enabled.
    This is really only helpful when you're interested in session_latest, use with care.
    """
    import subprocess

    completed_proc = subprocess.run(
        [
            "grep",
            "-q",
            "MemoryMonitor initialized",
            "/tmp/ray/session_latest/logs/raylet.out",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert completed_proc.returncode in [
        0,
        1,
    ], f"Unexpected returncode {completed_proc.returncode}"
    assert not completed_proc.stdout, f"Unexpected stdout {completed_proc.stdout}"
    assert not completed_proc.stderr, f"Unexpected stderr {completed_proc.stderr}"

    return completed_proc.returncode == 0


def test_max_mem_util_tracker():
    max_mem_tracker = MaxMemoryUtilizationTracker(sample_interval_s=1)
    max_mem_tracker.start()

    import numpy as np

    time.sleep(4)
    print("create numpy")
    large_tensor = np.random.randint(10, size=1 << 30, dtype=np.uint8)
    large_tensor += 1
    print("done create numpy")
    time.sleep(2)

    results = max_mem_tracker.stop()
    min_available_gb = results["min_available_bytes"] / (1 << 30)
    print(f"{min_available_gb:.02f}")


if __name__ == "__main__":
    test_max_mem_util_tracker()
