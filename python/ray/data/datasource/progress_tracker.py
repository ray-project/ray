import asyncio
import atexit
import json
import logging
import signal
import threading
from collections import defaultdict
from dataclasses import dataclass, field

import ray
from ray.util.queue import Queue

logger = logging.getLogger(__name__)


Key = str
Path = str

CACHED_PROGRESS_TRACKERS = {}


@dataclass
class Progress:
    pending: dict[Path, set[Key]] = field(
        default_factory=lambda: defaultdict(set), metadata="path -> keys"
    )
    completed: dict[Path, set[Key]] = field(
        default_factory=lambda: defaultdict(set), metadata="path -> keys"
    )

    @property
    def skip_files(self) -> set[Path]:
        return set(self.completed.keys()) - set(self.pending.keys())

    @property
    def skip_keys(self) -> set[Key]:
        return set().union(*self.completed.values())

    def to_json(self) -> str:
        return json.dumps(
            {
                "completed": {
                    path: list(keys) for path, keys in self.completed.items() if keys
                },
                "in_progress": {
                    path: list(keys) for path, keys in self.pending.items() if keys
                },
            }
        )

    @classmethod
    def load(cls, json_bytes: bytes) -> "Progress":
        raw_data = json.loads(json_bytes)
        return cls(
            pending=defaultdict(
                set, {path: set(keys) for path, keys in raw_data["in_progress"].items()}
            ),
            completed=defaultdict(
                set, {path: set(keys) for path, keys in raw_data["completed"].items()}
            ),
        )

    def deepcopy(self) -> "Progress":
        return Progress(
            pending=defaultdict(
                set, {path: set(keys) for path, keys in self.pending.items()}
            ),
            completed=defaultdict(
                set, {path: set(keys) for path, keys in self.completed.items()}
            ),
        )


@ray.remote
class ProgressTracker:
    def __init__(self, save_path: str, save_interval: int = 1_000):
        self.save_path = save_path
        self.initial_progress = self.load()

        self.progress = self.initial_progress.deepcopy()
        self.pending_queue = Queue()
        self.completed_queue = Queue(maxsize=save_interval)

        atexit.register(self.write)

        if threading.current_thread() is threading.main_thread():
            self._init_signal_handlers()

    def _sigkill_handler(self, signum, frame):
        asyncio.run(self.write())

        if self.original_handlers.get(signum):
            self.original_handlers[signum](signum, frame)

    def _init_signal_handlers(self):
        self.original_handlers = {
            signal.SIGTERM: signal.getsignal(signal.SIGTERM),
        }
        signal.signal(signal.SIGTERM, self._sigkill_handler)

    def get_initial_progress(self) -> Progress:
        return self.initial_progress

    def get_pending_queue(self) -> Queue:
        return self.pending_queue

    def get_completed_queue(self) -> Queue:
        return self.completed_queue

    def _flush(self):
        logger.debug("Syncing progress tracker")

        num_completed = self.completed_queue.size()
        num_pending = self.pending_queue.size()

        # flush the queues
        completed_keys: list[Key] = self.completed_queue.get_nowait_batch(num_completed)
        pending_path_and_keys: list[
            tuple[Path, Key]
        ] = self.pending_queue.get_nowait_batch(num_pending)

        pending: dict[Path, set[Key]] = {}
        for path, key in pending_path_and_keys:
            pending[path].add(key)

        # update pending in self.progress
        for path, keys in pending.items():
            self.progress.pending[path].update(keys)

        # update completed in self.progress, and remove from pending
        for key in completed_keys:
            all_search = list(self.progress.completed.items()) + list(
                self.progress.pending.items()
            )
            for path, keys in all_search:
                if key in keys:
                    self.progress.completed[path].add(key)
                    self.progress.pending[path].remove(key)
                    break

    def write(self):
        try:
            import fsspec
        except ImportError:
            raise ImportError("Please install fsspec")

        self._flush()

        logger.debug(f"Writing progress tracker to {self.save_path}")
        with fsspec.open(self.save_path, "wb", compression="gzip") as f:
            f.write(self.progress.to_json().encode("utf-8"))

        return True

    def load(self):
        try:
            import fsspec
        except ImportError:
            raise ImportError("Please install fsspec")

        try:
            with fsspec.open(self.save_path, "rb", compression="gzip") as f:
                progress = Progress.load(f.read())
            logger.info(f"Loading progress from {self.save_path}")
        except FileNotFoundError:
            logger.info(f"Creating new progress file at {self.save_path}")
            progress = Progress()
            with fsspec.open(self.save_path, "wb", compression="gzip") as f:
                f.write(progress.to_json().encode("utf-8"))

        return progress

    def shutdown(self):
        self.write()

    def __del__(self):
        self.shutdown()
