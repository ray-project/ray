import atexit
import logging
import signal

import pandas as pd

import ray

logger = logging.getLogger(__name__)

CACHED_PROGRESS_TRACKERS = {}


@ray.remote
class ProgressTracker:
    def __init__(
        self,
        save_path: str,
        save_interval: int = 1,
        write_paths_: bool = False,
    ):
        self.save_path = save_path
        self.write_paths_ = write_paths_

        try:
            self.progress = pd.read_parquet(self.save_path)
        except Exception:
            self.progress = pd.DataFrame(columns=["__key__", "path"])

        self.initial_keys = ray.put(self.progress["__key__"].tolist())
        self.initial_shards = ray.put(
            self.progress.drop_duplicates("path")["path"].tolist()
        )

        self.counter = 0
        self.save_interval = save_interval

        atexit.register(self.close)
        self.init_signal_handlers()

    def sigkill_handler(self, signum, frame):
        self.close()

        if self.original_handlers.get(signum):
            self.original_handlers[signum](signum, frame)

    def init_signal_handlers(self):
        self.original_handlers = {
            signal.SIGTERM: signal.getsignal(signal.SIGTERM),
        }
        signal.signal(signal.SIGTERM, self.sigkill_handler)

    def set_save_interval(self, save_interval: int):
        self.save_interval = save_interval

    def update(self, items: list[dict[str, str]]):
        assert all("__key__" in item for item in items)
        self.progress = pd.concat(
            [self.progress, pd.DataFrame(items)], ignore_index=True
        )

        self.counter += 1
        if self.counter % self.save_interval == 0:
            self.progress.to_parquet(self.save_path)

    def update_path(self, key: str, path: str):
        self.progress.loc[self.progress["__key__"] == key, "path"] = path

    def should_write_paths_(self) -> bool:
        return self.write_paths_

    def get_progress(self) -> list[str]:
        return self.progress["__key__"].tolist()

    def get_initial_progress(self) -> tuple[ray.ObjectRef, ray.ObjectRef]:
        return self.initial_keys, self.initial_shards

    def close(self) -> bool:
        logger.info(f"Writing progress tracker to {self.save_path}")
        self.progress.to_parquet(self.save_path)
        return True
