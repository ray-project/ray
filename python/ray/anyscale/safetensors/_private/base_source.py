import concurrent.futures
import threading
from abc import ABC, abstractmethod
from multiprocessing import shared_memory
from typing import List, Optional


class BaseSource(ABC):
    @abstractmethod
    def shutdown(
        self, sink_futures: List[concurrent.futures.Future]
    ) -> Optional[concurrent.futures.Future]:
        ...

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback):
        ...


class SharedMemorySource(BaseSource):
    def __init__(self, size_bytes: int, async_shutdown: bool):
        self.shm = shared_memory.SharedMemory(create=True, size=size_bytes)
        self.async_shutdown = async_shutdown

    def _shutdown(self):
        self.shm.close()
        self.shm.unlink()

    def shutdown(
        self, sink_futures: List[concurrent.futures.Future]
    ) -> Optional[concurrent.futures.Future]:
        future: concurrent.futures.Future = concurrent.futures.Future()

        def _async_shutdown():
            concurrent.futures.wait(sink_futures)
            try:
                future.set_result(self._shutdown())
            except Exception as e:
                future.set_exception(e)

        if self.async_shutdown:
            threading.Thread(target=_async_shutdown).start()
            return future
        else:
            self._shutdown()
            return None

    def __exit__(self, exc_type, exc_value, traceback):
        self.shutdown()
