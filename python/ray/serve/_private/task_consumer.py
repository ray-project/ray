from abc import ABC
from typing import Optional


class TaskConsumerWrapper(ABC):
    def __init__(self, *args, **kwargs):
        pass

    def initialize_callable(self, consumer_concurrency: int):
        pass

    def _serve_graceful_shutdown(self, timeout_s: Optional[float] = None):
        pass

    def __del__(self):
        pass
