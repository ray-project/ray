from abc import ABC


class TaskConsumerWrapper(ABC):
    def __init__(self, *args, **kwargs):
        pass

    def initialize_callable(self, consumer_concurrency: int):
        pass

    def __del__(self):
        pass
