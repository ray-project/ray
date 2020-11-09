from abc import ABC
from abc import abstractmethod
from ray.experimental.client.worker import Worker


class APIImpl(ABC):
    @abstractmethod
    def get(self, *args, **kwargs):
        pass

    @abstractmethod
    def put(self, *args, **kwargs):
        pass

    @abstractmethod
    def remote(self, *args, **kwargs):
        pass


class ClientAPI(APIImpl):
    def __init__(self, worker):
        self.worker: Worker = worker

    def get(self, *args, **kwargs):
        return self.worker.get(*args, **kwargs)

    def put(self, *args, **kwargs):
        return self.worker.put(*args, **kwargs)

    def remote(self, *args, **kwargs):
        return self.worker.remote(*args, **kwargs)
