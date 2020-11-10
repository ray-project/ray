from abc import ABC
from abc import abstractmethod
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

    @abstractmethod
    def call_remote(self, f, *args, **kwargs):
        pass

    @abstractmethod
    def close(self, *args, **kwargs):
        pass


class ClientAPI(APIImpl):
    def __init__(self, worker):
        self.worker = worker

    def get(self, *args, **kwargs):
        return self.worker.get(*args, **kwargs)

    def put(self, *args, **kwargs):
        return self.worker.put(*args, **kwargs)

    def remote(self, *args, **kwargs):
        return self.worker.remote(*args, **kwargs)

    def call_remote(self, f, *args, **kwargs):
        return self.worker.call_remote(f, *args, **kwargs)

    def close(self, *args, **kwargs):
        return self.worker.close()
