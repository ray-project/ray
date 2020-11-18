# This file defines an interface and client-side API stub
# for referring either to the core Ray API or the same interface
# from the Ray client.
#
# In tandem with __init__.py, we want to expose an API that's
# close to `python/ray/__init__.py` but with more than one implementation.
# The stubs in __init__ should call into a well-defined interface.
# Only the core Ray API implementation should actually `import ray`
# (and thus import all the raylet worker C bindings and such).
# But to make sure that we're matching these calls, we define this API.

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
    def wait(self, *args, **kwargs):
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

    def wait(self, *args, **kwargs):
        return self.worker.wait(*args, **kwargs)

    def remote(self, *args, **kwargs):
        return self.worker.remote(*args, **kwargs)

    def call_remote(self, f, *args, **kwargs):
        return self.worker.call_remote(f, *args, **kwargs)

    def close(self, *args, **kwargs):
        return self.worker.close()
