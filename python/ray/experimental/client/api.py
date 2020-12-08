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
    """
    APIImpl is the interface to implement for whichever version of the core
    Ray API that needs abstracting when run in client mode.
    """

    @abstractmethod
    def get(self, *args, **kwargs):
        """
        get is the hook stub passed on to replace `ray.get`

        :param args: opaque arguments
        :param kwargs: opaque keyword arguments
        """
        pass

    @abstractmethod
    def put(self, *args, **kwargs):
        """
        put is the hook stub passed on to replace `ray.put`

        :param args: opaque arguments
        :param kwargs: opaque keyword arguments
        """
        pass

    @abstractmethod
    def wait(self, *args, **kwargs):
        """
        wait is the hook stub passed on to replace `ray.wait`

        :param args: opaque arguments
        :param kwargs: opaque keyword arguments
        """
        pass

    @abstractmethod
    def remote(self, *args, **kwargs):
        """
        remote is the hook stub passed on to replace `ray.remote`.
        This sets up remote functions or actors.

        :param args: opaque arguments
        :param kwargs: opaque keyword arguments
        """
        pass

    @abstractmethod
    def call_remote(self, instance, *args, **kwargs):
        """
        call_remote is called by Client stub objects when they are going to
        be executed remotely, eg, `f.remote()` or `actor_cls.remote()`.
        This allows the client stub objects to be passed around and be
        implemented in the most effective way whether it's in the client,
        clientserver, or raylet worker.

        :param instance: The Client-side stub reference to a remote object
        :param args: opaque arguments
        :param kwargs: opaque keyword arguments
        """
        pass

    @abstractmethod
    def get_actor_from_object(self, id):
        """
        get_actor_from_object returns a reference to an actor
        from an opaque actor ID, passed in as bytes.

        :param id: Client-side actor ref to retrieve a Ray reference to
        """
        pass

    @abstractmethod
    def close(self, *args, **kwargs):
        """
        close cleans up an API connection by closing any channels or
        shutting down any servers gracefully.
        """
        pass


class ClientAPI(APIImpl):
    """
    The Client-side methods corresponding to the ray API. Delegates
    to the Client Worker that contains the connection to the ClientServer.
    """

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

    def get_actor_from_object(self, id):
        raise Exception("Calling get_actor_from_object on the client side")

    def close(self, *args, **kwargs):
        return self.worker.close()

    def __getattr__(self, key: str):
        if not key.startswith("_"):
            raise NotImplementedError(
                "Not available in Ray client: `ray.{}`. This method is only "
                "available within Ray remote functions and is not yet "
                "implemented in the client API.".format(key))
        return self.__getattribute__(key)
