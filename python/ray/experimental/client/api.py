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
from typing import TYPE_CHECKING, Any, Union
if TYPE_CHECKING:
    from ray.experimental.client.common import ClientActorHandle
    from ray.experimental.client.common import ClientStub
    from ray.experimental.client.common import ClientObjectRef
    from ray._raylet import ObjectRef

    # Use the imports for type checking.  This is a python 3.6 limitation.
    # See https://www.python.org/dev/peps/pep-0563/
    PutType = Union[ClientObjectRef, ObjectRef]


class APIImpl(ABC):
    """
    APIImpl is the interface to implement for whichever version of the core
    Ray API that needs abstracting when run in client mode.
    """

    @abstractmethod
    def get(self, *args, **kwargs) -> Any:
        """
        get is the hook stub passed on to replace `ray.get`

        Args:
            args: opaque arguments
            kwargs: opaque keyword arguments
        """
        pass

    @abstractmethod
    def put(self, vals: Any, *args,
            **kwargs) -> Union["ClientObjectRef", "ObjectRef"]:
        """
        put is the hook stub passed on to replace `ray.put`

        Args:
            vals: The value or list of values to `put`.
            args: opaque arguments
            kwargs: opaque keyword arguments
        """
        pass

    @abstractmethod
    def wait(self, *args, **kwargs):
        """
        wait is the hook stub passed on to replace `ray.wait`

        Args:
            args: opaque arguments
            kwargs: opaque keyword arguments
        """
        pass

    @abstractmethod
    def remote(self, *args, **kwargs):
        """
        remote is the hook stub passed on to replace `ray.remote`.

        This sets up remote functions or actors, as the decorator,
        but does not execute them.

        Args:
            args: opaque arguments
            kwargs: opaque keyword arguments
        """
        pass

    @abstractmethod
    def call_remote(self, instance: "ClientStub", *args, **kwargs):
        """
        call_remote is called by stub objects to execute them remotely.

        This is used by stub objects in situations where they're called
        with .remote, eg, `f.remote()` or `actor_cls.remote()`.
        This allows the client stub objects to delegate execution to be
        implemented in the most effective way whether it's in the client,
        clientserver, or raylet worker.

        Args:
            instance: The Client-side stub reference to a remote object
            args: opaque arguments
            kwargs: opaque keyword arguments
        """
        pass

    @abstractmethod
    def close(self) -> None:
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

    def call_remote(self, instance: "ClientStub", *args, **kwargs):
        return self.worker.call_remote(instance, *args, **kwargs)

    def close(self) -> None:
        return self.worker.close()

    def kill(self, actor: 'ClientActorHandle', *, no_restart=True):
        return self.worker.terminate_actor(actor, no_restart)

    def cancel(self, obj: 'ClientObjectRef', *, force=False, recursive=True):
        return self.worker.terminate_task(obj, force, recursive)

    def __getattr__(self, key: str):
        if not key.startswith("_"):
            raise NotImplementedError(
                "Not available in Ray client: `ray.{}`. This method is only "
                "available within Ray remote functions and is not yet "
                "implemented in the client API.".format(key))
        return self.__getattribute__(key)
