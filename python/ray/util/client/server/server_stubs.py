from contextlib import contextmanager
from abc import ABC
from abc import abstractmethod

_current_server = None


@contextmanager
def current_server(r):
    global _current_server
    remote = _current_server
    _current_server = r
    try:
        yield
    finally:
        _current_server = remote


class ClientReferenceSentinel(ABC):
    def __init__(self, client_id, id):
        self.client_id = client_id
        self.id = id

    def __reduce__(self):
        remote_obj = self.get_remote_obj()
        if remote_obj is None:
            return (self.__class__, (self.client_id, self.id))
        return (identity, (remote_obj,))

    @abstractmethod
    def get_remote_obj(self):
        pass

    def get_real_ref_from_server(self):
        global _current_server
        if _current_server is None:
            return None
        client_map = _current_server.client_side_ref_map.get(self.client_id, None)
        if client_map is None:
            return None
        return client_map.get(self.id, None)


class ClientReferenceActor(ClientReferenceSentinel):
    def get_remote_obj(self):
        global _current_server
        real_ref_id = self.get_real_ref_from_server()
        if real_ref_id is None:
            return None
        return _current_server.lookup_or_register_actor(
            real_ref_id, self.client_id, None
        )


class ClientReferenceFunction(ClientReferenceSentinel):
    def get_remote_obj(self):
        global _current_server
        real_ref_id = self.get_real_ref_from_server()
        if real_ref_id is None:
            return None
        return _current_server.lookup_or_register_func(
            real_ref_id, self.client_id, None
        )


def identity(x):
    return x
