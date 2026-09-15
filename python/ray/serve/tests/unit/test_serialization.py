import sys
import threading

import pytest

from ray import cloudpickle
from ray.serve._private.utils import ensure_serialization_context


def test_thread_lock_serializer_round_trips():
    """FastAPI >= 0.137 embeds a threading.Lock in the ASGI app object, breaking
    serve.ingress(app) with 'cannot pickle _thread.lock'. The addon serializer must let
    an object holding a lock round-trip through cloudpickle, reconstructing a fresh,
    usable lock (locks carry no transferable state).
    """
    ensure_serialization_context()

    class Holder:
        def __init__(self):
            self.lock = threading.Lock()
            self.rlock = threading.RLock()
            self.value = 7

    restored = cloudpickle.loads(cloudpickle.dumps(Holder()))

    assert restored.value == 7
    assert isinstance(restored.lock, type(threading.Lock()))
    assert isinstance(restored.rlock, type(threading.RLock()))
    # The reconstructed locks are fresh and usable.
    assert restored.lock.acquire(blocking=False)
    restored.lock.release()
    assert restored.rlock.acquire(blocking=False)
    restored.rlock.release()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
