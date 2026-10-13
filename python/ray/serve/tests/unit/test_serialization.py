import sys
import threading

import pytest
from fastapi import FastAPI
from pydantic import BaseModel, ConfigDict, ValidationError

from ray import cloudpickle, serve
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


def test_pydantic_deferred_build_model_round_trips():
    """A defer_build=True pydantic model holds a placeholder validator that recursed when
    unpickled; it must round-trip and still build a working validator on first use.
    """
    ensure_serialization_context()

    class Msg(BaseModel):
        model_config = ConfigDict(defer_build=True)
        text: str

    restored = cloudpickle.loads(cloudpickle.dumps(Msg))
    assert restored.model_validate({"text": "hi"}).text == "hi"
    with pytest.raises(ValidationError):
        restored.model_validate({})


def test_ingress_accepts_route_with_deferred_build_model():
    """serve.ingress(app) freezes the app by pickling it, which raised RecursionError when
    a route took a defer_build=True model, as OpenAI SDK models are."""

    class Msg(BaseModel):
        model_config = ConfigDict(defer_build=True)
        text: str

    app = FastAPI()

    @app.post("/")
    async def echo(m: Msg) -> Msg:
        return m

    @serve.ingress(app)
    class Ingress:
        pass


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
