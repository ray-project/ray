import subprocess
import sys
import threading

import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel, ConfigDict, ValidationError

from ray import cloudpickle
from ray.serve._private.utils import ensure_serialization_context


def _load_from_fresh_process(tmp_path, script):
    """Run script, which writes a pickle to argv[1], in a new process and load it here."""
    path = tmp_path / "payload.pkl"
    subprocess.run([sys.executable, "-c", script, str(path)], check=True)
    return cloudpickle.loads(path.read_bytes())


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


def test_pydantic_deferred_model_app_works_where_unpickled(tmp_path):
    """serve.ingress pickles the app; its deferred models must still validate and build
    in the process that unpickles it, as a replica does."""
    # Msg lives in the script's __main__, so cloudpickle pickles it by value.
    script = """
import sys
from fastapi import FastAPI
from pydantic import BaseModel, ConfigDict, ValidationError
from ray import cloudpickle, serve


class Msg(BaseModel):
    model_config = ConfigDict(defer_build=True)
    text: str


app = FastAPI()


@app.post("/")
async def shout(m: Msg) -> dict:
    return Msg(text=m.text.upper()).model_dump()


@serve.ingress(app)
class Ingress:
    pass


open(sys.argv[1], "wb").write(cloudpickle.dumps(app))
"""
    client = TestClient(_load_from_fresh_process(tmp_path, script))
    assert client.post("/", json={"text": "hi"}).json() == {"text": "HI"}
    assert client.post("/", json={}).status_code == 422


def test_pydantic_deferred_model_deployment_works_where_unpickled(tmp_path):
    """A deployment without serve.ingress is pickled at decoration, often before Ray
    starts; its deferred models must still work in the replica that unpickles it."""
    script = """
import sys
from pydantic import BaseModel, ConfigDict
from ray import serve


class Msg(BaseModel):
    model_config = ConfigDict(defer_build=True)
    text: str


@serve.deployment
class Echo:
    def __call__(self, text: str) -> str:
        return Msg(text=text).text.upper()


open(sys.argv[1], "wb").write(Echo._replica_config.serialized_deployment_def)
"""
    echo_cls = _load_from_fresh_process(tmp_path, script)
    assert echo_cls()("hi") == "HI"


def test_pydantic_nested_deferred_models_work_where_unpickled(tmp_path):
    """A restored model that embeds another must not reuse the empty schema of the
    field-less class cloudpickle builds before restoring the inner one."""
    # pydantic < 2.8 installs no core-schema placeholder; deleting it here mirrors that.
    order_cls = _load_from_fresh_process(
        tmp_path,
        """
import sys
from pydantic import BaseModel, ConfigDict
from ray import cloudpickle, serve  # Importing serve registers the reducer.


class Item(BaseModel):
    model_config = ConfigDict(defer_build=True)
    sku: str


class Order(BaseModel):
    model_config = ConfigDict(defer_build=True)
    items: list[Item]


for model in (Item, Order):
    if "__pydantic_core_schema__" in vars(model):
        delattr(model, "__pydantic_core_schema__")
open(sys.argv[1], "wb").write(cloudpickle.dumps(Order))
""",
    )
    order = order_cls.model_validate({"items": [{"sku": "a"}]})
    assert order.model_dump() == {"items": [{"sku": "a"}]}
    with pytest.raises(ValidationError):
        order_cls.model_validate({"items": [{}]})


def test_pydantic_deferred_model_reloaded_after_first_use():
    """Loading in the pickling process puts the captured placeholder back on a model
    that has since been built."""

    class Msg(BaseModel):
        model_config = ConfigDict(defer_build=True)
        text: str

    payload = cloudpickle.dumps(Msg)
    Msg.model_validate({"text": "a"})
    assert cloudpickle.loads(payload)(text="b").text == "b"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
