import re

import pytest

from ray import serve
from ray._common.test_utils import SignalActor
from ray.serve._private.common import OBJ_REF_NOT_SUPPORTED_ERROR
from ray.serve._private.replica_result import (
    ActorReplicaResult,
    ReplicaResult,
    gRPCReplicaResult,
)
from ray.serve.handle import DeploymentHandle
from ray.serve.tests.conftest import *  # noqa
from ray.serve.tests.conftest import _shared_serve_instance  # noqa


@pytest.mark.parametrize(
    "by_reference,expected_result",
    [(True, ActorReplicaResult), (False, gRPCReplicaResult)],
)
def test_init_by_reference(
    serve_instance, by_reference: bool, expected_result: ReplicaResult
):
    @serve.deployment
    def f():
        return "hi"

    h = serve.run(f.bind())

    resp = h.options(_by_reference=by_reference).remote()
    assert resp.result() == "hi"
    assert isinstance(resp._replica_result, expected_result)


@pytest.mark.timeout(60)
async def test_by_reference_false_raises_error(serve_instance):
    signal = SignalActor.remote()

    @serve.deployment
    async def f():
        await signal.wait.remote()
        return "hi"

    h = serve.run(f.bind())
    with pytest.raises(
        RuntimeError, match=re.escape(OBJ_REF_NOT_SUPPORTED_ERROR.args[0])
    ):
        response = h.options(_by_reference=False).remote()
        await response._to_object_ref()

    await signal.send.remote()


@pytest.mark.parametrize("inner_by_reference", [True, False])
@pytest.mark.parametrize("outer_by_reference", [True, False])
def test_compose_deployments_in_app(
    serve_instance, inner_by_reference, outer_by_reference
):
    """Test composing deployment handle refs within a deployment."""

    @serve.deployment
    class Downstream:
        def __init__(self, msg: str):
            self._msg = msg

        def __call__(self, inp1: str, inp2: str):
            return f"{self._msg}|{inp1}|{inp2}"

    @serve.deployment
    class Deployment:
        def __init__(self, handle1: DeploymentHandle, handle2: DeploymentHandle):
            self._handle1 = handle1.options(_by_reference=outer_by_reference)
            self._handle2 = handle2.options(_by_reference=inner_by_reference)

        async def __call__(self):
            result = await self._handle1.remote(
                self._handle2.remote("hi1", inp2="hi2"),
                inp2=self._handle2.remote("hi3", inp2="hi4"),
            )
            return f"driver|{result}"

    handle = serve.run(
        Deployment.bind(
            Downstream.options(name="downstream1").bind("downstream1"),
            Downstream.options(name="downstream2").bind("downstream2"),
        ),
    )
    assert (
        handle.remote().result()
        == "driver|downstream1|downstream2|hi1|hi2|downstream2|hi3|hi4"
    )


@pytest.mark.parametrize("inner_by_reference", [True, False])
@pytest.mark.parametrize("outer_by_reference", [True, False])
def test_compose_apps(serve_instance, inner_by_reference, outer_by_reference):
    """Test composing deployment handle refs outside of a deployment."""

    @serve.deployment
    class Deployment:
        def __init__(self, msg: str):
            self._msg = msg

        def __call__(self, inp1: str, inp2: str):
            return f"{self._msg}|{inp1}|{inp2}"

    handle1 = serve.run(
        Deployment.bind("app1"), name="app1", route_prefix="/app1"
    ).options(_by_reference=outer_by_reference)
    handle2 = serve.run(
        Deployment.bind("app2"), name="app2", route_prefix="/app2"
    ).options(_by_reference=inner_by_reference)

    assert (
        handle1.remote(
            handle2.remote("hi1", inp2="hi2"),
            inp2=handle2.remote("hi3", inp2="hi4"),
        ).result()
        == "app1|app2|hi1|hi2|app2|hi3|hi4"
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
