import pytest

from ray import serve
from ray.anyscale.serve._private.replica_result import gRPCReplicaResult
from ray.serve._private.replica_result import ActorReplicaResult, ReplicaResult
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
