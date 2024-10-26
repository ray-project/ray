from typing import List
from unittest import mock

import pytest

from ray import serve
from ray.serve._private.build_app import build_app
from ray.serve._private.common import DeploymentHandleSource
from ray.serve.deployment import Application, Deployment
from ray.serve.handle import DeploymentHandle, _HandleOptions

@pytest.fixture(autouse=True)
def patch_handle_eq():
    """Patch DeploymentHandle.__eq__ to compare options we care about."""
    def _patched_handle_eq(self, other):
        if type(self) != type(other):
            return False

        return all([
            type(self) == type(other),
            self.deployment_id == other.deployment_id,
            self.handle_options == other.handle_options,
        ])

    with mock.patch(
        "ray.serve.handle._DeploymentHandleBase.__eq__", _patched_handle_eq
    ):
        yield


def _build_and_check(
    app: Application,
    *,
    expected_ingress_name: str,
    expected_deployments: List[Deployment],
    app_name: str = "default",
):
    ingress_name, deployments = build_app(app, name=app_name)
    assert ingress_name == expected_ingress_name
    assert len(deployments) == len(expected_deployments)

    for expected_deployment in expected_deployments:
        generated_deployment = None
        for d in deployments:
            if d.name == expected_deployment.name:
                generated_deployment = d

        assert generated_deployment is not None, (
            f"Expected a deployment with name '{expected_deployment.name}' "
            "to be generated but none was found. All generated names: "
            f"{list(d.name for d in deployments)}"
        )

        assert expected_deployment == generated_deployment


def test_basic_single_deployment():
    @serve.deployment(
        num_replicas=123,
        max_ongoing_requests=10,
        max_queued_requests=10,
    )
    class D:
        pass

    app = D.bind("hello world!", hello="world")
    _build_and_check(
        app,
        expected_ingress_name="D",
        expected_deployments=[
            D.options(
                name="D", _init_args=("hello world!",), _init_kwargs={"hello": "world"}
            )
        ],
    )


def test_basic_multi_deployment():
    @serve.deployment(num_replicas=3)
    class Inner:
        pass

    @serve.deployment(num_replicas=1)
    class Outer:
        pass

    app = Outer.bind(Inner.bind())
    _build_and_check(
        app,
        expected_ingress_name="Outer",
        expected_deployments=[
            Inner.options(name="Inner", _init_args=tuple(), _init_kwargs={}),
            Outer.options(
                name="Outer",
                _init_args=(
                    DeploymentHandle(
                        "Inner",
                        app_name="default",
                        handle_options=_HandleOptions(
                            _source=DeploymentHandleSource.REPLICA
                        ),
                    ),
                ),
                _init_kwargs={},
            ),
        ],
    )
