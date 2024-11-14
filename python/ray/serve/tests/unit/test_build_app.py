import sys
from typing import Any, List

import pytest

from ray import serve
from ray.serve._private.build_app import BuiltApplication, build_app
from ray.serve._private.common import DeploymentID
from ray.serve.deployment import Application, Deployment
from ray.serve.handle import DeploymentHandle


class FakeDeploymentHandle:
    def __init__(self, deployment_name: str, app_name: str):
        self.deployment_id = DeploymentID(deployment_name, app_name)

    @classmethod
    def from_deployment(cls, deployment, app_name: str) -> "FakeDeploymentHandle":
        return cls(deployment.name, app_name)

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, FakeDeploymentHandle)
            and self.deployment_id == other.deployment_id
        )


def _build_and_check(
    app: Application,
    *,
    expected_ingress_name: str,
    expected_deployments: List[Deployment],
    app_name: str = "default",
):
    built_app: BuiltApplication = build_app(
        app,
        name=app_name,
        # Each real DeploymentHandle has a unique ID (intentionally), so the below
        # equality checks don't work. Use a fake implementation instead.
        make_deployment_handle=FakeDeploymentHandle.from_deployment,
    )
    assert built_app.name == app_name
    assert built_app.ingress_deployment_name == expected_ingress_name
    assert len(built_app.deployments) == len(expected_deployments)

    # Check that the returned deployment_handles are populated properly.
    assert len(built_app.deployment_handles) == len(expected_deployments)
    for d in expected_deployments:
        h = built_app.deployment_handles.get(d.name, None)
        assert h is not None, f"No handle returned for deployment {d.name}."
        assert isinstance(h, FakeDeploymentHandle)
        assert h.deployment_id == DeploymentID(d.name, app_name=app_name)

    for expected_deployment in expected_deployments:
        generated_deployment = None
        for d in built_app.deployments:
            if d.name == expected_deployment.name:
                generated_deployment = d

        assert generated_deployment is not None, (
            f"Expected a deployment with name '{expected_deployment.name}' "
            "to be generated but none was found. All generated names: "
            + str([d.name for d in built_app.deployments])
        )

        assert expected_deployment == generated_deployment


def test_real_deployment_handle_default():
    """Other tests inject a FakeDeploymentHandle, so check the default behavior."""

    @serve.deployment
    class D:
        pass

    built_app: BuiltApplication = build_app(
        D.bind(D.options(name="Inner").bind()),
        name="app-name",
    )
    assert len(built_app.deployments) == 2
    assert len(built_app.deployments[1].init_args) == 1
    assert isinstance(built_app.deployments[1].init_args[0], DeploymentHandle)
    assert built_app.deployments[1].init_args[0].deployment_id == DeploymentID(
        "Inner", app_name="app-name"
    )


def test_single_deployment_basic():
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


def test_single_deployment_custom_name():
    @serve.deployment(
        num_replicas=123,
        max_ongoing_requests=10,
        max_queued_requests=10,
    )
    class D:
        pass

    app = D.options(name="foobar123").bind("hello world!", hello="world")
    _build_and_check(
        app,
        expected_ingress_name="foobar123",
        expected_deployments=[
            D.options(
                name="foobar123",
                _init_args=("hello world!",),
                _init_kwargs={"hello": "world"},
            )
        ],
    )


def test_multi_deployment_basic():
    @serve.deployment(num_replicas=3)
    class Inner:
        pass

    @serve.deployment(num_replicas=1)
    class Outer:
        pass

    app = Outer.bind(Inner.bind(), other=Inner.options(name="Other").bind())
    _build_and_check(
        app,
        expected_ingress_name="Outer",
        expected_deployments=[
            Inner.options(name="Inner", _init_args=tuple(), _init_kwargs={}),
            Inner.options(name="Other", _init_args=tuple(), _init_kwargs={}),
            Outer.options(
                name="Outer",
                _init_args=(
                    FakeDeploymentHandle(
                        "Inner",
                        app_name="default",
                    ),
                ),
                _init_kwargs={
                    "other": FakeDeploymentHandle(
                        "Other",
                        app_name="default",
                    ),
                },
            ),
        ],
    )


def test_multi_deployment_handle_in_nested_obj():
    @serve.deployment(num_replicas=3)
    class Inner:
        pass

    @serve.deployment(num_replicas=1)
    class Outer:
        pass

    app = Outer.bind([Inner.bind()])
    _build_and_check(
        app,
        expected_ingress_name="Outer",
        expected_deployments=[
            Inner.options(name="Inner", _init_args=tuple(), _init_kwargs={}),
            Outer.options(
                name="Outer",
                _init_args=(
                    [
                        FakeDeploymentHandle(
                            "Inner",
                            app_name="default",
                        ),
                    ],
                ),
                _init_kwargs={},
            ),
        ],
    )


def test_multi_deployment_custom_app_name():
    @serve.deployment(num_replicas=3)
    class Inner:
        pass

    @serve.deployment(num_replicas=1)
    class Outer:
        pass

    app = Outer.bind(Inner.bind())
    _build_and_check(
        app,
        app_name="custom",
        expected_ingress_name="Outer",
        expected_deployments=[
            Inner.options(name="Inner", _init_args=tuple(), _init_kwargs={}),
            Outer.options(
                name="Outer",
                _init_args=(
                    FakeDeploymentHandle(
                        "Inner",
                        app_name="custom",
                    ),
                ),
                _init_kwargs={},
            ),
        ],
    )


def test_multi_deployment_name_collision():
    @serve.deployment
    class Inner:
        pass

    @serve.deployment
    class Outer:
        pass

    app = Outer.bind(
        Inner.bind("arg1"),
        Inner.bind("arg2"),
    )
    _build_and_check(
        app,
        expected_ingress_name="Outer",
        expected_deployments=[
            Inner.options(name="Inner", _init_args=("arg1",), _init_kwargs={}),
            Inner.options(name="Inner_1", _init_args=("arg2",), _init_kwargs={}),
            Outer.options(
                name="Outer",
                _init_args=(
                    FakeDeploymentHandle(
                        "Inner",
                        app_name="default",
                    ),
                    FakeDeploymentHandle(
                        "Inner_1",
                        app_name="default",
                    ),
                ),
                _init_kwargs={},
            ),
        ],
    )


def test_multi_deployment_same_app_passed_twice():
    @serve.deployment
    class Shared:
        pass

    @serve.deployment(num_replicas=3)
    class Inner:
        pass

    @serve.deployment(num_replicas=1)
    class Outer:
        pass

    shared = Shared.bind()
    app = Outer.bind(Inner.bind(shared), shared)
    shared_handle = FakeDeploymentHandle(
        "Shared",
        app_name="default",
    )
    _build_and_check(
        app,
        expected_ingress_name="Outer",
        expected_deployments=[
            Shared.options(
                name="Shared",
                _init_args=tuple(),
                _init_kwargs={},
            ),
            Inner.options(name="Inner", _init_args=(shared_handle,), _init_kwargs={}),
            Outer.options(
                name="Outer",
                _init_args=(
                    FakeDeploymentHandle(
                        "Inner",
                        app_name="default",
                    ),
                    shared_handle,
                ),
                _init_kwargs={},
            ),
        ],
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
