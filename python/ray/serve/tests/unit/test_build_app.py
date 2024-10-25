import sys
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
        return all(
            [
                isinstance(other, type(self)),
                self.deployment_id == other.deployment_id,
                self.handle_options == other.handle_options,
            ]
        )

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
            str([d.name for d in deployments])
        )

        assert expected_deployment == generated_deployment


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
                    DeploymentHandle(
                        "Inner",
                        app_name="default",
                        handle_options=_HandleOptions(
                            _source=DeploymentHandleSource.REPLICA
                        ),
                    ),
                ),
                _init_kwargs={
                    "other": DeploymentHandle(
                        "Other",
                        app_name="default",
                        handle_options=_HandleOptions(
                            _source=DeploymentHandleSource.REPLICA
                        ),
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
                        DeploymentHandle(
                            "Inner",
                            app_name="default",
                            handle_options=_HandleOptions(
                                _source=DeploymentHandleSource.REPLICA
                            ),
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
                    DeploymentHandle(
                        "Inner",
                        app_name="custom",
                        handle_options=_HandleOptions(
                            _source=DeploymentHandleSource.REPLICA
                        ),
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
                    DeploymentHandle(
                        "Inner",
                        app_name="default",
                        handle_options=_HandleOptions(
                            _source=DeploymentHandleSource.REPLICA
                        ),
                    ),
                    DeploymentHandle(
                        "Inner_1",
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
    shared_handle = DeploymentHandle(
        "Shared",
        app_name="default",
        handle_options=_HandleOptions(_source=DeploymentHandleSource.REPLICA),
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
                    DeploymentHandle(
                        "Inner",
                        app_name="default",
                        handle_options=_HandleOptions(
                            _source=DeploymentHandleSource.REPLICA
                        ),
                    ),
                    shared_handle,
                ),
                _init_kwargs={},
            ),
        ],
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
