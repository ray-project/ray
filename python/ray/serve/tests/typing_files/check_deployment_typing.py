"""Type checking tests for the `@serve.deployment` decorator.

This file is used with mypy to verify that the bare, called and direct-call forms
of the decorator produce a `Deployment`. Run with:
    mypy python/ray/serve/tests/typing_files/check_deployment_typing.py python/ray/serve/api.py  \
        --follow-imports=silent \
        --ignore-missing-imports

mypy will fail if any assert_type() call doesn't match the expected type.
"""

from typing_extensions import assert_type

from ray import serve
from ray.serve.deployment import Application, Deployment


def test_bare_decorator() -> None:
    """Test that `@serve.deployment` without parentheses returns a Deployment."""

    @serve.deployment
    def bare(name: str) -> str:
        return name

    assert_type(bare, Deployment)
    assert_type(bare.bind(), Application)
    assert_type(bare.options(num_replicas=2), Deployment)


def test_called_decorator() -> None:
    """Test that `@serve.deployment(...)` returns a Deployment."""

    @serve.deployment(num_replicas=2)
    def called(name: str) -> str:
        return name

    assert_type(called, Deployment)
    assert_type(called.bind(), Application)
    assert_type(called.options(num_replicas=3), Deployment)


def test_direct_call() -> None:
    """Test that calling `serve.deployment` on a class returns a Deployment."""

    class MyClass:
        def __call__(self) -> str:
            return "hello"

    # mypy and pyrefly ignore the return type of a class decorator
    # (python/mypy#3135), so the class is passed by call here.
    assert_type(serve.deployment(MyClass), Deployment)
    assert_type(serve.deployment(MyClass, num_replicas=2), Deployment)
    assert_type(serve.deployment(num_replicas=2)(MyClass), Deployment)
