"""Type checking tests for DeploymentHandle, DeploymentResponse, and DeploymentResponseGenerator.

This file is used with mypy to verify that the generic type annotations
on handle classes work correctly. Run with:
    mypy python/ray/serve/tests/typing_files/check_handle_typing.py python/ray/serve/handle.py  \
        --follow-imports=skip \
        --ignore-missing-imports

mypy will fail if any assert_type() call doesn't match the expected type.
"""

from typing import Any, AsyncIterator, Iterator, Union

from typing_extensions import assert_type

from ray.serve.handle import (
    DeploymentHandle,
    DeploymentResponse,
    DeploymentResponseGenerator,
)


def test_deployment_response_types() -> None:
    """Test that DeploymentResponse[R] preserves R through methods."""
    response: DeploymentResponse[str] = None  # type: ignore[assignment]

    # result() should return R (str)
    result = response.result()
    assert_type(result, str)

    # Properties should work
    assert_type(response.request_id, str)
    assert_type(response.by_reference, bool)


async def test_deployment_response_await() -> None:
    """Test that awaiting DeploymentResponse[R] returns R."""
    response: DeploymentResponse[str] = None  # type: ignore[assignment]

    # Awaiting should return R (str)
    awaited_result = await response
    assert_type(awaited_result, str)


def test_deployment_response_generator_sync() -> None:
    """Test that DeploymentResponseGenerator[R] iteration returns R."""
    gen: DeploymentResponseGenerator[int] = None  # type: ignore[assignment]

    # __iter__ should return Iterator[R]
    assert_type(iter(gen), Iterator[int])

    # __next__ should return R (int)
    item = next(gen)
    assert_type(item, int)

    # For loop iteration
    for value in gen:
        assert_type(value, int)
        break


async def test_deployment_response_generator_async() -> None:
    """Test that async iteration of DeploymentResponseGenerator[R] returns R."""
    gen: DeploymentResponseGenerator[int] = None  # type: ignore[assignment]

    # __aiter__ should return AsyncIterator[R]
    assert_type(gen.__aiter__(), AsyncIterator[int])

    # __anext__ should return R (int)
    item = await gen.__anext__()
    assert_type(item, int)

    # Async for loop iteration
    async for value in gen:
        assert_type(value, int)
        break


def test_deployment_handle_types() -> None:
    """Test DeploymentHandle type annotations."""

    class MyDeployment:
        def get_string(self) -> str:
            return "hello"

        def get_int(self) -> int:
            return 42

    handle: DeploymentHandle[MyDeployment] = None  # type: ignore[assignment]

    # Basic properties
    assert_type(handle.deployment_name, str)
    assert_type(handle.app_name, str)
    assert_type(handle.is_initialized, bool)

    # options() should return DeploymentHandle[T]
    new_handle = handle.options(method_name="get_string")
    assert_type(new_handle, DeploymentHandle[MyDeployment])

    # remote() returns Union[DeploymentResponse[Any], DeploymentResponseGenerator[Any]]
    # (until plugin is implemented to infer the actual return type)
    response = handle.remote()
    assert_type(
        response,
        Union[DeploymentResponse[Any], DeploymentResponseGenerator[Any]],
    )


def test_chained_handle_access() -> None:
    """Test that accessing methods on handle returns typed handle."""

    class MyDeployment:
        def my_method(self, x: int) -> str:
            return str(x)

    handle: DeploymentHandle[MyDeployment] = None  # type: ignore[assignment]

    # Accessing a method via __getattr__ should return DeploymentHandle[T]
    method_handle = handle.my_method
    assert_type(method_handle, DeploymentHandle[MyDeployment])


# =============================================================================
# TESTS THAT REQUIRE MYPY PLUGIN
# =============================================================================
# The following tests verify that the mypy plugin correctly infers return types
# based on which deployment method is being called. These tests are commented
# out because they will fail without the plugin.
#
# To enable after plugin is implemented:
# 1. Uncomment the tests below
# 2. Run: mypy python/ray/serve/tests/typing_files/check_handle_typing.py
# 3. All assert_type() calls should pass
# =============================================================================


def test_plugin_infers_method_return_type() -> None:
    """[REQUIRES PLUGIN] Test that remote() infers return type from method."""
    from typing import Generator

    class MyDeployment:
        def get_user(self, user_id: int) -> str:
            return f"user_{user_id}"

        def get_count(self) -> int:
            return 42

        def stream_items(self) -> Generator[bytes, None, None]:
            yield b"chunk1"
            yield b"chunk2"

    _: DeploymentHandle[MyDeployment] = None  # type: ignore[assignment]

    # # Calling a method that returns str should give DeploymentResponse[str]
    # response_str = handle.get_user.remote(123)
    # assert_type(response_str, DeploymentResponse[str])

    # # result() should return str
    # user = response_str.result()
    # assert_type(user, str)

    # # Calling a method that returns int should give DeploymentResponse[int]
    # response_int = handle.get_count.remote()
    # assert_type(response_int, DeploymentResponse[int])

    # # result() should return int
    # count = response_int.result()
    # assert_type(count, int)


async def test_plugin_infers_await_return_type() -> None:
    """[REQUIRES PLUGIN] Test that await infers return type from method."""

    class MyDeployment:
        def process(self, data: str) -> dict:
            return {"data": data}

    _: DeploymentHandle[MyDeployment] = None  # type: ignore[assignment]

    # response = handle.process.remote("test")
    # assert_type(response, DeploymentResponse[dict])

    # # Awaiting should return dict
    # result = await response
    # assert_type(result, dict)


def test_plugin_infers_generator_yield_type() -> None:
    """[REQUIRES PLUGIN] Test that streaming methods infer yield type."""
    from typing import Generator

    class MyDeployment:
        def stream_strings(self) -> Generator[str, None, None]:
            yield "a"
            yield "b"

        def stream_ints(self) -> Generator[int, None, None]:
            yield 1
            yield 2

    _: DeploymentHandle[MyDeployment] = None  # type: ignore[assignment]

    # # Streaming handle with generator method should give DeploymentResponseGenerator
    # streaming_handle = handle.options(stream=True)

    # gen_str = streaming_handle.stream_strings.remote()
    # assert_type(gen_str, DeploymentResponseGenerator[str])

    # # Iteration should yield str
    # for item in gen_str:
    #     assert_type(item, str)
    #     break

    # gen_int = streaming_handle.stream_ints.remote()
    # assert_type(gen_int, DeploymentResponseGenerator[int])

    # # Iteration should yield int
    # for item in gen_int:
    #     assert_type(item, int)
    #     break


async def test_plugin_async_generator_iteration() -> None:
    """[REQUIRES PLUGIN] Test async iteration with inferred yield type."""
    from typing import Generator

    class MyDeployment:
        def stream_bytes(self) -> Generator[bytes, None, None]:
            yield b"chunk"

    _: DeploymentHandle[MyDeployment] = None  # type: ignore[assignment]

    # streaming_handle = handle.options(stream=True)
    # gen = streaming_handle.stream_bytes.remote()
    # assert_type(gen, DeploymentResponseGenerator[bytes])

    # # Async iteration should yield bytes
    # async for chunk in gen:
    #     assert_type(chunk, bytes)
    #     break


def test_plugin_complex_return_types() -> None:
    from typing import Dict, List, Optional, Tuple

    class User:
        name: str
        age: int

    class MyDeployment:
        def get_users(self) -> List[User]:
            return []

        def get_user_dict(self) -> Dict[str, User]:
            return {}

        def get_optional(self) -> Optional[User]:
            return None

        def get_tuple(self) -> Tuple[str, int, User]:
            return ("", 0, User())

    _: DeploymentHandle[MyDeployment] = None  # type: ignore[assignment]

    # response_list = handle.get_users.remote()
    # assert_type(response_list, DeploymentResponse[List[User]])
    # users = response_list.result()
    # assert_type(users, List[User])

    # response_dict = handle.get_user_dict.remote()
    # assert_type(response_dict, DeploymentResponse[Dict[str, User]])

    # response_optional = handle.get_optional.remote()
    # assert_type(response_optional, DeploymentResponse[Optional[User]])

    # response_tuple = handle.get_tuple.remote()
    # assert_type(response_tuple, DeploymentResponse[Tuple[str, int, User]])
