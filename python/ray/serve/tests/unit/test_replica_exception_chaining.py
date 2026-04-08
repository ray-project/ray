from contextlib import asynccontextmanager, contextmanager

import pytest

from ray.serve._private.common import RequestMetadata, RequestProtocol
from ray.serve._private.replica import Replica


class FakeUserCallableWrapper:
    async def call_user_method(self, request_metadata, request_args, request_kwargs):
        raise ValueError("user code exception")

    async def call_http_entrypoint(
        self, request_metadata, status_code_callback, scope, receive
    ):
        raise ValueError("http entrypoint exception")
        yield

    async def call_user_generator(self, request_metadata, request_args, request_kwargs):
        raise ValueError("generator exception")
        yield


@contextmanager
def sync_cm():
    yield None


@asynccontextmanager
async def async_cm():
    yield None


class FakeReplica:
    max_ongoing_requests = 10

    def __init__(self, wrap_same_exception=True):
        self._user_callable_wrapper = FakeUserCallableWrapper()
        self._wrap_same_exception = wrap_same_exception

    def _unpack_proxy_args(self, request_metadata, request_args, request_kwargs):
        return request_args, request_kwargs, None

    def _wrap_request(self, request_metadata, ray_trace_ctx):
        return sync_cm()

    def _start_request(self, request_metadata):
        return async_cm()

    def _maybe_wrap_grpc_exception(self, e, request_metadata):
        if self._wrap_same_exception:
            return e
        return RuntimeError("grpc wrapper")

    def _raise_user_exception(self, e, request_metadata):
        wrapped_exception = self._maybe_wrap_grpc_exception(e, request_metadata)
        if wrapped_exception is e:
            raise e
        raise wrapped_exception from e

    def _can_accept_request(self, request_metadata):
        return True

    def get_num_ongoing_requests(self):
        return 1


def _non_grpc_metadata():
    metadata = RequestMetadata(request_id="req", internal_request_id="int")
    metadata._request_protocol = RequestProtocol.UNDEFINED
    return metadata


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("entrypoint", "is_generator"),
    [
        ("handle_request", False),
        ("handle_request_streaming", True),
        ("handle_request_with_rejection", True),
    ],
)
async def test_non_grpc_requests_do_not_create_self_cause(entrypoint, is_generator):
    replica = FakeReplica(wrap_same_exception=True)
    metadata = _non_grpc_metadata()

    with pytest.raises(ValueError) as exc_info:
        method = getattr(Replica, entrypoint)
        if is_generator:
            async for _ in method(replica, metadata):
                pass
        else:
            await method(replica, metadata)

    assert exc_info.value.__cause__ is None


@pytest.mark.asyncio
async def test_grpc_wrapped_exception_preserves_original_cause():
    replica = FakeReplica(wrap_same_exception=False)
    metadata = RequestMetadata(request_id="req", internal_request_id="int")
    metadata._request_protocol = RequestProtocol.GRPC

    with pytest.raises(RuntimeError) as exc_info:
        await Replica.handle_request(replica, metadata)

    assert isinstance(exc_info.value.__cause__, ValueError)
    assert exc_info.value.__cause__ is not exc_info.value
