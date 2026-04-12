import pytest

from ray.serve._private.common import RequestMetadata
from ray.serve._private.replica import Replica


class FakeReplica:
    _raise_user_exception = Replica._raise_user_exception

    def __init__(self, wrapped_exception_factory):
        self._wrapped_exception_factory = wrapped_exception_factory

    def _maybe_wrap_grpc_exception(self, exc, request_metadata):
        return self._wrapped_exception_factory(exc, request_metadata)


def _request_metadata():
    return RequestMetadata(request_id="req", internal_request_id="int")


def test_raise_user_exception_non_grpc_preserves_original_exception():
    original_exc = ValueError("user code exception")
    replica = FakeReplica(lambda exc, _: exc)

    with pytest.raises(ValueError) as exc_info:
        replica._raise_user_exception(original_exc, _request_metadata())

    assert exc_info.value is original_exc
    assert exc_info.value.__cause__ is None


def test_raise_user_exception_wrapped_exception_preserves_original_cause():
    original_exc = ValueError("user code exception")
    wrapped_exc = RuntimeError("grpc wrapper")
    replica = FakeReplica(lambda exc, _: wrapped_exc)

    with pytest.raises(RuntimeError) as exc_info:
        replica._raise_user_exception(original_exc, _request_metadata())

    assert exc_info.value is wrapped_exc
    assert exc_info.value.__cause__ is original_exc


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
