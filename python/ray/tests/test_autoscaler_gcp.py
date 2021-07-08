from typing import List

import pytest

from ray.autoscaler._private.gcp.node_provider import _retry


class MockGCPNodeProvider:
    def __init__(self, errors: List[type]):
        # List off errors to raise while retrying self.mock_method
        self.errors = errors
        # Incremented during each retry via self._construct_client
        self.error_index = -1
        # Mirrors the __init__ of GCPNodeProvider
        # Also called during each retry in _retry
        self._construct_clients()

    def _construct_clients(self):
        # In real life, called during each retry to reinitializes api clients.
        # Here, increments index in list of errors passed into test.
        self.error_index += 1

    @_retry
    def mock_method(self, *args, **kwargs):
        error = self.errors[self.error_index]
        if error:
            raise error
        return (args, kwargs)


def test_gcp_broken_pipe_retry():
    # Short names for two types of errors
    b, v = BrokenPipeError, ValueError
    # BrokenPipeError is supposed to caught with up to 5 tries.
    # ValueError is an arbitrarily chosen exception which should not be caught.

    error_inputs = [
        [None],  # No error, success
        [b, b, b, b, None],  # Four failures followed by success
        [b, b, v, b, None],  # ValueError raised
        [b, b, b, b, b, None],  # max 5 tries allowed,raise
        [b, b, b, b, b, b, None],  # also raise
    ]
    expected_errors_raised = [None, None, v, b, b]

    for error_input, expected_error_raised in zip(error_inputs,
                                                  expected_errors_raised):
        provider = MockGCPNodeProvider(error_input)
        if expected_error_raised:
            with pytest.raises(expected_error_raised):
                provider.mock_method(1, 2, a=4, b=5)
        else:
            assert provider.mock_method(
                1, 2, a=4, b=5) == ((1, 2), {
                    "a": 4,
                    "b": 5
                })


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-sv", __file__]))
