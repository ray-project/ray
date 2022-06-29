import pytest
import grpc

from ray.util.client.worker import Worker


class Credentials(grpc.ChannelCredentials):
    def __init__(self, name):
        self.name = name


def test_grpc_client_credentials_are_passed_to_channel(monkeypatch):
    class Stop(Exception):
        def __init__(self, credentials):
            self.credentials = credentials

    class MockChannel:
        def __init__(self, conn_str, credentials, options, compression):
            self.credentials = credentials

        def subscribe(self, f):
            raise Stop(self.credentials)

    def mock_secure_channel(conn_str, credentials, options=None, compression=None):
        return MockChannel(conn_str, credentials, options, compression)

    monkeypatch.setattr(grpc, "secure_channel", mock_secure_channel)

    # Credentials should be respected whether secure is set or not.

    with pytest.raises(Stop) as stop:
        Worker(secure=False, _credentials=Credentials("test"))
    assert stop.value.credentials.name == "test"

    with pytest.raises(Stop) as stop:
        Worker(secure=True, _credentials=Credentials("test"))
    assert stop.value.credentials.name == "test"


def test_grpc_client_credentials_are_generated(monkeypatch):
    # Test that credentials are generated when secure is True, but _credentials
    # isn't passed.
    class Stop(Exception):
        def __init__(self, result):
            self.result = result

    def mock_gen_credentials():
        raise Stop("ssl_channel_credentials called")

    monkeypatch.setattr(grpc, "ssl_channel_credentials", mock_gen_credentials)

    with pytest.raises(Stop) as stop:
        Worker(secure=True)
    assert stop.value.result == "ssl_channel_credentials called"


if __name__ == "__main__":
    import os
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
