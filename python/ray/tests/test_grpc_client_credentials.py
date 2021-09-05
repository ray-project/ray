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
        def __init__(self, conn_str, credentials, options):
            self.credentials = credentials

        def subscribe(self, f):
            raise Stop(self.credentials)

    def mock_secure_channel(conn_str,
                            credentials,
                            options=None,
                            compression=None):
        return MockChannel(conn_str, credentials, options, compression)

    monkeypatch.setattr(grpc, "secure_channel", mock_secure_channel)

    with pytest.raises(Stop) as stop:
        Worker(secure=True, credentials=Credentials("test"))
    assert stop.value.credentials.name == "test"
