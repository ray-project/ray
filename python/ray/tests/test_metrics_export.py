import pytest
import requests

from unittest.mock import patch

from ray.dashboard.metrics_exporter.actions import ActionHandler
from ray.dashboard.metrics_exporter.client import MetricsExportClient
from ray.dashboard.metrics_exporter.client import Exporter
from ray.dashboard.metrics_exporter.schema import (AuthResponse, BaseModel,
                                                   ValidationError, Field)

MOCK_DASHBOARD_ID = "1234"
MOCK_DASHBOARD_ADDRESS = "http://127.0.0.1:9081"
MOCK_ACCESS_TOKEN = "1234"


def _setup_client_and_exporter(controller):
    exporter = Exporter(MOCK_DASHBOARD_ID, MOCK_DASHBOARD_ADDRESS, controller)
    client = MetricsExportClient(MOCK_DASHBOARD_ADDRESS, controller,
                                 MOCK_DASHBOARD_ID, exporter)
    return exporter, client


"""
Test Exporter
"""


@patch("ray.dashboard.dashboard.DashboardController")
def test_verify_exporter_cannot_run_without_access_token(mock_controller):
    exporter, client = _setup_client_and_exporter(mock_controller)
    # Should raise an assertion error because there's no access token set.
    with pytest.raises(AssertionError):
        exporter.run()


"""
Test Client
"""


@patch("ray.dashboard.dashboard.DashboardController")
@patch(
    "ray.dashboard.metrics_exporter.api.authentication_request",
    side_effect=requests.exceptions.HTTPError)
def test_client_invalid_request_status_returned(auth_request, mock_controller):
    """
    If authentication request fails with an invalid status code,
       `start_exporting_metrics` should fail.
    """
    exporter, client = _setup_client_and_exporter(mock_controller)

    # authenticate should throw an exception because API request fails.
    with pytest.raises(requests.exceptions.HTTPError):
        client._authenticate()

    # This should fail because authentication throws an exception.
    result, error = client.start_exporting_metrics()
    assert result is False


@patch("ray.dashboard.dashboard.DashboardController")
@patch("ray.dashboard.metrics_exporter.api.authentication_request")
def test_authentication(auth_request, mock_controller):
    auth_request.return_value = AuthResponse(
        access_token_dashboard=MOCK_ACCESS_TOKEN,
        access_token_ingest=MOCK_ACCESS_TOKEN)
    exporter, client = _setup_client_and_exporter(mock_controller)

    assert client.enabled is False
    client._authenticate()
    assert client.dashboard_url == "{address}/dashboard/{access_token}".format(
        address=MOCK_DASHBOARD_ADDRESS, access_token=MOCK_ACCESS_TOKEN)
    assert client.enabled is True


@patch.object(Exporter, "start")
@patch("ray.dashboard.dashboard.DashboardController")
@patch("ray.dashboard.metrics_exporter.api.authentication_request")
def test_start_exporting_metrics_without_authentication(
        auth_request, mock_controller, start):
    """
    `start_exporting_metrics` should trigger authentication if users
        are not authenticated.
    """
    auth_request.return_value = AuthResponse(
        access_token_dashboard=MOCK_ACCESS_TOKEN,
        access_token_ingest=MOCK_ACCESS_TOKEN)
    exporter, client = _setup_client_and_exporter(mock_controller)

    # start_exporting_metrics should succeed.
    result, error = client.start_exporting_metrics()
    assert result is True
    assert error is None
    assert client.enabled is True


@patch.object(Exporter, "start")
@patch("ray.dashboard.dashboard.DashboardController")
@patch("ray.dashboard.metrics_exporter.api.authentication_request")
def test_start_exporting_metrics_with_authentication(auth_request,
                                                     mock_controller, start):
    """
    If users are already authenticated, `start_exporting_metrics`
       should not authenticate users.
    """
    auth_request.return_value = AuthResponse(
        access_token_dashboard=MOCK_ACCESS_TOKEN,
        access_token_ingest=MOCK_ACCESS_TOKEN)
    exporter, client = _setup_client_and_exporter(mock_controller)
    # Already authenticated.
    client._authenticate()
    assert client.enabled is True

    result, error = client.start_exporting_metrics()
    # Auth request should be called only once because
    # it was already authenticated.
    auth_request.call_count == 1
    assert result is True
    assert error is None


@patch.object(Exporter, "start")
@patch("ray.dashboard.dashboard.DashboardController")
@patch("ray.dashboard.metrics_exporter.api.authentication_request")
def test_start_exporting_metrics_succeed(auth_request, mock_controller, start):
    auth_request.return_value = AuthResponse(
        access_token_dashboard=MOCK_ACCESS_TOKEN,
        access_token_ingest=MOCK_ACCESS_TOKEN)
    exporter, client = _setup_client_and_exporter(mock_controller)

    result, error = client.start_exporting_metrics()
    assert result is True
    assert error is None
    assert client.is_exporting_started is True
    start.call_count == 1

    with pytest.raises(AssertionError):
        client.start_exporting_metrics()


"""
BaseModel Test
"""


def test_base_model():
    DEFAULT_VALUE = "default"

    class A(BaseModel):
        __schema__ = {
            "a": Field(required=True, default=None, type=str),
            "b": Field(required=False, default=DEFAULT_VALUE, type=str)
        }

    # Test the correct case.
    obj = {"a": "1", "b": "1"}
    a = A.parse_obj(obj)
    assert a.a == "1"
    assert a.b == "1"
    assert a._dict == obj
    string = "{name}\n{dict}".format(name=A.__name__, dict=str(obj))
    assert str(a) == string

    # Test wrong types. It is not checked in the current implementation.
    obj = {"a": 1, "b": 2}
    a = A.parse_obj(obj)
    assert a.a == 1
    assert a.b == 2

    # Test wrong types. parse_obj can only parse dictionary.
    obj = None
    with pytest.raises(AssertionError):
        a = A.parse_obj(obj)

    # Test when required fields are not provided.
    obj = {"b": "1"}
    with pytest.raises(ValidationError):
        a = A.parse_obj(obj)

    # Test optional fields are set to default when fields are not given.
    obj = {"a": "1"}
    a = A.parse_obj(obj)
    assert a.b == DEFAULT_VALUE

    # Test when fields that are not defined in the schema is given.
    # It should be ignoered
    obj = {"a": "a", "b": "b", "c": "c"}
    a = A.parse_obj(obj)
    assert a.a == "a"
    assert a.b == "b"
    assert a.c == "c"


"""
Test Action Handler
"""


def _get_mock_kill_action():
    return {
        "type": "KILL_ACTOR",
        "actor_id": "1234",
        "ip_address": "1234",
        "port": 30
    }


@patch("ray.dashboard.dashboard.DashboardController")
def test_handle_kill_action(mock_controller):
    action_handler = ActionHandler(mock_controller)
    kill_action = _get_mock_kill_action()
    action_handler.handle_kill_action(kill_action)
    assert mock_controller.kill_actor.call_count == 1


@patch("ray.dashboard.dashboard.DashboardController")
def test_handle_kill_action_invalid_dict(mock_controller):
    action_handler = ActionHandler(mock_controller)
    kill_action = {"type": "KILL_ACTOR", "ip_address": "1234", "port": 30}

    with pytest.raises(ValidationError):
        action_handler.handle_kill_action(kill_action)


@patch("ray.dashboard.dashboard.DashboardController")
def test_handle_actions_many_kill_actor(mock_controller):
    action_handler = ActionHandler(mock_controller)
    # 10 actions required.
    actions = [_get_mock_kill_action() for _ in range(10)]

    action_handler.handle_actions(actions)
    assert mock_controller.kill_actor.call_count == 10


@patch("ray.dashboard.dashboard.DashboardController")
def test_handle_actions_kill_actor_and_mixed_type(mock_controller):
    action_handler = ActionHandler(mock_controller)
    wrong_type_action = {"type": "NON_EXIST"}
    actions = [
        _get_mock_kill_action(), wrong_type_action,
        _get_mock_kill_action()
    ]

    action_handler.handle_actions(actions)
    assert mock_controller.kill_actor.call_count == 2


@patch("ray.dashboard.dashboard.DashboardController")
def test_handle_actions_only_wrong_type(mock_controller):
    action_handler = ActionHandler(mock_controller)
    wrong_type_action = {"type": "NON_EXIST"}
    actions = [wrong_type_action for _ in range(10)]

    action_handler.handle_actions(actions)
    assert mock_controller.kill_actor.call_count == 0


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
