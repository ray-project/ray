# coding: utf-8
"""Unit tests for ray._private.test_utils.get_system_metric_for_component."""
import sys
from unittest.mock import MagicMock, patch

import pytest
import requests

from ray._private.test_utils import get_system_metric_for_component

SUCCESS_JSON = {
    "data": {
        "result": [
            {"value": [1234567890, "42.0"]},
            {"value": [1234567890, "13.5"]},
        ]
    }
}


def _make_resp(status_code: int, json_data=None, text: str = "") -> MagicMock:
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    if json_data is not None:
        resp.json.return_value = json_data
    # Mimic real Response: raise_for_status() raises HTTPError for 4xx/5xx and
    # sets e.response to the response object.
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            f"{status_code} Error", response=resp
        )
    return resp


@pytest.fixture
def mock_global_node():
    """Stub out ray._private.worker._global_node so the helper can build a
    session_name without a real cluster."""
    fake_node = MagicMock()
    fake_node.get_session_dir_path.return_value = "/tmp/ray/session_2026-01-01_test"
    with patch("ray._private.worker._global_node", fake_node):
        yield fake_node


@pytest.fixture
def no_sleep():
    """Don't actually sleep during retry backoff."""
    with patch("ray._private.test_utils.time.sleep") as mock_sleep:
        yield mock_sleep


def test_success_first_try(mock_global_node, no_sleep):
    with patch("ray._private.test_utils.requests.get") as mock_get:
        mock_get.return_value = _make_resp(200, json_data=SUCCESS_JSON)
        result = get_system_metric_for_component("ray_x", "gcs", "http://prom")
    assert result == [42.0, 13.5]
    assert mock_get.call_count == 1
    no_sleep.assert_not_called()


def test_500_500_200_retries_then_succeeds(mock_global_node, no_sleep):
    with patch("ray._private.test_utils.requests.get") as mock_get:
        mock_get.side_effect = [
            _make_resp(500, text="boom 1"),
            _make_resp(500, text="boom 2"),
            _make_resp(200, json_data=SUCCESS_JSON),
        ]
        result = get_system_metric_for_component("ray_x", "gcs", "http://prom")
    assert result == [42.0, 13.5]
    assert mock_get.call_count == 3
    assert no_sleep.call_count == 2
    # Exponential backoff: 1s, 2s
    assert [c.args[0] for c in no_sleep.call_args_list] == [1.0, 2.0]


def test_all_500s_raises_with_full_context(mock_global_node, no_sleep):
    with patch("ray._private.test_utils.requests.get") as mock_get:
        mock_get.return_value = _make_resp(500, text="prometheus exploded")
        with pytest.raises(RuntimeError) as exc_info:
            get_system_metric_for_component(
                "ray_component_uss_bytes", "dashboard", "http://prom-server"
            )
    msg = str(exc_info.value)
    assert "last_status=500" in msg
    assert "http://prom-server" in msg
    assert "ray_component_uss_bytes" in msg
    assert "dashboard" in msg
    assert "prometheus exploded" in msg
    assert mock_get.call_count == 3
    # Two sleeps between the three failed attempts
    assert no_sleep.call_count == 2


def test_500_body_truncated_to_500_chars(mock_global_node, no_sleep):
    huge = "x" * 5000
    with patch("ray._private.test_utils.requests.get") as mock_get:
        mock_get.return_value = _make_resp(500, text=huge)
        with pytest.raises(RuntimeError) as exc_info:
            get_system_metric_for_component("ray_x", "gcs", "http://prom")
    msg = str(exc_info.value)
    # The truncated body (500 chars) must appear; the full 5000-char body must not
    assert "x" * 500 in msg
    assert "x" * 501 not in msg


def test_mixed_failures_then_success(mock_global_node, no_sleep):
    with patch("ray._private.test_utils.requests.get") as mock_get:
        mock_get.side_effect = [
            _make_resp(503, text="unavailable"),
            requests.exceptions.ConnectionError("connection refused"),
            _make_resp(200, json_data=SUCCESS_JSON),
        ]
        result = get_system_metric_for_component("ray_x", "gcs", "http://prom")
    assert result == [42.0, 13.5]
    assert mock_get.call_count == 3
    assert no_sleep.call_count == 2


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 422])
def test_4xx_not_retried(mock_global_node, no_sleep, status_code):
    with patch("ray._private.test_utils.requests.get") as mock_get:
        mock_get.return_value = _make_resp(status_code, text="bad query")
        with pytest.raises(RuntimeError) as exc_info:
            get_system_metric_for_component("ray_x", "gcs", "http://prom")
    msg = str(exc_info.value)
    assert f"last_status={status_code}" in msg
    assert "bad query" in msg
    assert mock_get.call_count == 1
    no_sleep.assert_not_called()


def test_all_timeouts_raises_with_error_class(mock_global_node, no_sleep):
    with patch("ray._private.test_utils.requests.get") as mock_get:
        mock_get.side_effect = requests.exceptions.Timeout("timed out")
        with pytest.raises(RuntimeError) as exc_info:
            get_system_metric_for_component("ray_x", "gcs", "http://prom")
    msg = str(exc_info.value)
    assert "last_error" in msg
    assert "Timeout" in msg
    assert "http://prom" in msg
    assert mock_get.call_count == 3
    assert no_sleep.call_count == 2


def test_max_attempts_kwarg_respected(mock_global_node, no_sleep):
    with patch("ray._private.test_utils.requests.get") as mock_get:
        mock_get.return_value = _make_resp(500, text="boom")
        with pytest.raises(RuntimeError):
            get_system_metric_for_component(
                "ray_x", "gcs", "http://prom", max_attempts=5
            )
    assert mock_get.call_count == 5
    assert no_sleep.call_count == 4


def test_request_timeout_kwarg_passed_through(mock_global_node, no_sleep):
    with patch("ray._private.test_utils.requests.get") as mock_get:
        mock_get.return_value = _make_resp(200, json_data=SUCCESS_JSON)
        get_system_metric_for_component(
            "ray_x", "gcs", "http://prom", request_timeout_s=7.5
        )
    assert mock_get.call_args.kwargs.get("timeout") == 7.5


def test_backoff_base_kwarg(mock_global_node, no_sleep):
    with patch("ray._private.test_utils.requests.get") as mock_get:
        mock_get.side_effect = [
            _make_resp(500, text="boom"),
            _make_resp(500, text="boom"),
            _make_resp(200, json_data=SUCCESS_JSON),
        ]
        get_system_metric_for_component(
            "ray_x", "gcs", "http://prom", backoff_base_s=0.5
        )
    # 0.5 * 2^0 = 0.5, 0.5 * 2^1 = 1.0
    assert [c.args[0] for c in no_sleep.call_args_list] == [0.5, 1.0]


def test_empty_result_returns_empty_list(mock_global_node, no_sleep):
    with patch("ray._private.test_utils.requests.get") as mock_get:
        mock_get.return_value = _make_resp(200, json_data={"data": {"result": []}})
        result = get_system_metric_for_component("ray_x", "gcs", "http://prom")
    assert result == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
