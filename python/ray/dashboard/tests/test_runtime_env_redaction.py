import json
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest
import requests

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import (
    format_web_url,
    wait_until_server_available,
)
from ray.dashboard.runtime_env_redaction import (
    REDACTED_PLACEHOLDER,
    redact_runtime_env,
    redact_runtime_env_deep,
    redact_runtime_env_info,
    redact_serialized_runtime_env,
    redact_state_rows,
    should_redact_runtime_env,
)

SECRET = "supersecretvalue"

# Header sets that `is_browser_request` must classify. A DNS-rebound page cannot
# suppress any of these: they are forbidden header names.
BROWSER_HEADERS = [
    {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/140.0.0.0"},
    {"Origin": "http://rebind.poc:8265"},
    {"Referer": "http://rebind.poc:8265/"},
    {"Sec-Fetch-Mode": "cors"},
    {"Sec-Fetch-Dest": "empty"},
    {"Sec-Fetch-Site": "same-origin"},
    {"Access-Control-Request-Method": "GET"},
]
NON_BROWSER_HEADERS = [
    {"User-Agent": "curl/8.17.0", "Accept": "*/*"},
    {"User-Agent": "python-requests/2.32.3"},
    {},
]


def _mock_request(headers):
    req = MagicMock()
    req.headers = headers
    return req


def _make_runtime_env(**extra):
    return {
        "env_vars": {"AWS_SECRET_ACCESS_KEY": SECRET, "DB_PASSWORD": SECRET},
        "working_dir": "gcs://_ray_pkg_abc123.zip",
        **extra,
    }


def test_redact_runtime_env_masks_values_and_keeps_keys():
    runtime_env = _make_runtime_env()

    redacted = redact_runtime_env(runtime_env)

    assert redacted["env_vars"] == {
        "AWS_SECRET_ACCESS_KEY": REDACTED_PLACEHOLDER,
        "DB_PASSWORD": REDACTED_PLACEHOLDER,
    }
    # Non-secret fields are untouched so the response stays useful.
    assert redacted["working_dir"] == "gcs://_ray_pkg_abc123.zip"


def test_redact_runtime_env_does_not_mutate_input():
    """The same dicts are used to launch drivers, so redaction must be a copy."""
    runtime_env = _make_runtime_env()

    redact_runtime_env(runtime_env)

    assert runtime_env["env_vars"]["AWS_SECRET_ACCESS_KEY"] == SECRET


@pytest.mark.parametrize(
    "runtime_env",
    [
        {},
        {"working_dir": "gcs://pkg.zip"},
        {"env_vars": {}},
        None,
        "not-a-dict",
    ],
)
def test_redact_runtime_env_handles_missing_env_vars(runtime_env):
    """Nothing to redact must not raise and must not alter the payload."""
    assert redact_runtime_env(runtime_env) == runtime_env


def test_redact_serialized_runtime_env():
    serialized = json.dumps(_make_runtime_env())

    redacted = json.loads(redact_serialized_runtime_env(serialized))

    assert redacted["env_vars"] == {
        "AWS_SECRET_ACCESS_KEY": REDACTED_PLACEHOLDER,
        "DB_PASSWORD": REDACTED_PLACEHOLDER,
    }
    assert redacted["working_dir"] == "gcs://_ray_pkg_abc123.zip"


@pytest.mark.parametrize("serialized", ["{not json", "[1, 2, 3]", "null", "42"])
def test_redact_serialized_runtime_env_fails_closed(serialized):
    """Anything we can't parse into a dict could hold secrets, so redact it whole."""
    assert redact_serialized_runtime_env(serialized) == REDACTED_PLACEHOLDER


@pytest.mark.parametrize("serialized", ["", None])
def test_redact_serialized_runtime_env_passes_through_empty(serialized):
    """An absent field has nothing to redact and no placeholder to show."""
    assert redact_serialized_runtime_env(serialized) == serialized


def test_redact_runtime_env_info():
    runtime_env_info = {
        "serialized_runtime_env": json.dumps(_make_runtime_env()),
        "uris": {"working_dir_uri": "gcs://pkg.zip"},
    }

    redacted = redact_runtime_env_info(runtime_env_info)

    assert SECRET not in redacted["serialized_runtime_env"]
    assert REDACTED_PLACEHOLDER in redacted["serialized_runtime_env"]
    assert redacted["uris"] == {"working_dir_uri": "gcs://pkg.zip"}


@pytest.mark.parametrize(
    "row",
    [
        # RuntimeEnvState.runtime_env
        {"runtime_env": _make_runtime_env()},
        # ActorState.serialized_runtime_env (detail=1)
        {"serialized_runtime_env": json.dumps(_make_runtime_env())},
        # TaskState.runtime_env_info (detail=1)
        {
            "runtime_env_info": {
                "serialized_runtime_env": json.dumps(_make_runtime_env())
            }
        },
    ],
)
def test_redact_state_rows_covers_every_runtime_env_shape(row):
    redacted = redact_state_rows([row])

    assert SECRET not in json.dumps(redacted)
    assert REDACTED_PLACEHOLDER in json.dumps(redacted)
    # The original row is left alone.
    assert SECRET in json.dumps(row)


@pytest.mark.parametrize("rows", [None, [], [{"node_id": "abc"}]])
def test_redact_state_rows_passes_through_rows_without_runtime_env(rows):
    assert redact_state_rows(rows) == rows


def test_redact_runtime_env_deep_walks_nested_payloads():
    """The Serve config nests `runtime_env` several levels down."""
    serve_details = {
        "controller_options": {"runtime_env": _make_runtime_env()},
        "applications": {
            "app": {
                "deployed_app_config": {"runtime_env": _make_runtime_env()},
                "deployments": {
                    "Hello": {
                        "deployment_config": {
                            "ray_actor_options": {"runtime_env": _make_runtime_env()},
                        }
                    }
                },
            }
        },
        "proxies": [{"runtime_env": _make_runtime_env()}],
    }

    redacted = redact_runtime_env_deep(serve_details)

    assert SECRET not in json.dumps(redacted)
    assert json.dumps(redacted).count(REDACTED_PLACEHOLDER) == 8  # 4 sites x 2 vars
    # Input untouched.
    assert SECRET in json.dumps(serve_details)


@pytest.mark.parametrize("payload", [None, 5, "str", [], {}, {"a": [1, {"b": None}]}])
def test_redact_runtime_env_deep_passes_through_other_payloads(payload):
    assert redact_runtime_env_deep(payload) == payload


"""
Unit tests for the browser gate.
"""


@pytest.mark.parametrize("headers", BROWSER_HEADERS)
def test_should_redact_for_browser_requests(headers):
    assert should_redact_runtime_env(_mock_request(headers)) is True


@pytest.mark.parametrize("headers", NON_BROWSER_HEADERS)
def test_should_not_redact_for_non_browser_requests(headers):
    """`ray list runtime-envs` and the Python SDK must keep working unchanged."""
    assert should_redact_runtime_env(_mock_request(headers)) is False


@pytest.mark.parametrize("headers", BROWSER_HEADERS + NON_BROWSER_HEADERS)
def test_flag_disables_redaction_entirely(headers, monkeypatch):
    monkeypatch.setattr(
        "ray.dashboard.runtime_env_redaction.RAY_DASHBOARD_REDACT_RUNTIME_ENV", False
    )
    assert should_redact_runtime_env(_mock_request(headers)) is False


@pytest.mark.asyncio
@pytest.mark.parametrize("redact", [True, False])
async def test_handle_list_api_redacts_result(redact, monkeypatch):
    """`handle_list_api` is the choke point for every `/api/v0/*` list route."""
    from ray.dashboard.state_api_utils import handle_list_api
    from ray.util.state.common import ListApiResponse

    monkeypatch.setattr(
        "ray.dashboard.state_api_utils.should_redact_runtime_env", lambda req: redact
    )

    mock_request = MagicMock()
    mock_request.query = MagicMock()
    mock_request.query.get = lambda key, default=None: default
    mock_request.query.getall = lambda key, default=None: default or []

    mock_backend = AsyncMock(
        return_value=ListApiResponse(
            result=[{"runtime_env": _make_runtime_env()}],
            total=1,
            num_after_truncation=1,
            num_filtered=1,
            partial_failure_warning="",
        )
    )

    response = await handle_list_api(mock_backend, mock_request)

    body = response.body.decode()
    assert (SECRET in body) is not redact
    assert (REDACTED_PLACEHOLDER in body) is redact


"""
End-to-end tests against a live dashboard.
"""


@pytest.fixture
def dashboard_url(ray_start_with_dashboard):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    return format_web_url(ray_start_with_dashboard["webui_url"])


def _actor_with_secret_env_var():
    @ray.remote
    class Echo:
        def ping(self):
            return "pong"

    handle = Echo.options(runtime_env={"env_vars": {"MY_SECRET": SECRET}}).remote()
    ray.get(handle.ping.remote())
    return handle


def _runtime_envs(url, headers):
    resp = requests.get(f"{url}/api/v0/runtime_envs", headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.text


def test_runtime_envs_endpoint_redacts_only_for_browsers(dashboard_url):
    handle = _actor_with_secret_env_var()  # noqa: F841 -- keep the actor alive

    # The runtime env agent reports asynchronously, so wait for it to show up.
    wait_for_condition(
        lambda: "MY_SECRET" in _runtime_envs(dashboard_url, NON_BROWSER_HEADERS[0]),
        timeout=30,
    )

    # A bare client (curl, `ray list runtime-envs`, the SDK) sees the real value.
    plain = _runtime_envs(dashboard_url, NON_BROWSER_HEADERS[0])
    assert SECRET in plain

    # A browser-shaped request -- which is what a DNS-rebound attacker page looks
    # like -- sees the key but not the value.
    for headers in BROWSER_HEADERS:
        body = _runtime_envs(dashboard_url, headers)
        assert SECRET not in body, f"secret leaked for headers {headers}"
        assert "MY_SECRET" in body
        assert REDACTED_PLACEHOLDER in body


@pytest.mark.parametrize(
    "ray_start_with_dashboard",
    [{"runtime_env": {"env_vars": {"MY_SECRET": SECRET}}}],
    indirect=True,
)
def test_jobs_endpoint_redacts_only_for_browsers(dashboard_url):
    """`/api/jobs/` is what the dashboard UI renders on the job detail page."""

    def get_jobs(headers):
        resp = requests.get(f"{dashboard_url}/api/jobs/", headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()

    wait_for_condition(
        lambda: SECRET in json.dumps(get_jobs(NON_BROWSER_HEADERS[0])), timeout=30
    )

    for headers in BROWSER_HEADERS:
        body = json.dumps(get_jobs(headers))
        assert SECRET not in body, f"secret leaked for headers {headers}"
        assert "MY_SECRET" in body
        assert REDACTED_PLACEHOLDER in body

    # Same for the single-job route.
    job_id = next(
        job["job_id"]
        for job in get_jobs(NON_BROWSER_HEADERS[0])
        if job.get("runtime_env", {}).get("env_vars", {}).get("MY_SECRET")
    )
    for headers in BROWSER_HEADERS:
        resp = requests.get(
            f"{dashboard_url}/api/jobs/{job_id}", headers=headers, timeout=30
        )
        resp.raise_for_status()
        assert SECRET not in resp.text, f"secret leaked for headers {headers}"
        assert REDACTED_PLACEHOLDER in resp.text


def test_serve_applications_endpoint_redacts_only_for_browsers(dashboard_url):
    """A deployment's `ray_actor_options.runtime_env` also carries `env_vars`."""
    serve = pytest.importorskip("ray.serve")

    @serve.deployment(
        ray_actor_options={"runtime_env": {"env_vars": {"MY_SECRET": SECRET}}}
    )
    class Hello:
        def __call__(self):
            return "hi"

    serve.run(Hello.bind(), name="secretapp")
    try:

        def get_applications(headers):
            resp = requests.get(
                f"{dashboard_url}/api/serve/applications/", headers=headers, timeout=45
            )
            resp.raise_for_status()
            return resp.text

        wait_for_condition(
            lambda: SECRET in get_applications(NON_BROWSER_HEADERS[0]), timeout=60
        )

        for headers in BROWSER_HEADERS:
            body = get_applications(headers)
            assert SECRET not in body, f"secret leaked for headers {headers}"
            assert "MY_SECRET" in body
            assert REDACTED_PLACEHOLDER in body
    finally:
        serve.shutdown()


def test_runtime_env_redaction_endpoint_reports_flag(dashboard_url):
    resp = requests.get(f"{dashboard_url}/api/v0/runtime_env_redaction", timeout=30)
    resp.raise_for_status()
    assert resp.json()["data"]["redactionEnabled"] is True


@pytest.fixture
def redaction_disabled(monkeypatch):
    """Set the opt-out before the cluster starts so the dashboard inherits it."""
    monkeypatch.setenv("RAY_DASHBOARD_REDACT_RUNTIME_ENV", "0")
    yield


# `redaction_disabled` is listed first so it applies before the cluster boots.
def test_flag_off_serves_plaintext_to_browsers(redaction_disabled, dashboard_url):
    handle = _actor_with_secret_env_var()  # noqa: F841 -- keep the actor alive

    resp = requests.get(f"{dashboard_url}/api/v0/runtime_env_redaction", timeout=30)
    resp.raise_for_status()
    assert resp.json()["data"]["redactionEnabled"] is False

    wait_for_condition(
        lambda: SECRET in _runtime_envs(dashboard_url, BROWSER_HEADERS[0]),
        timeout=30,
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
