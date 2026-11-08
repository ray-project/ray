import subprocess
import sys

import pytest


@pytest.fixture(autouse=True)
def clear_health_check_threshold_env(monkeypatch):
    monkeypatch.delenv(
        "RAY_SERVE_REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD", raising=False
    )
    monkeypatch.delenv(
        "RAY_SERVE_DEPLOYMENT_ACTOR_HEALTH_CHECK_UNHEALTHY_THRESHOLD", raising=False
    )


@pytest.mark.parametrize(
    "replica_env,deployment_actor_env,expected",
    [
        (None, None, "3 3"),
        ("1", None, "1 3"),
        ("5", None, "5 3"),
        (None, "5", "3 5"),
        ("5", "7", "5 7"),
    ],
)
def test_health_check_thresholds_from_env(
    monkeypatch, replica_env, deployment_actor_env, expected
):
    if replica_env is not None:
        monkeypatch.setenv(
            "RAY_SERVE_REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD", replica_env
        )
    if deployment_actor_env is not None:
        monkeypatch.setenv(
            "RAY_SERVE_DEPLOYMENT_ACTOR_HEALTH_CHECK_UNHEALTHY_THRESHOLD",
            deployment_actor_env,
        )

    # Constants are read at import time, so check what a fresh controller imports.
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from ray.serve._private import deployment_state as ds; "
            "print(ds.REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD, "
            "ds.DEPLOYMENT_ACTOR_HEALTH_CHECK_UNHEALTHY_THRESHOLD)",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == expected


@pytest.mark.parametrize(
    "value,error",
    [
        ("0", "Expected positive `int`"),
        ("-1", "Expected positive `int`"),
        ("1.5", "cannot be converted to `int`"),
        ("abc", "cannot be converted to `int`"),
        ("", "cannot be converted to `int`"),
    ],
)
def test_invalid_replica_health_check_threshold(monkeypatch, value, error):
    monkeypatch.setenv("RAY_SERVE_REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD", value)
    result = subprocess.run(
        [sys.executable, "-c", "import ray.serve._private.constants"],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "ValueError" in result.stderr
    assert "RAY_SERVE_REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD" in result.stderr
    assert error in result.stderr


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
