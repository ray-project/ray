import json
import subprocess
import sys

import pytest


def _run_python(code, **kwargs):
    return subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, **kwargs
    )


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
    result = _run_python(
        "from ray.serve._private.config import DeploymentConfig; "
        "from ray.serve._private.constants import "
        "DEPLOYMENT_ACTOR_HEALTH_CHECK_UNHEALTHY_THRESHOLD; "
        "print(DeploymentConfig().health_check_unhealthy_threshold, "
        "DEPLOYMENT_ACTOR_HEALTH_CHECK_UNHEALTHY_THRESHOLD)"
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == expected


@pytest.fixture
def driver_deployment_args():
    result = _run_python(
        """
import json
from ray import serve

@serve.deployment
def probe():
    pass

deployments = [
    probe,
    probe.options(health_check_unhealthy_threshold=3),
    probe.options(health_check_unhealthy_threshold=7),
]
print(json.dumps({
    "configs": [d._deployment_config.to_proto_bytes().hex() for d in deployments],
    "replica": probe._replica_config.to_proto_bytes().hex(),
}))
"""
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def test_controller_env_default_with_explicit_deployment_overrides(
    monkeypatch, driver_deployment_args
):
    # The driver has no ENV override; the controller supplies the default.
    monkeypatch.setenv("RAY_SERVE_REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD", "5")
    result = _run_python(
        """
import json
import sys
from ray.serve._private.deploy_utils import deploy_args_to_deployment_info

args = json.load(sys.stdin)
thresholds = []
for config in args["configs"]:
    info = deploy_args_to_deployment_info(
        deployment_name="probe",
        deployment_config_proto_bytes=bytes.fromhex(config),
        replica_config_proto_bytes=bytes.fromhex(args["replica"]),
        deployer_job_id="test",
        app_name="app",
    )
    thresholds.append(info.deployment_config.health_check_unhealthy_threshold)
print(json.dumps(thresholds))
""",
        input=driver_deployment_args,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == [5, 3, 7]


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
    result = _run_python("import ray.serve._private.constants")
    assert result.returncode != 0
    assert "ValueError" in result.stderr
    assert "RAY_SERVE_REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD" in result.stderr
    assert error in result.stderr


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
