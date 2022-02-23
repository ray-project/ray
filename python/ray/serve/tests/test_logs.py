import time
import pytest

from ray import serve
from ray.serve.deployment_state import (
    SLOW_STARTUP_WARNING_S,
    SLOW_STARTUP_WARNING_PERIOD_S,
)
from ray._private.test_utils import wait_for_condition


def test_slow_allocation_warning(serve_instance, capsys):
    # this deployment can never be scheduled
    @serve.deployment(ray_actor_options={"num_cpus": 99999})
    class D:
        def __init__(self):
            pass

    num_replicas = 2
    D.options(num_replicas=num_replicas).deploy(_blocking=False)

    expected_warning = (
        f"Deployment '{D.name}' has "
        f"{num_replicas} replicas that have taken "
        f"more than {SLOW_STARTUP_WARNING_S}s "
        f"to be scheduled."
    )

    # wait long enough for the warning to be printed
    # with a small grace period
    time.sleep(SLOW_STARTUP_WARNING_PERIOD_S)

    def check():
        captured = capsys.readouterr()
        return expected_warning in captured.err

    wait_for_condition(check)


def test_slow_initialization_warning(serve_instance, capsys):
    # this deployment will take a while to allocate

    @serve.deployment
    class D:
        def __init__(self):
            time.sleep(99999)

    num_replicas = 2
    D.options(num_replicas=num_replicas).deploy(_blocking=False)

    expected_warning = (
        f"Deployment '{D.name}' has "
        f"{num_replicas} replicas that have taken "
        f"more than {SLOW_STARTUP_WARNING_S}s "
        f"to initialize."
    )

    time.sleep(SLOW_STARTUP_WARNING_PERIOD_S)

    def check():
        captured = capsys.readouterr()
        return expected_warning in captured.err

    wait_for_condition(check)


def test_deployment_init_error_logging(serve_instance, capsys):
    @serve.deployment
    class D:
        def __init__(self):
            0 / 0

    with pytest.raises(RuntimeError):
        D.deploy()

    captured = capsys.readouterr()

    assert "Exception in deployment 'D'" in captured.err
    assert "ZeroDivisionError" in captured.err


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
