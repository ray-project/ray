import time
import pytest

from ray import serve
from ray.serve._private.deployment_state import (
    SLOW_STARTUP_WARNING_S,
    SLOW_STARTUP_WARNING_PERIOD_S,
)


def test_slow_allocation_warning(serve_instance, capsys):
    # this deployment can never be scheduled
    @serve.deployment(ray_actor_options={"num_cpus": 99999})
    class D:
        def __init__(self):
            pass

    num_replicas = 2
    serve.run(D.options(num_replicas=num_replicas).bind(), _blocking=False)

    expected_warning = (
        f"Deployment '{D.name}' has "
        f"{num_replicas} replicas that have taken "
        f"more than {SLOW_STARTUP_WARNING_S}s "
        f"to be scheduled."
    )

    # wait long enough for the warning to be printed
    # with a small grace period
    time.sleep(SLOW_STARTUP_WARNING_PERIOD_S * 1.5)

    captured = capsys.readouterr()

    print(captured.err)

    assert expected_warning in captured.err

    # make sure that exactly one warning was printed
    # for this deployment
    assert captured.err.count(expected_warning) == 1


def test_slow_initialization_warning(serve_instance, capsys):
    # this deployment will take a while to allocate

    @serve.deployment
    class D:
        def __init__(self):
            time.sleep(99999)

    num_replicas = 4
    serve.run(D.options(num_replicas=num_replicas).bind(), _blocking=False)

    expected_warning = (
        f"Deployment '{D.name}' has "
        f"{num_replicas} replicas that have taken "
        f"more than {SLOW_STARTUP_WARNING_S}s "
        f"to initialize."
    )

    # wait long enough for the warning to be printed
    # with a small grace period
    time.sleep(SLOW_STARTUP_WARNING_PERIOD_S * 1.5)

    captured = capsys.readouterr()

    assert expected_warning in captured.err

    # make sure that exactly one warning was printed
    # for this deployment
    assert captured.err.count(expected_warning) == 1


def test_deployment_init_error_logging(serve_instance, capsys):
    @serve.deployment
    class D:
        def __init__(self):
            0 / 0

    with pytest.raises(RuntimeError):
        serve.run(D.bind())

    captured = capsys.readouterr()

    assert "Exception in deployment 'D'" in captured.err
    assert "ZeroDivisionError" in captured.err
