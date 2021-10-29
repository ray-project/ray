import time

from ray import serve
from ray._private.test_utils import wait_for_condition


def test_slow_allocation_warning(serve_instance, capsys):
    # this deployment can never be scheduled
    @serve.deployment(ray_actor_options={"num_cpus": 99999})
    class D:
        def __init__(self):
            pass

    D.deploy(_blocking=False)

    expected_warning = (f"Deployment '{D.name}' has "
                        f"1 replicas that have taken "
                        f"more than 30s to be scheduled.")

    def warning_printed():
        captured = capsys.readouterr()
        return expected_warning in captured.err

    wait_for_condition(warning_printed, timeout=60, retry_interval_ms=1000)


def test_slow_initialization_warning(serve_instance, capsys):
    # this deployment will take a while to allocate
    @serve.deployment
    class D:
        def __init__(self):
            time.sleep(99999)

    D.deploy(_blocking=False)

    expected_warning = (f"Deployment '{D.name}' has "
                        f"1 replicas that have taken "
                        f"more than 30s to initialize.")

    def warning_printed():
        captured = capsys.readouterr()
        return expected_warning in captured.err

    wait_for_condition(warning_printed, timeout=60, retry_interval_ms=1000)
