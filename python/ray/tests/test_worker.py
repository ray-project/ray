import io
import os
from typing import Dict
from unittest import mock

import colorama

from ray._private.worker import print_worker_logs


def get_print_worker_logs_output(data: Dict[str, str]) -> str:
    """
    Helper function that returns the output of `print_worker_logs` as a str.
    """
    out = io.StringIO()
    print_worker_logs(data, out)
    out.seek(0)
    return out.readline()


def test_print_worker_logs_default_color() -> None:
    # Test multiple since pid may affect color
    for pid in (0, 1):
        data = dict(
            ip="10.0.0.1",
            localhost="172.0.0.1",
            pid=str(pid),
            task_name="my_task",
            lines=["is running"],
        )
        output = get_print_worker_logs_output(data)
        assert output == (
            f"{colorama.Fore.CYAN}(my_task pid={pid}, ip=10.0.0.1)"
            + f"{colorama.Style.RESET_ALL} is running\n"
        )

    # Special case
    raylet = dict(
        ip="10.0.0.1",
        localhost="172.0.0.1",
        pid="raylet",
        task_name="my_task",
        lines=["Warning: uh oh"],
    )
    output = get_print_worker_logs_output(raylet)
    assert output == (
        f"{colorama.Fore.YELLOW}(raylet, ip=10.0.0.1){colorama.Style.RESET_ALL} "
        + "Warning: uh oh\n"
    )


@mock.patch.dict(os.environ, {"RAY_COLOR_PREFIX": "0"})
def test_print_worker_logs_no_color() -> None:
    for pid in (0, 1):
        data = dict(
            ip="10.0.0.1",
            localhost="172.0.0.1",
            pid=str(pid),
            task_name="my_task",
            lines=["is running"],
        )
        output = get_print_worker_logs_output(data)
        assert output == f"(my_task pid={pid}, ip=10.0.0.1) is running\n"

    raylet = dict(
        ip="10.0.0.1",
        localhost="172.0.0.1",
        pid="raylet",
        task_name="my_task",
        lines=["Warning: uh oh"],
    )
    output = get_print_worker_logs_output(raylet)
    assert output == "(raylet, ip=10.0.0.1) Warning: uh oh\n"


@mock.patch.dict(os.environ, {"RAY_COLOR_PREFIX": "1"})
def test_print_worker_logs_multi_color() -> None:
    data_pid_0 = dict(
        ip="10.0.0.1",
        localhost="172.0.0.1",
        pid="0",
        task_name="my_task",
        lines=["is running"],
    )
    output = get_print_worker_logs_output(data_pid_0)
    assert output == (
        f"{colorama.Fore.MAGENTA}(my_task pid=0, ip=10.0.0.1)"
        + f"{colorama.Style.RESET_ALL} is running\n"
    )

    data_pid_2 = dict(
        ip="10.0.0.1",
        localhost="172.0.0.1",
        pid="2",
        task_name="my_task",
        lines=["is running"],
    )
    output = get_print_worker_logs_output(data_pid_2)
    assert output == (
        f"{colorama.Fore.GREEN}(my_task pid=2, ip=10.0.0.1){colorama.Style.RESET_ALL} "
        + "is running\n"
    )
